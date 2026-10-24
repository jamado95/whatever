package data

import (
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	proto "whatever/internal/protocol"
)

const (
	idBTC5m  = "BTCUSDT:5m:btc_5m"
	idBTC1h  = "BTCUSDT:1h:btc_1h"
	idETH1d  = "ETHUSDT:1d:eth_1d"
	idRoll1h = "BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m"
	idRoll1d = "BTCUSDT:1d:derived_BTCUSDT:1h:btc_1h"
)

// fixture is a stream table and its buffer.
type fixture struct {
	t   *testing.T
	st  *Streams
	buf *FrameBuffer
}

// newFixture is the shared fixture: a 5m grid, BTCUSDT 5m deriving 1h, BTCUSDT 1h deriving 1d,
// ETHUSDT 1d; lookback 20, headroom 4.
func newFixture(t *testing.T) fixture {
	return newFixtureWith(t, testSubs(), 20, 4)
}

func newFixtureWith(t *testing.T, subs []proto.Subscription, lookback int, headroom int64) fixture {
	t.Helper()
	st := NewStreams(subs)
	buf, err := newFrameBuffer(st, lookback, headroom)
	if err != nil {
		t.Fatalf("newFrameBuffer: %v", err)
	}
	return fixture{t: t, st: st, buf: buf}
}

func (f fixture) idx(id string) int { return mustIndex(f.t, f.st, id) }

// fresh puts a Fresh candle for stream s at each column.
func (f fixture) fresh(s int, cols ...int64) {
	for _, col := range cols {
		f.buf.put(s, col, candle(s, col), Fresh, 0)
	}
}

// slot returns stream s's slot for col.
func (f fixture) slot(s int, col int64) slot {
	return *f.buf.rings[s].at(col)
}

// candle is distinct per stream and column.
func candle(s int, col int64) proto.Candle {
	return proto.Candle{CloseTs: col, Close: float64(col), Volume: float64(s)}
}

// span returns the columns from..to inclusive, stepping by step (negative to count down).
func span(from, to, step int64) []int64 {
	var cols []int64
	for c := from; (step > 0 && c <= to) || (step < 0 && c >= to); c += step {
		cols = append(cols, c)
	}
	return cols
}

/*
TestNewFrameBuffer covers construction: slots per ring from lookback, derived windows and headroom;
the stream table rescaling with the grid; and the 365-day cap, applied per stream.
*/
func TestNewFrameBuffer(t *testing.T) {
	slots := func(f fixture, id string) int { return len(f.buf.rings[f.idx(id)].slots) }
	bars := func(f fixture, id string) int64 { return f.buf.rings[f.idx(id)].bars }

	t.Run("slots per ring", func(t *testing.T) {
		f := newFixture(t)
		want := map[string]int{idBTC5m: 25, idRoll1h: 245, idBTC1h: 27, idRoll1d: 482, idETH1d: 22}
		total := 0
		for id, n := range want {
			if got := slots(f, id); got != n {
				t.Errorf("%s: %d slots, want %d", id, got, n)
			}
			total += slots(f, id)
		}
		// shared rows would hold every stream at the deepest read: (20 × 288 + 4) × 5 = 28,820
		if total != 801 {
			t.Errorf("total slots = %d, want 801", total)
		}
	})

	t.Run("lookback 0 keeps one bar, a source its window plus one", func(t *testing.T) {
		f := newFixtureWith(t, testSubs(), 0, 4)
		want := map[string]int64{idBTC5m: 13, idBTC1h: 25, idETH1d: 1, idRoll1h: 1, idRoll1d: 1}
		for id, n := range want {
			if got := bars(f, id); got != n {
				t.Errorf("%s: %d bars, want %d", id, got, n)
			}
		}
	})

	t.Run("headroom 0 and below the period", func(t *testing.T) {
		if got := slots(newFixtureWith(t, testSubs(), 20, 0), idETH1d); got != 21 {
			t.Errorf("1d with headroom 0: %d slots, want 21", got)
		}
		if got := slots(newFixtureWith(t, testSubs(), 20, 0), idBTC5m); got != 21 {
			t.Errorf("5m with headroom 0: %d slots, want 21", got)
		}
		if got := slots(newFixture(t), idETH1d); got != 22 {
			t.Errorf("1d with headroom 4: %d slots, want 22 (2 extra)", got)
		}
	})

	t.Run("1m grid rescales period, window and slots", func(t *testing.T) {
		f := newFixtureWith(t, []proto.Subscription{
			sub("BTCUSDT", proto.Timeframe1m, "btc_1m", proto.Timeframe1h),
			sub("BTCUSDT", proto.Timeframe1h, "btc_1h", proto.Timeframe1d),
			sub("ETHUSDT", proto.Timeframe1d, "eth_1d"),
		}, 20, 4)
		roll1h := "BTCUSDT:1h:derived_BTCUSDT:1m:btc_1m"
		if p := f.st.All[f.idx(idBTC1h)].Period; p != 60 {
			t.Errorf("1h direct Period = %d, want 60", p)
		}
		if w := f.st.All[f.idx(roll1h)].Window; w != 60 {
			t.Errorf("rolling 1h Window = %d, want 60", w)
		}
		want := map[string]int{"BTCUSDT:1m:btc_1m": 66, roll1h: 1205, idBTC1h: 27, idRoll1d: 482, idETH1d: 22}
		for id, n := range want {
			if got := slots(f, id); got != n {
				t.Errorf("%s: %d slots, want %d", id, got, n)
			}
		}
	})

	t.Run("365 days of bars pass, one more fails naming the stream", func(t *testing.T) {
		st := NewStreams(testSubs())
		if _, err := newFrameBuffer(st, 365, 4); err != nil {
			t.Fatalf("365 days: %v", err)
		}
		_, err := newFrameBuffer(st, 366, 4)
		if err == nil || !strings.Contains(err.Error(), idETH1d) {
			t.Fatalf("366 days: err = %v, want one naming %s", err, idETH1d)
		}
	})

	t.Run("a derived stream over the cap is named, not its source", func(t *testing.T) {
		st := NewStreams([]proto.Subscription{sub("BTCUSDT", proto.Timeframe5m, "btc_5m", proto.Timeframe1d)})
		if _, err := newFrameBuffer(st, 365, 4); err != nil {
			t.Fatalf("365 days: %v", err)
		}
		_, err := newFrameBuffer(st, 366, 4)
		if err == nil || !strings.Contains(err.Error(), "stream BTCUSDT:1d:derived_") {
			t.Fatalf("366 days: err = %v, want one naming the rolling 1d stream", err)
		}
	})

	t.Run("negative lookback or headroom fails", func(t *testing.T) {
		st := NewStreams(testSubs())
		if _, err := newFrameBuffer(st, -1, 4); err == nil {
			t.Error("lookback -1: no error")
		}
		if _, err := newFrameBuffer(st, 20, -1); err == nil {
			t.Error("headroom -1: no error")
		}
	})
}

/*
TestPut covers the write path: what a Fresh, Gap or Ended slot holds, the last Fresh candle carried
through gaps, the end column, rewrites and the derived window count.
*/
func TestPut(t *testing.T) {
	t.Run("Fresh stores its candle", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		f.fresh(s, 3)
		got := f.slot(s, 3)
		if got.col != 3 || got.fresh != 3 || got.c != candle(s, 3) || got.state != Fresh {
			t.Errorf("slot = %+v", got)
		}
	})

	t.Run("Gap and Ended carry the previous slot's Fresh candle", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h) // period 12
		f.fresh(s, 0)
		f.buf.put(s, 12, proto.Candle{}, Gap, 0)
		f.buf.put(s, 24, proto.Candle{}, Ended, 0)
		for _, col := range []int64{12, 24} {
			if got := f.slot(s, col); got.c != candle(s, 0) || got.fresh != 0 {
				t.Errorf("col %d: carries %+v from %d, want the candle at 0", col, got.c, got.fresh)
			}
		}
	})

	t.Run("no previous slot carries a zero candle", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		f.buf.put(s, 0, candle(s, 0), Ended, 0) // no column before 0
		f.buf.put(s, 7, candle(s, 7), Gap, 0)   // column 6 empty
		for _, col := range []int64{0, 7} {
			if got := f.slot(s, col); got.c != (proto.Candle{}) || got.fresh != -1 {
				t.Errorf("col %d: slot = %+v, want a zero candle and fresh -1", col, got)
			}
		}
	})

	t.Run("consecutive gaps keep the same Fresh candle", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		f.fresh(s, 0)
		for col := int64(1); col <= 3; col++ {
			f.buf.put(s, col, proto.Candle{}, Gap, 0)
		}
		if got := f.slot(s, 3); got.c != candle(s, 0) || got.fresh != 0 {
			t.Errorf("third gap carries %+v from %d, want the candle at 0", got.c, got.fresh)
		}
	})

	t.Run("Ended records the end column", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		r := &f.buf.rings[s]
		if e := r.end.Load(); e != math.MaxInt64 {
			t.Fatalf("open stream: end = %d", e)
		}
		f.fresh(s, 6)
		f.buf.put(s, 7, proto.Candle{}, Ended, 0)
		if e := r.end.Load(); e != 7 {
			t.Errorf("end = %d, want 7", e)
		}
	})

	t.Run("rewriting a column replaces its slot", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		f.fresh(s, 2)
		f.buf.put(s, 2, candle(s, 99), Fresh, 0)
		if got := f.slot(s, 2); got.c != candle(s, 99) {
			t.Errorf("slot holds %+v, want the second write", got.c)
		}
	})

	t.Run("a derived slot stores its window count", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idRoll1h)
		f.buf.put(s, 4, candle(s, 4), Fresh, 5)
		if got := f.slot(s, 4); got.count != 5 {
			t.Errorf("count = %d, want 5", got.count)
		}
	})
}

/*
TestFrameBufferConcurrency runs a writer ahead of a reader the way an align stage runs ahead of the
engine: one goroutine writes the 5m stream at most headroom columns ahead and publishes each column
on a channel; the reader checks views pinned at the published columns. Meaningful under -race.
*/
func TestFrameBufferConcurrency(t *testing.T) {
	const headroom, last = 4, 2000
	f := newFixtureWith(t, testSubs(), 20, headroom)
	s := f.idx(idBTC5m)

	pub := make(chan int64, headroom-1) // with one column in hand, the writer is ≤ headroom ahead
	go func() {
		defer close(pub)
		for col := int64(0); col <= last; col++ {
			state := Fresh
			if col == last {
				state = Ended
			}
			f.buf.put(s, col, candle(s, col), state, 0)
			pub <- col
		}
	}()

	dst := make([]proto.Candle, 0, 20)
	for col := range pub {
		v := f.buf.view(col, true)
		c, state, _ := v.Leg(s)
		dst = v.Candles(s, 20, dst)

		newest, wantLen, wantState := col, int(min(col+1, 20)), Fresh
		if col == last {
			newest, wantLen, wantState = col-1, 19, Ended
		}
		if state != wantState || (state == Fresh && c != candle(s, col)) {
			t.Fatalf("col %d: Leg = %+v %d", col, c, state)
		}
		if len(dst) != wantLen {
			t.Fatalf("col %d: %d candles, want %d", col, len(dst), wantLen)
		}
		for k, got := range dst {
			if want := candle(s, newest-int64(k)); got != want {
				t.Fatalf("col %d: candle %d = %+v, want %+v", col, k, got, want)
			}
		}
	}
}

/*
TestFrameBufferStartup runs a writer filling a ring at startup the way an align stage does before the
engine reads: it starts the 5m stream at first, then writes every slot of the ring, wrapping onto the
slots of columns before first, and publishes each column. The reader checks views before first and
at the published columns. Meaningful under -race: reads before first touch no slot being written.
*/
func TestFrameBufferStartup(t *testing.T) {
	const first = 1000
	f := newFixture(t)
	s := f.idx(idBTC5m)
	n := int64(len(f.buf.rings[s].slots))

	pub := make(chan int64, n)
	go func() {
		defer close(pub)
		f.buf.start(s, first)
		for col := int64(first); col < first+n; col++ {
			f.buf.put(s, col, candle(s, col), Fresh, 0)
			pub <- col
		}
	}()

	dst := make([]proto.Candle, 0, 20)
	for col := range pub {
		for _, before := range span(first-n, first-1, 1) {
			if _, state, age := f.buf.view(before, true).Leg(s); state != Gap || age != -1 {
				t.Fatalf("col %d: Leg at %d = %d %d, want not started", col, before, state, age)
			}
		}

		v := f.buf.view(col, true)
		if c, state, _ := v.Leg(s); state != Fresh || c != candle(s, col) {
			t.Fatalf("col %d: Leg = %+v %d", col, c, state)
		}
		dst = v.Candles(s, 20, dst)
		if want := int(min(col-first+1, 20)); len(dst) != want || dst[len(dst)-1] != candle(s, col-int64(want)+1) {
			t.Fatalf("col %d: candles %v, want %d down to %d", col, dst, want, col-int64(want)+1)
		}
	}
}

/*
TestFrameBufferModel checks Leg, Count and Candles against a brute-force model that keeps every
write. Fixed seed; each stream starts at a random due column, writes Fresh or Gap at every due column
after that, and may end; the run covers three lengths of the longest ring. Views are pinned at the
newest column and up to headroom columns behind it, as the engine may be.
*/
func TestFrameBufferModel(t *testing.T) {
	const lookback, headroom = 20, 4
	f := newFixtureWith(t, testSubs(), lookback, headroom)
	rng := rand.New(rand.NewPCG(1, 2))

	type write struct {
		state LegState
		c     proto.Candle
		count uint16
	}

	n := len(f.st.All)
	var last int64 // three lengths of the longest ring, in columns
	for i, s := range f.st.All {
		last = max(last, 3*int64(len(f.buf.rings[i].slots))*s.Period)
	}

	hist := make([]map[int64]write, n) // stream → column → write
	startAt, endAt, end := make([]int64, n), make([]int64, n), make([]int64, n)
	for i := range n {
		hist[i] = map[int64]write{}
		startAt[i] = rng.Int64N(last / 6)
		endAt[i] = math.MaxInt64
		if rng.IntN(2) == 0 {
			endAt[i] = last/3 + rng.Int64N(last*2/3)
		}
		end[i] = math.MaxInt64
	}

	// model: the latest written column at or before v, and whether the stream has started
	latest := func(i int, v int64) (int64, bool) {
		a := v - v%f.st.All[i].Period
		if a >= end[i] {
			a = end[i]
		}
		_, ok := hist[i][a]
		return a, ok
	}
	lastFresh := func(i int, a int64) (proto.Candle, int64) {
		for c := a; c >= 0; c -= f.st.All[i].Period {
			w, ok := hist[i][c]
			if !ok {
				break
			}
			if w.state == Fresh {
				return w.c, c
			}
		}
		return proto.Candle{}, -1
	}

	check := func(v int64) {
		view := f.buf.view(v, true)
		for i, s := range f.st.All {
			var (
				wc     proto.Candle
				ws     = Gap
				wa     = -1
				wcount int
				wbars  []proto.Candle
			)
			if a, ok := latest(i, v); ok {
				fc, fcol := lastFresh(i, a)
				wc, ws = fc, hist[i][a].state
				if fcol >= 0 {
					wa = int(v - fcol)
				}
				switch {
				case s.Kind == Derived:
					wcount = int(hist[i][a].count)
				case fcol >= 0:
					wcount = 1
				}
				step := s.Period * s.Window
				for k, col := int64(0), a; k < f.buf.rings[i].bars && col >= 0; k, col = k+1, col-step {
					w, ok := hist[i][col]
					if !ok {
						break
					}
					if w.state == Fresh {
						wbars = append(wbars, w.c)
					}
				}
			}

			if c, st, age := view.Leg(i); c != wc || st != ws || age != wa {
				t.Fatalf("%s at %d: Leg = %+v %d %d, want %+v %d %d", s.Sub.ID(), v, c, st, age, wc, ws, wa)
			}
			if got := view.Count(i); got != wcount {
				t.Fatalf("%s at %d: Count = %d, want %d", s.Sub.ID(), v, got, wcount)
			}
			for _, nb := range []int{1, lookback, 2 * lookback} {
				want := wbars[:min(nb, len(wbars))]
				if got := view.Candles(i, nb, nil); !slices.Equal(got, want) {
					t.Fatalf("%s at %d: Candles(%d) = %d bars, want %d", s.Sub.ID(), v, nb, len(got), len(want))
				}
			}
		}
	}

	for col := int64(0); col < last; col++ {
		for i, s := range f.st.All {
			if col%s.Period != 0 || col < startAt[i] || end[i] != math.MaxInt64 {
				continue
			}
			w := write{state: Fresh, c: candle(i, col), count: uint16(1 + rng.IntN(int(s.Window)))}
			switch {
			case col >= endAt[i]:
				w.state = Ended
				end[i] = col
			case rng.IntN(100) < 15:
				w.state = Gap
			}
			hist[i][col] = w
			f.buf.put(i, col, w.c, w.state, w.count)
		}
		check(col)
		check(col - rng.Int64N(min(headroom, col)+1))
	}
}
