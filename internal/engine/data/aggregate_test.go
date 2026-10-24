package data

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	proto "whatever/internal/protocol"
)

const idRoll1dOf5m = "BTCUSDT:1d:derived_BTCUSDT:5m:btc_5m"

// staged is a direct stream's slots as its align stage writes them: Fresh at each column of fresh
// (sorted), Gap at its other due columns between the first and the last, then Ended one period after
// the last when ended.
type staged struct {
	fresh []int64
	ended bool
}

// state returns the stream's slot state at col, a due column of its stream with period p; ok false
// when it has no slot there.
func (l staged) state(col, p int64) (LegState, bool) {
	if len(l.fresh) == 0 || col < l.fresh[0] {
		return 0, false
	}
	last := l.fresh[len(l.fresh)-1]
	switch {
	case col > last+p || (col == last+p && !l.ended):
		return 0, false
	case col == last+p:
		return Ended, true
	}
	if _, ok := slices.BinarySearch(l.fresh, col); ok {
		return Fresh, true
	}
	return Gap, true
}

/*
drive writes each stream's staged slots at every column from..to, as its align stage would, steps
agg at each column, as Next would, then calls check. c gives stream s's Fresh candle at col.
*/
func drive(f fixture, agg *aggregator, streams map[int]staged, c func(s int, col int64) proto.Candle,
	from, to int64, check func(col int64)) {
	for col := from; col <= to; col++ {
		for s, l := range streams {
			p := f.st.All[s].Period
			if col%p != 0 {
				continue
			}
			if state, ok := l.state(col, p); ok {
				if col == l.fresh[0] {
					f.buf.start(s, col)
				}
				f.buf.put(s, col, c(s, col), state, 0)
			}
		}
		agg.step(col)
		check(col)
	}
}

// rollup aggregates bars, oldest first, into one candle.
func rollup(bars []proto.Candle) proto.Candle {
	if len(bars) == 0 {
		return proto.Candle{}
	}
	out := bars[0]
	out.High, out.Low, out.Volume = math.Inf(-1), math.Inf(1), 0
	for _, b := range bars {
		out.High, out.Low, out.Volume = max(out.High, b.High), min(out.Low, b.Low), out.Volume+b.Volume
	}
	last := bars[len(bars)-1]
	out.CloseTs, out.Close = last.CloseTs, last.Close
	return out
}

// leg is a stream's leg as read through a View.
type leg struct {
	c     proto.Candle
	state LegState
	count int
	age   int
}

func readLeg(v View, s int) leg {
	c, state, age := v.Leg(s)
	return leg{c: c, state: state, count: v.Count(s), age: age}
}

/*
expect computes derived stream d's leg at col by brute force over its source's staged slots: the source's
latest update at or before col decides the state; the candle aggregates the Fresh bars in the window
ending at the source's latest Fresh bar, which Gap and Ended carry.
*/
func expect(st *Streams, d int, l staged, c func(int64) proto.Candle, col int64) leg {
	ds := st.All[d]
	p := ds.Period
	a := col - col%p
	if len(l.fresh) == 0 || a < l.fresh[0] {
		return leg{state: Gap, age: -1}
	}

	state := Fresh
	if last := l.fresh[len(l.fresh)-1]; l.ended && a >= last+p {
		a, state = last+p, Ended
	} else if _, ok := slices.BinarySearch(l.fresh, a); !ok {
		state = Gap
	}

	hi, _ := slices.BinarySearch(l.fresh, a+1)
	x := l.fresh[hi-1] // latest Fresh at or before a
	lo, _ := slices.BinarySearch(l.fresh, x-ds.Window*p+1)
	bars := make([]proto.Candle, 0, hi-lo)
	for _, fc := range l.fresh[lo:hi] {
		bars = append(bars, c(fc))
	}
	return leg{c: rollup(bars), state: state, count: len(bars), age: int(col - x)}
}

// sameLeg compares legs exactly, except volume within 1e-9 relative.
func sameLeg(a, b leg) bool {
	va, vb := a.c.Volume, b.c.Volume
	a.c.Volume, b.c.Volume = 0, 0
	return a == b && math.Abs(va-vb) <= 1e-9*max(math.Abs(va), math.Abs(vb))
}

/*
TestAggregate covers the rolling legs over the shared fixture: warm-up counts and the closed bar at
each boundary, a source gap carried as Gap with the windows after it leaving it out, Open from the
next Fresh bar when the oldest is missing, a source's end carried as Ended, a coarse source updating
only at its period, and a source not yet started.
*/
func TestAggregate(t *testing.T) {
	const d = day0
	without := func(cols []int64, drop ...int64) []int64 {
		return slices.DeleteFunc(slices.Clone(cols), func(c int64) bool { return slices.Contains(drop, c) })
	}
	setup := func(t *testing.T) (fixture, *aggregator, func(s int, col int64) proto.Candle) {
		f := newFixture(t)
		return f, newAggregator(f.buf), func(s int, col int64) proto.Candle { return bar(f.st, s, col) }
	}
	got := func(f fixture, id string, col int64) leg { return readLeg(f.buf.view(col, false), f.idx(id)) }
	wantAt := func(t *testing.T, f fixture, col int64, want leg) {
		t.Helper()
		if g := got(f, idRoll1h, col); !sameLeg(g, want) {
			t.Errorf("col d+%d: rolling 1h %+v, want %+v", col-d, g, want)
		}
	}

	t.Run("warm-up and the closed bar", func(t *testing.T) {
		f, agg, c := setup(t)
		s := f.idx(idBTC5m)
		l := staged{fresh: span(d+1, d+48, 1)}
		drive(f, agg, map[int]staged{s: l}, c, d, d+48, func(col int64) {
			if col == d {
				return
			}
			want := leg{state: Fresh, count: int(min(col-d, 12))}
			if col%12 == 0 {
				want.c = rollup(bars(f.st, s, span(col-11, col, 1)...))
			} else {
				want.c = expect(f.st, f.idx(idRoll1h), l, func(x int64) proto.Candle { return c(s, x) }, col).c
			}
			wantAt(t, f, col, want)
		})
	})

	t.Run("a gap is carried, then left out", func(t *testing.T) {
		f, agg, c := setup(t)
		s := f.idx(idBTC5m)
		l := staged{fresh: without(span(d+1, d+60, 1), d+30)}
		cs := func(x int64) proto.Candle { return c(s, x) }
		drive(f, agg, map[int]staged{s: l}, c, d+1, d+60, func(col int64) {
			want := expect(f.st, f.idx(idRoll1h), l, cs, col)
			switch {
			case col == d+30:
				want = leg{c: expect(f.st, f.idx(idRoll1h), l, cs, d+29).c, state: Gap, count: 12, age: 1}
			case col > d+30 && col <= d+41:
				want.count = 11
			case col >= d+12:
				want.count = 12
			}
			wantAt(t, f, col, want)
		})
	})

	t.Run("open from the next Fresh bar", func(t *testing.T) {
		f, agg, c := setup(t)
		s := f.idx(idBTC5m)
		l := staged{fresh: without(span(d+1, d+60, 1), d+30, d+31, d+32)}
		next := bar(f.st, s, d+33)
		drive(f, agg, map[int]staged{s: l}, c, d+1, d+60, func(col int64) {
			if col < d+41 || col > d+44 {
				return
			}
			if g := got(f, idRoll1h, col).c; g.Open != next.Open || g.OpenTs != next.OpenTs {
				t.Errorf("col d+%d: Open %v OpenTs %d, want %v %d", col-d, g.Open, g.OpenTs, next.Open, next.OpenTs)
			}
		})
	})

	t.Run("the source's end is carried", func(t *testing.T) {
		f, agg, c := setup(t)
		s, r := f.idx(idBTC5m), f.idx(idRoll1h)
		l := staged{fresh: span(d+1, d+40, 1), ended: true}
		last := expect(f.st, r, l, func(x int64) proto.Candle { return c(s, x) }, d+40).c
		drive(f, agg, map[int]staged{s: l}, c, d+1, d+60, func(col int64) {
			if col < d+41 {
				return
			}
			wantAt(t, f, col, leg{c: last, state: Ended, count: 12, age: int(col - (d + 40))})
			if col > d+41 && f.slot(r, col).col == col {
				t.Errorf("col d+%d: written after the end", col-d)
			}
		})
	})

	t.Run("a coarse source updates at its period", func(t *testing.T) {
		f, agg, c := setup(t)
		s, r := f.idx(idBTC1h), f.idx(idRoll1d)
		l := staged{fresh: span(d+12, d+12*30, 12)}
		cs := func(x int64) proto.Candle { return c(s, x) }
		drive(f, agg, map[int]staged{s: l}, c, d+12, d+12*30, func(col int64) {
			if written := f.slot(r, col).col == col; written != (col%12 == 0) {
				t.Errorf("col d+%d: written %v", col-d, written)
			}
			want := expect(f.st, r, l, cs, col)
			want.count = int(min((col-d)/12, 24))
			if col == d+12*24 {
				want.c = rollup(bars(f.st, s, span(d+12, d+12*24, 12)...))
			}
			if g := got(f, idRoll1d, col); !sameLeg(g, want) {
				t.Errorf("col d+%d: rolling 1d %+v, want %+v", col-d, g, want)
			}
		})
	})

	t.Run("a source not yet started", func(t *testing.T) {
		f, agg, c := setup(t)
		s, r := f.idx(idBTC5m), f.idx(idRoll1h)
		l := staged{fresh: span(d+30, d+40, 1)}
		drive(f, agg, map[int]staged{s: l}, c, d, d+40, func(col int64) {
			if col >= d+30 {
				return
			}
			wantAt(t, f, col, leg{state: Gap, age: -1})
		})
		if first := f.buf.rings[r].first.Load(); first != d+30 {
			t.Errorf("rolling 1h first column d+%d, want d+30", first-d)
		}
	})
}

/*
TestAggregateModel compares the rolling 1h and 1d legs of one 5m source with a brute-force model at
every column of ten days: random prices and volumes over nine orders of magnitude, scattered gaps, a
gap longer than a day that empties the 1d window, and an end before the last column.
*/
func TestAggregateModel(t *testing.T) {
	const d = day0
	f := newFixtureWith(t, []proto.Subscription{
		sub("BTCUSDT", proto.Timeframe5m, "btc_5m", proto.Timeframe1h, proto.Timeframe1d),
	}, 20, 4)
	s, r1h, r1d := f.idx(idBTC5m), f.idx(idRoll1h), f.idx(idRoll1dOf5m)

	rng := rand.New(rand.NewPCG(1, 2))
	candles := make(map[int64]proto.Candle)
	var fresh []int64
	p := 100.0
	for col := int64(d + 1); col <= d+2700; col++ {
		long := col >= d+2000 && col < d+2300
		if long || (col >= d+1000 && col < d+1040) || rng.Float64() < 0.05 {
			continue
		}
		c := bar(f.st, s, col)
		next := p * (1 + 0.01*rng.NormFloat64())
		c.Open, c.Close = p, next
		c.High, c.Low = max(p, next)+rng.Float64(), min(p, next)-rng.Float64()
		c.Volume = math.Pow(10, rng.Float64()*9-3)
		candles[col], p = c, next
		fresh = append(fresh, col)
	}
	l := staged{fresh: fresh, ended: true}
	cs := func(x int64) proto.Candle { return candles[x] }

	drive(f, newAggregator(f.buf), map[int]staged{s: l}, func(_ int, col int64) proto.Candle { return candles[col] },
		d, d+2880, func(col int64) {
			v := f.buf.view(col, false)
			for _, r := range []int{r1h, r1d} {
				if got, want := readLeg(v, r), expect(f.st, r, l, cs, col); !sameLeg(got, want) {
					t.Fatalf("col d+%d, %s: %+v,\nwant %+v", col-d, f.st.All[r].Sub.ID(), got, want)
				}
			}
		})
}

// TestAggregateAllocations covers the hot path: step allocates nothing.
func TestAggregateAllocations(t *testing.T) {
	f := newFixtureWith(t, []proto.Subscription{
		sub("BTCUSDT", proto.Timeframe5m, "btc_5m", proto.Timeframe1h, proto.Timeframe1d),
	}, 20, 4)
	s := f.idx(idBTC5m)
	agg := newAggregator(f.buf)
	col := int64(day0)
	f.buf.start(s, col)

	allocs := testing.AllocsPerRun(1000, func() {
		col++
		f.buf.put(s, col, bar(f.st, s, col), Fresh, 0)
		agg.step(col)
	})
	if allocs != 0 {
		t.Errorf("step allocates %v times per column, want 0", allocs)
	}
}

/*
BenchmarkAggregate measures step per column with n derived streams, each a rolling 1h over its own
direct 1m source. Each iteration puts every source's next Fresh slot, then steps the column.
ns/derivation is the time per column divided by n.
*/
func BenchmarkAggregate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			subs := make([]proto.Subscription, n)
			for i := range subs {
				subs[i] = sub(fmt.Sprintf("S%04d", i), proto.Timeframe1m, "bench", proto.Timeframe1h)
			}
			buf, err := newFrameBuffer(NewStreams(subs), 1, defaultHeadroom)
			if err != nil {
				b.Fatal(err)
			}
			agg := newAggregator(buf)
			for s := range n {
				buf.start(s, 0)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for col := range int64(b.N) {
				for s := range n {
					buf.put(s, col, candle(s, col), Fresh, 0)
				}
				agg.step(col)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/derivation")
		})
	}
}
