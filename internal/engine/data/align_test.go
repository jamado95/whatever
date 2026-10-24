package data

import (
	"slices"
	"sync/atomic"
	"testing"
	"time"

	proto "whatever/internal/protocol"
)

// items returns stream s's lane items at cols, carrying its test candles.
func items(s int, cols ...int64) []laneItem {
	out := make([]laneItem, len(cols))
	for i, col := range cols {
		out[i] = laneItem{col: col, c: candle(s, col)}
	}
	return out
}

// feedLane returns a closed lane holding its.
func feedLane(its []laneItem) <-chan laneItem {
	ch := make(chan laneItem, len(its))
	for _, it := range its {
		ch <- it
	}
	close(ch)
	return ch
}

// evals returns stream s's evaluates at cols, all in one state.
func evals(s int, state LegState, cols ...int64) []evaluate {
	out := make([]evaluate, len(cols))
	for i, col := range cols {
		out[i] = evaluate{stream: s, col: col, state: state}
	}
	return out
}

// settled receives from out until it closes or nothing arrives for 50 ms.
func settled(out <-chan evaluate) []evaluate {
	var got []evaluate
	for {
		select {
		case e, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, e)
		case <-time.After(50 * time.Millisecond):
			return got
		}
	}
}

// closed reports whether out is closed, waiting up to 50 ms.
func closed(out <-chan evaluate) bool {
	select {
	case _, ok := <-out:
		return !ok
	case <-time.After(50 * time.Millisecond):
		return false
	}
}

/*
TestGate covers the consumed column: wait returns once the engine has consumed its column — at once
when it already has — and consume wakes exactly the waiters it covers.
*/
func TestGate(t *testing.T) {
	g := newGate()
	cols := []int64{-1, 5, 10, 15}
	var done [4]atomic.Bool
	for i, col := range cols {
		go func() {
			g.wait(col)
			done[i].Store(true)
		}()
	}

	check := func(want ...bool) {
		t.Helper()
		time.Sleep(50 * time.Millisecond)
		for i := range done {
			if got := done[i].Load(); got != want[i] {
				t.Errorf("wait(%d) returned: %v, want %v", cols[i], got, want[i])
			}
		}
	}
	check(true, false, false, false) // −1 is consumed before the first column
	g.consume(10)
	check(true, true, true, false)
	g.consume(15)
	check(true, true, true, true)
}

/*
TestAlign covers staging with rings large enough never to wait: Fresh at each candle's column, a Gap
at every due column skipped, carrying the last Fresh candle, Ended one period after the last candle,
and a lane without candles. The first candle sets the ring's first column.
*/
func TestAlign(t *testing.T) {
	const d = day0
	type step struct {
		col   int64
		state LegState
	}

	tests := []struct {
		name string
		id   string
		cols []int64
		want []step
	}{
		{"consecutive candles", idBTC5m, []int64{d, d + 1, d + 2},
			[]step{{d, Fresh}, {d + 1, Fresh}, {d + 2, Fresh}, {d + 3, Ended}}},
		{"a skipped 5m column", idBTC5m, []int64{d, d + 2},
			[]step{{d, Fresh}, {d + 1, Gap}, {d + 2, Fresh}, {d + 3, Ended}}},
		{"a skipped 1h bar", idBTC1h, []int64{d, d + 24},
			[]step{{d, Fresh}, {d + 12, Gap}, {d + 24, Fresh}, {d + 36, Ended}}},
		{"a gap of several due columns", idBTC5m, []int64{d, d + 4},
			[]step{{d, Fresh}, {d + 1, Gap}, {d + 2, Gap}, {d + 3, Gap}, {d + 4, Fresh}, {d + 5, Ended}}},
		{"no candle", idBTC5m, nil, []step{{-1, Ended}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixtureWith(t, testSubs(), 20, 400)
			s := f.idx(tt.id)
			got := settled(newAligner(f.buf, newGate(), s).run(feedLane(items(s, tt.cols...))))

			want := make([]evaluate, len(tt.want))
			for i, w := range tt.want {
				want[i] = evaluate{stream: s, col: w.col, state: w.state}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("evaluates = %v, want %v", got, want)
			}

			var fresh proto.Candle // the last Fresh candle, carried by Gap and Ended slots
			for _, e := range want {
				if e.col < 0 {
					continue
				}
				if e.state == Fresh {
					fresh = candle(s, e.col)
				}
				if sl := f.slot(s, e.col); sl.col != e.col || sl.state != e.state || sl.c != fresh {
					t.Errorf("slot at %d = %+v, want %d carrying %+v", e.col, sl, e.state, fresh)
				}
			}

			var first int64
			if len(tt.cols) > 0 {
				first = tt.cols[0]
			} else if i := slices.IndexFunc(f.buf.rings[s].slots, func(sl slot) bool { return sl.col >= 0 }); i >= 0 {
				t.Errorf("no candle: slot %d written", i)
			}
			if got := f.buf.rings[s].first.Load(); got != first {
				t.Errorf("first column = %d, want %d", got, first)
			}
		})
	}
}

/*
TestAlignCapacity covers the ring-capacity limit with headroom 4: a stream fills its whole ring at
startup, then writes as consumption frees slots — up to consumed + spare·Period, stepping by its
period for a coarse stream — and a long gap is reported slot by slot before align blocks.
*/
func TestAlignCapacity(t *testing.T) {
	const d = day0
	expect := func(t *testing.T, got, want []evaluate) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Fatalf("evaluates = %v,\nwant %v", got, want)
		}
	}

	t.Run("startup fills the ring, then consumption frees it", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m) // 25 slots, 20 bars kept, spare 5
		g := newGate()
		out := newAligner(f.buf, g, s).run(feedLane(items(s, span(d, d+99, 1)...)))

		expect(t, settled(out), evals(s, Fresh, span(d, d+24, 1)...))
		g.consume(d + 19)
		expect(t, settled(out), nil)
		g.consume(d + 20)
		expect(t, settled(out), evals(s, Fresh, d+25))
		g.consume(d + 30)
		expect(t, settled(out), evals(s, Fresh, span(d+26, d+35, 1)...))
		g.consume(d + 200)
		expect(t, settled(out), append(evals(s, Fresh, span(d+36, d+99, 1)...), evals(s, Ended, d+100)...))
		if !closed(out) {
			t.Error("output not closed")
		}
	})

	t.Run("a coarse stream steps by its period", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h) // 27 slots, 25 bars kept, spare 2, period 12
		g := newGate()
		out := newAligner(f.buf, g, s).run(feedLane(items(s, span(d, d+468, 12)...)))

		expect(t, settled(out), evals(s, Fresh, span(d, d+312, 12)...))
		g.consume(d + 299)
		expect(t, settled(out), nil)
		g.consume(d + 300)
		expect(t, settled(out), evals(s, Fresh, d+324))
		g.consume(d + 311)
		expect(t, settled(out), nil)
		g.consume(d + 312)
		expect(t, settled(out), evals(s, Fresh, d+336))
		g.consume(d + 1000)
		expect(t, settled(out), append(evals(s, Fresh, span(d+348, d+468, 12)...), evals(s, Ended, d+480)...))
		if !closed(out) {
			t.Error("output not closed")
		}
	})

	t.Run("a long gap is reported slot by slot", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		g := newGate()
		out := newAligner(f.buf, g, s).run(feedLane(items(s, d, d+100)))

		expect(t, settled(out), append(evals(s, Fresh, d), evals(s, Gap, span(d+1, d+24, 1)...)...))
		g.consume(d + 95)
		expect(t, settled(out), append(evals(s, Gap, span(d+25, d+99, 1)...), evals(s, Fresh, d+100)...))
		g.consume(d + 96)
		expect(t, settled(out), evals(s, Ended, d+101))
		if !closed(out) {
			t.Error("output not closed")
		}
	})
}
