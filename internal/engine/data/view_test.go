package data

import (
	"slices"
	"testing"
	"time"

	proto "whatever/internal/protocol"
)

// candles returns stream s's test candles at cols, in the given order.
func candles(s int, cols ...int64) []proto.Candle {
	out := make([]proto.Candle, len(cols))
	for i, col := range cols {
		out[i] = candle(s, col)
	}
	return out
}

/*
TestViewLeg covers what a view reports for one stream: a Fresh leg, a coarse stream carried between
its closes, gaps carrying the last Fresh candle, ends, a stream not started, and pinning before an
end already written.
*/
func TestViewLeg(t *testing.T) {
	type leg struct {
		c   proto.Candle
		s   LegState
		age int
	}
	get := func(f fixture, s int, col int64) leg {
		c, st, age := f.buf.view(col, true).Leg(s)
		return leg{c, st, age}
	}
	expect := func(t *testing.T, f fixture, s int, col int64, want leg) {
		t.Helper()
		if got := get(f, s, col); got != want {
			t.Errorf("view at %d: Leg = %+v, want %+v", col, got, want)
		}
	}

	t.Run("Fresh at a due column has age 0", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h)
		f.fresh(s, 24)
		expect(t, f, s, 24, leg{candle(s, 24), Fresh, 0})
	})

	t.Run("a coarse direct stream carries its close", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h) // period 12
		f.fresh(s, 24)
		expect(t, f, s, 25, leg{candle(s, 24), Fresh, 1})
		expect(t, f, s, 35, leg{candle(s, 24), Fresh, 11})
	})

	t.Run("a gap carries the last Fresh candle, its age growing", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h)
		f.fresh(s, 0)
		f.buf.put(s, 12, proto.Candle{}, Gap, 0)
		expect(t, f, s, 12, leg{candle(s, 0), Gap, 12})
		f.buf.put(s, 24, proto.Candle{}, Gap, 0)
		expect(t, f, s, 30, leg{candle(s, 0), Gap, 30})
	})

	t.Run("Fresh after a gap resets the age", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h)
		f.fresh(s, 0)
		f.buf.put(s, 12, proto.Candle{}, Gap, 0)
		f.fresh(s, 24)
		expect(t, f, s, 24, leg{candle(s, 24), Fresh, 0})
	})

	t.Run("Ended straight after a gap carries the candle before it", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h)
		f.fresh(s, 0)
		f.buf.put(s, 12, proto.Candle{}, Gap, 0)
		f.buf.put(s, 24, proto.Candle{}, Ended, 0)
		expect(t, f, s, 24, leg{candle(s, 0), Ended, 24})
		expect(t, f, s, 100, leg{candle(s, 0), Ended, 100})
	})

	t.Run("a lane ended before any candle", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h)
		f.buf.put(s, 12, proto.Candle{}, Ended, 0)
		expect(t, f, s, 12, leg{proto.Candle{}, Ended, -1})
		expect(t, f, s, 50, leg{proto.Candle{}, Ended, -1})
	})

	t.Run("not started", func(t *testing.T) {
		f := newFixture(t)
		expect(t, f, f.idx(idBTC1h), 50, leg{proto.Candle{}, Gap, -1})
	})

	t.Run("pinned before an end already written", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h)
		f.fresh(s, 0, 12)
		f.buf.put(s, 24, proto.Candle{}, Ended, 0)
		expect(t, f, s, 23, leg{candle(s, 12), Fresh, 11})
	})

	t.Run("derived from a coarse direct stream anchors at the source's update", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idRoll1d) // period 12: the 1h source's
		f.buf.put(s, 12, candle(s, 12), Fresh, 2)
		expect(t, f, s, 20, leg{candle(s, 12), Fresh, 8})
	})
}

/*
TestViewCount covers the source bars in a stream's latest window: a derived stream warming up, a
direct stream before and after it starts, and pinning.
*/
func TestViewCount(t *testing.T) {
	t.Run("derived during warm-up counts up to its window", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idRoll1h) // window 12
		for col := int64(0); col < 16; col++ {
			f.buf.put(s, col, candle(s, col), Fresh, uint16(min(col+1, 12)))
			if got, want := f.buf.view(col, true).Count(s), int(min(col+1, 12)); got != want {
				t.Errorf("col %d: Count = %d, want %d", col, got, want)
			}
		}
	})

	t.Run("direct is 1 once started, 0 before", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		if got := f.buf.view(0, true).Count(s); got != 0 {
			t.Errorf("not started: Count = %d, want 0", got)
		}
		f.fresh(s, 0)
		f.buf.put(s, 1, proto.Candle{}, Gap, 0)
		for _, col := range []int64{0, 1} {
			if got := f.buf.view(col, true).Count(s); got != 1 {
				t.Errorf("col %d: Count = %d, want 1", col, got)
			}
		}

		e := f.idx(idETH1d)
		f.buf.put(e, 0, proto.Candle{}, Ended, 0)
		if got := f.buf.view(0, true).Count(e); got != 0 {
			t.Errorf("ended before any candle: Count = %d, want 0", got)
		}
	})

	t.Run("a count written at a later column is not visible", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idRoll1h)
		f.buf.put(s, 0, candle(s, 0), Fresh, 1)
		f.buf.put(s, 1, candle(s, 1), Fresh, 2)
		if got := f.buf.view(0, true).Count(s); got != 1 {
			t.Errorf("Count = %d, want 1", got)
		}
	})
}

/*
TestViewCandles covers history reads: order and length, pinning against bars already written, the
derived step, gaps and ends skipped, empty reads, and the ring's wrap and depth.
*/
func TestViewCandles(t *testing.T) {
	read := func(f fixture, s int, col int64, n int) []proto.Candle {
		return f.buf.view(col, true).Candles(s, n, nil)
	}
	expect := func(t *testing.T, got, want []proto.Candle) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Errorf("Candles = %v, want %v", got, want)
		}
	}

	t.Run("newest first, fewer when history is short", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		f.fresh(s, span(0, 9, 1)...)
		expect(t, read(f, s, 9, 5), candles(s, 9, 8, 7, 6, 5))
		expect(t, read(f, s, 9, 15), candles(s, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0))
	})

	t.Run("a bar written beyond the view's column is not read", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC1h)
		f.fresh(s, 96, 108, 120, 132) // 132 is 11:00; the view is at 10:30
		expect(t, read(f, s, 126, 5), candles(s, 120, 108, 96))

		r := f.idx(idRoll1h)
		f.fresh(r, span(0, 30, 1)...)
		expect(t, read(f, r, 20, 5), candles(r, 20, 8))
	})

	t.Run("a rolling stream steps one bar and skips gaps", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idRoll1h) // 12 slots per bar
		for col := int64(0); col <= 40; col++ {
			state := Fresh
			if col == 16 {
				state = Gap
			}
			f.buf.put(s, col, candle(s, col), state, 12)
		}
		expect(t, read(f, s, 40, 5), candles(s, 40, 28, 4))
	})

	t.Run("after the end, the bars before it", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		f.fresh(s, span(0, 4, 1)...)
		f.buf.put(s, 5, proto.Candle{}, Ended, 0)
		expect(t, read(f, s, 8, 10), candles(s, 4, 3, 2, 1, 0))
	})

	t.Run("not started and n = 0 are empty", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		if got := read(f, s, 5, 10); len(got) != 0 {
			t.Errorf("not started: %d candles", len(got))
		}
		f.fresh(s, span(0, 5, 1)...)
		if got := read(f, s, 5, 0); len(got) != 0 {
			t.Errorf("n = 0: %d candles", len(got))
		}
	})

	t.Run("n beyond the kept bars returns the kept bars", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m) // 20 bars kept
		f.fresh(s, span(0, 99, 1)...)
		expect(t, read(f, s, 99, 100), candles(s, span(99, 80, -1)...))
	})

	t.Run("across the ring's wrap, never past the kept bars", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)           // 25 slots, 20 bars kept
		f.fresh(s, span(0, 89, 1)...) // three ring lengths, then up to headroom past the view
		// view at 85: slots 10 … 0, then 24 … 16 — across the index wrap
		expect(t, read(f, s, 85, 20), candles(s, span(85, 66, -1)...))
		expect(t, read(f, s, 85, 30), candles(s, span(85, 66, -1)...))
	})
}

/*
TestViewFirst covers reads bounded by a stream's first column: a ring filled from there wraps onto
the slots of earlier columns, which still read as not started, and history never goes below it.
*/
func TestViewFirst(t *testing.T) {
	const first = 100

	t.Run("a ring filled from the first column", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m) // 25 slots, 20 bars kept
		f.buf.start(s, first)
		f.fresh(s, span(first, first+24, 1)...)

		for _, col := range []int64{first - 25, first - 10, first - 1} {
			v := f.buf.view(col, true)
			if c, st, age := v.Leg(s); c != (proto.Candle{}) || st != Gap || age != -1 {
				t.Errorf("view at %d: Leg = %+v %d %d, want not started", col, c, st, age)
			}
			if n := v.Count(s); n != 0 {
				t.Errorf("view at %d: Count = %d, want 0", col, n)
			}
			if got := v.Candles(s, 10, nil); len(got) != 0 {
				t.Errorf("view at %d: %d candles, want none", col, len(got))
			}
		}
	})

	t.Run("nothing below the first column is read", func(t *testing.T) {
		f := newFixture(t)
		s := f.idx(idBTC5m)
		f.fresh(s, first-2, first-1)
		f.buf.start(s, first)
		f.fresh(s, span(first, first+3, 1)...)

		if _, st, age := f.buf.view(first-1, true).Leg(s); st != Gap || age != -1 {
			t.Errorf("Leg before the first column = %d %d, want not started", st, age)
		}
		got := f.buf.view(first+3, true).Candles(s, 10, nil)
		if want := candles(s, first+3, first+2, first+1, first); !slices.Equal(got, want) {
			t.Errorf("Candles = %v, want %v", got, want)
		}
	})
}

/*
TestViewAllocations checks the hot path: with dst capacity, reads allocate nothing.
*/
func TestViewAllocations(t *testing.T) {
	f := newFixture(t)
	for _, id := range []string{idBTC5m, idRoll1h} {
		s := f.idx(id)
		f.fresh(s, span(0, 300, 1)...)
		v := f.buf.view(300, true)
		dst := make([]proto.Candle, 0, 20)
		allocs := testing.AllocsPerRun(100, func() {
			dst = v.Candles(s, 20, dst)
			v.Leg(s)
			v.Count(s)
		})
		if allocs != 0 {
			t.Errorf("%s: %v allocations per read, want 0", id, allocs)
		}
	}
}

/*
TestViewBoundary covers the column's timestamp: col × Stride, a UTC boundary — at realistic epoch
columns, a UTC-midnight column is due for a 1d stream.
*/
func TestViewBoundary(t *testing.T) {
	t.Run("col × Stride", func(t *testing.T) {
		f := newFixture(t)
		if got := f.buf.view(7, true).Boundary(); got != 7*f.st.Stride {
			t.Errorf("Boundary = %d, want %d", got, 7*f.st.Stride)
		}
	})

	t.Run("a UTC-midnight column is due for a 1d stream", func(t *testing.T) {
		const midnight = 1704067200000 // 2024-01-01T00:00:00Z
		f := newFixture(t)
		s := f.idx(idETH1d)
		col := int64(midnight) / f.st.Stride
		if col%f.st.All[s].Period != 0 {
			t.Fatalf("column %d is not due for 1d (period %d)", col, f.st.All[s].Period)
		}

		f.fresh(s, col)
		v := f.buf.view(col, true)
		if got := v.Boundary(); got != midnight || time.UnixMilli(got).UTC().Hour() != 0 {
			t.Errorf("Boundary = %d, want %d", got, int64(midnight))
		}
		c, state, age := f.buf.view(col+100, true).Leg(s)
		if c != candle(s, col) || state != Fresh || age != 100 {
			t.Errorf("100 columns later: Leg = %+v %d %d, want the midnight candle with age 100", c, state, age)
		}
	})
}
