package frame

import (
	"math"
	"slices"
	"testing"

	proto "whatever/internal/protocol"
)

// newRing returns the ring of a one-entry buffer of float64 values.
func newRing(slots, period, window, bars int64) *Ring[float64] {
	return NewBuffer[float64]([]Shape{{Slots: slots, Period: period, Window: window, Bars: bars}}).Ring(0)
}

// val is the test value written at col.
func val(col int64) float64 { return float64(col) + 0.5 }

// vals returns the test values at cols, in the given order.
func vals(cols ...int64) []float64 {
	out := make([]float64, len(cols))
	for i, col := range cols {
		out[i] = val(col)
	}
	return out
}

// fresh puts a Fresh test value at each col.
func fresh(r *Ring[float64], cols ...int64) {
	for _, col := range cols {
		r.Put(col, val(col), Fresh)
	}
}

// TestRingPut covers the write path: Fresh values, carried Gap and Ended slots, the end column, and
// rewrites.
func TestRingPut(t *testing.T) {
	t.Run("Fresh stores its value", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 3)
		want := Slot[float64]{Col: 3, ValCol: 3, Val: val(3), State: Fresh}
		if got := *r.At(3); got != want {
			t.Errorf("slot = %+v, want %+v", got, want)
		}
	})

	t.Run("Gap and Ended carry the previous update's Fresh value", func(t *testing.T) {
		r := newRing(8, 12, 1, 8)
		fresh(r, 0)
		r.Put(12, 99, Gap)
		r.Put(24, 99, Ended)
		for _, col := range []int64{12, 24} {
			if got := r.At(col); got.Val != val(0) || got.ValCol != 0 {
				t.Errorf("col %d: carries %v from %d, want the value at 0", col, got.Val, got.ValCol)
			}
		}
	})

	t.Run("no previous update carries the zero value", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		r.Put(0, 99, Ended) // no column before 0
		r.Put(7, 99, Gap)   // column 6 empty
		for _, col := range []int64{0, 7} {
			if got := r.At(col); got.Val != 0 || got.ValCol != -1 {
				t.Errorf("col %d: slot = %+v, want the zero value and ValCol -1", col, got)
			}
		}
	})

	t.Run("consecutive gaps keep the same Fresh value", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 0)
		for col := int64(1); col <= 3; col++ {
			r.Put(col, 99, Gap)
		}
		if got := r.At(3); got.Val != val(0) || got.ValCol != 0 {
			t.Errorf("third gap carries %v from %d, want the value at 0", got.Val, got.ValCol)
		}
	})

	t.Run("Ended records the end column", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		if e := r.end.Load(); e != math.MaxInt64 {
			t.Fatalf("open ring: end = %d", e)
		}
		fresh(r, 4)
		r.Put(5, 0, Ended)
		if e := r.end.Load(); e != 5 {
			t.Errorf("end = %d, want 5", e)
		}
	})

	t.Run("rewriting a column replaces its slot", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 2)
		r.Put(2, 9, Fresh)
		if got := r.At(2); got.Val != 9 {
			t.Errorf("rewritten value = %v, want 9", got.Val)
		}
	})
}

// TestRingAt covers slot lookup: a held column returns its slot; any other is nil.
func TestRingAt(t *testing.T) {
	r := newRing(4, 1, 1, 4)
	r.Start(2)
	fresh(r, 2, 3, 4, 5, 6) // 6 overwrites 2's slot
	if sl := r.At(5); sl == nil || sl.Col != 5 {
		t.Errorf("At(5) = %+v, want the slot holding 5", sl)
	}
	for _, col := range []int64{1, 2, 7} { // below the first column, overwritten, never written
		if sl := r.At(col); sl != nil {
			t.Errorf("At(%d) = %+v, want nil", col, sl)
		}
	}
	if sl := newRing(0, 0, 0, 0).At(0); sl != nil {
		t.Errorf("empty ring: At(0) = %+v, want nil", sl)
	}
}

/*
TestRingLatest covers what a ring reports at a column: a Fresh value, a coarse entry carried between
its updates, gaps carrying the last Fresh value, the end, and a ring not started.
*/
func TestRingLatest(t *testing.T) {
	type latest struct {
		v   float64
		s   State
		age int
	}
	expect := func(t *testing.T, r *Ring[float64], col int64, want latest) {
		t.Helper()
		v, s, age := r.Latest(col)
		if got := (latest{v, s, age}); got != want {
			t.Errorf("Latest(%d) = %+v, want %+v", col, got, want)
		}
	}
	notStarted := latest{0, Gap, -1}

	t.Run("Fresh at its update column has age 0", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 5)
		expect(t, r, 5, latest{val(5), Fresh, 0})
	})

	t.Run("a coarse entry carries its last update", func(t *testing.T) {
		r := newRing(8, 12, 1, 8)
		fresh(r, 24)
		expect(t, r, 25, latest{val(24), Fresh, 1})
		expect(t, r, 35, latest{val(24), Fresh, 11})
	})

	t.Run("a gap carries the last Fresh value, its age growing", func(t *testing.T) {
		r := newRing(8, 12, 1, 8)
		fresh(r, 0)
		r.Put(12, 0, Gap)
		expect(t, r, 12, latest{val(0), Gap, 12})
		expect(t, r, 13, latest{val(0), Gap, 13})
	})

	t.Run("a gap longer than the ring carries a value no slot holds", func(t *testing.T) {
		r := newRing(4, 1, 1, 4)
		fresh(r, 0)
		for col := int64(1); col <= 10; col++ {
			r.Put(col, 0, Gap)
		}
		if r.At(0) != nil {
			t.Fatal("column 0's slot not overwritten")
		}
		expect(t, r, 10, latest{val(0), Gap, 10})
	})

	t.Run("Fresh after a gap resets the age", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 0)
		r.Put(1, 0, Gap)
		fresh(r, 2)
		expect(t, r, 2, latest{val(2), Fresh, 0})
	})

	t.Run("Ended straight after a gap carries the value before it", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 0)
		r.Put(1, 0, Gap)
		r.Put(2, 0, Ended)
		expect(t, r, 2, latest{val(0), Ended, 2})
		expect(t, r, 9, latest{val(0), Ended, 9})
	})

	t.Run("an entry ended before any value has no age", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		r.Put(1, 0, Ended)
		expect(t, r, 1, latest{0, Ended, -1})
		expect(t, r, 6, latest{0, Ended, -1})
	})

	t.Run("pinned before an end already written", func(t *testing.T) {
		r := newRing(8, 12, 1, 8)
		fresh(r, 0, 12)
		r.Put(24, 0, Ended)
		expect(t, r, 23, latest{val(12), Fresh, 11})
	})

	t.Run("a gap before any Fresh value has no age", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		r.Put(0, 0, Gap)
		expect(t, r, 0, latest{0, Gap, -1})
	})

	t.Run("past the end the Ended slot is read", func(t *testing.T) {
		r := newRing(4, 1, 1, 4)
		fresh(r, 0, 1)
		r.Put(2, 0, Ended)
		expect(t, r, 2, latest{val(1), Ended, 1})
		expect(t, r, 10, latest{val(1), Ended, 9})
	})

	t.Run("nothing below the first column is read", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 8)
		r.Start(10)
		fresh(r, 10)
		expect(t, r, 9, notStarted)
	})

	t.Run("a ring with nothing written is not started", func(t *testing.T) {
		expect(t, newRing(8, 1, 1, 8), 3, notStarted)
	})

	t.Run("an empty ring is not started", func(t *testing.T) {
		expect(t, newRing(0, 0, 0, 0), 5, notStarted)
	})
}

/*
TestRingData covers history reads: newest first, one bar per step on direct, coarse and rolling
shapes, Gap and Ended skipped, bounded by the ring's bars and its first column, and empty rings.
*/
func TestRingData(t *testing.T) {
	expect := func(t *testing.T, r *Ring[float64], col int64, n int, want []float64) {
		t.Helper()
		if got := r.Data(col, n, nil); !slices.Equal(got, want) {
			t.Errorf("Data(%d, %d) = %v, want %v", col, n, got, want)
		}
	}

	t.Run("newest first, one value per update", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 0, 1, 2, 3, 4, 5)
		expect(t, r, 5, 3, vals(5, 4, 3))
	})

	t.Run("a coarse ring steps its period", func(t *testing.T) {
		r := newRing(3, 12, 1, 3)
		fresh(r, 0, 12, 24)
		expect(t, r, 30, 3, vals(24, 12, 0))
	})

	t.Run("a rolling ring steps one bar of window updates", func(t *testing.T) {
		r := newRing(12, 1, 4, 3)
		for col := int64(0); col < 12; col++ {
			fresh(r, col)
		}
		expect(t, r, 11, 3, vals(11, 7, 3))
	})

	t.Run("Gap and Ended are skipped", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 0, 1)
		r.Put(2, 0, Gap)
		fresh(r, 3)
		r.Put(4, 0, Ended)
		expect(t, r, 4, 8, vals(3, 1, 0))
	})

	t.Run("after the end, the history before it", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 0, 1, 2, 3, 4)
		r.Put(5, 0, Ended)
		expect(t, r, 8, 10, vals(4, 3, 2, 1, 0))
	})

	t.Run("across the slot index wrap, never past the bars", func(t *testing.T) {
		r := newRing(6, 1, 1, 4) // slots beyond the bars, as headroom
		for col := int64(0); col <= 9; col++ {
			fresh(r, col)
		}
		// at 7: slots 1, 0, then 5, 4
		expect(t, r, 7, 10, vals(7, 6, 5, 4))
	})

	t.Run("a ring filled from its first column reads nothing below it", func(t *testing.T) {
		r := newRing(4, 1, 1, 4)
		r.Start(100)
		fresh(r, 100, 101, 102, 103) // wraps onto the slots of 96 … 99
		expect(t, r, 98, 10, nil)
	})

	t.Run("at most the ring's bars", func(t *testing.T) {
		r := newRing(4, 1, 1, 2)
		fresh(r, 0, 1, 2, 3)
		expect(t, r, 3, 10, vals(3, 2))
	})

	t.Run("nothing below the first column is read", func(t *testing.T) {
		const first = 100
		r := newRing(8, 1, 1, 8)
		fresh(r, first-2, first-1)
		r.Start(first)
		fresh(r, first, first+1, first+2, first+3)
		expect(t, r, first+3, 10, vals(first+3, first+2, first+1, first))
		expect(t, r, first-1, 10, nil)
	})

	t.Run("an overwritten bar stops the walk", func(t *testing.T) {
		r := newRing(4, 1, 1, 8) // fewer slots than bars
		fresh(r, 0, 1, 2, 3, 4, 5)
		expect(t, r, 5, 8, vals(5, 4, 3, 2))
	})

	t.Run("n of zero reads nothing", func(t *testing.T) {
		r := newRing(8, 1, 1, 8)
		fresh(r, 0)
		expect(t, r, 0, 0, nil)
	})

	t.Run("an empty ring reads nothing", func(t *testing.T) {
		expect(t, newRing(0, 0, 0, 0), 5, 10, nil)
	})
}

// TestRingAllocations checks the hot path: with dst capacity, reads allocate nothing.
func TestRingAllocations(t *testing.T) {
	floats := newRing(32, 1, 1, 32)
	candles := NewBuffer[proto.Candle]([]Shape{{Slots: 32, Period: 1, Window: 1, Bars: 32}}).Ring(0)
	for col := int64(0); col < 32; col++ {
		fresh(floats, col)
		candles.Put(col, proto.Candle{Close: val(col)}, Fresh)
	}

	fdst := make([]float64, 0, 20)
	cdst := make([]proto.Candle, 0, 20)
	allocs := testing.AllocsPerRun(100, func() {
		fdst = floats.Data(31, 20, fdst)
		floats.Latest(31)
		cdst = candles.Data(31, 20, cdst)
		candles.Latest(31)
	})
	if allocs != 0 {
		t.Errorf("%v allocations per read, want 0", allocs)
	}
}

// TestView covers a view's frame and its reads at the frame's column, and Pin keeping the frame over
// another buffer.
func TestView(t *testing.T) {
	floats := NewBuffer[float64]([]Shape{{Slots: 8, Period: 1, Window: 1, Bars: 8}})
	fresh(floats.Ring(0), 0, 1, 2, 3)

	v := NewView(2, 300_000, Current, true, floats)
	if v.Col() != 2 || v.Boundary() != 600_000 || v.Status() != Current || !v.Complete() {
		t.Errorf("view frame = col %d, boundary %d, status %d, complete %t",
			v.Col(), v.Boundary(), v.Status(), v.Complete())
	}

	// pinned at column 2: column 3 is not read
	if got := v.Data(0, 10, nil); !slices.Equal(got, vals(2, 1, 0)) {
		t.Errorf("Data = %v, want %v", got, vals(2, 1, 0))
	}
	if x, s, age := v.Latest(0); x != val(2) || s != Fresh || age != 0 {
		t.Errorf("Latest = %v %d %d, want %v Fresh 0", x, s, age, val(2))
	}

	candles := NewBuffer[proto.Candle]([]Shape{{Slots: 8, Period: 1, Window: 1, Bars: 8}})
	candles.Ring(0).Put(2, proto.Candle{Close: 7}, Fresh)
	candles.Ring(0).Put(3, proto.Candle{Close: 8}, Fresh)

	p := Pin(v, candles)
	if p.Col() != v.Col() || p.Boundary() != v.Boundary() || p.Status() != v.Status() ||
		p.Complete() != v.Complete() {
		t.Errorf("pinned frame differs from the view's")
	}
	if c, s, _ := p.Latest(0); c.Close != 7 || s != Fresh {
		t.Errorf("pinned Latest = %+v %d, want the candle at column 2", c, s)
	}
}
