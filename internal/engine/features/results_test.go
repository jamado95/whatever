package features

import (
	"slices"
	"testing"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
)

// stream is the single-stream coordinate of stream i.
func stream(i int) [2]int { return [2]int{i, -1} }

// mustColumn builds f's column over shapes, its results typed T.
func mustColumn[T any](t *testing.T, f feature.Feature, sh []frame.Shape) *results[T] {
	t.Helper()
	col, err := newColumn(f, sh)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := col.(*results[T])
	if !ok {
		t.Fatalf("%s: column is %T", f.ID, col)
	}
	return r
}

// bound builds f's column over n rings, binds it on stream i at its ring, and allocates it.
func bound[T any](t *testing.T, f feature.Feature, n, i int, cv *frame.View[proto.Candle]) *results[T] {
	t.Helper()
	r := mustColumn[T](t, f, shapes(n))
	if err := r.bind(feature.NewBinder(stream(i), cv, nil), i); err != nil {
		t.Fatal(err)
	}
	r.alloc()
	return r
}

// constant is a feature whose Bind is written as a method value.
type constant[T any] struct{ v T }

func (c constant[T]) bind(*feature.Binder) func() (T, bool) {
	return func() (T, bool) { return c.v, true }
}

func bindFloat(*feature.Binder) func() (float64, bool) {
	return func() (float64, bool) { return 2, true }
}

func bindInt(*feature.Binder) func() (int, bool) {
	return func() (int, bool) { return 3, true }
}

// readsBack fails unless bind reaches the column of its value type T, and that column holds what its
// node returns.
func readsBack[T comparable](t *testing.T, name string, bind any, want T) {
	t.Helper()
	buf := candles(1, 1)
	var cv frame.View[proto.Candle]
	r := bound[T](t, feature.Feature{ID: name, Bind: bind}, 1, 0, &cv)
	r.pin(at(buf, 0))
	r.fire(0, 0)
	if v, s, _ := r.view.Latest(0); v != want || s != frame.Fresh {
		t.Errorf("%s: Latest = %v %d, want %v Fresh", name, v, s, want)
	}
}

// TestColumnValueTypes covers the switch: every way of writing a Bind reaches its value type's case,
// and whatever has no case is an error naming the feature and the type found.
func TestColumnValueTypes(t *testing.T) {
	readsBack(t, "float64 literal", func(*feature.Binder) func() (float64, bool) {
		return func() (float64, bool) { return 2, true }
	}, 2.0)
	readsBack(t, "float64 method value", constant[float64]{2}.bind, 2.0)
	readsBack(t, "float64 function", bindFloat, 2.0)
	readsBack(t, "int literal", func(*feature.Binder) func() (int, bool) {
		return func() (int, bool) { return 3, true }
	}, 3)
	readsBack(t, "int method value", constant[int]{3}.bind, 3)
	readsBack(t, "int function", bindInt, 3)

	for _, c := range []struct {
		f    feature.Feature
		want string
	}{
		{label(), "features: label: unsupported Bind func(*feature.Binder) func() (string, bool)"},
		{feature.Feature{ID: "old", Bind: func(*feature.Binder) (func() (float64, bool), error) {
			return nil, nil
		}}, "features: old: unsupported Bind func(*feature.Binder) (func() (float64, bool), error)"},
		{feature.Feature{ID: "none"}, "features: none: unsupported Bind <nil>"},
	} {
		if _, err := newColumn(c.f, shapes(1)); err == nil || err.Error() != c.want {
			t.Errorf("%s: err = %v, want %q", c.f.ID, err, c.want)
		}
	}
}

// TestColumnBind covers binding before any ring exists: once allocated and pinned, a node reads its
// stream's candle, and a dependent reads its dependency's result at the same column, each following
// the view from frame to frame.
func TestColumnBind(t *testing.T) {
	buf := candles(2, 6)
	var cv frame.View[proto.Candle]
	lastCol := mustColumn[float64](t, last(), shapes(2))
	diffCol := mustColumn[float64](t, diff(), shapes(2))

	deps := map[string]any{"last": lastCol.cell()}
	if err := diffCol.bind(feature.NewBinder(stream(1), &cv, deps), 1); err != nil {
		t.Fatal(err)
	}
	if err := lastCol.bind(feature.NewBinder(stream(1), &cv, nil), 1); err != nil {
		t.Fatal(err)
	}
	if lastCol.buf != nil || diffCol.buf != nil {
		t.Fatalf("a column allocated before alloc")
	}
	lastCol.alloc()
	diffCol.alloc()

	for col := int64(0); col < 6; col++ {
		cv = at(buf, col)
		lastCol.pin(cv)
		diffCol.pin(cv)
		lastCol.fire(1, col)
		diffCol.fire(1, col)

		if v, s, _ := lastCol.view.Latest(1); v != closeAt(1, col) || s != frame.Fresh {
			t.Errorf("last at %d = %v %d, want %v Fresh", col, v, s, closeAt(1, col))
		}
		if col == 0 {
			continue
		}
		want := closeAt(1, col) - closeAt(1, col-1)
		if v, s, _ := diffCol.view.Latest(1); v != want || s != frame.Fresh {
			t.Errorf("diff at %d = %v %d, want %v Fresh", col, v, s, want)
		}
	}

	t.Run("a failed bind returns the binder's error and keeps no node", func(t *testing.T) {
		signCol := mustColumn[int](t, sign(), shapes(2))
		for _, c := range []struct {
			name string
			deps map[string]any
			want string
		}{
			{"no dependency", nil, `feature: no dependency of type "last"`},
			{"the wrong type", map[string]any{"last": signCol.cell()},
				`feature: dependency "last" is not float64`},
		} {
			err := diffCol.bind(feature.NewBinder(stream(0), &cv, c.deps), 0)
			if err == nil || err.Error() != c.want {
				t.Errorf("%s: err = %v, want %q", c.name, err, c.want)
			}
			if diffCol.nodes[0] != nil {
				t.Errorf("%s: kept a node", c.name)
			}
		}
	})
}

// TestColumnFire covers what firing writes: Fresh when the node returns a value, of any value type,
// and Gap when it cannot compute, carrying the last value once there is one.
func TestColumnFire(t *testing.T) {
	t.Run("Fresh", func(t *testing.T) {
		buf := candles(1, 2)
		var cv frame.View[proto.Candle]
		lastCol := bound[float64](t, last(), 1, 0, &cv)
		signCol := bound[int](t, sign(), 1, 0, &cv)
		for col := int64(0); col < 2; col++ {
			cv = at(buf, col)
			lastCol.fire(0, col)
			signCol.fire(0, col)
		}
		want := frame.Slot[float64]{Col: 1, ValCol: 1, Val: closeAt(0, 1), State: frame.Fresh}
		if got := *lastCol.buf.Ring(0).At(1); got != want {
			t.Errorf("last = %+v, want %+v", got, want)
		}
		if got := signCol.buf.Ring(0).At(0); got.Val != 1 || got.State != frame.Fresh {
			t.Errorf("sign rising = %+v, want 1 Fresh", got)
		}
		if got := signCol.buf.Ring(0).At(1); got.Val != -1 || got.State != frame.Fresh {
			t.Errorf("sign falling = %+v, want -1 Fresh", got)
		}
	})

	t.Run("cannot compute", func(t *testing.T) {
		// Three bars readable, so the candle gap at column 4 leaves sum_3 short until column 7.
		buf := frame.NewBuffer[proto.Candle]([]frame.Shape{{Slots: 8, Period: 1, Window: 1, Bars: 3}})
		for col := int64(0); col < 8; col++ {
			if col == 4 {
				buf.Ring(0).Put(col, proto.Candle{}, frame.Gap)
				continue
			}
			buf.Ring(0).Put(col, proto.Candle{Close: closeAt(0, col)}, frame.Fresh)
		}
		var cv frame.View[proto.Candle]
		r := bound[float64](t, sum(3), 1, 0, &cv)
		for col := int64(0); col < 8; col++ {
			cv = at(buf, col)
			r.fire(0, col)
		}

		sum3 := func(col int64) float64 { return closeAt(0, col) + closeAt(0, col-1) + closeAt(0, col-2) }
		for _, want := range []frame.Slot[float64]{
			{Col: 0, ValCol: -1, Val: 0, State: frame.Gap}, // warming up, no value yet
			{Col: 1, ValCol: -1, Val: 0, State: frame.Gap},
			{Col: 2, ValCol: 2, Val: sum3(2), State: frame.Fresh},
			{Col: 3, ValCol: 3, Val: sum3(3), State: frame.Fresh},
			{Col: 4, ValCol: 3, Val: sum3(3), State: frame.Gap}, // short again, carrying the last value
			{Col: 5, ValCol: 3, Val: sum3(3), State: frame.Gap},
			{Col: 6, ValCol: 3, Val: sum3(3), State: frame.Gap},
			{Col: 7, ValCol: 7, Val: sum3(7), State: frame.Fresh},
		} {
			if got := *r.buf.Ring(0).At(want.Col); got != want {
				t.Errorf("slot = %+v, want %+v", got, want)
			}
		}
	})
}

// TestColumnSkip covers the columns the engine does not fire: Gap carries the last value, and Ended
// carries it and ends the ring.
func TestColumnSkip(t *testing.T) {
	buf := candles(1, 2)
	var cv frame.View[proto.Candle]
	r := bound[float64](t, last(), 1, 0, &cv)
	for col := int64(0); col < 2; col++ {
		cv = at(buf, col)
		r.fire(0, col)
	}
	r.skip(0, 2, frame.Gap)
	r.skip(0, 3, frame.Ended)

	for _, want := range []frame.Slot[float64]{
		{Col: 2, ValCol: 1, Val: closeAt(0, 1), State: frame.Gap},
		{Col: 3, ValCol: 1, Val: closeAt(0, 1), State: frame.Ended},
	} {
		if got := *r.buf.Ring(0).At(want.Col); got != want {
			t.Errorf("slot = %+v, want %+v", got, want)
		}
	}

	r.pin(at(buf, 6)) // past the end
	if v, s, age := r.view.Latest(0); v != closeAt(0, 1) || s != frame.Ended || age != 5 {
		t.Errorf("Latest past the end = %v %d %d, want %v Ended 5", v, s, age, closeAt(0, 1))
	}
	want := []float64{closeAt(0, 1), closeAt(0, 0)}
	if got := r.view.Data(0, 4, nil); !slices.Equal(got, want) {
		t.Errorf("Data past the end = %v, want %v", got, want)
	}
}

// TestColumnAllocations checks the hot path: pinning, firing and skipping allocate nothing.
func TestColumnAllocations(t *testing.T) {
	buf := candles(1, 6)
	var cv frame.View[proto.Candle]
	sumCol := bound[float64](t, sum(3), 1, 0, &cv)
	signCol := bound[int](t, sign(), 1, 0, &cv)
	view := at(buf, 5)
	cv = view

	var col column = sumCol
	var ints column = signCol
	allocs := testing.AllocsPerRun(100, func() {
		col.pin(view)
		ints.pin(view)
		col.fire(0, 5)
		ints.fire(0, 5)
		col.skip(0, 6, frame.Gap)
		ints.skip(0, 6, frame.Gap)
	})
	if allocs != 0 {
		t.Errorf("%v allocations per frame, want 0", allocs)
	}
}
