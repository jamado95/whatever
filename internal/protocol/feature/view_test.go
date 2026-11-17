package feature

import (
	"slices"
	"testing"

	"whatever/internal/protocol/frame"
)

/*
TestFeaturesView covers the consumer read: ViewOf pins a result at its view's frame, so views sharing
one index each read their own, and rejects an unknown ID or another value type; Pair finds a pair in
either order. Neither allocates.
*/
func TestFeaturesView(t *testing.T) {
	candles, results := buffers()
	cv := frame.NewView(3, 300_000, frame.Current, true, candles)
	index, pairs := map[string]any{"x": results}, map[[2]int]int{{0, 2}: 0}
	fv := NewFeaturesView(cv, index, pairs)

	if fv.Col() != 3 {
		t.Errorf("col = %d, want the data view's 3", fv.Col())
	}
	got, err := ViewOf[float64](fv, "x")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if v, s, _ := got.Latest(i); v != val(i, 3) || s != frame.Fresh {
			t.Errorf("stream %d latest = %v %d, want %v Fresh", i, v, s, val(i, 3))
		}
		if d, want := got.Data(i, 4, nil), frame.Pin(cv, results).Data(i, 4, nil); !slices.Equal(d, want) {
			t.Errorf("stream %d data = %v, want %v", i, d, want)
		}
	}

	earlier := NewFeaturesView(frame.NewView(2, 300_000, frame.Current, true, candles), index, pairs)
	if got, _ := ViewOf[float64](earlier, "x"); got.Col() != 2 {
		t.Errorf("an earlier frame's col = %d, want 2", got.Col())
	}
	if got, _ := ViewOf[float64](fv, "x"); got.Col() != 3 {
		t.Errorf("col = %d after an earlier frame's read, want 3", got.Col())
	}

	for _, c := range []struct {
		err  error
		want string
	}{
		{viewErr[int](fv, "x"), `feature: result "x" is not int`},
		{viewErr[float64](fv, "y"), `feature: unknown result "y"`},
	} {
		if c.err == nil || c.err.Error() != c.want {
			t.Errorf("err = %v, want %q", c.err, c.want)
		}
	}

	for _, p := range [][2]int{{0, 2}, {2, 0}} {
		if i, ok := fv.Pair(p[0], p[1]); !ok || i != 0 {
			t.Errorf("Pair%v = %d, %v; want 0, true", p, i, ok)
		}
	}
	if _, ok := fv.Pair(0, 1); ok {
		t.Error("Pair(0, 1) found, want none")
	}

	allocs := testing.AllocsPerRun(100, func() {
		ViewOf[float64](fv, "x")
		fv.Pair(2, 0)
	})
	if allocs != 0 {
		t.Errorf("%v allocations per read, want 0", allocs)
	}
}

func viewErr[T any](v FeaturesView, id string) error {
	_, err := ViewOf[T](v, id)
	return err
}
