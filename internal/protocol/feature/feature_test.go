package feature

import (
	"slices"
	"testing"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/frame"
)

// val is the test value of stream i at col: its close, and its result.
func val(i int, col int64) float64 { return float64(10*i) + float64(col) }

func closeOf(c proto.Candle) float64 { return c.Close }

func self(x float64) float64 { return x }

// buffers returns a candle buffer and a float64 result buffer over three streams, Fresh at columns 0
// to 3.
func buffers() (*frame.Buffer[proto.Candle], *frame.Buffer[float64]) {
	shapes := slices.Repeat([]frame.Shape{{Slots: 8, Period: 1, Window: 1, Bars: 8}}, 3)
	candles, results := frame.NewBuffer[proto.Candle](shapes), frame.NewBuffer[float64](shapes)
	for i := range shapes {
		for col := int64(0); col <= 3; col++ {
			candles.Ring(i).Put(col, proto.Candle{Close: val(i, col)}, frame.Fresh)
			results.Ring(i).Put(col, val(i, col), frame.Fresh)
		}
	}
	return candles, results
}

// reads fails unless in reads stream i at col: its latest value Fresh there, and its history from
// col down to 0.
func reads[T any](t *testing.T, name string, in Input[T], value func(T) float64, i int, col int64) {
	t.Helper()
	if x, s, age := in.Latest(); value(x) != val(i, col) || s != frame.Fresh || age != 0 {
		t.Errorf("%s at %d: Latest = %v %d %d, want %v Fresh 0", name, col, value(x), s, age, val(i, col))
	}
	var got, want []float64
	for _, x := range in.Data(10, nil) {
		got = append(got, value(x))
	}
	for c := col; c >= 0; c-- {
		want = append(want, val(i, c))
	}
	if !slices.Equal(got, want) {
		t.Errorf("%s at %d: Data = %v, want %v", name, col, got, want)
	}
}

// lasts fails unless in.Last reads stream i's candle copy at col: its first n candles, newest first,
// and nothing for n of zero.
func lasts(t *testing.T, name string, in Input[proto.Candle], i int, col int64) {
	t.Helper()
	for _, n := range []int{2, 10} {
		var got, want []float64
		for _, c := range in.Last(n) {
			got = append(got, c.Close)
		}
		for c := col; c >= 0 && len(want) < n; c-- {
			want = append(want, val(i, c))
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s at %d: Last(%d) = %v, want %v", name, col, n, got, want)
		}
	}
	if got := in.Last(0); got != nil {
		t.Errorf("%s at %d: Last(0) = %v, want nil", name, col, got)
	}
}

/*
TestBindInputs covers the inputs a binder hands out: taken before any frame exists, each reads its
own ring at the frame its view is pinned at, following the view as the engine re-pins it; candle
inputs also read their stream's copy as the engine refills it, and dependency inputs have none.
*/
func TestBindInputs(t *testing.T) {
	candles, results := buffers()
	var cv frame.View[proto.Candle]
	var rv frame.View[float64]
	deps := map[string]any{"rsi": &rv}
	history := make([][]proto.Candle, 3)

	single := NewBinder([2]int{1, -1}, &cv, history, deps)
	c := Candles(single)
	d := Dep[float64](single, "rsi")

	pair := NewBinder([2]int{0, 2}, &cv, history, deps)
	ca, cb := PairCandles(pair)
	da, db := PairDep[float64](pair, "rsi")

	for _, b := range []*Binder{single, pair} {
		if err := b.Err(); err != nil {
			t.Fatalf("Err = %v, want nil", err)
		}
	}

	for _, col := range []int64{2, 3} {
		cv = frame.NewView(col, 300_000, frame.Current, true, candles)
		rv = frame.Pin(cv, results)
		for i := range history { // the engine's refill
			history[i] = cv.Data(i, 10, history[i])
		}
		reads(t, "Candles", c, closeOf, 1, col)
		reads(t, "Dep", d, self, 1, col)
		reads(t, "PairCandles first", ca, closeOf, 0, col)
		reads(t, "PairCandles second", cb, closeOf, 2, col)
		reads(t, "PairDep first", da, self, 0, col)
		reads(t, "PairDep second", db, self, 2, col)
		lasts(t, "Candles", c, 1, col)
		lasts(t, "PairCandles first", ca, 0, col)
		lasts(t, "PairCandles second", cb, 2, col)
		if d.Last(10) != nil || da.Last(10) != nil || db.Last(10) != nil {
			t.Errorf("at %d: Last on a dependency input is not nil", col)
		}
	}
}

// TestBindErrors covers what a binder records: the wrong arity's function, an undeclared dependency,
// and a dependency read as the wrong value type.
func TestBindErrors(t *testing.T) {
	var cv frame.View[proto.Candle]
	var rv frame.View[float64]
	deps := map[string]any{"rsi": &rv}
	single, pair := [2]int{1, -1}, [2]int{0, 2}

	for _, c := range []struct {
		name string
		at   [2]int
		bind func(b *Binder)
		want string
	}{
		{"Candles on a pair node", pair,
			func(b *Binder) { Candles(b) },
			"feature: Candles on a pair node"},
		{"Dep on a pair node", pair,
			func(b *Binder) { Dep[float64](b, "rsi") },
			`feature: Dep "rsi" on a pair node`},
		{"PairCandles on a single-stream node", single,
			func(b *Binder) { PairCandles(b) },
			"feature: PairCandles on a single-stream node"},
		{"PairDep on a single-stream node", single,
			func(b *Binder) { PairDep[float64](b, "rsi") },
			`feature: PairDep "rsi" on a single-stream node`},
		{"an undeclared dependency", single,
			func(b *Binder) { Dep[float64](b, "ema") },
			`feature: no dependency of type "ema"`},
		{"an undeclared pair dependency", pair,
			func(b *Binder) { PairDep[float64](b, "ema") },
			`feature: no dependency of type "ema"`},
		{"a dependency read as the wrong type", single,
			func(b *Binder) { Dep[int](b, "rsi") },
			`feature: dependency "rsi" is not int`},
		{"a pair dependency read as the wrong type", pair,
			func(b *Binder) { PairDep[int](b, "rsi") },
			`feature: dependency "rsi" is not int`},
		{"the first error is kept", single,
			func(b *Binder) {
				Dep[float64](b, "ema")
				PairCandles(b)
				Dep[int](b, "rsi")
			},
			`feature: no dependency of type "ema"`},
		{"an arity error comes before the dependency's", pair,
			func(b *Binder) { Dep[float64](b, "ema") },
			`feature: Dep "ema" on a pair node`},
	} {
		b := NewBinder(c.at, &cv, nil, deps)
		c.bind(b)
		if err := b.Err(); err == nil || err.Error() != c.want {
			t.Errorf("%s: Err = %v, want %q", c.name, err, c.want)
		}
	}
}

// TestValidate covers the checks on one variant: an empty ID, a '-' in it, and an empty or repeated
// dependency type; a valid variant passes, with and without Deps.
func TestValidate(t *testing.T) {
	for _, c := range []struct {
		name string
		f    Feature
		want string // "" when valid
	}{
		{"valid, without Deps", Feature{ID: "rsi_14"}, ""},
		{"valid, with Deps", Feature{ID: "zscore_50", Deps: []string{"rsi", "vwap"}}, ""},
		{"an empty ID", Feature{}, "feature: empty ID"},
		{"a '-' in the ID", Feature{ID: "x-1"}, `feature: ID "x-1" contains '-'`},
		{"an empty dependency type", Feature{ID: "x_1", Deps: []string{"rsi", ""}},
			"feature: x_1: empty dependency type"},
		{"a dependency type twice", Feature{ID: "x_1", Deps: []string{"rsi", "vwap", "rsi"}},
			`feature: x_1: dependency type "rsi" listed twice`},
	} {
		err := c.f.Validate()
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.want != "" && (err == nil || err.Error() != c.want):
			t.Errorf("%s: Validate = %v, want %q", c.name, err, c.want)
		}
	}
}

// TestInputAllocations checks the hot path: with dst capacity, input reads allocate nothing, nor do
// reads of a candle copy.
func TestInputAllocations(t *testing.T) {
	candles, results := buffers()
	cv := frame.NewView(3, 300_000, frame.Current, true, candles)
	rv := frame.Pin(cv, results)
	history := [][]proto.Candle{nil, cv.Data(1, 4, nil), nil}
	b := NewBinder([2]int{1, -1}, &cv, history, map[string]any{"rsi": &rv})
	c := Candles(b)
	d := Dep[float64](b, "rsi")

	cdst := make([]proto.Candle, 0, 4)
	ddst := make([]float64, 0, 4)
	allocs := testing.AllocsPerRun(100, func() {
		cdst = c.Data(4, cdst)
		c.Last(4)
		c.Latest()
		ddst = d.Data(4, ddst)
		d.Latest()
	})
	if allocs != 0 {
		t.Errorf("%v allocations per read, want 0", allocs)
	}
}
