package featv2

import (
	"strings"
	"testing"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
	reg "whatever/internal/registry"
)

func create(t *testing.T, typ string, opts map[string]any) feature.Feature {
	t.Helper()
	f, err := reg.FeatureTypes.Create(typ, opts)
	if err != nil {
		t.Fatalf("create %s %v: %v", typ, opts, err)
	}
	return f
}

// TestFeatures covers each type's ID, lookback and shape: one stream, no dependencies, and a Bind of
// its value type.
func TestFeatures(t *testing.T) {
	for _, tc := range []struct {
		typ      string
		opts     map[string]any
		id       string
		lookback int
		isInt    bool
	}{
		{SMAID, map[string]any{"period": 20}, "sma_20", 20, false},
		{EMAID, map[string]any{"period": 20}, "ema_20", 40, false},
		{EMAID, map[string]any{"period": 20, "smoothing": 2.0}, "ema_20", 40, false},
		{EMAID, map[string]any{"period": 20, "smoothing": 3.0}, "ema_20_s3", 40, false},
		{EMAID, map[string]any{"period": 20, "smoothing": 2.5}, "ema_20_s2.5", 40, false},
		{EMAID, map[string]any{"period": 20, "smoothing": 0.00001}, "ema_20_s0.00001", 40, false},
		{RSIID, map[string]any{"period": 14}, "rsi_14", 15, false},
		{VWAPID, map[string]any{"period": 50}, "vwap_50", 50, false},
		{EngulfingID, nil, "engulfing_candle", 2, true},
	} {
		f := create(t, tc.typ, tc.opts)
		if f.ID != tc.id || f.Lookback != tc.lookback {
			t.Errorf("%s %v: ID, Lookback = %q, %d, want %q, %d", tc.typ, tc.opts, f.ID, f.Lookback, tc.id, tc.lookback)
		}
		if strings.Contains(f.ID, "-") {
			t.Errorf("%s: ID contains '-'", f.ID)
		}
		if f.Pair || f.Deps != nil {
			t.Errorf("%s: Pair, Deps = %v, %v, want false, none", f.ID, f.Pair, f.Deps)
		}
		_, isFloat := f.Bind.(feature.Bind[float64])
		_, isInt := f.Bind.(feature.Bind[int])
		if isFloat == tc.isInt || isInt != tc.isInt {
			t.Errorf("%s: Bind is %T", f.ID, f.Bind)
		}
	}
}

// TestConfigErrors covers the options each type rejects.
func TestConfigErrors(t *testing.T) {
	type row struct {
		typ  string
		opts map[string]any
	}
	var rows []row
	for _, typ := range []string{SMAID, EMAID, RSIID, VWAPID} {
		rows = append(rows,
			row{typ, nil},
			row{typ, map[string]any{"period": 0}},
			row{typ, map[string]any{"period": -1}},
			row{typ, map[string]any{"period": 20, "unknown": 1}},
		)
	}
	rows = append(rows,
		row{EMAID, map[string]any{"period": 20, "smoothing": 0}},
		row{EMAID, map[string]any{"period": 20, "smoothing": -1}},
		row{EngulfingID, map[string]any{"period": 2}},
	)
	for _, r := range rows {
		if f, err := reg.FeatureTypes.Create(r.typ, r.opts); err == nil {
			t.Errorf("%s %v: built %s, want an error", r.typ, r.opts, f.ID)
		}
	}
}

// TestNodeAllocations verifies each type's node computes a value from its stream's candle copy
// without allocating.
func TestNodeAllocations(t *testing.T) {
	const cols = 8
	buf := frame.NewBuffer[proto.Candle]([]frame.Shape{{Slots: cols, Period: 1, Window: 1, Bars: cols}})
	for col := range int64(cols) {
		open := 100 + float64(col)
		closePrice := open + float64(col%3) - 1
		buf.Ring(0).Put(col, proto.Candle{
			Open: open, High: open + 2, Low: open - 2, Close: closePrice, Volume: 10 + float64(col),
		}, frame.Fresh)
	}
	cv := frame.NewView(cols-1, 300_000, frame.Current, true, buf)
	history := [][]proto.Candle{cv.Data(0, cols, nil)} // the engine's copy, as deep as the stream's reads

	for _, f := range []feature.Feature{
		create(t, SMAID, map[string]any{"period": 3}),
		create(t, EMAID, map[string]any{"period": 3}),
		create(t, RSIID, map[string]any{"period": 3}),
		create(t, VWAPID, map[string]any{"period": 3}),
		create(t, EngulfingID, nil),
	} {
		b := feature.NewBinder([2]int{0, -1}, &cv, history, nil)
		var node func() bool
		switch bind := f.Bind.(type) {
		case feature.Bind[float64]:
			n := bind(b)
			node = func() bool { _, ok := n(); return ok }
		case feature.Bind[int]:
			n := bind(b)
			node = func() bool { _, ok := n(); return ok }
		}
		if err := b.Err(); err != nil {
			t.Fatalf("%s: Bind: %v", f.ID, err)
		}
		if !node() {
			t.Errorf("%s: no value over %d candles", f.ID, cols)
		}
		if allocs := testing.AllocsPerRun(100, func() { node() }); allocs != 0 {
			t.Errorf("%s: %v allocations per call, want 0", f.ID, allocs)
		}
	}
}
