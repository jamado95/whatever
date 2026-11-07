package features

import (
	"slices"
	"strings"
	"testing"

	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
)

// pair is the coordinate of the pair (a, b).
func pair(a, b int) [2]int { return [2]int{a, b} }

// TestGraphNoRings covers the split between graph and engine: newGraph binds every node and allocates
// no ring, and New allocates them, the nodes then reading and writing without being bound again.
func TestGraphNoRings(t *testing.T) {
	st := testStreams()
	g, err := newGraph(st, slices.Concat(
		over(diff(), "diff-last", map[string]string{"last": "last"}, stream(btc5m)),
		on(last(), stream(btc5m)),
	))
	if err != nil {
		t.Fatal(err)
	}

	lastCol := g.columns["last"].(*results[float64])
	diffCol := g.columns["diff-last"].(*results[float64])
	if lastCol.buf != nil || diffCol.buf != nil {
		t.Fatalf("newGraph allocated a buffer")
	}
	if lastCol.nodes[btc5m] == nil || diffCol.nodes[btc5m] == nil {
		t.Errorf("a node on btc5m is not bound")
	}
	if lastCol.nodes[eth5m] != nil {
		t.Errorf("a node is bound on a stream the ID does not run on")
	}

	e := New(g)
	buf := streamCandles(st, 3)
	for col := int64(0); col < 3; col++ {
		e.pin(at(buf, col))
		lastCol.fire(btc5m, col)
		diffCol.fire(btc5m, col)
	}
	want := closeAt(btc5m, 2) - closeAt(btc5m, 1)
	if v, s, _ := diffCol.view.Latest(btc5m); v != want || s != frame.Fresh {
		t.Errorf("diff = %v %d, want %v Fresh", v, s, want)
	}
}

// TestGraphShapes covers ring sizes: a stream's ring holds the longest Lookback in bars at its period
// and window, and is empty where the ID does not run.
func TestGraphShapes(t *testing.T) {
	st := testStreams()

	t.Run("the longest lookback, at each stream's period and window", func(t *testing.T) {
		g, err := newGraph(st, slices.Concat(
			on(last(), stream(direct1h), stream(btc5m), stream(rolling1h)),
			on(sum(5), stream(eth5m)),
		))
		if err != nil {
			t.Fatal(err)
		}
		lastCol := g.columns["last"].(*results[float64])
		want := []frame.Shape{
			direct1h:  {Slots: 5, Period: 12, Window: 1, Bars: 5},
			btc5m:     {Slots: 5, Period: 1, Window: 1, Bars: 5},
			eth5m:     {}, // last does not run here
			rolling1h: {Slots: 60, Period: 1, Window: 12, Bars: 5},
		}
		if !slices.Equal(lastCol.shapes, want) {
			t.Errorf("shapes = %+v, want %+v", lastCol.shapes, want)
		}

		New(g)
		if n := lastCol.buf.Ring(eth5m).Len(); n != 0 {
			t.Errorf("ring where the ID does not run has %d slots", n)
		}
		if n := lastCol.buf.Ring(rolling1h).Len(); n != 60 {
			t.Errorf("rolling ring has %d slots, want 60", n)
		}
	})

	t.Run("one bar without lookback", func(t *testing.T) {
		f := last()
		f.Lookback = 0
		g, err := newGraph(st, on(f, stream(btc5m)))
		if err != nil {
			t.Fatal(err)
		}
		want := frame.Shape{Slots: 1, Period: 1, Window: 1, Bars: 1}
		if got := g.columns["last"].(*results[float64]).shapes[btc5m]; got != want {
			t.Errorf("shape = %+v, want %+v", got, want)
		}
	})
}

// TestGraphBindErrors covers what binding finds, all of it reported by one call: a value type with no
// case, a dependency read as the wrong type or undeclared, and a Bind at odds with its feature's Pair.
func TestGraphBindErrors(t *testing.T) {
	wrongPair := feature.Feature{ID: "wrongpair", Lookback: 1,
		Bind: func(b *feature.Binder) func() (float64, bool) {
			feature.PairCandles(b)
			return nil
		}}
	wrongSingle := feature.Feature{ID: "wrongsingle", Lookback: 1, Pair: true,
		Bind: func(b *feature.Binder) func() (float64, bool) {
			feature.Candles(b)
			return nil
		}}

	_, err := newGraph(testStreams(), slices.Concat(
		on(last(), stream(btc5m)),
		on(sign(), stream(btc5m)),
		over(diff(), "diff-sign", map[string]string{"last": "sign"}, stream(btc5m)),
		over(diff(), "diff-none", nil, stream(btc5m)),
		on(wrongPair, stream(btc5m)),
		on(wrongSingle, pair(btc5m, eth5m)),
		on(label(), stream(btc5m)),
	))
	if err == nil {
		t.Fatal("no error")
	}

	want := []string{
		"features: label: unsupported Bind func(*feature.Binder) func() (string, bool)",
		`features: diff-sign on BTCUSDT:5m:btc_5m: feature: dependency "last" is not float64`,
		`features: diff-none on BTCUSDT:5m:btc_5m: feature: no dependency of type "last"`,
		"features: wrongpair on BTCUSDT:5m:btc_5m: feature: PairCandles on a single-stream node",
		"features: wrongsingle on BTCUSDT:5m:btc_5m, ETHUSDT:5m:eth_5m: feature: Candles on a pair node",
	}
	if got := strings.Split(err.Error(), "\n"); !slices.Equal(got, want) {
		t.Errorf("errors:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestGraphErrors covers what newGraph checks before binding, each error naming the composed ID and
// coordinate.
func TestGraphErrors(t *testing.T) {
	const (
		btcID     = "BTCUSDT:5m:btc_5m"
		ethID     = "ETHUSDT:5m:eth_5m"
		directID  = "BTCUSDT:1h:btc_1h"
		rollingID = "BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m"
	)
	lastDep := map[string]string{"last": "last"}

	for _, c := range []struct {
		name      string
		instances []instance
		want      string
	}{
		{"an ID twice on one coordinate", on(last(), stream(btc5m), stream(btc5m)),
			"features: last on " + btcID + ": runs twice on one coordinate"},
		{"a dependency not running at the stream", slices.Concat(
			on(last(), stream(eth5m)),
			over(diff(), "diff-last", lastDep, stream(btc5m))),
			`features: diff-last on ` + btcID + `: dependency "last": last does not run on ` + btcID},
		{"a dependency not running on one end", slices.Concat(
			on(last(), stream(direct1h)),
			over(pdiff(), "pdiff-last", lastDep, pair(direct1h, rolling1h))),
			`features: pdiff-last on ` + directID + `, ` + rollingID +
				`: dependency "last": last does not run on ` + rollingID},
		{"a dependency on a pair result", slices.Concat(
			on(spread(), pair(btc5m, eth5m)),
			over(diff(), "diff-spread", map[string]string{"last": "spread"}, stream(btc5m))),
			`features: diff-spread on ` + btcID + `: dependency "last": spread does not run on ` + btcID},
		{"two variants under one ID", slices.Concat(
			over(last(), "x", nil, stream(btc5m)),
			over(sum(2), "x", nil, stream(eth5m))),
			`features: x on ` + ethID + `: variants "last" and "sum_2" under one ID`},
		{"one ID as a stream and a pair feature", slices.Concat(
			over(last(), "x", nil, stream(btc5m)),
			over(feature.Feature{ID: "last", Pair: true, Bind: spread().Bind}, "x", nil, pair(btc5m, eth5m))),
			"features: x on " + btcID + ", " + ethID + `: variants "last" and "last" under one ID`},
		{"a stream outside the table", on(last(), stream(9)),
			"features: last on stream 9: no such stream"},
		{"a pair's ends equal", on(spread(), pair(btc5m, btc5m)),
			"features: spread on " + btcID + ", " + btcID +
				": a pair's ends must be two streams of the table, lower first"},
		{"a pair's ends reversed", on(spread(), pair(eth5m, btc5m)),
			"features: spread on " + ethID + ", " + btcID +
				": a pair's ends must be two streams of the table, lower first"},
		{"a pair's end outside the table", on(spread(), pair(btc5m, 9)),
			"features: spread on " + btcID + ", stream 9" +
				": a pair's ends must be two streams of the table, lower first"},
		{"a pair feature on one stream", on(spread(), stream(btc5m)),
			"features: spread on " + btcID + ": a pair feature on one stream"},
		{"a single-stream feature on a pair", on(last(), pair(btc5m, eth5m)),
			"features: last on " + btcID + ", " + ethID + ": a single-stream feature on a pair"},
		{"a dependent of an unsupported value type reports only its cause", slices.Concat(
			on(label(), stream(btc5m)),
			over(diff(), "diff-label", map[string]string{"last": "label"}, stream(btc5m))),
			"features: label: unsupported Bind func(*feature.Binder) func() (string, bool)"},
	} {
		_, err := newGraph(testStreams(), c.instances)
		if err == nil || err.Error() != c.want {
			t.Errorf("%s:\n got %v\nwant %s", c.name, err, c.want)
		}
	}
}
