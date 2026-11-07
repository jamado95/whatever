package features

import (
	"slices"
	"testing"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/frame"
)

// run is an engine over the test stream table, its nodes fired by hand until stage 6.
type run struct {
	st        *proto.Streams
	g         *Graph
	e         *Engine
	buf       *frame.Buffer[proto.Candle]
	instances []instance
}

func newRun(t *testing.T, cols int64, instances []instance) *run {
	t.Helper()
	st := testStreams()
	g, err := newGraph(st, instances)
	if err != nil {
		t.Fatal(err)
	}
	return &run{st: st, g: g, e: New(g), buf: streamCandles(st, cols), instances: instances}
}

// pin pins the engine at col.
func (r *run) pin(col int64) View { return r.e.pin(at(r.buf, col)) }

// step pins the engine at col and fires every instance due there, in order.
func (r *run) step(col int64) View {
	v := r.pin(col)
	for _, in := range r.instances {
		if col%ringShape(r.st, in.at, 1).Period == 0 {
			r.g.columns[in.id].fire(r.g.ring(in.at), col)
		}
	}
	return v
}

// mustView is ViewOf, failing the test on an error.
func mustView[T any](t *testing.T, v View, id string) frame.View[T] {
	t.Helper()
	out, err := ViewOf[T](v, id)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestEngineFrame covers one frame end to end on every stream: a dependent reads its dependency's
// result at the same column, consumers read both through ViewOf, and no read sees a later column.
func TestEngineFrame(t *testing.T) {
	all := [][2]int{stream(direct1h), stream(btc5m), stream(eth5m), stream(rolling1h)}
	r := newRun(t, 26, slices.Concat(
		on(last(), all...),
		over(diff(), "diff-last", map[string]string{"last": "last"}, all...),
		on(sign(), all...),
	))
	var v View
	for col := int64(0); col <= 24; col++ {
		v = r.step(col)
	}

	lastAt24 := mustView[float64](t, v, "last")
	diffAt24 := mustView[float64](t, v, "diff-last")
	signAt24 := mustView[int](t, v, "sign")
	bars := []int64{direct1h: 12, btc5m: 1, eth5m: 1, rolling1h: 12} // columns per bar
	for i := range all {
		if x, s, age := lastAt24.Latest(i); x != closeAt(i, 24) || s != frame.Fresh || age != 0 {
			t.Errorf("stream %d: last = %v %d %d, want %v Fresh 0", i, x, s, age, closeAt(i, 24))
		}
		want := closeAt(i, 24) - closeAt(i, 24-bars[i])
		if x, s, _ := diffAt24.Latest(i); x != want || s != frame.Fresh {
			t.Errorf("stream %d: diff = %v %d, want %v Fresh", i, x, s, want)
		}
		if x, s, _ := signAt24.Latest(i); x != 1 || s != frame.Fresh { // even columns rise
			t.Errorf("stream %d: sign = %v %d, want 1 Fresh", i, x, s)
		}
	}

	frame24 := v
	v = r.step(25)
	if x, _, _ := lastAt24.Latest(btc5m); x != closeAt(btc5m, 24) {
		t.Errorf("a view pinned at 24 read %v, a later column", x)
	}
	if x, _, _ := mustView[float64](t, frame24, "last").Latest(btc5m); x != closeAt(btc5m, 24) {
		t.Errorf("ViewOf at 24's frame read %v, a later column", x)
	}
	lastAt25 := mustView[float64](t, v, "last")
	if x, _, _ := lastAt25.Latest(btc5m); x != closeAt(btc5m, 25) {
		t.Errorf("last at 25 = %v, want %v", x, closeAt(btc5m, 25))
	}
	if x, s, age := lastAt25.Latest(direct1h); x != closeAt(direct1h, 24) || s != frame.Fresh || age != 1 {
		t.Errorf("a coarse stream between updates = %v %d %d, want its value at 24, Fresh, age 1", x, s, age)
	}
}

// TestEngineGap covers a dependency's skipped column as its dependents read it: Latest sees the Gap
// and Data skips it.
func TestEngineGap(t *testing.T) {
	lastDep := map[string]string{"last": "last"}
	r := newRun(t, 4, slices.Concat(
		on(last(), stream(btc5m)),
		on(sum(3), stream(eth5m)), // three bars readable
		over(seen(), "seen-last", lastDep, stream(btc5m)),
		over(diff(), "diff-last", lastDep, stream(btc5m)),
	))
	for col := int64(0); col < 3; col++ {
		r.step(col)
	}

	v := r.pin(3)
	r.g.columns["last"].skip(btc5m, 3, frame.Gap)
	r.g.columns["seen-last"].fire(btc5m, 3)
	r.g.columns["diff-last"].fire(btc5m, 3)

	if x, _, _ := mustView[int](t, v, "seen-last").Latest(btc5m); frame.State(x) != frame.Gap {
		t.Errorf("seen = %d, want Gap", x)
	}
	want := closeAt(btc5m, 2) - closeAt(btc5m, 1)
	if x, s, _ := mustView[float64](t, v, "diff-last").Latest(btc5m); x != want || s != frame.Fresh {
		t.Errorf("diff = %v %d, want %v Fresh, skipping the gap", x, s, want)
	}
	x, s, age := mustView[float64](t, v, "last").Latest(btc5m)
	if x != closeAt(btc5m, 2) || s != frame.Gap || age != 1 {
		t.Errorf("last = %v %d %d, want the value at 2, Gap, age 1", x, s, age)
	}
}

// TestEngineHistory covers result history stepping one bar like the candles beneath it, on a direct
// stream coarser than the grid and on a rolling one, with a Gap skipped.
func TestEngineHistory(t *testing.T) {
	r := newRun(t, 37, slices.Concat(
		on(last(), stream(direct1h), stream(rolling1h)),
		on(sum(3), stream(eth5m)), // three bars readable
	))
	for col := int64(0); col <= 36; col++ {
		if col == 24 {
			r.pin(col)
			r.g.columns["last"].skip(direct1h, col, frame.Gap)
			r.g.columns["last"].fire(rolling1h, col)
			continue
		}
		r.step(col)
	}

	view := mustView[float64](t, r.pin(36), "last")
	want := []float64{closeAt(direct1h, 36), closeAt(direct1h, 12)} // the gap at 24 skipped
	if got := view.Data(direct1h, 3, nil); !slices.Equal(got, want) {
		t.Errorf("direct: Data = %v, want %v", got, want)
	}
	want = []float64{closeAt(rolling1h, 36), closeAt(rolling1h, 24), closeAt(rolling1h, 12)}
	if got := view.Data(rolling1h, 3, nil); !slices.Equal(got, want) {
		t.Errorf("rolling: Data = %v, want %v", got, want)
	}
}

// TestEnginePairs covers pair results: one sorted pair table shared by every pair ID, a result per
// pair, read on both ends, stepping at the pair's sampling period with its coarser end's bar.
func TestEnginePairs(t *testing.T) {
	coarse, fine := pair(direct1h, rolling1h), pair(eth5m, rolling1h)
	r := newRun(t, 25, slices.Concat(
		on(last(), stream(direct1h), stream(rolling1h)),
		on(sum(2), stream(btc5m)), // two bars readable
		on(spread(), fine, coarse),
		over(pdiff(), "pdiff-last", map[string]string{"last": "last"}, coarse),
	))
	var v View
	for col := int64(0); col <= 24; col++ {
		v = r.step(col)
	}

	c, ok := v.Pair(direct1h, rolling1h)
	if !ok || c != 0 {
		t.Errorf("Pair(direct, rolling) = %d %t, want 0 true", c, ok)
	}
	f, ok := v.Pair(eth5m, rolling1h)
	if !ok || f != 1 {
		t.Errorf("Pair(eth, rolling) = %d %t, want 1 true", f, ok)
	}
	if rev, ok := v.Pair(rolling1h, eth5m); !ok || rev != f {
		t.Errorf("Pair in the other order = %d %t, want %d true", rev, ok, f)
	}
	if _, ok := v.Pair(btc5m, eth5m); ok {
		t.Errorf("Pair of two streams no instance runs on: found")
	}

	spreadCol := r.g.columns["spread"].(*results[float64])
	want := []frame.Shape{
		{Slots: 2, Period: 12, Window: 1, Bars: 2},  // rolling 1h × direct 1h
		{Slots: 24, Period: 1, Window: 12, Bars: 2}, // rolling 1h × 5m
	}
	if !slices.Equal(spreadCol.shapes, want) {
		t.Errorf("shapes = %+v, want %+v", spreadCol.shapes, want)
	}

	pdiffCol := r.g.columns["pdiff-last"].(*results[float64])
	if pdiffCol.buf.Len() != 2 || pdiffCol.buf.Ring(c).Len() != 2 || pdiffCol.buf.Ring(f).Len() != 0 {
		t.Errorf("pdiff's rings do not follow the shared pair table")
	}

	gap := func(col int64, end int) float64 { return closeAt(end, col) - closeAt(rolling1h, col) }
	spreads := mustView[float64](t, v, "spread")
	if got, want := spreads.Data(c, 2, nil), []float64{gap(24, direct1h), gap(12, direct1h)}; !slices.Equal(got, want) {
		t.Errorf("coarse pair: Data = %v, want %v", got, want)
	}
	if got, want := spreads.Data(f, 2, nil), []float64{gap(24, eth5m), gap(12, eth5m)}; !slices.Equal(got, want) {
		t.Errorf("fine pair: Data = %v, want %v", got, want)
	}
	if x, s, _ := mustView[float64](t, v, "pdiff-last").Latest(c); x != gap(24, direct1h) || s != frame.Fresh {
		t.Errorf("pdiff = %v %d, want %v Fresh", x, s, gap(24, direct1h))
	}

	// Between its due columns the coarse pair carries; the fine one has moved on.
	v = r.step(23)
	spreads = mustView[float64](t, v, "spread")
	if x, _, age := spreads.Latest(c); x != gap(12, direct1h) || age != 11 {
		t.Errorf("coarse pair between updates = %v age %d, want its value at 12, age 11", x, age)
	}
	if x, _, age := spreads.Latest(f); x != gap(23, eth5m) || age != 0 {
		t.Errorf("fine pair = %v age %d, want its value at 23, age 0", x, age)
	}
}

// TestViewOf covers the consumers' view: the data view embedded, and the errors of ViewOf.
func TestViewOf(t *testing.T) {
	r := newRun(t, 2, slices.Concat(on(last(), stream(btc5m)), on(sign(), stream(btc5m))))
	v := r.step(1)

	if v.Col() != 1 || v.Boundary() != testStride {
		t.Errorf("frame = col %d, boundary %d", v.Col(), v.Boundary())
	}
	if c, s, _ := v.Latest(btc5m); c.Close != closeAt(btc5m, 1) || s != frame.Fresh {
		t.Errorf("candle = %+v %d, want the close at 1, Fresh", c, s)
	}

	for _, c := range []struct {
		err  error
		want string
	}{
		{viewErr[int](v, "last"), `features: result "last" is not int`},
		{viewErr[float64](v, "sign"), `features: result "sign" is not float64`},
		{viewErr[float64](v, "nope"), `features: unknown result "nope"`},
	} {
		if c.err == nil || c.err.Error() != c.want {
			t.Errorf("err = %v, want %q", c.err, c.want)
		}
	}
}

func viewErr[T any](v View, id string) error {
	_, err := ViewOf[T](v, id)
	return err
}

// TestEngineAllocations checks a frame's hot path: pinning, firing with input reads into scratch, and
// consumer reads through ViewOf allocate nothing.
func TestEngineAllocations(t *testing.T) {
	coords := [][2]int{stream(btc5m), stream(eth5m)}
	r := newRun(t, 8, slices.Concat(
		on(last(), coords...),
		on(sum(3), coords...),
		on(sign(), coords...),
		over(diff(), "diff-last", map[string]string{"last": "last"}, coords...),
		on(spread(), pair(btc5m, eth5m)),
	))
	for col := int64(0); col < 7; col++ {
		r.step(col)
	}

	data := at(r.buf, 7)
	type firing struct {
		c    column
		ring int
	}
	var fires []firing
	for _, in := range r.instances {
		fires = append(fires, firing{r.g.columns[in.id], r.g.ring(in.at)})
	}
	dst := make([]float64, 0, 3)
	allocs := testing.AllocsPerRun(100, func() {
		v := r.e.pin(data)
		for _, f := range fires {
			f.c.fire(f.ring, 7)
		}
		view, _ := ViewOf[float64](v, "last")
		view.Latest(btc5m)
		dst = view.Data(btc5m, 3, dst)
		v.Pair(btc5m, eth5m)
	})
	if allocs != 0 {
		t.Errorf("%v allocations per frame, want 0", allocs)
	}
}
