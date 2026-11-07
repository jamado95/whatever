package features

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
)

// instance is a feature at one coordinate: its composed ID, and the composed ID feeding each of its
// dependencies there — on each end, for a pair.
type instance struct {
	feature feature.Feature
	id      string
	at      [2]int            // a stream, or a pair a < b; the second is -1 on a stream
	deps    map[string]string // dependency type → composed ID
}

/*
Graph is a validated feature graph: one column per composed ID with every node bound, and no ring
allocated. The engine's factory builds it, so --validate-only covers it. It runs once: its nodes hold
the addresses of its views.
*/
type Graph struct {
	columns map[string]column        // by composed ID
	all     []column                 // pinned each frame
	pairs   map[[2]int]int           // the pair table: (a, b), a < b → ring
	candles frame.View[proto.Candle] // the candles nodes read, re-pinned each frame
}

/*
newGraph builds the pair table from the pair instances, sizes one column per composed ID and binds
every instance. Rings hold L bars, L the longest Lookback, at least 1: a stream's at its Period and
Window, a pair's at max(Pa, Pb) with its coarser end's bar. Every problem is reported, joined.
*/
func newGraph(streams *proto.Streams, instances []instance) (*Graph, error) {
	fit, errs := check(streams, instances)

	g := &Graph{columns: make(map[string]column), pairs: pairTable(fit)}
	shapes := g.shapes(streams, fit)
	for _, in := range fit {
		sh, ok := shapes[in.id]
		if !ok {
			continue // built, or reported
		}
		delete(shapes, in.id)

		c, err := newColumn(in.feature, sh)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		g.columns[in.id] = c
		g.all = append(g.all, c)
	}

	for _, in := range fit {
		c, ok := g.columns[in.id]
		if !ok {
			continue
		}
		deps, ok := g.cells(in)
		if !ok {
			continue // a dependency's column failed, already reported
		}
		b := feature.NewBinder(in.at, &g.candles, deps)
		if err := c.bind(b, g.ring(in.at)); err != nil {
			errs = append(errs, in.wrap(streams, err))
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return g, nil
}

// wrap names the instance's composed ID and coordinate on err.
func (in instance) wrap(st *proto.Streams, err error) error {
	return fmt.Errorf("features: %s on %s: %w", in.id, coordinate(st, in.at), err)
}

// coordinate names a stream or a pair by stream ID, and an index outside the table as such.
func coordinate(st *proto.Streams, at [2]int) string {
	name := func(i int) string {
		if i < 0 || i >= len(st.All) {
			return fmt.Sprintf("stream %d", i)
		}
		return st.All[i].Sub.ID()
	}
	if at[1] < 0 {
		return name(at[0])
	}
	return name(at[0]) + ", " + name(at[1])
}

/*
check reports a coordinate outside the table or at odds with its feature's Pair, an ID twice on one
coordinate, two variants under one ID, and a dependency that does not run at the instance's stream, or
on both ends of its pair. It returns the instances that can be sized and bound, so the rest of the
graph is still checked.
*/
func check(st *proto.Streams, instances []instance) ([]instance, []error) {
	type placed struct {
		id string
		at [2]int
	}
	var errs []error
	variants := make(map[string]feature.Feature)
	running := make(map[placed]bool, len(instances))
	fit := make([]instance, 0, len(instances))
	for _, in := range instances {
		if err := placement(st, in); err != nil {
			errs = append(errs, in.wrap(st, err))
			continue
		}
		if running[placed{in.id, in.at}] {
			errs = append(errs, in.wrap(st, errors.New("runs twice on one coordinate")))
			continue
		}
		v, ok := variants[in.id]
		if ok && (v.ID != in.feature.ID || v.Pair != in.feature.Pair) {
			err := fmt.Errorf("variants %q and %q under one ID", v.ID, in.feature.ID)
			errs = append(errs, in.wrap(st, err))
			continue
		}
		variants[in.id] = in.feature
		running[placed{in.id, in.at}] = true
		fit = append(fit, in)
	}

	for _, in := range fit {
		ends := []int{in.at[0]}
		if in.at[1] >= 0 {
			ends = append(ends, in.at[1])
		}
		for _, typ := range slices.Sorted(maps.Keys(in.deps)) {
			for _, end := range ends {
				if !running[placed{in.deps[typ], [2]int{end, -1}}] {
					err := fmt.Errorf("dependency %q: %s does not run on %s",
						typ, in.deps[typ], coordinate(st, [2]int{end, -1}))
					errs = append(errs, in.wrap(st, err))
				}
			}
		}
	}
	return fit, errs
}

// placement checks an instance's coordinate against the stream table and its feature's Pair.
func placement(st *proto.Streams, in instance) error {
	a, b := in.at[0], in.at[1]
	if b < 0 {
		if a < 0 || a >= len(st.All) {
			return errors.New("no such stream")
		}
		if in.feature.Pair {
			return errors.New("a pair feature on one stream")
		}
		return nil
	}
	if a < 0 || a >= b || b >= len(st.All) {
		return errors.New("a pair's ends must be two streams of the table, lower first")
	}
	if !in.feature.Pair {
		return errors.New("a single-stream feature on a pair")
	}
	return nil
}

// pairTable numbers every pair a pair instance runs on, sorted.
func pairTable(instances []instance) map[[2]int]int {
	set := make(map[[2]int]bool)
	for _, in := range instances {
		if in.at[1] >= 0 {
			set[in.at] = true
		}
	}
	sorted := slices.SortedFunc(maps.Keys(set), func(a, b [2]int) int {
		return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
	})

	pairs := make(map[[2]int]int, len(sorted))
	for i, p := range sorted {
		pairs[p] = i
	}
	return pairs
}

// shapes sizes every composed ID's rings: one per stream of the table, or per pair of the pair table,
// empty where the ID does not run.
func (g *Graph) shapes(st *proto.Streams, instances []instance) map[string][]frame.Shape {
	lookback := 1
	for _, in := range instances {
		lookback = max(lookback, in.feature.Lookback)
	}

	shapes := make(map[string][]frame.Shape)
	for _, in := range instances {
		sh, ok := shapes[in.id]
		if !ok {
			n := len(st.All)
			if in.feature.Pair {
				n = len(g.pairs)
			}
			sh = make([]frame.Shape, n)
			shapes[in.id] = sh
		}
		sh[g.ring(in.at)] = ringShape(st, in.at, int64(lookback))
	}
	return shapes
}

/*
ringShape sizes the ring at a coordinate to hold bars bars. A stream's steps at its Period and Window.
A pair's steps where both ends are due, max(Pa, Pb) on the timeframes' divisibility chain, with its
coarser end's bar.
*/
func ringShape(st *proto.Streams, at [2]int, bars int64) frame.Shape {
	a := st.All[at[0]]
	period, window := a.Period, a.Window
	if at[1] >= 0 {
		b := st.All[at[1]]
		period = max(a.Period, b.Period)
		window = max(a.Period*a.Window, b.Period*b.Window) / period
	}
	return frame.Shape{Slots: bars * window, Period: period, Window: window, Bars: bars}
}

// ring returns the ring of a coordinate: a stream's index, or its pair's in the pair table.
func (g *Graph) ring(at [2]int) int {
	if at[1] < 0 {
		return at[0]
	}
	return g.pairs[at]
}

// cells returns the views an instance's dependencies are read through, or false when one has no
// column.
func (g *Graph) cells(in instance) (map[string]any, bool) {
	deps := make(map[string]any, len(in.deps))
	for typ, id := range in.deps {
		c, ok := g.columns[id]
		if !ok {
			return nil, false
		}
		deps[typ] = c.cell()
	}
	return deps, true
}
