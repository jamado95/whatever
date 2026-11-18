package features

import (
	"cmp"
	"slices"

	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
)

// Engine is one run of a Graph: its schedules and each frame's firing state.
type Engine struct {
	g         *Graph
	results   map[string]any // by composed ID: its column's buffer, which each FeaturesView pins
	periods   []int64        // the nodes' periods, ascending: a divisibility chain
	schedules [][]int        // by pattern: the nodes due, in serial order
	legs      []frame.State  // by stream, at the frame: Fresh when updated there, Ended, or Gap
	fresh     []bool         // by node: fired with a value at its last due column
	ended     []bool         // by node: Ended written
}

/*
New warns on log when the feature buffers would exceed memoryLimit, then allocates every column's
buffer and each read stream's candle copy, indexes the columns' buffers for consumers, and builds the
schedules: the nodes' periods, and for each due pattern the nodes due there, in serial order.
*/
func New(g *Graph, log *logger.Logger) *Engine {
	if msg, ok := memoryWarning(g); ok {
		log.Warn(msg)
	}

	for _, c := range g.all {
		c.alloc()
	}
	for s, d := range g.depth {
		if d > 0 {
			g.history[s] = make([]proto.Candle, 0, d)
		}
	}

	results := make(map[string]any, len(g.columns))
	for id, c := range g.columns {
		results[id] = c.buffer()
	}

	periods, scheds := schedules(g, order(g))
	return &Engine{
		g:         g,
		results:   results,
		periods:   periods,
		schedules: scheds,
		legs:      make([]frame.State, len(g.history)),
		fresh:     make([]bool, len(g.nodes)),
		ended:     make([]bool, len(g.nodes)),
	}
}

/*
Step runs v's frame, a Current frame after the last one stepped. It pins every result at the frame and
refills the candle copies of the streams updated there, then runs the nodes due at its column in
serial order. A node fires when its leg is Fresh — both ends', for a pair — and each of its
dependencies fired with a value at the column; otherwise it is written Gap, or Ended at its first due
column at or after its leg ended, and nothing after. It returns the feature layer at the frame, valid
until the next Step.
*/
func (e *Engine) Step(v frame.View[proto.Candle]) feature.FeaturesView {
	fv := e.pin(v)
	col := v.Col()
	for _, i := range e.schedules[e.pattern(col)] {
		if e.ended[i] {
			continue
		}

		n := &e.g.nodes[i]
		switch leg := e.leg(n.at); {
		case leg == frame.Ended:
			n.c.skip(n.ring, col, frame.Ended)
			e.ended[i], e.fresh[i] = true, false
		case leg == frame.Fresh && e.depsFresh(n):
			e.fresh[i] = n.c.fire(n.ring, col)
		default:
			n.c.skip(n.ring, col, frame.Gap)
			e.fresh[i] = false
		}
	}
	return fv
}

// depsFresh reports whether each of n's dependencies fired with a value at the frame. Each is due
// wherever n is and runs before it, so its flag is the frame's.
func (e *Engine) depsFresh(n *node) bool {
	for _, d := range n.deps {
		if !e.fresh[d] {
			return false
		}
	}
	return true
}

/*
pin re-pins the candles and every result at v's frame, reads every stream's leg there, and refills the
candle copies of the streams updated there, before any node fires at it.
*/
func (e *Engine) pin(v frame.View[proto.Candle]) feature.FeaturesView {
	e.g.candles = v
	for s := range e.legs {
		e.legs[s] = streamLeg(v, s)
	}
	refill(e.g.history, e.g.depth, e.legs, v)
	for _, c := range e.g.all {
		c.pin(v)
	}
	return feature.NewFeaturesView(v, e.results, e.g.pairs)
}

/*
refill refills the copy of each stream with a depth whose leg is Fresh at v's frame: its depth's
newest candles. Elsewhere no node fires, and a copy keeps its last frame's candles.
*/
func refill(history [][]proto.Candle, depth []int, legs []frame.State, v frame.View[proto.Candle]) {
	for s, d := range depth {
		if d > 0 && legs[s] == frame.Fresh {
			history[s] = v.Data(s, d, history[s])
		}
	}
}

// streamLeg is stream s's leg at v's frame: Fresh when updated there (Fresh, age 0), Ended once ended,
// Gap otherwise: a Gap slot, not started, or between its due columns.
func streamLeg(v frame.View[proto.Candle], s int) frame.State {
	switch _, state, age := v.Latest(s); {
	case state == frame.Ended:
		return frame.Ended
	case state == frame.Fresh && age == 0:
		return frame.Fresh
	}
	return frame.Gap
}

// leg is a node's leg at the frame: its stream's; for a pair, Ended when either end has ended, Fresh
// when both are, Gap otherwise.
func (e *Engine) leg(at [2]int) frame.State {
	a := e.legs[at[0]]
	if at[1] < 0 {
		return a
	}

	b := e.legs[at[1]]
	switch {
	case a == frame.Ended || b == frame.Ended:
		return frame.Ended
	case a == frame.Fresh && b == frame.Fresh:
		return frame.Fresh
	}
	return frame.Gap
}

// pattern is col's due pattern: how many of the periods, ascending, divide it.
func (e *Engine) pattern(col int64) int {
	k := 0
	for k < len(e.periods) && col%e.periods[k] == 0 {
		k++
	}
	return k
}

/*
order is the serial order of g's nodes: each stream's, streams ascending, then each pair's, by pair
table index; on one coordinate, dependencies first. A node's depth is 0 without dependencies, and one
more than its deepest dependency's otherwise.
*/
func order(g *Graph) []int {
	depth := make([]int, len(g.nodes))
	for i := range depth {
		depth[i] = -1
	}
	var deep func(i int) int
	deep = func(i int) int {
		if depth[i] < 0 {
			d := 0
			for _, dep := range g.nodes[i].deps {
				d = max(d, deep(dep)+1)
			}
			depth[i] = d
		}
		return depth[i]
	}

	pair := func(n *node) int {
		if n.at[1] >= 0 {
			return 1
		}
		return 0
	}
	out := make([]int, len(g.nodes))
	for i := range out {
		out[i] = i
		deep(i)
	}
	slices.SortFunc(out, func(a, b int) int {
		na, nb := &g.nodes[a], &g.nodes[b]
		return cmp.Or(
			cmp.Compare(pair(na), pair(nb)),
			cmp.Compare(na.ring, nb.ring),
			cmp.Compare(depth[a], depth[b]),
			cmp.Compare(a, b),
		)
	})
	return out
}

/*
schedules returns the nodes' periods, ascending, and one schedule per due pattern: schedule k holds the
nodes of order whose period is one of the k smallest, in order. The periods form a divisibility chain,
so those dividing a column are always the smallest ones.
*/
func schedules(g *Graph, order []int) ([]int64, [][]int) {
	var periods []int64
	for _, n := range g.nodes {
		if !slices.Contains(periods, n.period) {
			periods = append(periods, n.period)
		}
	}
	slices.Sort(periods)

	scheds := make([][]int, len(periods)+1)
	for k := 1; k <= len(periods); k++ {
		for _, i := range order {
			if g.nodes[i].period <= periods[k-1] {
				scheds[k] = append(scheds[k], i)
			}
		}
	}
	return periods, scheds
}
