package features

import (
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/frame"
)

// Engine is one run of a Graph.
type Engine struct {
	g *Graph
}

// New allocates every column's buffer and each read stream's candle copy.
func New(g *Graph) *Engine {
	for _, c := range g.all {
		c.alloc()
	}
	for s, d := range g.depth {
		if d > 0 {
			g.history[s] = make([]proto.Candle, 0, d)
		}
	}
	return &Engine{g: g}
}

/*
pin re-pins the candles and every result at v's frame, and refills the candle copies of the streams
updated there, before any node fires at it.
*/
func (e *Engine) pin(v frame.View[proto.Candle]) View {
	e.g.candles = v
	refill(e.g.history, e.g.depth, v)
	for _, c := range e.g.all {
		c.pin(v)
	}
	return View{View: v, g: e.g}
}

/*
refill refills the copy of each stream with a depth that v's frame updates (Fresh, age 0): its
depth's newest candles. Elsewhere no node fires, and a copy keeps its last frame's candles.
*/
func refill(history [][]proto.Candle, depth []int, v frame.View[proto.Candle]) {
	for s, d := range depth {
		if d == 0 {
			continue
		}
		if _, state, age := v.Latest(s); state == frame.Fresh && age == 0 {
			history[s] = v.Data(s, d, history[s])
		}
	}
}
