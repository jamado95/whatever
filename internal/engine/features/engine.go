package features

import (
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/frame"
)

// Engine is one run of a Graph.
type Engine struct {
	g *Graph
}

// New allocates every column's buffer.
func New(g *Graph) *Engine {
	for _, c := range g.all {
		c.alloc()
	}
	return &Engine{g: g}
}

// pin re-pins the candles and every result at v's frame, before any node fires at it.
func (e *Engine) pin(v frame.View[proto.Candle]) View {
	e.g.candles = v
	for _, c := range e.g.all {
		c.pin(v)
	}
	return View{View: v, g: e.g}
}
