package features

import (
	"fmt"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/frame"
)

// View is the feature layer at one frame: the data view, and every result pinned at its column.
// Valid until the next pin.
type View struct {
	frame.View[proto.Candle]
	g *Graph
}

// ViewOf returns composed ID id's results at v's frame: entry i is stream i, or pair i of the pair
// table. An unknown ID, or a value type other than T, is an error.
func ViewOf[T any](v View, id string) (frame.View[T], error) {
	c, ok := v.g.columns[id]
	if !ok {
		return frame.View[T]{}, fmt.Errorf("features: unknown result %q", id)
	}
	r, ok := c.(*results[T])
	if !ok {
		var zero T
		return frame.View[T]{}, fmt.Errorf("features: result %q is not %T", id, zero)
	}
	return frame.Pin(v.View, r.buf), nil
}

// Pair returns the pair (a, b)'s entry in pair results, in either order.
func (v View) Pair(a, b int) (int, bool) {
	if a > b {
		a, b = b, a
	}
	i, ok := v.g.pairs[[2]int{a, b}]
	return i, ok
}
