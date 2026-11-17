package feature

import (
	"fmt"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/frame"
)

// FeaturesView is the feature layer at one frame: the data view, and every result pinned at its
// column. Valid until the next frame is stepped.
type FeaturesView struct {
	frame.View[proto.Candle]
	results map[string]any // by composed ID: its *frame.Buffer[T], pinned by ViewOf
	pairs   map[[2]int]int // the pair table: (a, b), a < b → entry
}

// NewFeaturesView returns the feature layer at v's frame. results holds each composed ID's
// *frame.Buffer[T]; pairs is the pair table. Both are shared, never written.
func NewFeaturesView(v frame.View[proto.Candle], results map[string]any, pairs map[[2]int]int) FeaturesView {
	return FeaturesView{View: v, results: results, pairs: pairs}
}

// ViewOf returns composed ID id's results at v's frame: entry i is stream i, or pair i of the pair
// table. An unknown ID, or a value type other than T, is an error.
func ViewOf[T any](v FeaturesView, id string) (frame.View[T], error) {
	r, ok := v.results[id]
	if !ok {
		return frame.View[T]{}, fmt.Errorf("feature: unknown result %q", id)
	}
	buf, ok := r.(*frame.Buffer[T])
	if !ok {
		var zero T
		return frame.View[T]{}, fmt.Errorf("feature: result %q is not %T", id, zero)
	}
	return frame.Pin(v.View, buf), nil
}

// Pair returns the pair (a, b)'s entry in pair results, in either order.
func (v FeaturesView) Pair(a, b int) (int, bool) {
	if a > b {
		a, b = b, a
	}
	i, ok := v.pairs[[2]int{a, b}]
	return i, ok
}
