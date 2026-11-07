package features

import (
	"fmt"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
)

// column is one composed ID's results with their value type erased: what the graph holds.
type column interface {
	cell() any                               // &view, the *frame.View[T] dependents bind to
	bind(b *feature.Binder, ring int) error  // the feature at b's coordinate, its node writing ring
	alloc()                                  // the buffer, one ring per shape; New calls it once
	pin(v frame.View[proto.Candle])          // view at v's frame; every read follows it
	fire(ring int, col int64)                // the node; writes Fresh, or Gap when it cannot compute
	skip(ring int, col int64, s frame.State) // writes s, Gap or Ended, carrying the last Fresh value
}

// results is one composed ID's results: its buffer, its readers' view and the node of each ring.
type results[T any] struct {
	newNode feature.Bind[T]
	shapes  []frame.Shape
	buf     *frame.Buffer[T]   // nil until alloc
	view    frame.View[T]      // re-pinned each frame; nodes hold its address from Bind
	nodes   []func() (T, bool) // by ring; nil where the ID does not run
}

/*
newColumn is the engine's switch: one case per supported value type. A struct value type adds a case,
importing its feature's domain package. It allocates no ring, so any feature can be checked with it.
*/
func newColumn(f feature.Feature, shapes []frame.Shape) (column, error) {
	switch bind := f.Bind.(type) {
	case feature.Bind[float64]:
		return newResults(bind, shapes), nil
	case feature.Bind[int]:
		return newResults(bind, shapes), nil
	}
	return nil, fmt.Errorf("features: %s: unsupported Bind %T", f.ID, f.Bind)
}

func newResults[T any](bind feature.Bind[T], shapes []frame.Shape) *results[T] {
	return &results[T]{
		newNode: bind,
		shapes:  shapes,
		nodes:   make([]func() (T, bool), len(shapes)),
	}
}

func (r *results[T]) cell() any { return &r.view }

func (r *results[T]) bind(b *feature.Binder, ring int) error {
	node := r.newNode(b)
	if err := b.Err(); err != nil {
		return err
	}
	r.nodes[ring] = node
	return nil
}

func (r *results[T]) alloc() { r.buf = frame.NewBuffer[T](r.shapes) }

func (r *results[T]) pin(v frame.View[proto.Candle]) { r.view = frame.Pin(v, r.buf) }

func (r *results[T]) fire(ring int, col int64) {
	v, ok := r.nodes[ring]()
	if !ok {
		r.skip(ring, col, frame.Gap)
		return
	}
	r.buf.Ring(ring).Put(col, v, frame.Fresh)
}

func (r *results[T]) skip(ring int, col int64, s frame.State) {
	var zero T
	r.buf.Ring(ring).Put(col, zero, s)
}
