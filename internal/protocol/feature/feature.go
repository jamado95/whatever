package feature

import (
	"errors"
	"fmt"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/frame"
)

/*
Feature is a feature as its factory configured it: what the engine needs to place it, and how to bind
it at a coordinate.
*/
type Feature struct {
	ID       string   // variant ID: the base ID and its parameters; never contains '-'
	Lookback int      // bars of its inputs it reads: candles, or its dependencies' results
	Pair     bool     // runs on a pair of streams, unordered; otherwise on one stream
	Deps     []string // feature types read, each once, in composition order; none reads candles
	Bind     any      // a Bind[T], T its value type: what the feature engine switches on
}

/*
Bind places a feature at one coordinate: it takes its inputs from b, allocates its scratch and returns
its node, which returns the value at the frame its inputs are pinned at, or false when it cannot
compute. Bind never reads an input: a failed one has no view.
*/
type Bind[T any] = func(b *Binder) func() (T, bool)

/*
Binder is a node's coordinate at Bind, with its candles and dependencies there. It keeps its first
error: the engine reads Err after Bind and discards the node.
*/
type Binder struct {
	at      [2]int // a stream, or a pair; the second is -1 on a stream
	candles *frame.View[proto.Candle]
	deps    map[string]any // by dependency type: the *frame.View[T] its results are read through
	err     error
}

/*
NewBinder places a feature at at. candles and each dependency's view are re-pinned by the engine each
frame; deps maps a dependency type to a *frame.View[T] of its value type.
*/
func NewBinder(at [2]int, candles *frame.View[proto.Candle], deps map[string]any) *Binder {
	return &Binder{at: at, candles: candles, deps: deps}
}

// Err returns the first error of the inputs taken from b.
func (b *Binder) Err() error { return b.err }

func (b *Binder) fail(err error) {
	if b.err == nil {
		b.err = err
	}
}

// Input is one ring at a node's coordinate, read at the frame the engine pinned.
type Input[T any] struct {
	v *frame.View[T]
	i int
}

// Latest returns the value at the ring's latest update at or before the frame, its state and its
// age in columns since its last Fresh value: the zero value, Gap and -1 when there is none.
func (in Input[T]) Latest() (T, frame.State, int) { return in.v.Latest(in.i) }

// Data appends up to n Fresh values to dst[:0], newest first, one bar per step back from the ring's
// latest update at or before the frame.
func (in Input[T]) Data(n int, dst []T) []T { return in.v.Data(in.i, n, dst) }

// Candles returns a single-stream node's candles.
func Candles(b *Binder) Input[proto.Candle] {
	if b.at[1] >= 0 {
		b.fail(errors.New("feature: Candles on a pair node"))
	}
	return Input[proto.Candle]{v: b.candles, i: b.at[0]}
}

// PairCandles returns a pair node's candles: its first stream's, then its second's.
func PairCandles(b *Binder) (Input[proto.Candle], Input[proto.Candle]) {
	if b.at[1] < 0 {
		b.fail(errors.New("feature: PairCandles on a single-stream node"))
	}
	return Input[proto.Candle]{v: b.candles, i: b.at[0]}, Input[proto.Candle]{v: b.candles, i: b.at[1]}
}

// Dep returns a single-stream node's results of its dependency of type typ, read as T.
func Dep[T any](b *Binder, typ string) Input[T] {
	if b.at[1] >= 0 {
		b.fail(fmt.Errorf("feature: Dep %q on a pair node", typ))
	}
	return Input[T]{v: dep[T](b, typ), i: b.at[0]}
}

// PairDep returns a pair node's results of its dependency of type typ, read as T: its first
// stream's, then its second's.
func PairDep[T any](b *Binder, typ string) (Input[T], Input[T]) {
	if b.at[1] < 0 {
		b.fail(fmt.Errorf("feature: PairDep %q on a single-stream node", typ))
	}
	v := dep[T](b, typ)
	return Input[T]{v: v, i: b.at[0]}, Input[T]{v: v, i: b.at[1]}
}

// dep returns the view of b's dependency of type typ, or nil when it has none of value type T.
func dep[T any](b *Binder, typ string) *frame.View[T] {
	d, ok := b.deps[typ]
	if !ok {
		b.fail(fmt.Errorf("feature: no dependency of type %q", typ))
		return nil
	}
	v, ok := d.(*frame.View[T])
	if !ok {
		var zero T
		b.fail(fmt.Errorf("feature: dependency %q is not %T", typ, zero))
	}
	return v
}
