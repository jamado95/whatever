// Package data is the data layer v2 module: it turns subscriptions and provider
// instances into in-order frames and owns the FrameBuffer every other layer reads
// and writes through. See docs/discussions/data-layer-v2.md.
package data

import (
	"slices"
	"strings"

	proto "whatever/internal/protocol"
)

// Kind is a stream's origin.
type Kind uint8

const (
	Direct  Kind = iota // fed by a provider
	Derived             // rolling window over a direct stream
)

// Stream is one entry of the stream table; its index in Streams.All is its identity in the data layer.
type Stream struct {
	Sub    proto.Subscription // derived: symbol, window timeframe, Provider "derived_" + source ID
	Kind   Kind
	Period int64 // columns between updates: direct, timeframe / Stride; derived, its source's Period
	Window int64 // updates per bar: direct 1; derived, source bars per window
	Source int   // derived: index of its source stream; direct: -1
}

// Streams is the stream table: direct streams first, then derived, each group sorted by ID.
type Streams struct {
	Stride  int64 // ms per column: the finest direct timeframe
	All     []Stream
	Directs int // All[:Directs] are direct
	index   map[string]int
}

/*
NewStreams builds the stream table from validated direct subscriptions: one
stream per subscription and one per window in its Derive set. The grid stride is
the finest direct timeframe. Rules are enforced by config parsing, so nothing is
validated here.
*/
func NewStreams(subs []proto.Subscription) *Streams {
	st := &Streams{index: make(map[string]int)}
	for _, sub := range subs {
		if ms := sub.Timeframe.TimeframeMs(); st.Stride == 0 || ms < st.Stride {
			st.Stride = ms
		}
	}

	for _, sub := range subs {
		st.All = append(st.All, Stream{
			Sub:    sub,
			Kind:   Direct,
			Period: sub.Timeframe.TimeframeMs() / st.Stride,
			Window: 1,
			Source: -1,
		})
	}

	byID := func(a, b Stream) int { return strings.Compare(a.Sub.ID(), b.Sub.ID()) }

	slices.SortFunc(st.All, byID)
	st.Directs = len(st.All)

	var derived []Stream
	for src, s := range st.All {
		for _, tf := range s.Sub.Derive.List() {
			derived = append(derived, Stream{
				Sub: proto.Subscription{
					Symbol:    s.Sub.Symbol,
					Timeframe: tf,
					Provider:  proto.DerivedProviderPrefix + s.Sub.ID(),
				},
				Kind:   Derived,
				Period: s.Period,
				Window: tf.TimeframeMs() / s.Sub.Timeframe.TimeframeMs(),
				Source: src,
			})
		}
	}
	slices.SortFunc(derived, byID)
	st.All = append(st.All, derived...)

	for i, s := range st.All {
		st.index[s.Sub.ID()] = i
	}
	return st
}

// Index returns the index of the stream with the given ID.
func (s *Streams) Index(id string) (int, bool) {
	i, ok := s.index[id]
	return i, ok
}
