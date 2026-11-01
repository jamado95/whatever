package proto

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// StreamKind is a stream's origin; a string, so a config filter decodes it.
type StreamKind string

const (
	StreamDirect  StreamKind = "direct"  // fed by a provider
	StreamDerived StreamKind = "derived" // rolling window over a direct stream
)

// UnmarshalJSON rejects any kind outside the known set.
func (k *StreamKind) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("stream kind must be a string: %w", err)
	}

	switch kind := StreamKind(s); kind {
	case StreamDirect, StreamDerived:
		*k = kind
		return nil
	}
	return fmt.Errorf("invalid stream kind %q, want one of [%s %s]", s, StreamDirect, StreamDerived)
}

// Stream is one entry of the stream table; its index in Streams.All is its identity in every layer.
type Stream struct {
	Sub    Subscription // derived: symbol, window timeframe, Provider "derived_" + source ID
	Kind   StreamKind
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
func NewStreams(subs []Subscription) *Streams {
	st := &Streams{index: make(map[string]int)}
	for _, sub := range subs {
		if ms := sub.Timeframe.TimeframeMs(); st.Stride == 0 || ms < st.Stride {
			st.Stride = ms
		}
	}

	for _, sub := range subs {
		st.All = append(st.All, Stream{
			Sub:    sub,
			Kind:   StreamDirect,
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
				Sub: Subscription{
					Symbol:    s.Sub.Symbol,
					Timeframe: tf,
					Provider:  DerivedProviderPrefix + s.Sub.ID(),
				},
				Kind:   StreamDerived,
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

// Column maps a close timestamp to the first column boundary at or after it (ceilDiv): a close at
// or up to one stride before a boundary lands in that boundary's column.
func (s *Streams) Column(ts int64) int64 {
	return (ts + s.Stride - 1) / s.Stride
}

// Index returns the index of the stream with the given ID.
func (s *Streams) Index(id string) (int, bool) {
	i, ok := s.index[id]
	return i, ok
}

// Provider returns the provider instance feeding stream i: its own, or its source's when derived.
func (s *Streams) Provider(i int) string {
	if src := s.All[i].Source; src >= 0 {
		return s.All[src].Sub.Provider
	}
	return s.All[i].Sub.Provider
}
