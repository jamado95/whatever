package proto

import (
	"encoding/json"
	"fmt"
)

type Timeframe string

const (
	Timeframe1m  Timeframe = "1m"
	Timeframe5m  Timeframe = "5m"
	Timeframe15m Timeframe = "15m"
	Timeframe1h  Timeframe = "1h"
	Timeframe1d  Timeframe = "1d"
)

func (t Timeframe) IsValid() bool {
	switch t {
	case Timeframe1m, Timeframe5m, Timeframe15m, Timeframe1h, Timeframe1d:
		return true
	default:
		return false
	}
}

// ValidTimeframes lists every accepted value, for error messages.
func ValidTimeframes() []Timeframe {
	return []Timeframe{Timeframe1m, Timeframe5m, Timeframe15m, Timeframe1h, Timeframe1d}
}

// UnmarshalJSON rejects any timeframe outside the known set, so a typo such as
// "1hr" fails at config load instead of being carried as an opaque string.
func (t *Timeframe) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("timeframe must be a string: %w", err)
	}

	tf := Timeframe(s)
	if !tf.IsValid() {
		return fmt.Errorf("invalid timeframe %q, want one of %v", s, ValidTimeframes())
	}

	*t = tf
	return nil
}

func (t *Timeframe) TimeframeS() int {
	switch *t {
	case Timeframe1m:
		return 60
	case Timeframe5m:
		return 5 * 60
	case Timeframe15m:
		return 60 * 15
	case Timeframe1h:
		return 60 * 60
	case Timeframe1d:
		return 24 * 60 * 60
	}

	return 0
}

func (t *Timeframe) TimeframeMs() int64 {
	return int64(t.TimeframeS()) * 1000
}

// bit is the timeframe's bit in a TimeframeDerivationSet; 0 for an invalid timeframe.
func (t Timeframe) bit() TimeframeDerivationSet {
	for i, v := range ValidTimeframes() {
		if v == t {
			return 1 << i
		}
	}
	return 0
}

// TimeframeDerivationSet is the bitmap of rolling windows derived from a subscription: bit i set
// means ValidTimeframes()[i] is derived. Decoded from a JSON list; a repeated timeframe is an error.
type TimeframeDerivationSet uint8

// NewTimeframeDerivationSet returns the set holding tfs; invalid timeframes are ignored.
func NewTimeframeDerivationSet(tfs ...Timeframe) TimeframeDerivationSet {
	var s TimeframeDerivationSet
	for _, tf := range tfs {
		s |= tf.bit()
	}
	return s
}

func (s TimeframeDerivationSet) Has(tf Timeframe) bool {
	b := tf.bit()
	return b != 0 && s&b != 0
}

// List returns the set's timeframes in ascending duration.
func (s TimeframeDerivationSet) List() []Timeframe {
	var tfs []Timeframe
	for i, tf := range ValidTimeframes() {
		if s&(1<<i) != 0 {
			tfs = append(tfs, tf)
		}
	}
	return tfs
}

// UnmarshalJSON decodes a list of timeframes, rejecting unknown and repeated ones.
func (s *TimeframeDerivationSet) UnmarshalJSON(b []byte) error {
	var tfs []Timeframe
	if err := json.Unmarshal(b, &tfs); err != nil {
		return err
	}

	var set TimeframeDerivationSet
	for _, tf := range tfs {
		if set.Has(tf) {
			return fmt.Errorf("repeated derivation timeframe %q", tf)
		}
		set |= tf.bit()
	}

	*s = set
	return nil
}

// DerivedProviderPrefix marks a derived stream's Provider; provider names may not use it.
const DerivedProviderPrefix = "derived_"

type Subscription struct {
	Symbol    string
	Timeframe Timeframe
	Provider  string
	Derive    TimeframeDerivationSet // rolling windows derived from this direct subscription
}

// ID is the canonical identity of a subscription, "symbol:timeframe:provider".
// It is the reference handle strategies use and the key the seam events carry.
func (s Subscription) ID() string {
	return s.Symbol + ":" + string(s.Timeframe) + ":" + s.Provider
}
