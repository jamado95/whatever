package proto

import (
	"encoding/json"
	"fmt"
)

type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
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

type Candle struct {
	OpenTs  int64
	CloseTs int64
	Open    float64
	High    float64
	Low     float64
	Close   float64
	Volume  float64
}

type MarketData struct {
	Symbol     string
	ProviderID string
	Timeframe  string
	Candle     Candle
	ReceivedAt *int64
}

func (m MarketData) Timestamp() int64 {
	return m.Candle.CloseTs
}

type Subscription struct {
	Symbol    string
	Timeframe Timeframe
}

type DataError struct {
	Code    string
	Message string
}

func (e DataError) Error() string {
	return e.Message
}

type DataProvider interface {
	ID() string
	Init(sub Subscription, limit int) error
	Streams() (<-chan MarketData, <-chan error)
	Start() error
	Close()
}
