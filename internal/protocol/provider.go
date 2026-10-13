package proto

import (
	"encoding/json"
	"fmt"
	"slices"
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
	Subscription Subscription
	Candle       Candle
	ReceivedAt   *int64
}

func (m MarketData) Timestamp() int64 {
	return m.Candle.CloseTs
}

type Subscription struct {
	Symbol    string
	Timeframe Timeframe
	Provider  string
}

// ID is the canonical identity of a subscription, "symbol:timeframe:provider".
// It is the reference handle strategies use and the key the seam events carry.
func (s Subscription) ID() string {
	return fmt.Sprintf("%s:%s:%s", s.Symbol, s.Timeframe, s.Provider)
}

// SyncEvent is the aligned boundary event the seam emits. Defined now; first
// consumed in stage 3 by the arrival-order synchronizer.
type SyncEvent struct {
	Time       int64                               // boundary CloseTs the group aligns on
	Now        []ExtendedMarketData                // legs that closed at Time (≥1)
	NowFwdFill map[Subscription]ExtendedMarketData // forward-filled latest per sub; Now ⊆ NowFwdFill
}

//////////////////// SYNC V2

type SyncDataFreshness int

const (
	CurrentMarketData SyncDataFreshness = iota
	StaleMarketData
	AheadMarketData
)

type SyncEventV2 struct {
	time   int64 // boundary CloseTs the group aligns on
	status SyncDataFreshness
	data   []ExtendedMarketData // legs that closed at Time (≥1)
}

func NewSyncEventV2(status SyncDataFreshness, data []ExtendedMarketData) (*SyncEventV2, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("sync event requires at least one market data entry")
	}

	// Avoid mutating the caller's slice
	ordered := slices.Clone(data)

	// oldest to newest
	slices.SortFunc(ordered, func(a, b ExtendedMarketData) int {
		return int(a.MarketData.Timestamp() - b.MarketData.Timestamp())
	})

	return &SyncEventV2{
		time:   ordered[len(ordered)-1].MarketData.Timestamp(), // newest
		status: status,
		data:   ordered,
	}, nil
}

func (se *SyncEventV2) Time() int64 {
	return se.time
}

func (se *SyncEventV2) Status() SyncDataFreshness {
	return se.status
}

func (se *SyncEventV2) Data() []ExtendedMarketData {
	return slices.Clone(se.data)
}

//////////////////// SYNC V2

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
