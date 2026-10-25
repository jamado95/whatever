package proto

import (
	"fmt"
	"slices"
)

type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

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
	Subscription() Subscription
	Init(sub Subscription) error
	Streams() (<-chan MarketData, <-chan error)
	Start() error
	Close()
}
