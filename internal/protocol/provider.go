package proto

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
	Source     string
	Timeframe  string
	Candle     Candle
	ReceivedAt *int64
}

func (m MarketData) Timestamp() int64 {
	return m.Candle.CloseTs
}

type ProcessedMarketData struct {
	MarketData
	Indicators *Snapshot
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
