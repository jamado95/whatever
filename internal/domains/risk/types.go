package risk

import proto "whatever/internal/protocol"

type Position struct {
	Symbol        string
	Side          Side
	Size          float64
	EntryPrice    float64
	CurrentPrice  float64
	UnrealizedPnL float64
	Timestamp     int64
}

type PortfolioState struct {
	Balances      map[string]float64
	Positions     map[string]Position
	TotalValue    float64
	UnrealizedPnL float64
	RealizedPnL   float64
	Exposure      map[string]float64
	Timestamp     int64
}

type RiskManager interface {
	Init(data <-chan proto.MarketData, signals <-chan proto.Signal, fills <-chan proto.Fill) error
	Streams() (<-chan proto.Order, <-chan error)
	Start() error
	Portfolio() PortfolioState
	Close()
}
