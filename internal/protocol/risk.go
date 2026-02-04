package proto

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
	Init(data <-chan MarketData, signals <-chan Signal, fills <-chan Fill) error
	Streams() (<-chan Order, <-chan error)
	Start() error
	Portfolio() PortfolioState
	Close()
}
