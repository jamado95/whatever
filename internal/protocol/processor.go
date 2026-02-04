package proto

type EnrichedMarketData struct {
	MarketData
	Indicators *Snapshot
}

type Processor interface {
	ID() string
	Dependencies() []KeyRef
	Update(candle MarketData, snap *Snapshot)
}
