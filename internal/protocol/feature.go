package proto

type EnrichedMarketData struct {
	MarketData
	Indicators *Snapshot
}

// Features are PURE FUNCTIONS wrapped in structs
// They have NO mutable state - only configuration (immutable after construction)
// All state comes from parameters: candle, history, snap

type Feature interface {
	ID() KeyRef
	Dependencies() []KeyRef
	Lookback() int // NEW: Number of historical candles needed
	Update(history *SortedWindow[MarketData], snap *Snapshot)
}
