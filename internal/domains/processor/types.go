package processor

import (
	"whatever/types"
)

type EnrichedMarketData struct {
	types.MarketData
	Indicators *Snapshot
}

type Processor interface {
	ID() string
	Dependencies() []KeyRef
	Update(candle types.MarketData, snap *Snapshot)
}
