package feat

import (
	"fmt"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

func init() {
	reg.Features.Register("ema", func(opts map[string]any) (proto.Feature, error) {
		period, ok := opts["period"].(float64)
		if !ok {
			return nil, fmt.Errorf("ema register: missing or invalid period")
		}

		id := iDWithPeriod("ema", int(period))

		return &EMA{
			id:     proto.NewKey[float64](id),
			period: int(period),
		}, nil
	})
}

type EMA struct {
	id     proto.Key[float64]
	period int
}

func (e *EMA) ID() proto.KeyRef {
	return e.id.Ref()
}

func (e *EMA) Dependencies() []proto.KeyRef {
	return nil
}

func (e *EMA) Lookback() int {
	// need 2x for proper EMA calculation
	return e.period * 2
}

func (e *EMA) Update(history *proto.SortedWindow[proto.MarketData], snap *proto.Snapshot) {
	candles := history.Last(e.period * 2)

	if len(candles) < e.period {
		return
	}

	multiplier := 2.0 / float64(e.period+1)

	// Initialize EMA with SMA of first 'period' candles
	sum := 0.0
	startIdx := len(candles) - 1

	for i := 0; i < e.period && startIdx-i >= 0; i++ {
		sum += candles[startIdx-i].Candle.Close
	}
	ema := sum / float64(e.period)

	// Apply EMA formula to remaining candles (oldest to newest)
	for i := startIdx - e.period; i >= 0; i-- {
		ema = (candles[i].Candle.Close * multiplier) + (ema * (1 - multiplier))
	}

	proto.SetSnapshot(snap, e.id, roundDecimals(ema, 6))
}
