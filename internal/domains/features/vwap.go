package feat

import (
	"fmt"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

func init() {
	reg.Features.Register("vwap", func(opts map[string]any) (proto.Feature, error) {
		period, ok := opts["period"].(float64)
		if !ok {
			return nil, fmt.Errorf("vwap register: missing or invalid period")
		}

		id := iDWithPeriod("vwap", int(period))

		return &VWAP{
			id:     proto.NewKey[float64](id),
			period: int(period),
		}, nil
	})
}

// ============================================================================
// VWAP - Volume Weighted Average Price
// ============================================================================

type VWAP struct {
	id     proto.Key[float64]
	period int
}

func (v *VWAP) ID() proto.KeyRef {
	return v.id.Ref()
}

func (v *VWAP) Dependencies() []proto.KeyRef {
	return nil
}

func (v *VWAP) Lookback() int {
	return v.period
}

// see https://www.investopedia.com/terms/v/vwap.asp
func (v *VWAP) Update(history *proto.SortedWindow[proto.MarketData], snap *proto.Snapshot) {
	candles := history.Last(v.period)

	if len(candles) < v.period {
		return
	}

	var cumTypicalPriceVolume float64
	var cumVolume float64

	for _, md := range candles {
		c := md.Candle
		typicalPrice := (c.High + c.Low + c.Close) / 3.0
		cumTypicalPriceVolume += typicalPrice * c.Volume
		cumVolume += c.Volume
	}

	if cumVolume == 0 {
		return
	}

	vwap := cumTypicalPriceVolume / cumVolume

	proto.SetSnapshot(snap, v.id, roundDecimals(vwap, 6))
}
