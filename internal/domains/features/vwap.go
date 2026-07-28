package feat

import (
	"whatever/internal/config"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const VWAPID = "vwap"

func init() {
	reg.Features.Register(VWAPID, func(opts map[string]any) (proto.Feature, error) {
		cfg := PeriodConfig{}
		if err := config.Decode(opts, &cfg); err != nil {
			return nil, err
		}

		id := iDWithPeriod(VWAPID, cfg.Period)

		return &VWAP{
			id:     proto.NewKey[float64](id),
			period: cfg.Period,
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
