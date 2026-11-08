package featv2

import (
	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
)

const VWAPID = "vwap"

type VWAPConfig struct {
	PeriodConfig
}

func init() {
	reg.FeatureTypes.Register(VWAPID, newVWAP)
}

// newVWAP builds vwap_<period>: the volume-weighted average typical price of the last period candles.
func newVWAP(opts map[string]any) (feature.Feature, error) {
	cfg := VWAPConfig{}
	if err := config.Decode(opts, &cfg); err != nil {
		return feature.Feature{}, err
	}

	return feature.Feature{
		ID:       variantID(VWAPID, cfg),
		Lookback: cfg.Period,
		Bind:     overCandles(cfg.Period, cfg.Period, vwap),
	}, nil
}

// vwap is the VWAP of candles; false when their volume is zero.
// see https://www.investopedia.com/terms/v/vwap.asp
func vwap(candles []proto.Candle) (float64, bool) {
	var cumTypicalPriceVolume float64
	var cumVolume float64
	for _, c := range candles {
		typicalPrice := (c.High + c.Low + c.Close) / 3.0
		cumTypicalPriceVolume += typicalPrice * c.Volume
		cumVolume += c.Volume
	}
	if cumVolume == 0 {
		return 0, false
	}
	return roundDecimals(cumTypicalPriceVolume/cumVolume, 6), true
}
