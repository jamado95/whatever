package featv2

import (
	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
)

const SMAID = "sma"

func init() {
	reg.FeatureTypes.Register(SMAID, newSMA)
}

// newSMA builds sma_<period>: the simple moving average of the last period closes.
func newSMA(opts map[string]any) (feature.Feature, error) {
	cfg := PeriodConfig{}
	if err := config.Decode(opts, &cfg); err != nil {
		return feature.Feature{}, err
	}
	p := cfg.Period
	return feature.Feature{
		ID:       iDWithPeriod(SMAID, p),
		Lookback: p,
		Bind: overCandles(p, p, func(candles []proto.Candle) (float64, bool) {
			return sma(candles, p), true
		}),
	}, nil
}

// sma is the mean close of period candles.
func sma(candles []proto.Candle, period int) float64 {
	sum := 0.0
	for _, c := range candles {
		sum += c.Close
	}
	return roundDecimals(sum/float64(period), 6)
}
