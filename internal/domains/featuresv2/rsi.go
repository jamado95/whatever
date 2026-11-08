package featv2

import (
	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
)

const RSIID = "rsi"

type RSIConfig struct {
	PeriodConfig
}

func init() {
	reg.FeatureTypes.Register(RSIID, newRSI)
}

// newRSI builds rsi_<period>: the relative strength index of the last period close changes.
func newRSI(opts map[string]any) (feature.Feature, error) {
	cfg := RSIConfig{}
	if err := config.Decode(opts, &cfg); err != nil {
		return feature.Feature{}, err
	}

	lb := cfg.Period + 1
	return feature.Feature{
		ID:       variantID(RSIID, cfg),
		Lookback: lb, // period changes
		Bind: overCandles(lb, lb, func(candles []proto.Candle) (float64, bool) {
			return rsi(candles, cfg.Period), true
		}),
	}, nil
}

// rsi is the RSI of period+1 candles, newest first; 100 when no change is a loss.
func rsi(candles []proto.Candle, period int) float64 {
	var gains, losses float64
	for i := 0; i < period; i++ {
		change := candles[i].Close - candles[i+1].Close
		if change > 0 {
			gains += change
		} else {
			losses += -change
		}
	}

	avgGain := gains / float64(period)
	avgLoss := losses / float64(period)
	if avgLoss == 0 {
		return 100.0
	}
	rs := avgGain / avgLoss
	return roundDecimals(100.0-(100.0/(1.0+rs)), 6)
}
