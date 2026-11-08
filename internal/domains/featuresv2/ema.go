package featv2

import (
	"fmt"

	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
)

const EMAID = "ema"

// DefaultEMASmoothing is the conventional smoothing constant; the multiplier is
// smoothing / (period + 1).
const DefaultEMASmoothing = 2.0

type EMAConfig struct {
	Period    int     `json:"period"`
	Smoothing float64 `json:"smoothing"`
}

func (c *EMAConfig) Validate() error {
	if c.Period <= 0 {
		return fmt.Errorf("'period' is required and must be positive")
	}
	if c.Smoothing <= 0 {
		return fmt.Errorf("'smoothing' must be positive")
	}
	return nil
}

func init() {
	reg.FeatureTypes.Register(EMAID, newEMA)
}

/*
newEMA builds ema_<period>, or ema_<period>_s<smoothing> off the default smoothing: the exponential
moving average of the last 2·period closes, seeded with the SMA of the oldest period of them. It needs
at least period candles.
*/
func newEMA(opts map[string]any) (feature.Feature, error) {
	cfg := EMAConfig{Smoothing: DefaultEMASmoothing}
	if err := config.Decode(opts, &cfg); err != nil {
		return feature.Feature{}, err
	}
	p, s := cfg.Period, cfg.Smoothing
	return feature.Feature{
		ID:       iDWithParam(iDWithPeriod(EMAID, p), "s", s, DefaultEMASmoothing),
		Lookback: 2 * p,
		Bind: overCandles(2*p, p, func(candles []proto.Candle) (float64, bool) {
			return ema(candles, p, s), true
		}),
	}, nil
}

// ema is the EMA over candles, newest first, seeded with the SMA of the oldest period of them.
func ema(candles []proto.Candle, period int, smoothing float64) float64 {
	multiplier := smoothing / float64(period+1)

	sum := 0.0
	startIdx := len(candles) - 1
	for i := 0; i < period && startIdx-i >= 0; i++ {
		sum += candles[startIdx-i].Close
	}
	ema := sum / float64(period)

	// oldest to newest
	for i := startIdx - period; i >= 0; i-- {
		ema = (candles[i].Close * multiplier) + (ema * (1 - multiplier))
	}
	return roundDecimals(ema, 6)
}
