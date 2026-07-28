package feat

import (
	"fmt"

	"whatever/internal/config"
	proto "whatever/internal/protocol"
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
	reg.Features.Register(EMAID, func(opts map[string]any) (proto.Feature, error) {
		cfg := EMAConfig{Smoothing: DefaultEMASmoothing}
		if err := config.Decode(opts, &cfg); err != nil {
			return nil, err
		}

		id := iDWithParam(
			iDWithPeriod(EMAID, cfg.Period),
			"s", cfg.Smoothing, DefaultEMASmoothing,
		)

		return &EMA{
			id:        proto.NewKey[float64](id),
			period:    cfg.Period,
			smoothing: cfg.Smoothing,
		}, nil
	})
}

type EMA struct {
	id        proto.Key[float64]
	period    int
	smoothing float64
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

	multiplier := e.smoothing / float64(e.period+1)

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
