package feat

import (
	"whatever/internal/config"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const RSIID = "rsi"

func init() {
	reg.Features.Register(RSIID, func(opts map[string]any) (proto.Feature, error) {
		cfg := PeriodConfig{}
		if err := config.Decode(opts, &cfg); err != nil {
			return nil, err
		}

		id := iDWithPeriod(RSIID, cfg.Period)

		return &RSI{
			id:     proto.NewKey[float64](id),
			period: cfg.Period,
		}, nil
	})
}

type RSI struct {
	id     proto.Key[float64]
	period int
}

func (r *RSI) ID() proto.KeyRef {
	return r.id.Ref()
}

func (r *RSI) Dependencies() []proto.KeyRef {
	return nil
}

func (r *RSI) Lookback() int {
	return r.period + 1 // Need period+1 for price changes
}

func (r *RSI) Update(history *proto.SortedWindow[proto.MarketData], snap *proto.Snapshot) {
	candles := history.Last(r.period + 1)
	if len(candles) < r.period+1 {
		return
	}

	var gains, losses float64

	// Calculate gains and losses over period
	// candles[0] is most recent, iterate to calculate changes
	for i := 0; i < r.period; i++ {
		change := candles[i].Candle.Close - candles[i+1].Candle.Close

		if change > 0 {
			gains += change
		} else {
			losses += -change
		}
	}

	avgGain := gains / float64(r.period)
	avgLoss := losses / float64(r.period)

	if avgLoss == 0 {
		proto.SetSnapshot(snap, r.id, 100.0)
		return
	}

	rs := avgGain / avgLoss
	rsi := 100.0 - (100.0 / (1.0 + rs))

	proto.SetSnapshot(snap, r.id, roundDecimals(rsi, 6))
}
