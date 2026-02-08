package feat

import (
	"fmt"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

func init() {
	reg.Features.Register("sma", func(opts map[string]any) (proto.Feature, error) {
		period, ok := opts["period"].(float64)
		if !ok {
			return nil, fmt.Errorf("sma register: missing or invalid period")
		}

		id := iDWithPeriod("sma", int(period))

		return &EMA{
			id:     proto.NewKey[float64](id),
			period: int(period),
		}, nil
	})
}

// ============================================================================
// SMA - Simple Moving Average
// ============================================================================

type SMA struct {
	id     proto.Key[float64]
	period int
}

func (s *SMA) ID() proto.KeyRef {
	return s.id.Ref()
}

func (s *SMA) Dependencies() []proto.KeyRef {
	return nil
}

func (s *SMA) Lookback() int {
	return s.period
}

func (s *SMA) Update(history *proto.SortedWindow[proto.MarketData], snap *proto.Snapshot) {
	candles := history.Last(s.period)

	if len(candles) < s.period {
		return
	}

	sum := 0.0
	for _, c := range candles {
		sum += c.Candle.Close
	}

	proto.SetSnapshot(snap, s.id, sum/float64(s.period))
}
