//go:build wip

package feat

import (
	"fmt"
	"math"

	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const SwingHighLowID = "swing_highlow"

func init() {
	reg.Features.Register(SwingHighLowID, func(opts map[string]any) (proto.Feature, error) {
		period, ok := opts["period"].(float64)
		if !ok || math.Mod(period, 2) != 0 {
			return nil, fmt.Errorf("%s register: missing or invalid period", SwingHighLowID)
		}

		id := iDWithPeriod(SwingHighLowID, int(period))

		return &SwingHighLow{
			id:     proto.NewKey[DirectionalMarker](id),
			period: int(period),
		}, nil
	})
}

// ============================================================================
// Swing High Low
// ============================================================================
// Identifies swing highs and lows using a lookback window.
// - Enforces an odd lookback period
// ============================================================================

type SwingHighLow struct {
	id     proto.Key[DirectionalMarker]
	period int
}

func (e *SwingHighLow) ID() proto.KeyRef {
	return e.id.Ref()
}

func (e *SwingHighLow) Dependencies() []proto.KeyRef {
	return nil
}

func (e *SwingHighLow) Lookback() int {
	return e.period
}

func (e *SwingHighLow) Update(history *proto.SortedWindow[proto.MarketData], snap *proto.Snapshot) {
	candles := history.Last(e.period)

	// nil snapshot if we don't have enough candles
	if len(candles) < e.period {
		return
	}

	// check if swing low
	candidate := candles[e.period/2].Candle

	for neigh := range candles {

	}

	isSwingLow := true
	for i := checkIdx - lb; i <= checkIdx+lb; i++ {
		if i == checkIdx {
			continue
		}
		if state.candles[i].Low <= candidate.Low {
			isSwingLow = false
			break
		}
	}

	proto.SetSnapshot(snap, e.id, engulfing)
}
