package feat

import (
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const EngulfingID = "engulfing_candle"

func init() {
	reg.Features.Register(EngulfingID, func(opts map[string]any) (proto.Feature, error) {
		return &Engulfing{
			id: proto.NewKey[DirectionalMarker](EngulfingID),
		}, nil
	})
}

// ============================================================================
// Engulfing Candle Pattern
// ============================================================================
// This implementation uses STRICT engulfing definition.
// The current candle's real body must strictly exceed the previous candle's real body
// real body. Equality is NOT sufficient.
//
// Rationale:
// - Reduces signal frequency / increased meaning
// - More suitable for systematic / statistical strategies
// ============================================================================

type Engulfing struct {
	id proto.Key[DirectionalMarker]
}

func (e *Engulfing) ID() proto.KeyRef {
	return e.id.Ref()
}

func (e *Engulfing) Dependencies() []proto.KeyRef {
	return nil
}

func (e *Engulfing) Lookback() int {
	return 2
}

func (e *Engulfing) Update(history *proto.SortedWindow[proto.MarketData], snap *proto.Snapshot) {
	candles := history.Last(e.Lookback())

	if len(candles) != 2 {
		return
	}

	prevBullish := candles[1].Candle.Close > candles[1].Candle.Open
	currBullish := candles[0].Candle.Close > candles[0].Candle.Open

	// same direction candles, noop
	if prevBullish == currBullish {
		proto.SetSnapshot(snap, e.id, DirectionalMarkerNoop)
		return
	}

	// bullish engulfing detection
	if currBullish && isBullishEngulfing(candles[1].Candle, candles[0].Candle) {
		proto.SetSnapshot(snap, e.id, DirectionalMarkerUp)
		return
	}

	// bearish engulfing detection
	if !currBullish && isBearishEngulfing(candles[1].Candle, candles[0].Candle) {
		proto.SetSnapshot(snap, e.id, DirectionalMarkerDown)
		return
	}

	// no engulfing pattern
	proto.SetSnapshot(snap, e.id, DirectionalMarkerNoop)
}

// current bullish candle engulfs previous bearish candle
func isBullishEngulfing(prev, curr proto.Candle) bool {
	return curr.Open < prev.Close && curr.Close > prev.Open
}

// current bearish candle engulfs previous bullish candle
func isBearishEngulfing(prev, curr proto.Candle) bool {
	return curr.Open > prev.Close && curr.Close < prev.Open
}
