package featv2

import (
	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
)

const EngulfingID = "engulfing_candle"

// EngulfingConfig takes no options. Decoding into it still matters: it turns
// any key supplied for this feature into an error rather than silence.
type EngulfingConfig struct{}

func init() {
	reg.FeatureTypes.Register(EngulfingID, newEngulfing)
}

// newEngulfing builds engulfing_candle: the direction of an engulfing pattern on the last two candles.
func newEngulfing(opts map[string]any) (feature.Feature, error) {
	cfg := EngulfingConfig{}
	if err := config.Decode(opts, &cfg); err != nil {
		return feature.Feature{}, err
	}

	period := 2
	return feature.Feature{
		ID:       variantID(EngulfingID, cfg),
		Lookback: period,
		Bind:     overCandles(period, period, engulfing),
	}, nil
}

/*
engulfing is Up when the newest of two candles, newest first, is a bullish engulfing, Down when a
bearish one, Noop otherwise. Engulfing is strict: the current real body must strictly exceed the
previous one, equality is not enough.
*/
func engulfing(candles []proto.Candle) (DirectionalMarker, bool) {
	prev, curr := candles[1], candles[0]
	prevBullish := prev.Close > prev.Open
	currBullish := curr.Close > curr.Open

	switch {
	case prevBullish == currBullish:
		return DirectionalMarkerNoop, true
	case currBullish && isBullishEngulfing(prev, curr):
		return DirectionalMarkerUp, true
	case !currBullish && isBearishEngulfing(prev, curr):
		return DirectionalMarkerDown, true
	}
	return DirectionalMarkerNoop, true
}

// current bullish candle engulfs previous bearish candle
func isBullishEngulfing(prev, curr proto.Candle) bool {
	return curr.Open < prev.Close && curr.Close > prev.Open
}

// current bearish candle engulfs previous bullish candle
func isBearishEngulfing(prev, curr proto.Candle) bool {
	return curr.Open > prev.Close && curr.Close < prev.Open
}
