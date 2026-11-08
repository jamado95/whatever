/*
Package featv2 holds the feature types on the feature contract (internal/protocol/feature), registered
in reg.FeatureTypes: sma, ema, rsi, vwap and engulfing_candle, ported from internal/domains/features
with the same config and maths. It becomes internal/domains/features when the originals retire.
*/
package featv2

import (
	"fmt"
	"math"
	"strconv"

	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
)

// DirectionalMarker is an alias of int, so a Bind[DirectionalMarker] is the engine's Bind[int].
type DirectionalMarker = int

const (
	DirectionalMarkerUp   DirectionalMarker = 1
	DirectionalMarkerDown DirectionalMarker = -1
	DirectionalMarkerNoop DirectionalMarker = 0
)

// PeriodConfig is the config shape shared by the period-parameterised features.
type PeriodConfig struct {
	Period int `json:"period"`
}

func (c *PeriodConfig) Validate() error {
	if c.Period <= 0 {
		return fmt.Errorf("'period' is required and must be positive")
	}
	return nil
}

func iDWithPeriod(base string, period int) string {
	return fmt.Sprintf("%s_%d", base, period)
}

/*
iDWithParam extends a feature ID with a non-default parameter, so two variants differing only in it
cannot collide; a default value is omitted (ema_20). Values are formatted without an exponent, so a
positive value never puts '-' in the ID.
*/
func iDWithParam(base, param string, value, defaultValue float64) string {
	if value == defaultValue {
		return base
	}
	return fmt.Sprintf("%s_%s%s", base, param, strconv.FormatFloat(value, 'f', -1, 64))
}

func roundDecimals(value float64, precision int) float64 {
	multiplier := math.Pow(10, float64(precision))
	return math.Round(value*multiplier) / multiplier
}

/*
overCandles binds a feature that reads its stream's last n candles, newest first, from the engine's
copy. Its node returns false with fewer than least candles, otherwise f's result.
*/
func overCandles[T any](n, least int, f func(candles []proto.Candle) (T, bool)) feature.Bind[T] {
	return func(b *feature.Binder) func() (T, bool) {
		in := feature.Candles(b)
		return func() (T, bool) {
			candles := in.Last(n)
			if len(candles) < least {
				var zero T
				return zero, false
			}
			return f(candles)
		}
	}
}
