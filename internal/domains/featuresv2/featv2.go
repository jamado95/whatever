/*
Package featv2 holds the feature types on the feature contract (internal/protocol/feature), registered
in reg.FeatureTypes: sma, ema, rsi, vwap and engulfing_candle, ported from internal/domains/features
with the same config and maths. It becomes internal/domains/features when the originals retire.
*/
package featv2

import (
	"fmt"
	"math"
	"reflect"
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

/*
PeriodConfig is embedded first in the config of each period-parameterised feature, giving its period,
the period's place in the ID and its validation. A config with its own Validate calls PeriodConfig's.
*/
type PeriodConfig struct {
	Period int `json:"period" id:""`
}

func (c *PeriodConfig) Validate() error {
	if c.Period <= 0 {
		return fmt.Errorf("'period' is required and must be positive")
	}
	return nil
}

/*
variantID builds a feature ID from base and every field of params, a config struct, in field order:
each adds _<id tag><value> (ema_20_s2), fields of an embedded struct in its place. Defaults are
included, so an ID states every parameter. Values are formatted without an exponent, so a positive
value never puts '-' in the ID. It panics on a field without an id tag or of a type other than int or
float, a programming error in the config struct.
*/
func variantID(base string, params any) string {
	paramsV := reflect.ValueOf(params)
	if paramsV.Kind() != reflect.Struct {
		panic(fmt.Errorf("variantID: can't produce feature %s id from non-struct params", base))
	}

	id := base
	for _, field := range reflect.VisibleFields(paramsV.Type()) {
		if field.Anonymous {
			continue
		}

		fId, ok := field.Tag.Lookup("id")
		if !ok {
			panic(fmt.Errorf("variantID: field %s has no id tag", field.Name))
		}

		fv := paramsV.FieldByIndex(field.Index)
		var valueS string

		switch {
		case fv.CanFloat():
			valueS = strconv.FormatFloat(fv.Float(), 'f', -1, 64)
		case fv.CanInt():
			valueS = strconv.FormatInt(fv.Int(), 10)
		default:
			panic(fmt.Errorf("variantID: unsupported field %s type", field.Name))
		}

		id = fmt.Sprintf("%s_%s%s", id, fId, valueS)
	}

	return id
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
