package feat

import (
	"fmt"
	"math"
)

func iDWithPeriod(base string, period int) string {
	return fmt.Sprintf("%s_%d", base, period)
}

func roundDecimals(value float64, precision int) float64 {
	multiplier := math.Pow(10, float64(precision))
	return math.Round(value*multiplier) / multiplier
}
