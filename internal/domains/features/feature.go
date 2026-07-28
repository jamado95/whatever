package feat

import (
	"fmt"
	"math"
	"strconv"

	proto "whatever/internal/protocol"
)

// ////////////////////////////////////
// Feature Types
// ////////////////////////////////////

type DirectionalMarker = int

const (
	DirectionalMarkerUp   DirectionalMarker = 1
	DirectionalMarkerDown DirectionalMarker = -1
	DirectionalMarkerNoop DirectionalMarker = 0
)

type FeatureChain struct {
	features []proto.Feature
	window   *proto.SortedWindow[proto.MarketData]
}

func NewFeatureChain(features []proto.Feature) (*FeatureChain, error) {
	if err := checkDuplicates(features); err != nil {
		return nil, err
	}

	sorted, err := topologicalSort(features)
	if err != nil {
		return nil, err
	}

	windowCap := calculateRequiredCapacity(sorted)

	return &FeatureChain{
		features: sorted,
		window:   proto.NewSortedWindow[proto.MarketData](windowCap),
	}, nil
}

// calculateRequiredCapacity finds the maximum lookback period needed
func calculateRequiredCapacity(features []proto.Feature) int {
	maxLookback := 0

	for _, feat := range features {
		if lookback := feat.Lookback(); lookback > maxLookback {
			maxLookback = lookback
		}
	}

	return maxLookback
}

func (fc *FeatureChain) Process(in <-chan proto.MarketData) <-chan proto.ExtendedMarketData {
	out := make(chan proto.ExtendedMarketData) // TODO: buffer size

	go func() {
		defer close(out)

		for candle := range in {
			snap := proto.NewSnapshot()

			// push candle to shared state window
			fc.window.Push(candle)

			for _, feat := range fc.features {
				feat.Update(fc.window, &snap)
			}

			out <- proto.ExtendedMarketData{
				MarketData: candle,
				Indicators: &snap,
			}
		}
	}()

	return out
}

func (fc *FeatureChain) AvailableKeys() []proto.KeyRef {
	keys := make([]proto.KeyRef, len(fc.features))
	for i, feat := range fc.features {
		keys[i] = feat.ID()
	}
	return keys
}

func topologicalSort(features []proto.Feature) ([]proto.Feature, error) {
	// overrides duplicate keys
	idToFeature := make(map[proto.KeyRef]proto.Feature)
	for _, feat := range features {
		idToFeature[feat.ID()] = feat
	}

	inDegree := make(map[proto.KeyRef]int)
	dependents := make(map[proto.KeyRef][]proto.KeyRef)

	for _, feat := range features {
		inDegree[feat.ID()] = len(feat.Dependencies())
		for _, dep := range feat.Dependencies() {
			dependents[dep] = append(dependents[dep], feat.ID())
		}
	}

	var queue []proto.Feature
	for _, f := range features {
		if inDegree[f.ID()] == 0 {
			queue = append(queue, f)
		}
	}

	var sorted []proto.Feature
	for len(queue) > 0 {
		proc := queue[0]
		queue = queue[1:]
		sorted = append(sorted, proc)

		for _, depID := range dependents[proc.ID()] {
			inDegree[depID]--
			if inDegree[depID] == 0 {
				queue = append(queue, idToFeature[depID])
			}
		}
	}

	if len(sorted) != len(features) {
		return nil, fmt.Errorf("circular dependency detected in processors")
	}

	return sorted, nil
}

// ////////////////////////////////////
// Utility functions
// ////////////////////////////////////

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

// checkDuplicates rejects features sharing an ID. Because an ID now encodes
// every parameter that distinguishes one instance from another, a collision
// means two variants were configured identically — a config mistake rather
// than an intent to deduplicate.
func checkDuplicates(features []proto.Feature) error {
	seen := make(map[proto.KeyRef]bool, len(features))
	for _, feat := range features {
		id := feat.ID()
		if seen[id] {
			return fmt.Errorf("duplicate feature %q: two variants are configured identically", id.Name)
		}
		seen[id] = true
	}
	return nil
}

func iDWithPeriod(base string, period int) string {
	return fmt.Sprintf("%s_%d", base, period)
}

// iDWithParam extends a feature ID with a non-default parameter, so that two
// variants differing only in that parameter cannot collide. Defaulted values
// are omitted to keep the common IDs (ema_20, vwap_50) stable — they appear in
// exports and in plotting arguments.
func iDWithParam(base, param string, value, defaultValue float64) string {
	if value == defaultValue {
		return base
	}
	return fmt.Sprintf("%s_%s%s", base, param, strconv.FormatFloat(value, 'g', -1, 64))
}

func roundDecimals(value float64, precision int) float64 {
	multiplier := math.Pow(10, float64(precision))
	return math.Round(value*multiplier) / multiplier
}
