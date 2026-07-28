package engine

import (
	"encoding/json"
	"fmt"

	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/timing"
)

// TickerType is a string rather than an iota so that "absent" is
// distinguishable from every legal value. With an integer enum the zero value
// is a valid selection, which is what let a misspelled or unparsed ticker type
// silently mean "realtime".
type TickerType string

const (
	Realtime      TickerType = "realtime"
	FixedInterval TickerType = "fixed"
)

func (t TickerType) IsValid() bool {
	switch t {
	case Realtime, FixedInterval:
		return true
	default:
		return false
	}
}

func ValidTickerTypes() []TickerType {
	return []TickerType{Realtime, FixedInterval}
}

func (t *TickerType) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("ticker type must be a string: %w", err)
	}

	tt := TickerType(s)
	if !tt.IsValid() {
		return fmt.Errorf("invalid ticker type %q, want one of %v", s, ValidTickerTypes())
	}

	*t = tt
	return nil
}

type TickerConfig struct {
	Type     TickerType      `json:"type"`
	Interval config.Duration `json:"interval"`
}

func (c *TickerConfig) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("ticker: missing required 'type' field, want one of %v", ValidTickerTypes())
	}
	if c.Type == FixedInterval && c.Interval.Duration() <= 0 {
		return fmt.Errorf("ticker: 'interval' is required and must be positive when type is %q", FixedInterval)
	}
	return nil
}

// newTicker builds the ticker for a config block. A nil block means no ticker
// was configured, which is an explicit request for realtime pacing.
func newTicker(c *TickerConfig) (timing.Ticker[proto.MarketData], error) {
	if c == nil {
		return timing.Realtime[proto.MarketData](), nil
	}

	switch c.Type {
	case FixedInterval:
		return timing.FixedInterval[proto.MarketData](c.Interval.Duration()), nil
	case Realtime:
		return timing.Realtime[proto.MarketData](), nil
	default:
		return nil, fmt.Errorf("unsupported ticker type %q", c.Type)
	}
}
