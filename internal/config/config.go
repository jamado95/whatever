package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	proto "whatever/internal/protocol"
)

type Config struct {
	Providers    []ComponentConfig  `json:"providers"`
	Strategies   []ComponentConfig  `json:"strategies"`
	Features     []FeatureConfig    `json:"features"`
	Subscription proto.Subscription `json:"subscription"`
	Engine       EngineConfig       `json:"engine"`
}

type FeatureConfig struct {
	Type     string           `json:"type"`
	Disabled bool             `json:"disabled"`
	Variants []map[string]any `json:"variants"`
}

// ComponentConfig is a component block whose non-reserved keys are the
// component's own options, validated by its factory rather than here.
type ComponentConfig struct {
	Type     string         `json:"type"`
	Disabled bool           `json:"disabled"`
	Opts     map[string]any `json:"-"`
}

// EngineConfig separates the keys the composition root resolves into instances
// (Provider, Strategies, Exporter) from the engine's own tuning, which stays
// raw under Options for the engine factory to decode. Keeping the two key sets
// disjoint is what lets each be validated strictly by exactly one reader.
type EngineConfig struct {
	Type       string         `json:"type"`
	Provider   string         `json:"provider"`
	Strategies []string       `json:"strategies"`
	Exporter   map[string]any `json:"exporter"`
	Options    map[string]any `json:"options"`
}

// Opts builds the option map handed to an engine factory: the raw options
// block, into which the composition root injects resolved dependencies. The
// resolution keys (provider, strategies, exporter) are deliberately absent —
// they are this package's to read, not the engine's.
func (e EngineConfig) Opts() map[string]any {
	opts := make(map[string]any, 5)
	if e.Options != nil {
		opts[OptionsKey] = e.Options
	}
	return opts
}

// UnmarshalJSON collects every key that is not reserved into Opts. This is the
// one block type that cannot reject unknown keys here — the remainder is by
// definition the component's own config, and is checked when its factory
// decodes it.
func (c *ComponentConfig) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if t, ok := raw["type"].(string); ok {
		c.Type = t
	}
	if c.Type == "" {
		return fmt.Errorf("component config missing required 'type' field")
	}
	if d, ok := raw["disabled"].(bool); ok {
		c.Disabled = d
	}

	delete(raw, "type")
	delete(raw, "disabled")
	c.Opts = raw

	return nil
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	for i, f := range c.Features {
		if f.Type == "" {
			return fmt.Errorf("features[%d]: missing required 'type' field", i)
		}
	}

	if c.Subscription.Symbol == "" {
		return fmt.Errorf("subscription: missing required 'symbol' field")
	}
	if c.Subscription.Timeframe == "" {
		return fmt.Errorf("subscription: missing required 'timeframe' field")
	}

	if c.Engine.Type == "" {
		return fmt.Errorf("engine: missing required 'type' field")
	}
	if c.Engine.Provider == "" {
		return fmt.Errorf("engine: missing required 'provider' field")
	}

	return nil
}
