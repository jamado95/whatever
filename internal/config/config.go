package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	proto "whatever/internal/protocol"
)

type Config struct {
	Providers     []ComponentConfig    `json:"providers"`
	Strategies    []ComponentConfig    `json:"strategies"`
	Features      []FeatureConfig      `json:"features"`
	Subscriptions []proto.Subscription `json:"subscriptions"`
	Engine        EngineConfig         `json:"engine"`
}

type FeatureConfig struct {
	Type     string           `json:"type"`
	Disabled bool             `json:"disabled"`
	Variants []map[string]any `json:"variants"`
}

// ComponentConfig is a component block whose non-reserved keys are the
// component's own options, validated by its factory rather than here.
type ComponentConfig struct {
	Type     string `json:"type"`
	Disabled bool   `json:"disabled"`
	// Name is a provider's instance identity, referenced by a subscription's
	// provider field; it defaults to Type.
	Name string `json:"-"`
	// Subscriptions holds a strategy's subscription ID() references, resolved
	// by the composition root rather than the component factory.
	Subscriptions []string       `json:"-"`
	Opts          map[string]any `json:"-"`
}

// EngineConfig separates the keys the composition root resolves into instances
// (Strategies, Exporter) from the engine's own tuning, which stays raw under
// Options for the engine factory to decode. Keeping the two key sets disjoint is
// what lets each be validated strictly by exactly one reader. The provider is no
// longer named here — subscriptions carry it.
type EngineConfig struct {
	Type       string         `json:"type"`
	Strategies []string       `json:"strategies"`
	Exporter   map[string]any `json:"exporter"`
	Options    map[string]any `json:"options"`
}

// Opts builds the option map handed to an engine factory: the raw options
// block, into which the composition root injects resolved dependencies. The
// resolution keys (strategies, exporter) are deliberately absent — they are this
// package's to read, not the engine's.
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

	// name defaults to type; a provider uses it as the instance identity a
	// subscription references.
	c.Name = c.Type
	if n, ok := raw["name"].(string); ok && n != "" {
		c.Name = n
	}

	// subscriptions are a strategy's subscription references, read by the
	// composition root.
	if subs, ok := raw["subscriptions"].([]any); ok {
		c.Subscriptions = make([]string, 0, len(subs))
		for _, s := range subs {
			str, ok := s.(string)
			if !ok {
				return fmt.Errorf("component %q: 'subscriptions' entries must be strings", c.Type)
			}
			c.Subscriptions = append(c.Subscriptions, str)
		}
	}

	delete(raw, "type")
	delete(raw, "disabled")
	delete(raw, "name")
	delete(raw, "subscriptions")
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

	if len(c.Subscriptions) == 0 {
		return fmt.Errorf("subscriptions: at least one subscription is required")
	}
	seen := make(map[string]bool, len(c.Subscriptions))
	for i, s := range c.Subscriptions {
		if s.Symbol == "" {
			return fmt.Errorf("subscriptions[%d]: missing required 'symbol' field", i)
		}
		if s.Timeframe == "" {
			return fmt.Errorf("subscriptions[%d]: missing required 'timeframe' field", i)
		}
		if s.Provider == "" {
			return fmt.Errorf("subscriptions[%d]: missing required 'provider' field", i)
		}
		if seen[s.ID()] {
			return fmt.Errorf("subscriptions[%d]: duplicate subscription %q", i, s.ID())
		}
		seen[s.ID()] = true
	}

	// Provider instance names must be unique so a subscription's provider
	// reference is unambiguous.
	names := make(map[string]bool, len(c.Providers))
	for i, p := range c.Providers {
		if p.Disabled {
			continue
		}
		if names[p.Name] {
			return fmt.Errorf("providers[%d]: duplicate provider name %q", i, p.Name)
		}
		names[p.Name] = true
	}

	if c.Engine.Type == "" {
		return fmt.Errorf("engine: missing required 'type' field")
	}

	return nil
}
