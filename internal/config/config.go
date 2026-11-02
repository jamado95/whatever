package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	proto "whatever/internal/protocol"
)

type Config struct {
	Providers     []ComponentConfig    `json:"providers"`
	Strategies    []ComponentConfig    `json:"strategies"`
	Features      []FeatureConfig      `json:"features"`
	Universes     []Universe           `json:"universes"`
	Subscriptions []proto.Subscription `json:"subscriptions"`
	Engine        EngineConfig         `json:"engine"`
}

type FeatureConfig struct {
	Type     string           `json:"type"`
	Disabled bool             `json:"disabled"`
	Universe string           `json:"universe"` // a universe's name; every variant runs on it
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

	if errs := cfg.validate(); len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &cfg, nil
}

// validate returns every shape error in the file: features, subscriptions, providers, universes and
// the engine, each path-named.
func (c *Config) validate() []error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	universes := make(map[string]bool, len(c.Universes))
	for _, u := range c.Universes {
		universes[u.Name] = true
	}

	for i, f := range c.Features {
		if f.Type == "" {
			fail("features[%d]: missing required 'type' field", i)
		}
		if !f.Disabled && f.Universe != "" && !universes[f.Universe] {
			fail("features[%d] (%s): unknown universe %q", i, f.Type, f.Universe)
		}
	}

	if len(c.Subscriptions) == 0 {
		fail("subscriptions: at least one subscription is required")
	}
	seen := make(map[string]bool, len(c.Subscriptions))
	for i, s := range c.Subscriptions {
		missing := len(errs)

		sv := reflect.ValueOf(s)
		for f := 0; f < sv.NumField(); f++ {
			if field := sv.Field(f); field.Kind() == reflect.String && field.String() == "" {
				fail("subscriptions[%d]: missing required '%s' field", i, strings.ToLower(sv.Type().Field(f).Name))
			}
		}
		// an incomplete subscription has no ID, and no timeframe to derive from
		if len(errs) > missing {
			continue
		}

		if seen[s.ID()] {
			fail("subscriptions[%d]: duplicate subscription %q", i, s.ID())
		}
		seen[s.ID()] = true

		// Each derived window must be coarser than its source and a multiple of it.
		for _, tf := range s.Derive.List() {
			if tf.TimeframeMs() <= s.Timeframe.TimeframeMs() {
				fail("subscriptions[%d] (%s): derive %q must be coarser than %q", i, s.ID(), tf, s.Timeframe)
			} else if tf.TimeframeMs()%s.Timeframe.TimeframeMs() != 0 {
				fail("subscriptions[%d] (%s): derive %q must be a multiple of %q", i, s.ID(), tf, s.Timeframe)
			}
		}
	}

	// Provider instance names must be unique so a subscription's provider
	// reference is unambiguous.
	names := make(map[string]bool, len(c.Providers))
	for i, p := range c.Providers {
		if p.Disabled {
			continue
		}
		if names[p.Name] {
			fail("providers[%d]: duplicate provider name %q", i, p.Name)
		}
		// Reserved for derived streams, so their IDs never collide with direct ones.
		if strings.HasPrefix(p.Name, proto.DerivedProviderPrefix) {
			fail("providers[%d]: name %q must not start with %q", i, p.Name, proto.DerivedProviderPrefix)
		}
		names[p.Name] = true
	}

	named := make(map[string]bool, len(c.Universes))
	for i, u := range c.Universes {
		if u.Name == "" {
			fail("universes[%d]: missing required 'name' field", i)
		} else if named[u.Name] {
			fail("universes[%d]: duplicate universe name %q", i, u.Name)
		}
		named[u.Name] = true

		for _, err := range u.check() {
			errs = append(errs, fmt.Errorf("%s: %w", universePath(i, u.Name), err))
		}
	}

	if c.Engine.Type == "" {
		fail("engine: missing required 'type' field")
	}

	return errs
}
