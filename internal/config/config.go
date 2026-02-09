package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Providers  []ComponentConfig `json:"providers"`
	Strategies []ComponentConfig `json:"strategies"`
	Features   []FeatureConfig   `json:"features"`
	Engine     EngineConfig      `json:"engine"`
}

type FeatureConfig struct {
	Type     string           `json:"type"`
	Disabled bool             `json:"disabled"`
	Variants []map[string]any `json:"variants"`
}

func (f *FeatureConfig) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if t, ok := raw["type"].(string); ok {
		f.Type = t
	}
	if f.Type == "" {
		return fmt.Errorf("feature config missing required 'type' field")
	}
	if d, ok := raw["disabled"].(bool); ok {
		f.Disabled = d
	}
	if v, ok := raw["variants"].([]any); ok {
		f.Variants = make([]map[string]any, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				f.Variants = append(f.Variants, m)
			}
		}
	}

	return nil
}

type ComponentConfig struct {
	Type     string         `json:"type"`
	Disabled bool           `json:"disabled"`
	Opts     map[string]any `json:"-"`
}

type EngineConfig struct {
	Type string         `json:"type"`
	Opts map[string]any `json:"-"`
}

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

func (e *EngineConfig) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if t, ok := raw["type"].(string); ok {
		e.Type = t
	}
	if e.Type == "" {
		return fmt.Errorf("engine config missing required 'type' field")
	}

	delete(raw, "type")
	e.Opts = raw

	return nil
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &cfg, nil
}
