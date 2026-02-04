package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Providers  []ComponentConfig `json:"providers"`
	Strategies []ComponentConfig `json:"strategies"`
	Engine     EngineConfig      `json:"engine"`
}

type ComponentConfig struct {
	Type    string         `json:"type"`
	Enabled bool           `json:"enabled"`
	Opts    map[string]any `json:"-"`
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
	if e, ok := raw["enabled"].(bool); ok {
		c.Enabled = e
	}

	delete(raw, "type")
	delete(raw, "enabled")
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
