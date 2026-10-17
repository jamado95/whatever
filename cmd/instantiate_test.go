package main

import (
	"testing"

	"whatever/internal/config"
)

func TestInstantiateAccumulatesErrors(t *testing.T) {
	cfg := &config.Config{
		Providers:  []config.ComponentConfig{{Type: "ghost_provider", Name: "p"}},
		Strategies: []config.ComponentConfig{{Type: "ghost_strategy"}},
		Features:   []config.FeatureConfig{{Type: "ghost_feature"}},
	}
	c, errs := instantiate(cfg, testLogger())
	if c == nil {
		t.Fatal("expected non-nil components")
	}
	for _, want := range []string{
		`provider "ghost_provider" not registered`,
		`strategy "ghost_strategy" not registered`,
		`feature "ghost_feature" not registered`,
	} {
		if !hasError(errs, want) {
			t.Errorf("missing error %q, got: %v", want, errs)
		}
	}
}

func TestInstantiateSkipsDisabled(t *testing.T) {
	cfg := &config.Config{
		Providers:  []config.ComponentConfig{{Type: "ghost_provider", Name: "p", Disabled: true}},
		Strategies: []config.ComponentConfig{{Type: "ghost_strategy", Disabled: true}},
		Features:   []config.FeatureConfig{{Type: "ghost_feature", Disabled: true}},
	}
	c, errs := instantiate(cfg, testLogger())
	if len(errs) != 0 {
		t.Fatalf("expected disabled components to be skipped, got: %v", errs)
	}
	if len(c.providers)+len(c.strategies)+len(c.features) != 0 {
		t.Fatal("expected no instantiated components")
	}
}

func TestInstantiateExporterFailure(t *testing.T) {
	cfg := &config.Config{
		Engine: config.EngineConfig{Exporter: map[string]any{"type": "ghost_exporter"}},
	}
	c, errs := instantiate(cfg, testLogger())
	if !hasError(errs, `exporter "ghost_exporter" not registered`) {
		t.Fatalf("expected an exporter error, got: %v", errs)
	}
	if !c.exporterFailed {
		t.Fatal("expected exporterFailed to be set")
	}
}
