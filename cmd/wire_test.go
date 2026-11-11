package main

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"whatever/internal/config"
	"whatever/internal/engine/features"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
)

type stubProvider struct{}

func (stubProvider) ID() string                                       { return "stub" }
func (stubProvider) Subscription() proto.Subscription                 { return proto.Subscription{} }
func (stubProvider) Init(proto.Subscription) error                    { return nil }
func (stubProvider) Streams() (<-chan proto.MarketData, <-chan error) { return nil, nil }
func (stubProvider) Start() error                                     { return nil }
func (stubProvider) Close()                                           {}

func stubComponents() *components {
	return &components{
		providers:  map[string]proto.DataProvider{"p": stubProvider{}},
		strategies: map[string]proto.Strategy{},
	}
}

func wireConfig() *config.Config {
	return &config.Config{
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "p"}},
		Engine:        config.EngineConfig{Type: "data_logger"},
	}
}

func TestWireResolved(t *testing.T) {
	eng, errs := wire(wireConfig(), stubComponents(), testLogger())
	if len(errs) != 0 {
		t.Fatalf("wire errors: %v", errs)
	}
	if eng == nil {
		t.Fatal("expected a constructed engine")
	}
}

func TestWireUnresolvedSubscriptionProvider(t *testing.T) {
	cfg := wireConfig()
	cfg.Subscriptions[0].Provider = "ghost"

	eng, errs := wire(cfg, stubComponents(), testLogger())
	if !hasError(errs, `references provider "ghost"`) {
		t.Fatalf("expected an unresolved-provider error, got: %v", errs)
	}
	if eng != nil {
		t.Fatal("expected no engine")
	}
}

// TestWireSplitsEngineErrors covers an engine factory's error: a joined one becomes one error per
// part, each prefixed with the engine's type, and a single one stays one.
func TestWireSplitsEngineErrors(t *testing.T) {
	reg.Engines.Register("joined_errors_test", func(map[string]any) (reg.Runnable, error) {
		return nil, errors.Join(errors.New("first"), errors.New("second"))
	})
	reg.Engines.Register("single_error_test", func(map[string]any) (reg.Runnable, error) {
		return nil, errors.New("only")
	})

	for engine, want := range map[string][]string{
		"joined_errors_test": {`engine "joined_errors_test": first`, `engine "joined_errors_test": second`},
		"single_error_test":  {`engine "single_error_test": only`},
	} {
		cfg := wireConfig()
		cfg.Engine.Type = engine

		eng, errs := wire(cfg, stubComponents(), testLogger())
		got := make([]string, len(errs))
		for i, err := range errs {
			got[i] = err.Error()
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s: errors %q, want %q", engine, got, want)
		}
		if eng != nil {
			t.Errorf("%s: expected no engine", engine)
		}
	}
}

// TestWireInjectsSpecs covers the feature engine's inputs: the components' specs reach the engine's
// options as _featuresv2, and their universes as _universes.
func TestWireInjectsSpecs(t *testing.T) {
	var opts map[string]any
	reg.Engines.Register("recording_test", func(o map[string]any) (reg.Runnable, error) {
		opts = o
		return nil, nil
	})

	c := stubComponents()
	c.featuresV2 = []features.Spec{{Type: "rsi", Universe: "btc", Feature: feature.Feature{ID: "rsi_14"}}}
	c.universes = map[string]config.Resolved{"btc": {Streams: []int{0}}}
	cfg := wireConfig()
	cfg.Engine.Type = "recording_test"

	if _, errs := wire(cfg, c, testLogger()); len(errs) != 0 {
		t.Fatalf("wire errors: %v", errs)
	}
	if !reflect.DeepEqual(opts["_featuresv2"], c.featuresV2) {
		t.Errorf("_featuresv2 = %v, want %v", opts["_featuresv2"], c.featuresV2)
	}
	if !reflect.DeepEqual(opts["_universes"], c.universes) {
		t.Errorf("_universes = %v, want %v", opts["_universes"], c.universes)
	}
}

func TestWireMissingEngineStrategy(t *testing.T) {
	cfg := wireConfig()
	cfg.Engine.Strategies = []string{"ghost"}

	eng, errs := wire(cfg, stubComponents(), testLogger())
	if !hasError(errs, `engine references strategy "ghost"`) {
		t.Fatalf("expected a missing-strategy error, got: %v", errs)
	}
	if eng != nil {
		t.Fatal("expected no engine")
	}
}
