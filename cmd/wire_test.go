package main

import (
	"errors"
	"slices"
	"testing"

	"whatever/internal/config"
	proto "whatever/internal/protocol"
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

func TestWireExporterFailedSkipsEngine(t *testing.T) {
	c := stubComponents()
	c.exporterFailed = true

	eng, errs := wire(wireConfig(), c, testLogger())
	if len(errs) != 0 {
		t.Fatalf("expected no wiring errors, got: %v", errs)
	}
	if eng != nil {
		t.Fatal("expected engine construction to be skipped")
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
