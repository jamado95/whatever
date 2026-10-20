package main

import (
	"strings"
	"testing"

	"whatever/internal/config"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

func testLogger() *logger.Logger {
	return logger.NewLogger(logger.DefaultLoggerConfig())
}

func csvProvider(name string) config.ComponentConfig {
	return config.ComponentConfig{
		Type: "binance_csv",
		Name: name,
		Opts: map[string]any{"file": "x.csv"},
	}
}

func hasError(errs []error, substr string) bool {
	for _, e := range errs {
		if strings.Contains(e.Error(), substr) {
			return true
		}
	}
	return false
}

func TestBuildSingleSubscription(t *testing.T) {
	cfg := &config.Config{
		Providers:     []config.ComponentConfig{csvProvider("binance_csv")},
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "binance_csv"}},
		Engine:        config.EngineConfig{Type: "data_logger"},
	}
	eng, errs := build(cfg, testLogger())
	if len(errs) != 0 {
		t.Fatalf("build errors: %v", errs)
	}
	if eng == nil {
		t.Fatal("expected a constructed engine")
	}
}

func TestBuildMultiSubscription(t *testing.T) {
	cfg := &config.Config{
		Providers: []config.ComponentConfig{csvProvider("btcusdt_1m"), csvProvider("btcusdt_1h")},
		Subscriptions: []proto.Subscription{
			{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1m, Provider: "btcusdt_1m"},
			{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "btcusdt_1h"},
		},
		Engine: config.EngineConfig{Type: "data_logger"},
	}
	eng, errs := build(cfg, testLogger())
	if len(errs) != 0 {
		t.Fatalf("build errors: %v", errs)
	}
	if eng == nil {
		t.Fatal("expected a constructed engine for a multi-subscription config")
	}
}

func TestBuildUnresolvedSubscriptionProvider(t *testing.T) {
	cfg := &config.Config{
		Providers:     []config.ComponentConfig{csvProvider("binance_csv")},
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "ghost"}},
		Engine:        config.EngineConfig{Type: "data_logger"},
	}
	_, errs := build(cfg, testLogger())
	if !hasError(errs, `references provider "ghost"`) {
		t.Fatalf("expected an unresolved-provider error, got: %v", errs)
	}
}

func TestBuildUnresolvedStrategySubscription(t *testing.T) {
	cfg := &config.Config{
		Providers: []config.ComponentConfig{csvProvider("binance_csv")},
		Strategies: []config.ComponentConfig{
			{Type: "buy_the_dip", Name: "buy_the_dip", Subscriptions: []string{"ghost:1h:binance_csv"}},
		},
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "binance_csv"}},
		Engine:        config.EngineConfig{Type: "data_logger"},
	}
	_, errs := build(cfg, testLogger())
	if !hasError(errs, "not declared") {
		t.Fatalf("expected an unresolved-subscription error, got: %v", errs)
	}
}

func TestBuildDataLoggerRejectsDerivations(t *testing.T) {
	cfg := &config.Config{
		Providers: []config.ComponentConfig{csvProvider("binance_csv")},
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "binance_csv",
			Derive: proto.NewTimeframeDerivationSet(proto.Timeframe1h)}},
		Engine: config.EngineConfig{Type: "data_logger"},
	}
	_, errs := build(cfg, testLogger())
	if !hasError(errs, "derived subscriptions are not supported") {
		t.Fatalf("expected a derivations-not-supported error, got: %v", errs)
	}
}
