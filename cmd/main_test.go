package main

import (
	"bytes"
	"errors"
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

func TestBuildDataLoggerV2AcceptsDerivations(t *testing.T) {
	cfg := &config.Config{
		Providers: []config.ComponentConfig{csvProvider("binance_csv")},
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "binance_csv",
			Derive: proto.NewTimeframeDerivationSet(proto.Timeframe1h)}},
		Engine: config.EngineConfig{Type: "data_logger_v2"},
	}
	eng, errs := build(cfg, testLogger())
	if len(errs) > 0 || eng == nil {
		t.Fatalf("expected data_logger_v2 to build with derivations, got: %v", errs)
	}
}

// TestBuildReportsExporterAndEngineErrors covers a failed exporter: the engine is still built,
// without it, so its own option errors are reported in the same pass.
func TestBuildReportsExporterAndEngineErrors(t *testing.T) {
	cfg := &config.Config{
		Providers:     []config.ComponentConfig{csvProvider("binance_csv")},
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "binance_csv"}},
		Engine: config.EngineConfig{
			Type:     "data_logger_v2",
			Exporter: map[string]any{"type": "ghost_exporter"},
			Options:  map[string]any{"limit": float64(-1)},
		},
	}
	eng, errs := build(cfg, testLogger())
	for _, want := range []string{
		`exporter "ghost_exporter" not registered`,
		`engine "data_logger_v2": limit must not be negative`,
	} {
		if !hasError(errs, want) {
			t.Errorf("missing error %q, got: %v", want, errs)
		}
	}
	if eng != nil {
		t.Error("expected no engine")
	}
}

func TestReportErrorsSplitsJoined(t *testing.T) {
	var out bytes.Buffer
	log := logger.NewLogger(logger.LoggerConfig{Level: "debug", Output: &out})
	reportErrors(log, "c.json", []error{errors.Join(errors.New("first"), errors.New("second")), errors.New("third")})

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	want := []string{"c.json: first", "c.json: second", "c.json: third", "c.json: 3 config error(s)"}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d:\n%s", len(lines), len(want), out.String())
	}
	for i, w := range want {
		if !strings.Contains(lines[i], w) {
			t.Errorf("line %d = %s, want it to contain %q", i, lines[i], w)
		}
	}
}

func TestBuildReportsUniverseErrors(t *testing.T) {
	cfg := &config.Config{
		Providers: []config.ComponentConfig{csvProvider("binance_csv")},
		Features:  []config.FeatureConfig{{Type: "ghost_feature", Universe: "btc"}},
		Universes: []config.Universe{
			{Name: "btc", Rules: []config.Rule{{References: []string{"BTCUSDT:1h:ghost"}}}},
		},
		Subscriptions: []proto.Subscription{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "binance_csv"}},
		Engine:        config.EngineConfig{Type: "data_logger"},
	}
	_, errs := build(cfg, testLogger())
	for _, want := range []string{
		`universe "btc": rules[0].references[0]: no stream has ID "BTCUSDT:1h:ghost"`,
		`feature "ghost_feature" not registered`,
	} {
		if !hasError(errs, want) {
			t.Errorf("missing error %q, got: %v", want, errs)
		}
	}
}
