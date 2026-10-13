package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	proto "whatever/internal/protocol"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const validConfig = `{
  "providers": [{ "type": "binance_csv", "file": "x.csv" }],
  "features": [],
  "strategies": [],
  "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv" }],
  "engine": { "type": "data_logger" }
}`

func TestLoadSubscriptions(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Subscriptions) != 1 {
		t.Fatalf("got %d subscriptions, want 1", len(cfg.Subscriptions))
	}
	s := cfg.Subscriptions[0]
	if s.Symbol != "BTCUSDT" || s.Timeframe != proto.Timeframe1h || s.Provider != "binance_csv" {
		t.Errorf("subscription = %+v, want BTCUSDT/1h/binance_csv", s)
	}
}

func TestSubscriptionID(t *testing.T) {
	s := proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "binance_csv"}
	if got := s.ID(); got != "BTCUSDT:1h:binance_csv" {
		t.Errorf("ID() = %q, want BTCUSDT:1h:binance_csv", got)
	}
}

func TestDuplicateSubscriptionRejected(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [
        { "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv" },
        { "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv" }
      ],
      "engine": { "type": "data_logger" }
    }`
	_, err := Load(writeConfig(t, body))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected a duplicate-subscription error, got: %v", err)
	}
}

func TestProviderNameDefaultsToType(t *testing.T) {
	cfg, err := Load(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Providers[0].Name != "binance_csv" {
		t.Errorf("provider name = %q, want the type as default", cfg.Providers[0].Name)
	}
}

func TestProviderExplicitNameStrippedFromOpts(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "name": "primary", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "primary" }],
      "engine": { "type": "data_logger" }
    }`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Providers[0].Name != "primary" {
		t.Errorf("provider name = %q, want primary", cfg.Providers[0].Name)
	}
	if _, ok := cfg.Providers[0].Opts["name"]; ok {
		t.Error("'name' should be a reserved key, not left in Opts")
	}
	if _, ok := cfg.Providers[0].Opts["file"]; !ok {
		t.Error("'file' should remain in Opts for the factory")
	}
}

func TestStrategySubscriptionsReserved(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "strategies": [{ "type": "pairs", "subscriptions": ["BTCUSDT:1h:binance_csv"], "window": 20 }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv" }],
      "engine": { "type": "data_logger" }
    }`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Strategies[0]
	if len(got.Subscriptions) != 1 || got.Subscriptions[0] != "BTCUSDT:1h:binance_csv" {
		t.Errorf("strategy subscriptions = %v, want [BTCUSDT:1h:binance_csv]", got.Subscriptions)
	}
	if _, ok := got.Opts["subscriptions"]; ok {
		t.Error("'subscriptions' should be a reserved key, not left in Opts")
	}
	if _, ok := got.Opts["window"]; !ok {
		t.Error("strategy options should remain in Opts for the factory")
	}
}

func TestEngineProviderKeyRejected(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv" }],
      "engine": { "type": "data_logger", "provider": "binance_csv" }
    }`
	_, err := Load(writeConfig(t, body))
	if err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("expected engine.provider to be rejected as an unknown field, got: %v", err)
	}
}

func TestMissingSubscriptionProvider(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h" }],
      "engine": { "type": "data_logger" }
    }`
	_, err := Load(writeConfig(t, body))
	if err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("expected a missing-provider error, got: %v", err)
	}
}

func TestNoSubscriptionsRejected(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [],
      "engine": { "type": "data_logger" }
    }`
	_, err := Load(writeConfig(t, body))
	if err == nil || !strings.Contains(err.Error(), "at least one subscription") {
		t.Fatalf("expected an empty-subscriptions error, got: %v", err)
	}
}
