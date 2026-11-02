package config

import (
	"os"
	"path/filepath"
	"slices"
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

// A subscription without a timeframe is reported alone, its derive windows unchecked.
func TestMissingSubscriptionTimeframe(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "provider": "binance_csv", "derive": ["1h"] }],
      "engine": { "type": "data_logger" }
    }`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("Load succeeded, want a missing-timeframe error")
	}
	wantErrs(t, err.(interface{ Unwrap() []error }).Unwrap(),
		"subscriptions[0]: missing required 'timeframe' field")
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

func TestLoadDerive(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "5m", "provider": "binance_csv", "derive": ["1h", "1d"] }],
      "engine": { "type": "data_logger" }
    }`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got, want := cfg.Subscriptions[0].Derive, proto.NewTimeframeDerivationSet(proto.Timeframe1h, proto.Timeframe1d); got != want {
		t.Errorf("derive = %v, want %v", got.List(), want.List())
	}
}

func TestDeriveNotCoarserRejected(t *testing.T) {
	for _, derive := range []string{"1h", "5m"} {
		body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv", "derive": ["` + derive + `"] }],
      "engine": { "type": "data_logger" }
    }`
		_, err := Load(writeConfig(t, body))
		if err == nil || !strings.Contains(err.Error(), "must be coarser") {
			t.Errorf("derive %s from 1h: expected a must-be-coarser error, got: %v", derive, err)
		}
	}
}

func TestSymbolTimeframesOnOneProviderAccepted(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [
        { "symbol": "BTCUSDT", "timeframe": "5m", "provider": "binance_csv" },
        { "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv" }
      ],
      "engine": { "type": "data_logger" }
    }`
	if _, err := Load(writeConfig(t, body)); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestDerivedProviderPrefixRejected(t *testing.T) {
	body := `{
      "providers": [{ "type": "binance_csv", "name": "derived_x", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "derived_x" }],
      "engine": { "type": "data_logger" }
    }`
	_, err := Load(writeConfig(t, body))
	if err == nil || !strings.Contains(err.Error(), "must not start with") {
		t.Fatalf("expected a reserved-prefix error, got: %v", err)
	}
}

// universeConfig is a valid config around a universes block and a features block.
func universeConfig(universes, features string) string {
	return `{
      "providers": [{ "type": "binance_csv", "file": "x.csv" }],
      "subscriptions": [{ "symbol": "BTCUSDT", "timeframe": "1h", "provider": "binance_csv" }],
      "universes": ` + universes + `,
      "features": ` + features + `,
      "engine": { "type": "data_logger" }
    }`
}

func TestLoadUniverses(t *testing.T) {
	body := universeConfig(`[
      { "name": "usdt", "rules": [{ "symbol": ["*USDT"], "kind": ["direct"] }] },
      { "name": "lead", "rules": [
        { "references": ["BTCUSDT:1h:binance_csv"], "timeframe": ["1h"] },
        { "pairs": [["BTCUSDT:1h:binance_csv", "ETHUSDT:1h:binance_csv"]] }
      ] },
      { "name": "all_pairs", "rules": [{ "pairs": "*" }] }
    ]`, `[{ "type": "rsi", "universe": "usdt" }]`)
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(cfg.Universes) != 3 {
		t.Fatalf("got %d universes, want 3", len(cfg.Universes))
	}
	usdt, lead, all := cfg.Universes[0], cfg.Universes[1], cfg.Universes[2]
	if usdt.Name != "usdt" || usdt.Rules[0].Symbol[0] != "*USDT" || usdt.Rules[0].Kind[0] != proto.StreamDirect {
		t.Errorf("usdt = %+v, want a *USDT direct filter", usdt)
	}
	if lead.Rules[0].References[0] != "BTCUSDT:1h:binance_csv" || !lead.Rules[1].Pairs.explicit() ||
		len(lead.Rules[1].Pairs.List) != 1 {
		t.Errorf("lead = %+v, want a references rule and one explicit pair", lead)
	}
	if !all.Rules[0].Pairs.All {
		t.Errorf("all_pairs = %+v, want \"*\"", all)
	}
	if cfg.Features[0].Universe != "usdt" {
		t.Errorf("feature universe = %q, want usdt", cfg.Features[0].Universe)
	}
}

func TestUniverseUnknownFieldRejected(t *testing.T) {
	body := universeConfig(`[{ "name": "u", "rules": [{ "refrences": ["BTCUSDT:1h:binance_csv"] }] }]`, `[]`)
	_, err := Load(writeConfig(t, body))
	if err == nil || !strings.Contains(err.Error(), `unknown field "refrences"`) {
		t.Fatalf("expected refrences to be rejected as an unknown field, got: %v", err)
	}
}

func TestUniverseErrors(t *testing.T) {
	cases := []struct {
		name      string
		universes string
		features  string
		want      []string
	}{
		{"empty and duplicate names",
			`[{ "rules": [{}] }, { "name": "u", "rules": [{}] }, { "name": "u", "rules": [{}] }]`, `[]`,
			[]string{"universes[0]: missing required 'name' field", `universes[2]: duplicate universe name "u"`}},
		{"an unknown universe on an enabled feature",
			`[]`, `[{ "type": "rsi", "universe": "usdt" }, { "type": "ema", "universe": "usdt", "disabled": true }]`,
			[]string{`features[0] (rsi): unknown universe "usdt"`}},
		{"rule shape errors prefixed with their universe",
			`[{ "name": "u", "rules": [{ "symbol": [] }, { "references": [""], "pairs": "*" }] }, { "name": "v", "rules": [] }]`,
			`[]`,
			[]string{
				"universes[0] (u): rules[0].symbol: empty list",
				"universes[0] (u): rules[1]: references and pairs are exclusive",
				"universes[0] (u): rules[1].references: empty value",
				"universes[1] (v): rules: at least one rule is required",
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, universeConfig(tc.universes, tc.features)))
			if err == nil {
				t.Fatal("Load succeeded, want errors")
			}
			wantErrs(t, err.(interface{ Unwrap() []error }).Unwrap(), tc.want...)
		})
	}
}

func TestResolveUniverses(t *testing.T) {
	named := func(name string, rules ...string) Universe {
		u := universe(t, rules...)
		u.Name = name
		return u
	}
	cfg := &Config{
		Universes: []Universe{
			named("btc", `{"symbol": ["BTC*"], "timeframe": ["1h"]}`),
			named("typo", `{"references": ["BTCUSDT:1h:binance"]}`),
			named("empty", `{"symbol": ["SOL*"]}`),
			named("unused", `{"symbol": ["SOL*"]}`),
			named("disabled_only", `{"symbol": ["SOL*"]}`),
		},
		Features: []FeatureConfig{
			{Type: "rsi", Universe: "btc"},
			{Type: "sma", Universe: "typo"},
			{Type: "ema", Universe: "empty"},
			{Type: "vwap", Universe: "disabled_only", Disabled: true},
		},
	}
	st := testStreams()

	got, errs := cfg.ResolveUniverses(st)
	wantErrs(t, errs,
		`universes[1] (typo): rules[0].references[0]: no stream has ID "BTCUSDT:1h:binance"`,
		"universes[2] (empty): selects no streams")

	if len(got) != len(cfg.Universes) {
		t.Fatalf("got %d universes, want one per name", len(got))
	}
	want := []int{index(t, st, btc1h), index(t, st, btc1hD)}
	slices.Sort(want)
	if !slices.Equal(got["btc"].Streams, want) {
		t.Errorf("btc = %s, want BTC 1h streams", render(st, got["btc"]))
	}
}

func TestValidateCollectsErrors(t *testing.T) {
	body := `{
      "providers": [
        { "type": "binance_csv", "name": "p", "file": "x.csv" },
        { "type": "binance_csv", "name": "p", "file": "y.csv" }
      ],
      "features": [{ "type": "" }],
      "universes": [{ "name": "u", "rules": [{ "kind": [] }] }],
      "subscriptions": [
        { "symbol": "BTCUSDT", "timeframe": "1h" },
        { "symbol": "BTCUSDT", "timeframe": "1h", "provider": "p", "derive": ["5m"] }
      ],
      "engine": {}
    }`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("Load succeeded, want errors")
	}
	wantErrs(t, err.(interface{ Unwrap() []error }).Unwrap(),
		"features[0]: missing required 'type' field",
		"subscriptions[0]: missing required 'provider' field",
		`subscriptions[1] (BTCUSDT:1h:p): derive "5m" must be coarser than "1h"`,
		`providers[1]: duplicate provider name "p"`,
		"universes[0] (u): rules[0].kind: empty list",
		"engine: missing required 'type' field")
}
