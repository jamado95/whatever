package main

import (
	"maps"
	"slices"
	"testing"

	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
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
	if c.exporter != nil {
		t.Fatal("expected no exporter")
	}
}

/*
TestInstantiateSpecs covers the feature engine's variants: one spec per enabled variant with its type
and universe, none for a disabled entry, none on a universe that failed to resolve; a type missing from
reg.FeatureTypes and a variant failing Validate are reported.
*/
func TestInstantiateSpecs(t *testing.T) {
	reg.Features.Register("v1_only_test", func(opts map[string]any) (proto.Feature, error) {
		return reg.Features.Create("sma", opts)
	})
	reg.FeatureTypes.Register("dash_test", func(map[string]any) (feature.Feature, error) {
		return feature.Feature{ID: "dash-1"}, nil
	})

	cfg := &config.Config{
		Subscriptions: []config.SubscriptionConfig{{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "p"}},
		Universes: []proto.Universe{
			{Name: "btc", Rules: []proto.UniverseRule{{Symbol: []string{"BTC*"}}}},
			{Name: "typo", Rules: []proto.UniverseRule{{References: []string{"BTCUSDT:1h:ghost"}}}},
		},
		Features: []config.FeatureConfig{
			{Type: "rsi", Universe: "btc", Variants: []map[string]any{{"period": 14}, {"period": 21}}},
			{Type: "engulfing_candle"},
			{Type: "ema", Disabled: true, Variants: []map[string]any{{"period": 20}}},
			{Type: "vwap", Universe: "typo", Variants: []map[string]any{{"period": 20}}},
			{Type: "v1_only_test", Variants: []map[string]any{{"period": 3}}},
			{Type: "dash_test"},
		},
	}
	c, errs := instantiate(cfg, testLogger())

	got := make([]string, len(c.featuresV2))
	for i, s := range c.featuresV2 {
		got[i] = s.Type + " " + s.Feature.ID + " " + s.Universe
	}
	want := []string{"rsi rsi_14 btc", "rsi rsi_21 btc", "engulfing_candle engulfing_candle "}
	if !slices.Equal(got, want) {
		t.Errorf("specs %q, want %q", got, want)
	}
	if _, ok := c.universes["typo"]; ok {
		t.Error("a universe that failed to resolve is in the map")
	}

	for _, want := range []string{
		`feature "v1_only_test" not registered`,
		`feature "dash_test" variant 0: feature: ID "dash-1" contains '-'`,
	} {
		if !hasError(errs, want) {
			t.Errorf("missing error %q, got: %v", want, errs)
		}
	}
}

/*
TestInstantiateUniverses covers universe resolution: an unmatched name and a used universe selecting no
stream are errors, each kept out of the map; an unused or disabled-only empty universe resolves.
*/
func TestInstantiateUniverses(t *testing.T) {
	btc := proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "btc_1h"}
	eth := proto.Subscription{Symbol: "ETHUSDT", Timeframe: proto.Timeframe1h, Provider: "eth_1h"}
	c := &components{
		streams:   proto.NewStreams([]proto.Subscription{btc, eth}),
		universes: make(map[string]proto.ResolvedUniverse),
	}
	sol := []proto.UniverseRule{{Symbol: []string{"SOL*"}}}
	universes := []proto.Universe{
		{Name: "btc", Rules: []proto.UniverseRule{{Symbol: []string{"BTC*"}}}},
		{Name: "typo", Rules: []proto.UniverseRule{{References: []string{"BTCUSDT:1h:binance"}}}},
		{Name: "empty", Rules: sol},
		{Name: "unused", Rules: sol},
		{Name: "disabled_only", Rules: sol},
	}
	features := []config.FeatureConfig{
		{Type: "rsi", Universe: "btc"},
		{Type: "sma", Universe: "typo"},
		{Type: "ema", Universe: "empty"},
		{Type: "ema", Universe: "empty"}, // reported once
		{Type: "vwap", Universe: "disabled_only", Disabled: true},
	}

	var got []string
	for _, u := range universes {
		for _, err := range c.addUniverse(u, features) {
			got = append(got, err.Error())
		}
	}
	want := []string{
		`universe "typo": rules[0].references[0]: no stream has ID "BTCUSDT:1h:binance"`,
		`universe "empty": selects no streams`,
	}
	if !slices.Equal(got, want) {
		t.Errorf("errors %q, want %q", got, want)
	}

	if names := slices.Sorted(maps.Keys(c.universes)); !slices.Equal(names, []string{"btc", "disabled_only", "unused"}) {
		t.Errorf("resolved %v, want btc, disabled_only and unused", names)
	}
	if i, _ := c.streams.Index(btc.ID()); !slices.Equal(c.universes["btc"].Streams, []int{i}) {
		t.Errorf("btc = %v, want stream %d", c.universes["btc"].Streams, i)
	}
}

/*
TestInstantiateSubscriptions covers building the subscriptions: valid ones are built in order, with the
stream table and the universes over it. An invalid entry and a duplicate are errors, an invalid entry
not checked for duplicates but a later valid copy of it is; any of them leaves no subscriptions, table
or universes.
*/
func TestInstantiateSubscriptions(t *testing.T) {
	btcUniverse := []proto.Universe{{Name: "btc", Rules: []proto.UniverseRule{{Symbol: []string{"BTC*"}}}}}

	t.Run("built", func(t *testing.T) {
		cfg := &config.Config{
			Subscriptions: []config.SubscriptionConfig{
				{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "p", Derive: proto.NewTimeframeDerivationSet(proto.Timeframe1h)},
				{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "p"},
			},
			Universes: btcUniverse,
		}
		c, errs := instantiate(cfg, testLogger())
		if len(errs) != 0 {
			t.Fatalf("errors: %v", errs)
		}
		ids := make([]string, len(c.subscriptions))
		for i, s := range c.subscriptions {
			ids[i] = s.ID()
		}
		if want := []string{"BTCUSDT:5m:p", "BTCUSDT:1h:p"}; !slices.Equal(ids, want) {
			t.Errorf("subscriptions %v, want %v", ids, want)
		}
		if c.streams == nil || len(c.streams.All) != 3 {
			t.Errorf("stream table %+v, want 3 streams", c.streams)
		}
		if _, ok := c.universes["btc"]; !ok {
			t.Error("universe btc not resolved")
		}
	})

	for _, tc := range []struct {
		name string
		subs []config.SubscriptionConfig
		want []string
	}{
		{"invalid entries and duplicates", []config.SubscriptionConfig{
			{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "p", Derive: proto.NewTimeframeDerivationSet(proto.Timeframe5m)},
			{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "p", Derive: proto.NewTimeframeDerivationSet(proto.Timeframe5m)},
			{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "p"},
			{Symbol: "ETHUSDT", Timeframe: proto.Timeframe1h},
			{Symbol: "ETHUSDT", Timeframe: proto.Timeframe1h},
		}, []string{
			`subscriptions[0]: derive "5m" of BTCUSDT:1h:p must be coarser than "1h"`,
			`subscriptions[1]: derive "5m" of BTCUSDT:1h:p must be coarser than "1h"`,
			`subscriptions[2]: duplicate subscription "BTCUSDT:1h:p"`,
			"subscriptions[3]: missing required 'provider' field",
			"subscriptions[4]: missing required 'provider' field",
		}},
		{"a missing timeframe, derivations unchecked", []config.SubscriptionConfig{
			{Symbol: "BTCUSDT", Provider: "p", Derive: proto.NewTimeframeDerivationSet(proto.Timeframe1h)},
		}, []string{"subscriptions[0]: missing required 'timeframe' field"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, errs := instantiate(&config.Config{Subscriptions: tc.subs, Universes: btcUniverse}, testLogger())
			got := make([]string, len(errs))
			for i, err := range errs {
				got[i] = err.Error()
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("errors %q, want %q", got, tc.want)
			}
			if c.subscriptions != nil || c.streams != nil || len(c.universes) != 0 {
				t.Errorf("subscriptions %v, table %v, universes %v; want none", c.subscriptions, c.streams, c.universes)
			}
		})
	}
}
