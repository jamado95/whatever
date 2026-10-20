package data

import (
	"slices"
	"testing"

	proto "whatever/internal/protocol"
)

func sub(symbol string, tf proto.Timeframe, provider string, derive ...proto.Timeframe) proto.Subscription {
	return proto.Subscription{Symbol: symbol, Timeframe: tf, Provider: provider,
		Derive: proto.NewTimeframeDerivationSet(derive...)}
}

func testSubs() []proto.Subscription {
	return []proto.Subscription{
		sub("ETHUSDT", proto.Timeframe1d, "eth_1d"),
		sub("BTCUSDT", proto.Timeframe1h, "btc_1h", proto.Timeframe1d),
		sub("BTCUSDT", proto.Timeframe5m, "btc_5m", proto.Timeframe1h),
	}
}

func mustIndex(t *testing.T, st *Streams, id string) int {
	t.Helper()
	i, ok := st.Index(id)
	if !ok {
		t.Fatalf("Index(%q) not found", id)
	}
	return i
}

func TestStreamsGridIsFinestDirect(t *testing.T) {
	st := NewStreams(testSubs())
	if want := proto.Timeframe5m; st.Stride != want.TimeframeMs() {
		t.Fatalf("Stride = %d, want %d (5m)", st.Stride, want.TimeframeMs())
	}
}

func TestStreamsEntries(t *testing.T) {
	st := NewStreams(testSubs())
	cases := []struct {
		id     string
		kind   Kind
		step   int64
		source string // ID of the source stream; "" for direct
		window int64
	}{
		{id: "BTCUSDT:5m:btc_5m", kind: Direct, step: 1},
		{id: "BTCUSDT:1h:btc_1h", kind: Direct, step: 12},
		{id: "ETHUSDT:1d:eth_1d", kind: Direct, step: 288},
		{id: "BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m", kind: Derived, step: 12,
			source: "BTCUSDT:5m:btc_5m", window: 12},
		{id: "BTCUSDT:1d:derived_BTCUSDT:1h:btc_1h", kind: Derived, step: 288,
			source: "BTCUSDT:1h:btc_1h", window: 24},
	}
	if len(st.All) != len(cases) {
		t.Fatalf("got %d streams, want %d", len(st.All), len(cases))
	}
	for _, c := range cases {
		s := st.All[mustIndex(t, st, c.id)]
		if s.Sub.ID() != c.id {
			t.Errorf("%s: Sub.ID() = %q", c.id, s.Sub.ID())
		}
		if s.Kind != c.kind || s.Step != c.step || s.Window != c.window {
			t.Errorf("%s: kind/step/window = %d/%d/%d, want %d/%d/%d",
				c.id, s.Kind, s.Step, s.Window, c.kind, c.step, c.window)
		}
		wantSource := -1
		if c.source != "" {
			wantSource = mustIndex(t, st, c.source)
		}
		if s.Source != wantSource {
			t.Errorf("%s: Source = %d, want %d", c.id, s.Source, wantSource)
		}
		if s.Kind == Derived && s.Sub.Derive != 0 {
			t.Errorf("%s: derived stream carries derivations %v", c.id, s.Sub.Derive.List())
		}
	}
	if _, ok := st.Index("BTCUSDT:1d:btc_5m"); ok {
		t.Error("Index found an undeclared stream")
	}
}

func TestStreamsOrder(t *testing.T) {
	want := []string{
		"BTCUSDT:1h:btc_1h",
		"BTCUSDT:5m:btc_5m",
		"ETHUSDT:1d:eth_1d",
		"BTCUSDT:1d:derived_BTCUSDT:1h:btc_1h",
		"BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m",
	}
	ids := func(st *Streams) []string {
		out := make([]string, len(st.All))
		for i, s := range st.All {
			out[i] = s.Sub.ID()
		}
		return out
	}

	subs := testSubs()
	st := NewStreams(subs)
	if got := ids(st); !slices.Equal(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
	if st.Directs != 3 {
		t.Errorf("Directs = %d, want 3", st.Directs)
	}

	slices.Reverse(subs)
	if got := ids(NewStreams(subs)); !slices.Equal(got, want) {
		t.Errorf("order after reversing the config = %v, want %v", got, want)
	}
}
