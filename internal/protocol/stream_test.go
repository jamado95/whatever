package proto

import (
	"encoding/json"
	"slices"
	"testing"
)

// day0 is the 5m column of 2024-01-01 00:00 UTC.
const day0 = 5680224

func sub(symbol string, tf Timeframe, provider string, derive ...Timeframe) Subscription {
	return Subscription{Symbol: symbol, Timeframe: tf, Provider: provider,
		Derive: NewTimeframeDerivationSet(derive...)}
}

func testSubs() []Subscription {
	return []Subscription{
		sub("ETHUSDT", Timeframe1d, "eth_1d"),
		sub("BTCUSDT", Timeframe1h, "btc_1h", Timeframe1d),
		sub("BTCUSDT", Timeframe5m, "btc_5m", Timeframe1h),
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
	if want := Timeframe5m; st.Stride != want.TimeframeMs() {
		t.Fatalf("Stride = %d, want %d (5m)", st.Stride, want.TimeframeMs())
	}
}

func TestStreamsEntries(t *testing.T) {
	st := NewStreams(testSubs())
	cases := []struct {
		id     string
		kind   StreamKind
		period int64
		window int64
		source string // ID of the source stream; "" for direct
	}{
		{id: "BTCUSDT:5m:btc_5m", kind: StreamDirect, period: 1, window: 1},
		{id: "BTCUSDT:1h:btc_1h", kind: StreamDirect, period: 12, window: 1},
		{id: "ETHUSDT:1d:eth_1d", kind: StreamDirect, period: 288, window: 1},
		{id: "BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m", kind: StreamDerived, period: 1, window: 12,
			source: "BTCUSDT:5m:btc_5m"},
		{id: "BTCUSDT:1d:derived_BTCUSDT:1h:btc_1h", kind: StreamDerived, period: 12, window: 24,
			source: "BTCUSDT:1h:btc_1h"},
	}
	if len(st.All) != len(cases) {
		t.Fatalf("got %d streams, want %d", len(st.All), len(cases))
	}
	for _, c := range cases {
		s := st.All[mustIndex(t, st, c.id)]
		if s.Sub.ID() != c.id {
			t.Errorf("%s: Sub.ID() = %q", c.id, s.Sub.ID())
		}
		if s.Kind != c.kind || s.Period != c.period || s.Window != c.window {
			t.Errorf("%s: kind/period/window = %s/%d/%d, want %s/%d/%d",
				c.id, s.Kind, s.Period, s.Window, c.kind, c.period, c.window)
		}
		wantSource := -1
		if c.source != "" {
			wantSource = mustIndex(t, st, c.source)
		}
		if s.Source != wantSource {
			t.Errorf("%s: Source = %d, want %d", c.id, s.Source, wantSource)
		}
		if s.Kind == StreamDerived && s.Sub.Derive != 0 {
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

/*
TestStreamsColumn covers placement (ceilDiv): a close at or just before a boundary lands in that
boundary's column; one past it lands in the next.
*/
func TestStreamsColumn(t *testing.T) {
	st := NewStreams(testSubs())
	b := day0 * st.Stride
	for _, tc := range []struct {
		name string
		ts   int64
		want int64
	}{
		{"boundary − 1 ms (Binance)", b - 1, day0},
		{"exact boundary", b, day0},
		{"boundary + 1 ms", b + 1, day0 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := st.Column(tc.ts); got != tc.want {
				t.Fatalf("Column(%d) = %d, want %d", tc.ts, got, tc.want)
			}
		})
	}
}

// TestStreamsProvider covers the instance feeding a stream: a direct stream's own, a derived
// stream's source's.
func TestStreamsProvider(t *testing.T) {
	st := NewStreams(testSubs())
	for id, want := range map[string]string{
		"BTCUSDT:5m:btc_5m":                    "btc_5m",
		"ETHUSDT:1d:eth_1d":                    "eth_1d",
		"BTCUSDT:1h:derived_BTCUSDT:5m:btc_5m": "btc_5m",
		"BTCUSDT:1d:derived_BTCUSDT:1h:btc_1h": "btc_1h",
	} {
		if got := st.Provider(mustIndex(t, st, id)); got != want {
			t.Errorf("Provider(%s) = %q, want %q", id, got, want)
		}
	}
}

func TestStreamKindUnmarshal(t *testing.T) {
	for in, want := range map[string]StreamKind{`"direct"`: StreamDirect, `"derived"`: StreamDerived} {
		var k StreamKind
		if err := json.Unmarshal([]byte(in), &k); err != nil || k != want {
			t.Errorf("Unmarshal(%s) = %q, %v; want %q", in, k, err, want)
		}
	}
	for _, in := range []string{`"rolling"`, `""`, `1`} {
		var k StreamKind
		if err := json.Unmarshal([]byte(in), &k); err == nil {
			t.Errorf("Unmarshal(%s) = %q, want an error", in, k)
		}
	}
}
