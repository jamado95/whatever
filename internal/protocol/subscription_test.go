package proto

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestTimeframeMs(t *testing.T) {
	for _, tf := range ValidTimeframes() {
		if got, want := tf.TimeframeMs(), int64(tf.TimeframeS())*1000; got != want {
			t.Errorf("%s: TimeframeMs = %d, want %d", tf, got, want)
		}
	}
}

func TestSubscriptionID(t *testing.T) {
	s := Subscription{Symbol: "BTCUSDT", Timeframe: Timeframe5m, Provider: "binance_csv"}
	if got, want := s.ID(), "BTCUSDT:5m:binance_csv"; got != want {
		t.Fatalf("ID() = %q, want %q", got, want)
	}
}

func TestSubscriptionIDIgnoresDerive(t *testing.T) {
	s := Subscription{Symbol: "BTCUSDT", Timeframe: Timeframe5m, Provider: "binance_csv",
		Derive: NewTimeframeDerivationSet(Timeframe1h)}
	if got, want := s.ID(), "BTCUSDT:5m:binance_csv"; got != want {
		t.Fatalf("ID() = %q, want %q", got, want)
	}
}

func TestTimeframeDerivationSetUnmarshal(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []Timeframe
		err  string
	}{
		{name: "list", in: `["1h", "1d"]`, want: []Timeframe{Timeframe1h, Timeframe1d}},
		{name: "ascending regardless of order", in: `["1d", "5m", "1h"]`,
			want: []Timeframe{Timeframe5m, Timeframe1h, Timeframe1d}},
		{name: "empty", in: `[]`},
		{name: "null", in: `null`},
		{name: "unknown timeframe", in: `["2h"]`, err: "invalid timeframe"},
		{name: "repeated timeframe", in: `["1h", "1h"]`, err: "repeated"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var s TimeframeDerivationSet
			err := json.Unmarshal([]byte(c.in), &s)
			if c.err != "" {
				if err == nil || !strings.Contains(err.Error(), c.err) {
					t.Fatalf("err = %v, want one containing %q", err, c.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if got := s.List(); !slices.Equal(got, c.want) {
				t.Errorf("List() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestTimeframeDerivationSetBits(t *testing.T) {
	s := NewTimeframeDerivationSet(Timeframe1m, Timeframe1d)
	if s != 1|1<<4 {
		t.Fatalf("bits = %08b, want 00010001", s)
	}
	if !s.Has(Timeframe1m) || !s.Has(Timeframe1d) || s.Has(Timeframe1h) {
		t.Errorf("Has: got 1m=%v 1d=%v 1h=%v, want true true false",
			s.Has(Timeframe1m), s.Has(Timeframe1d), s.Has(Timeframe1h))
	}
	if s.Has(Timeframe("2h")) {
		t.Error("Has reports an invalid timeframe")
	}
}

func TestSubscriptionDecodesDerive(t *testing.T) {
	var subs []Subscription
	in := `[{"symbol": "BTCUSDT", "timeframe": "5m", "provider": "p", "derive": ["1h"]},
	        {"symbol": "ETHUSDT", "timeframe": "5m", "provider": "p"}]`
	if err := json.Unmarshal([]byte(in), &subs); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if subs[0].Derive != NewTimeframeDerivationSet(Timeframe1h) {
		t.Errorf("derive = %v, want [1h]", subs[0].Derive.List())
	}
	if subs[1].Derive != 0 {
		t.Errorf("absent derive = %v, want empty", subs[1].Derive.List())
	}
}

func TestSubscriptionWithDeriveIsMapKey(t *testing.T) {
	s := Subscription{Symbol: "BTCUSDT", Timeframe: Timeframe5m, Provider: "p",
		Derive: NewTimeframeDerivationSet(Timeframe1h)}
	m := map[Subscription]int{s: 1}
	if m[s] != 1 {
		t.Fatal("lookup by an equal Subscription failed")
	}
}
