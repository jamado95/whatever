package proto

import (
	"encoding/json"
	"reflect"
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

/*
TestTimeframeInvariants checks what code relies on across ValidTimeframes: strictly ascending
durations, as TimeframeDerivationSet.List assumes; no more timeframes than a TimeframeDerivationSet has
bits; and each duration dividing the next, so each divides every longer one: the divisibility chain
stream periods, pair rings and the feature engine's due patterns rely on.
*/
func TestTimeframeInvariants(t *testing.T) {
	tfs := ValidTimeframes()
	if bits := reflect.TypeFor[TimeframeDerivationSet]().Bits(); len(tfs) > bits {
		t.Errorf("%d timeframes, more than a TimeframeDerivationSet's %d bits", len(tfs), bits)
	}
	for i := 1; i < len(tfs); i++ {
		prev, cur := tfs[i-1].TimeframeMs(), tfs[i].TimeframeMs()
		switch {
		case prev >= cur:
			t.Errorf("%s (%d ms) is not shorter than %s (%d ms)", tfs[i-1], prev, tfs[i], cur)
		case cur%prev != 0:
			t.Errorf("%s (%d ms) does not divide %s (%d ms)", tfs[i-1], prev, tfs[i], cur)
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

func TestSubscriptionWithDeriveIsMapKey(t *testing.T) {
	s := Subscription{Symbol: "BTCUSDT", Timeframe: Timeframe5m, Provider: "p",
		Derive: NewTimeframeDerivationSet(Timeframe1h)}
	m := map[Subscription]int{s: 1}
	if m[s] != 1 {
		t.Fatal("lookup by an equal Subscription failed")
	}
}

/*
TestSubscriptionValidate covers a subscription's own invariants: each missing field alone, then an
unknown timeframe, then each derived window not coarser than its source, named by the subscription's
ID. Every valid timeframe divides the coarser ones (TestTimeframeInvariants), so no derivation can fail
as a non-multiple.
*/
func TestSubscriptionValidate(t *testing.T) {
	cases := []struct {
		name string
		s    Subscription
		want []string
	}{
		{"complete", sub("BTCUSDT", Timeframe1h, "p"), nil},
		{"coarser derivations", sub("BTCUSDT", Timeframe5m, "p", Timeframe1h, Timeframe1d), nil},
		{"missing fields", Subscription{Timeframe: Timeframe1h},
			[]string{"missing required 'symbol' field", "missing required 'provider' field"}},
		{"a missing timeframe, derivations unchecked", sub("BTCUSDT", "", "p", Timeframe1h),
			[]string{"missing required 'timeframe' field"}},
		{"an unknown timeframe, derivations unchecked", sub("BTCUSDT", "7m", "p", Timeframe1h),
			[]string{`invalid timeframe "7m"`}},
		{"derivations not coarser", sub("BTCUSDT", Timeframe1h, "p", Timeframe5m, Timeframe1h, Timeframe1d),
			[]string{
				`derive "5m" of BTCUSDT:1h:p must be coarser than "1h"`,
				`derive "1h" of BTCUSDT:1h:p must be coarser than "1h"`,
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantErrs(t, tc.s.Validate(), tc.want...)
		})
	}
}

// TestNewSubscription covers the constructor: the fields as given, with Validate's errors, an invalid
// subscription returned too so it can be named by its ID.
func TestNewSubscription(t *testing.T) {
	s, errs := NewSubscription("BTCUSDT", Timeframe5m, "p", NewTimeframeDerivationSet(Timeframe1h))
	if want := sub("BTCUSDT", Timeframe5m, "p", Timeframe1h); s != want {
		t.Errorf("NewSubscription = %+v, want %+v", s, want)
	}
	wantErrs(t, errs)

	s, errs = NewSubscription("BTCUSDT", Timeframe1h, "", NewTimeframeDerivationSet(Timeframe5m))
	if got, want := s.ID(), "BTCUSDT:1h:"; got != want {
		t.Errorf("invalid ID() = %q, want %q", got, want)
	}
	wantErrs(t, errs, "missing required 'provider' field")
}
