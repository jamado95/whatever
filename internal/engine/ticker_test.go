package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"whatever/internal/config"
)

func TestTickerTypeRejectsUnknownValue(t *testing.T) {
	// The original defect: "fixedinterval" did not match "fixed", so the
	// configured pacing was silently replaced by realtime.
	var opts DataLoggerOptions
	err := config.Decode(map[string]any{
		"ticker": map[string]any{"type": "fixedinterval", "interval": "100ms"},
	}, &opts)

	if err == nil {
		t.Fatal("expected an error for an unrecognised ticker type, got nil")
	}
	if !strings.Contains(err.Error(), "fixedinterval") {
		t.Errorf("error should name the offending value, got: %v", err)
	}
}

func TestTickerTypeAcceptsKnownValues(t *testing.T) {
	for _, tc := range []struct {
		token string
		want  TickerType
	}{
		{"fixed", FixedInterval},
		{"realtime", Realtime},
	} {
		var opts DataLoggerOptions
		in := map[string]any{"ticker": map[string]any{"type": tc.token, "interval": "100ms"}}
		if err := config.Decode(in, &opts); err != nil {
			t.Fatalf("%s: Decode: %v", tc.token, err)
		}
		if opts.Ticker.Type != tc.want {
			t.Errorf("%s: got %q, want %q", tc.token, opts.Ticker.Type, tc.want)
		}
	}
}

func TestTickerIntervalParsed(t *testing.T) {
	var opts DataLoggerOptions
	in := map[string]any{"ticker": map[string]any{"type": "fixed", "interval": "100ms"}}
	if err := config.Decode(in, &opts); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := opts.Ticker.Interval.Duration(); got != 100*time.Millisecond {
		t.Errorf("interval = %v, want 100ms", got)
	}
}

func TestFixedTickerRequiresInterval(t *testing.T) {
	var opts DataLoggerOptions
	err := config.Decode(map[string]any{
		"ticker": map[string]any{"type": "fixed"},
	}, &opts)
	if err == nil {
		t.Fatal("expected an error when a fixed ticker has no interval, got nil")
	}
}

func TestPresentTickerRequiresType(t *testing.T) {
	// A ticker block that is present but has no type is a mistake, not a
	// request for the default.
	var opts DataLoggerOptions
	err := config.Decode(map[string]any{
		"ticker": map[string]any{"interval": "100ms"},
	}, &opts)
	if err == nil {
		t.Fatal("expected an error for a ticker block with no type, got nil")
	}
}

func TestAbsentTickerMeansRealtime(t *testing.T) {
	var opts DataLoggerOptions
	if err := config.Decode(map[string]any{"limit": float64(10)}, &opts); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if opts.Ticker != nil {
		t.Fatalf("Ticker = %+v, want nil for an absent block", opts.Ticker)
	}

	ticker, err := newTicker(opts.Ticker)
	if err != nil {
		t.Fatalf("newTicker: %v", err)
	}
	if ticker == nil {
		t.Error("expected a realtime ticker for an absent block")
	}
}

// TestNewPacer verifies an absent or realtime ticker block paces nothing and a fixed one waits its
// interval before the first frame.
func TestNewPacer(t *testing.T) {
	for _, c := range []*TickerConfig{nil, {Type: Realtime}} {
		p, err := newPacer(c)
		if err != nil {
			t.Fatalf("newPacer(%+v): %v", c, err)
		}
		start := time.Now()
		if err := p.Wait(context.Background(), 0); err != nil {
			t.Fatalf("newPacer(%+v): Wait: %v", c, err)
		}
		if d := time.Since(start); d > 10*time.Millisecond {
			t.Errorf("newPacer(%+v) waited %v, want no wait", c, d)
		}
	}

	p, err := newPacer(&TickerConfig{Type: FixedInterval, Interval: config.Duration(20 * time.Millisecond)})
	if err != nil {
		t.Fatalf("newPacer(fixed): %v", err)
	}
	start := time.Now()
	if err := p.Wait(context.Background(), 0); err != nil {
		t.Fatalf("newPacer(fixed): Wait: %v", err)
	}
	if d := time.Since(start); d < 15*time.Millisecond {
		t.Errorf("fixed 20ms pacer waited %v before its first frame, want at least 15ms", d)
	}
}
