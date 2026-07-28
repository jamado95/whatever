package provider

import (
	"testing"

	reg "whatever/internal/registry"
)

// The factory used to assert opts["startTime"].(int64). Config is decoded into
// map[string]any, where every JSON number is a float64, so that assertion could
// never succeed: the window silently stayed at 0 and the provider always
// requested from epoch 0 with no end bound.
func TestBinanceHistoricalAppliesTimeWindow(t *testing.T) {
	const (
		start = int64(1714732800000)
		end   = int64(1714819200000)
	)

	p, err := reg.Providers.Create(BinanceHistoricalID, map[string]any{
		"startTime": float64(start),
		"endTime":   float64(end),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	bp, ok := p.(*BinanceHistoricalProvider)
	if !ok {
		t.Fatalf("got %T, want *BinanceHistoricalProvider", p)
	}
	if bp.startTime != start {
		t.Errorf("startTime = %d, want %d", bp.startTime, start)
	}
	if bp.endTime != end {
		t.Errorf("endTime = %d, want %d", bp.endTime, end)
	}
}

func TestBinanceHistoricalRejectsInvertedWindow(t *testing.T) {
	_, err := reg.Providers.Create(BinanceHistoricalID, map[string]any{
		"startTime": float64(1714819200000),
		"endTime":   float64(1714732800000),
	})
	if err == nil {
		t.Fatal("expected an error when endTime precedes startTime, got nil")
	}
}

func TestBinanceHistoricalRejectsUnknownKey(t *testing.T) {
	_, err := reg.Providers.Create(BinanceHistoricalID, map[string]any{
		"startTime":  float64(1714732800000),
		"start_time": float64(1714732800000),
	})
	if err == nil {
		t.Fatal("expected an error for an unknown key, got nil")
	}
}

func TestBinanceCSVRequiresFile(t *testing.T) {
	if _, err := reg.Providers.Create(BinanceCSVID, map[string]any{}); err == nil {
		t.Fatal("expected an error when 'file' is absent, got nil")
	}

	if _, err := reg.Providers.Create(BinanceCSVID, map[string]any{
		"file": "./data/x.csv",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
}
