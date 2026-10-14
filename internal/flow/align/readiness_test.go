package align

import (
	"fmt"
	"math/rand/v2"
	"testing"

	proto "whatever/internal/protocol"
)

// TestOrderedCurrentSequence checks orderedCurrentReadiness dispatch order and
// Current/Stale status over the shared fixtures.
func TestOrderedCurrentSequence(t *testing.T) {
	tests := []struct {
		name string
		fx   fixture
		want []frame
	}{
		// In-order arrivals dispatch in order, all Current.
		{"in-order forward fill", fxInOrderForwardFill(), frames(60, 62, cur)},
		// Held cols 10..11 release in order as Current: nothing later was dispatched.
		{"late boundary candle", fxLateBoundaryCandle(), frames(5, 12, cur)},
		// Frames held at cold start release in order as Current.
		{"cold start", fxColdStart(), frames(5, 8, cur)},
		// Frames before the stall dispatch in order as Current.
		{"stalled stream", fxStalledStream(), frames(5, 9, cur)},
		// Drop and duplicate do not disturb the Current sequence.
		{"drop and duplicate", fxDropAndDuplicate(), frames(10, 14, cur)},
		// No wait for the earliest frames: 12..23 Current, then 0..11 Stale behind them.
		{"late earliest candle", fxLateEarliestCandle(), append(frames(12, 23, cur), frames(0, 11, stale)...)},
		// Late floor's in-window cols 7..11 emit Stale after 12..23.
		{"late floor", fxLateFloor(), append(frames(12, 23, cur), frames(7, 11, stale)...)},
		// Coarse data first: frames dispatch in order, all Current.
		{"early coarse", fxEarlyCoarse(), frames(0, 23, cur)},
		// Col 3 completes before col 2: 3 Current, then 2 Stale.
		{"multi-symbol out of order", fxMultiSymbolOutOfOrder(), []frame{{1, cur}, {3, cur}, {2, stale}}},
		// Held cols 10..11 release in order as Current.
		{"multi-symbol multi-timeframe", fxMultiSymbolMultiTimeframe(), frames(5, 11, cur)},
		// Frames on both sides of the evicted gap dispatch as Current.
		{"gap after eviction", fxGapAfterEviction(), append(frames(12, 23, cur), frames(36, 47, cur)...)},
		// Col 36 Current; the gap-filled cols 24..35 follow as Stale.
		{"gap in window", fxGapInWindow(), append(append(frames(12, 23, cur), frame{36, cur}), frames(24, 35, stale)...)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertFrames(t, runFx(tc.fx, orderedCurrentReadiness{}), tc.fx.stride, tc.want)
		})
	}
}

// fullScanReadiness ignores the arriving cell and evaluates the whole window:
// the reference scope for orderedCurrentReadiness.
type fullScanReadiness struct{}

func (fullScanReadiness) Evaluate(st *syncState, _ string, _ int64) []int64 {
	var cols []int64
	for c := st.baseCol; c <= st.maxCol; c++ {
		if st.ready(c) {
			cols = append(cols, c)
		}
	}
	return cols
}

// eventKeys renders events as comparable strings (time, status, legs).
func eventKeys(events []*proto.SyncEventV2) []string {
	keys := make([]string, len(events))
	for i, e := range events {
		k := fmt.Sprintf("%d/%v", e.Time(), e.Status())
		for _, leg := range e.Data() {
			k += fmt.Sprintf(" %s@%d", leg.Subscription.ID(), leg.Timestamp())
		}
		keys[i] = k
	}
	return keys
}

// TestOrderedCurrentScopeMatchesFullScan: orderedCurrentReadiness, scoped to the
// arriving cell, emits exactly what a whole-window scan does.
func TestOrderedCurrentScopeMatchesFullScan(t *testing.T) {
	fxs := map[string]fixture{
		"in-order forward fill":        fxInOrderForwardFill(),
		"late boundary candle":         fxLateBoundaryCandle(),
		"cold start":                   fxColdStart(),
		"stalled stream":               fxStalledStream(),
		"drop and duplicate":           fxDropAndDuplicate(),
		"late earliest candle":         fxLateEarliestCandle(),
		"late floor":                   fxLateFloor(),
		"early coarse":                 fxEarlyCoarse(),
		"multi-symbol out of order":    fxMultiSymbolOutOfOrder(),
		"multi-symbol multi-timeframe": fxMultiSymbolMultiTimeframe(),
		"gap after eviction":           fxGapAfterEviction(),
		"gap in window":                fxGapInWindow(),
	}

	// Random arrival orders over 2 symbols × (5m, 1h) spanning > width, with gaps.
	eth1h := sub("ETHUSDT", proto.Timeframe1h)
	multi := []proto.Subscription{btc5m, btc1h, eth5m, eth1h}
	var base []proto.ExtendedMarketData
	for c := int64(0); c <= 60; c++ {
		if c != 20 {
			base = append(base, emd(btc5m, c*300))
		}
		if c != 41 {
			base = append(base, emd(eth5m, c*300))
		}
		if c%12 == 0 {
			if c != 24 {
				base = append(base, emd(btc1h, c*300))
			}
			base = append(base, emd(eth1h, c*300))
		}
	}
	for seed := uint64(0); seed < 50; seed++ {
		emds := append([]proto.ExtendedMarketData(nil), base...)
		r := rand.New(rand.NewPCG(seed, 0))
		// Local shuffle: swap within a 30-arrival span so most data stays in-window.
		for i := range emds {
			j := min(i+r.IntN(30), len(emds)-1)
			emds[i], emds[j] = emds[j], emds[i]
		}
		fxs[fmt.Sprintf("random seed %d", seed)] = fixture{multi, 300, emds}
	}

	total := 0
	for name, fx := range fxs {
		got := eventKeys(runFx(fx, orderedCurrentReadiness{}))
		want := eventKeys(runFx(fx, fullScanReadiness{}))
		total += len(want)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: ordered emitted\n  %v\nfull scan emitted\n  %v", name, got, want)
		}
	}
	if total == 0 {
		t.Fatal("no events emitted across scenarios — comparison is vacuous")
	}
}
