package features

import (
	"slices"
	"testing"
)

/*
TestMemoryWarning covers the estimate New warns on, from graphs never allocated: none below the limit;
above it, the total with the candle copies, the lookback, and the three composed IDs holding the most
memory, ties by ID.
*/
func TestMemoryWarning(t *testing.T) {
	st := testStreams()
	if msg, ok := memoryWarning(mustGraph(t, st, on(last(), stream(btc5m)))); ok {
		t.Errorf("a small graph warned: %s", msg)
	}

	/*
		Rings of l bars, 32-byte slots on 64-bit: sum_4194304 12l slots on rolling1h; last l on btc5m and
		12l on rolling1h; sign and sum_2 l each, tied. Candle copies of 56-byte candles: l on rolling1h,
		2 on btc5m, 1 on eth5m. 32 × 27l + 56 × (l + 3) bytes is 3.59 GiB; 3.38 without the copies.
	*/
	const l = 1 << 22
	g := mustGraph(t, st, slices.Concat(
		on(sum(l), stream(rolling1h)),
		on(last(), stream(btc5m), stream(rolling1h)),
		on(sign(), stream(eth5m)),
		on(sum(2), stream(btc5m)),
	))
	want := "features: buffers estimated at 3.6 GiB, above 1 GiB, at a lookback of 4194304 bars; " +
		"largest: last 1664.0 MiB, sum_4194304 1536.0 MiB, sign 128.0 MiB"
	if msg, ok := memoryWarning(g); !ok || msg != want {
		t.Errorf("warning = %q %t, want %q", msg, ok, want)
	}
}
