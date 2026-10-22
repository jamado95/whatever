package data

import (
	"bytes"
	"io"
	"math"
	"slices"
	"strings"
	"testing"

	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

// day0 is 2024-01-01 00:00 UTC in 5m columns: due for every direct stream of the fixture.
const day0 = 5680224

// quiet is a logger that discards its output.
func quiet() *logger.Logger {
	return logger.NewLogger(logger.LoggerConfig{Level: "debug", Output: io.Discard})
}

/*
bar is a valid Binance-style candle of stream s closing at col: CloseTs is the boundary − 1 ms and
the span is the timeframe − 1 ms. Prices are distinct per column.
*/
func bar(st *Streams, s int, col int64) proto.Candle {
	closeTs := col*st.Stride - 1
	p := float64(col)
	return proto.Candle{
		OpenTs: closeTs + 1 - st.All[s].Sub.Timeframe.TimeframeMs(), CloseTs: closeTs,
		Open: p, High: p + 2, Low: p - 2, Close: p + 1, Volume: 1,
	}
}

// bars returns stream s's bars at cols.
func bars(st *Streams, s int, cols ...int64) []proto.Candle {
	out := make([]proto.Candle, len(cols))
	for i, col := range cols {
		out[i] = bar(st, s, col)
	}
	return out
}

// runNormaliser feeds cs through a normalise stage and returns everything it emits.
func runNormaliser(n *normaliser, cs ...proto.Candle) []laneItem {
	in := make(chan proto.MarketData, len(cs))
	for _, c := range cs {
		in <- proto.MarketData{Candle: c}
	}
	close(in)

	var out []laneItem
	for it := range n.run(in) {
		out = append(out, it)
	}
	return out
}

/*
TestNormaliseAccept covers the normalise rules: validation (timestamps, OHLCV, span), the due
column, an identical duplicate, and a column not beyond the last accepted one. Rule order decides the
reason, and a dropped candle moves nothing.
*/
func TestNormaliseAccept(t *testing.T) {
	st := NewStreams(testSubs())
	s5m, s1h := mustIndex(t, st, idBTC5m), mustIndex(t, st, idBTC1h)
	const col = day0 + 7

	expect := func(t *testing.T, n *normaliser, c proto.Candle, wantCol int64, want drop) {
		t.Helper()
		got, d := n.accept(c)
		if d != want || (want != invalid && got != wantCol) {
			t.Fatalf("accept = (%d, %v), want (%d, %v)", got, d, wantCol, want)
		}
	}

	t.Run("valid candle at its column", func(t *testing.T) {
		expect(t, newNormaliser(st, s5m, quiet()), bar(st, s5m, col), col, keep)
	})

	t.Run("invalid", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			mutate func(c *proto.Candle)
		}{
			{"OpenTs 0", func(c *proto.Candle) { c.OpenTs = 0 }},
			{"µs timestamps", func(c *proto.Candle) { c.OpenTs *= 1000; c.CloseTs *= 1000 }},
			{"NaN close", func(c *proto.Candle) { c.Close = math.NaN() }},
			{"infinite high", func(c *proto.Candle) { c.High = math.Inf(1) }},
			{"negative volume", func(c *proto.Candle) { c.Volume = -1 }},
			{"low above the open", func(c *proto.Candle) { c.Low = c.Open + 0.5 }},
			{"high below the close", func(c *proto.Candle) { c.High = c.Close - 0.5 }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := bar(st, s5m, col)
				tc.mutate(&c)
				expect(t, newNormaliser(st, s5m, quiet()), c, 0, invalid)
			})
		}
	})

	t.Run("span", func(t *testing.T) {
		tf := st.All[s5m].Sub.Timeframe.TimeframeMs()
		for _, tc := range []struct {
			name string
			span int64
			want drop
		}{
			{"tf − 1 s passes", tf - 1000, keep},
			{"tf passes", tf, keep},
			{"tf − 1001 ms fails", tf - 1001, invalid},
			{"tf + 1 ms fails", tf + 1, invalid},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := bar(st, s5m, col)
				c.OpenTs = c.CloseTs - tc.span
				expect(t, newNormaliser(st, s5m, quiet()), c, col, tc.want)
			})
		}
	})

	t.Run("off due", func(t *testing.T) {
		n := newNormaliser(st, s1h, quiet())
		expect(t, n, bar(st, s1h, day0+126), day0+126, offDue) // 10:30
		expect(t, n, bar(st, s1h, day0+132), day0+132, keep)   // 11:00
	})

	t.Run("duplicate and order", func(t *testing.T) {
		n := newNormaliser(st, s5m, quiet())
		c := bar(st, s5m, col)
		expect(t, n, c, col, keep)
		expect(t, n, c, col, duplicate)

		other := c
		other.Close++
		expect(t, n, other, col, outOfOrder)
		expect(t, n, bar(st, s5m, col-1), col-1, outOfOrder)
		expect(t, n, bar(st, s5m, col+1), col+1, keep)
	})

	t.Run("rule order decides the reason", func(t *testing.T) {
		n := newNormaliser(st, s5m, quiet())
		c := bar(st, s5m, col)
		expect(t, n, c, col, keep)

		us := c
		us.OpenTs, us.CloseTs = us.OpenTs*1000, us.CloseTs*1000
		expect(t, n, us, 0, invalid)

		h := newNormaliser(st, s1h, quiet())
		expect(t, h, bar(st, s1h, day0+126), day0+126, offDue)
		expect(t, h, bar(st, s1h, day0+126), day0+126, offDue)
	})

	t.Run("a dropped candle moves nothing", func(t *testing.T) {
		n := newNormaliser(st, s5m, quiet())
		bad := bar(st, s5m, col)
		bad.Close = math.NaN()
		expect(t, n, bad, 0, invalid)
		expect(t, n, bar(st, s5m, col), col, keep)
	})
}

/*
TestNormaliseRun covers the stage: accepted candles in input order, then a closed output with no
marker; drop counters; and logging, which warns once per rule and summarises when the input ends.
*/
func TestNormaliseRun(t *testing.T) {
	st := NewStreams(testSubs())
	s5m, s1h := mustIndex(t, st, idBTC5m), mustIndex(t, st, idBTC1h)
	item := func(s int, col int64) laneItem { return laneItem{col: col, c: bar(st, s, col)} }

	t.Run("accepted candles in order, then the output closes", func(t *testing.T) {
		got := runNormaliser(newNormaliser(st, s1h, quiet()), bars(st, s1h, day0+120, day0+132)...) // 10:00, 11:00
		want := []laneItem{item(s1h, day0+120), item(s1h, day0+132)}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v,\nwant %v", got, want)
		}
	})

	t.Run("drops at the tail: only the accepted candles", func(t *testing.T) {
		bad := bar(st, s1h, day0+132)
		bad.Volume = -1
		got := runNormaliser(newNormaliser(st, s1h, quiet()), bar(st, s1h, day0+120), bad)
		if want := []laneItem{item(s1h, day0+120)}; !slices.Equal(got, want) {
			t.Fatalf("got %v,\nwant %v", got, want)
		}
	})

	t.Run("nothing accepted: the output closes empty", func(t *testing.T) {
		bad := bar(st, s5m, day0+1)
		bad.OpenTs = 0
		for name, cs := range map[string][]proto.Candle{"empty": nil, "all dropped": {bad}} {
			if got := runNormaliser(newNormaliser(st, s5m, quiet()), cs...); len(got) != 0 {
				t.Fatalf("%s: got %v, want nothing", name, got)
			}
		}
	})

	t.Run("counters per rule", func(t *testing.T) {
		bad := bar(st, s1h, day0+132)
		bad.Close = math.NaN()
		n := newNormaliser(st, s1h, quiet())
		runNormaliser(n,
			bar(st, s1h, day0+120),
			bar(st, s1h, day0+120), bar(st, s1h, day0+120), // duplicates
			bar(st, s1h, day0+126), // off due
			bad,
			bar(st, s1h, day0+108), // out of order
			bar(st, s1h, day0+132))
		want := [numDrops]int{invalid: 1, offDue: 1, duplicate: 2, outOfOrder: 1}
		if n.drops != want {
			t.Fatalf("drops = %v, want %v", n.drops, want)
		}
	})

	t.Run("logging", func(t *testing.T) {
		count := func(log, msg string) int { return strings.Count(log, msg) }
		const warn, summary = "dropping candle", "normalise dropped candles"

		var buf bytes.Buffer
		log := logger.NewLogger(logger.LoggerConfig{Level: "debug", Output: &buf})
		runNormaliser(newNormaliser(st, s1h, log),
			bar(st, s1h, day0+120),
			bar(st, s1h, day0+126), bar(st, s1h, day0+138), // off due ×2
			bar(st, s1h, day0+108), bar(st, s1h, day0+96), // out of order ×2
			bar(st, s1h, day0+120)) // duplicate
		if n := count(buf.String(), warn); n != 2 {
			t.Fatalf("%d warnings, want 2 (one per rule, none for duplicates):\n%s", n, buf.String())
		}
		if n := count(buf.String(), summary); n != 1 {
			t.Fatalf("%d summaries, want 1:\n%s", n, buf.String())
		}

		buf.Reset()
		runNormaliser(newNormaliser(st, s1h, log), bar(st, s1h, day0+120), bar(st, s1h, day0+120))
		if count(buf.String(), warn) != 0 || count(buf.String(), summary) != 1 {
			t.Fatalf("duplicates only: want no warning and one summary:\n%s", buf.String())
		}

		buf.Reset()
		runNormaliser(newNormaliser(st, s1h, log), bar(st, s1h, day0+120))
		if buf.Len() != 0 {
			t.Fatalf("no drops: want no log lines:\n%s", buf.String())
		}
	})
}
