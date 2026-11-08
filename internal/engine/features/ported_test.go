package features

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	feat "whatever/internal/domains/features"
	_ "whatever/internal/domains/featuresv2"
	"whatever/internal/domains/provider"
	"whatever/internal/engine/data"
	"whatever/internal/engine/data/datatest"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	"whatever/internal/protocol/frame"
	reg "whatever/internal/registry"
)

// variant is a feature type's options, built in both registries.
type variant struct {
	typ  string
	opts map[string]any
}

func period(p int) map[string]any { return map[string]any{"period": p} }

// referenceVariants are configs/data_logger_v2.json's variants, period 3 of each period type, and
// ema_20_s3. The longest lookback is 100.
var referenceVariants = []variant{
	{feat.SMAID, period(20)}, {feat.SMAID, period(50)}, {feat.SMAID, period(100)}, {feat.SMAID, period(3)},
	{feat.EMAID, period(20)}, {feat.EMAID, period(50)}, {feat.EMAID, period(3)},
	{feat.EMAID, map[string]any{"period": 20, "smoothing": 3.0}},
	{feat.RSIID, period(14)}, {feat.RSIID, period(21)}, {feat.RSIID, period(3)},
	{feat.VWAPID, period(20)}, {feat.VWAPID, period(50)}, {feat.VWAPID, period(3)},
	{feat.EngulfingID, nil},
}

// quietLog is a logger that discards its output.
func quietLog() *logger.Logger {
	return logger.NewLogger(logger.LoggerConfig{Level: "debug", Output: io.Discard})
}

/*
ported runs the ports of a set of variants on every stream of a table beside a FeatureChain of the
originals, and counts what each variant was compared on.
*/
type ported struct {
	st      *proto.Streams
	e       *Engine
	ids     []string // each variant's port ID
	origIDs []string // each variant's original ID, which omits a default smoothing (ema_20)
	ints    []bool   // the variant's port is a Bind[int]
	chain   *feat.FeatureChain
	history []proto.Candle // the bridge's
	seen    map[seenKey]*[2]int
}

// seenKey is a variant on a stream; its count is the values and the absences compared there.
type seenKey struct {
	id string
	s  int
}

func newPorted(t *testing.T, st *proto.Streams, variants []variant) *ported {
	t.Helper()
	p := &ported{st: st, seen: make(map[seenKey]*[2]int)}
	var instances []instance
	var originals []proto.Feature
	for _, v := range variants {
		port, err := reg.FeatureTypes.Create(v.typ, v.opts)
		if err != nil {
			t.Fatalf("port %s %v: %v", v.typ, v.opts, err)
		}
		original, err := reg.Features.Create(v.typ, v.opts)
		if err != nil {
			t.Fatalf("original %s %v: %v", v.typ, v.opts, err)
		}
		_, isInt := port.Bind.(feature.Bind[int])
		p.ids, p.origIDs = append(p.ids, port.ID), append(p.origIDs, original.ID().Name)
		p.ints = append(p.ints, isInt)
		originals = append(originals, original)
		for s := range st.All {
			instances = append(instances, on(port, stream(s))...)
			p.seen[seenKey{port.ID, s}] = new([2]int)
		}
	}

	chain, err := feat.NewFeatureChain(originals)
	if err != nil {
		t.Fatal(err)
	}
	g, err := newGraph(st, instances)
	if err != nil {
		t.Fatal(err)
	}
	p.chain, p.e = chain, New(g)
	p.history = make([]proto.Candle, 0, chain.Lookback())
	return p
}

/*
compare pins the engine at v's frame and, at each stream updated there (Fresh, age 0), fires every
port and evaluates the chain as the bridge does. Each port reads Fresh with the original's value where
the snapshot holds the original's ID, and Gap where it does not.
*/
func (p *ported) compare(t *testing.T, v frame.View[proto.Candle]) {
	t.Helper()
	fv := p.e.pin(v)
	for s, stream := range p.st.All {
		if _, state, age := v.Latest(s); state != frame.Fresh || age != 0 {
			continue
		}
		for _, id := range p.ids {
			p.e.g.columns[id].fire(s, v.Col())
		}

		p.history = v.Data(s, p.chain.Lookback(), p.history)
		snap := p.chain.Evaluate(stream.Sub, p.history)
		want := snap.Data()
		for i, id := range p.ids {
			var got any
			var state frame.State
			if p.ints[i] {
				got, state, _ = mustView[int](t, fv, id).Latest(s)
			} else {
				got, state, _ = mustView[float64](t, fv, id).Latest(s)
			}

			w, ok := want[p.origIDs[i]]
			switch {
			case ok && (state != frame.Fresh || got != w):
				t.Fatalf("%s %s %s: port %v (state %d), original %v",
					time.UnixMilli(v.Boundary()).UTC(), stream.Sub.ID(), id, got, state, w)
			case !ok && state != frame.Gap:
				t.Fatalf("%s %s %s: port %v (state %d), original absent",
					time.UnixMilli(v.Boundary()).UTC(), stream.Sub.ID(), id, got, state)
			}
			if ok {
				p.seen[seenKey{id, s}][0]++
			} else {
				p.seen[seenKey{id, s}][1]++
			}
		}
	}
}

// TestFeatureTypes covers every registered feature type: each is in the table, and its variant
// reaches the engine's switch and binds at a stream.
func TestFeatureTypes(t *testing.T) {
	table := map[string]map[string]any{
		feat.SMAID:       period(3),
		feat.EMAID:       period(3),
		feat.RSIID:       period(3),
		feat.VWAPID:      period(3),
		feat.EngulfingID: nil,
	}
	got, want := slices.Sorted(slices.Values(reg.FeatureTypes.List())), slices.Sorted(maps.Keys(table))
	if !slices.Equal(got, want) {
		t.Fatalf("registered types %v, table %v", got, want)
	}

	for _, typ := range want {
		f, err := reg.FeatureTypes.Create(typ, table[typ])
		if err != nil {
			t.Errorf("%s: %v", typ, err)
			continue
		}
		c, err := newColumn(f, shapes(1))
		if err != nil {
			t.Errorf("%s: %v", typ, err)
			continue
		}
		var cv frame.View[proto.Candle]
		if err := c.bind(feature.NewBinder(stream(0), &cv, make([][]proto.Candle, 1), nil), 0); err != nil {
			t.Errorf("%s: bind: %v", typ, err)
		}
	}
}

/*
TestPortedReference compares the ports with the originals over a random four-month sample of the
BTCUSDT reference dataset: 5m deriving 1h and 1d, beside direct 1h and 1d, at every frame and stream
where the bridge evaluates. Every variant is compared on at least one value and one absence on every
stream. Skipped without data/BTCUSDT.
*/
func TestPortedReference(t *testing.T) {
	sample := datatest.Reference(t, proto.Timeframe5m, proto.Timeframe1h, proto.Timeframe1d)

	subs := []proto.Subscription{
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "btc_5m",
			Derive: proto.NewTimeframeDerivationSet(proto.Timeframe1h, proto.Timeframe1d)},
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "btc_1h"},
		{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1d, Provider: "btc_1d"},
	}
	st := proto.NewStreams(subs)
	p := newPorted(t, st, referenceVariants)

	provs := make(map[proto.Subscription]proto.DataProvider, len(subs))
	for _, s := range subs {
		provs[s] = sample.Provider(t, s)
	}
	dp, err := data.New(st, provs, p.chain.Lookback(), quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := dp.Start(); err != nil {
		t.Fatal(err)
	}
	defer dp.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	frames := 0
	for {
		v, err := dp.Next(ctx)
		if errors.Is(err, data.ErrDone) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		p.compare(t, v)
		frames++
	}

	for _, id := range p.ids {
		for s, stream := range st.All {
			if n := p.seen[seenKey{id, s}]; n[0] == 0 || n[1] == 0 {
				t.Errorf("%s on %s: compared on %d values and %d absences, want both",
					id, stream.Sub.ID(), n[0], n[1])
			}
		}
	}
	t.Logf("%d frames, %d variants on %d streams", frames, len(p.ids), len(st.All))
}

/*
TestPortedLookbackCap checks that a ported feature's Lookback reaches the data layer's history cap:
on a direct 1d stream, sma_365 is accepted and sma_366 rejected, naming the stream. The 365 is the
cap's current value; changing the cap means changing this test.
*/
func TestPortedLookbackCap(t *testing.T) {
	sub := proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1d, Provider: "btc_1d"}
	st := proto.NewStreams([]proto.Subscription{sub})

	for _, c := range []struct {
		period int
		ok     bool
	}{{365, true}, {366, false}} {
		f, err := reg.FeatureTypes.Create(feat.SMAID, period(c.period))
		if err != nil {
			t.Fatal(err)
		}
		// created, not initialised: New does no I/O
		prov, err := reg.Providers.Create(provider.BinanceCSVID, map[string]any{"file": "unread.csv"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = data.New(st, map[proto.Subscription]proto.DataProvider{sub: prov}, f.Lookback, quietLog())
		switch {
		case c.ok && err != nil:
			t.Errorf("%s: %v, want accepted", f.ID, err)
		case !c.ok && (err == nil || !strings.Contains(err.Error(), sub.ID())):
			t.Errorf("%s: %v, want an error naming %s", f.ID, err, sub.ID())
		}
	}
}
