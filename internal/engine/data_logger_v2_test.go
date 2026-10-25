package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"whatever/internal/config"
	feat "whatever/internal/domains/features"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// day0 is 2024-01-01 00:00 UTC in Unix ms.
const day0 = int64(1704067200000)

// barProvider is a DataProvider over a candle slice: once started it sends its candles, each after
// delay, from a goroutine that stops on Close.
type barProvider struct {
	candles   []proto.Candle
	delay     time.Duration
	initErr   error
	startErr  error
	sub       proto.Subscription
	data      chan proto.MarketData
	errs      chan error
	done      chan struct{}
	inits     atomic.Int32
	starts    atomic.Int32
	closes    atomic.Int32
	closeOnce sync.Once
}

func (p *barProvider) ID() string { return "bars" }

func (p *barProvider) Subscription() proto.Subscription { return p.sub }

func (p *barProvider) Init(sub proto.Subscription) error {
	p.inits.Add(1)
	if p.initErr != nil {
		return p.initErr
	}
	p.sub = sub
	p.data, p.errs, p.done = make(chan proto.MarketData, 100), make(chan error, 1), make(chan struct{})
	return nil
}

func (p *barProvider) Streams() (<-chan proto.MarketData, <-chan error) { return p.data, p.errs }

func (p *barProvider) Start() error {
	p.starts.Add(1)
	if p.startErr != nil {
		return p.startErr
	}
	go func() {
		defer close(p.data)
		defer close(p.errs)
		for _, c := range p.candles {
			if p.delay > 0 {
				time.Sleep(p.delay)
			}
			select {
			case p.data <- proto.MarketData{Subscription: p.sub, Candle: c}:
			case <-p.done:
				return
			}
		}
	}()
	return nil
}

func (p *barProvider) Close() {
	p.closes.Add(1)
	p.closeOnce.Do(func() {
		if p.done != nil {
			close(p.done)
		}
	})
}

// bars returns n UTC-aligned candles of timeframe tf, the first opening at from, with Binance-style
// CloseTs (one ms before the next open) and prices varying by index.
func bars(tf proto.Timeframe, from int64, n int) []proto.Candle {
	ms := tf.TimeframeMs()
	cs := make([]proto.Candle, n)
	for i := range cs {
		open := from + int64(i)*ms
		o, c := 100+float64(i%17), 100+float64((i*7)%23)
		cs[i] = proto.Candle{
			OpenTs:  open,
			CloseTs: open + ms - 1,
			Open:    o,
			High:    max(o, c) + 1,
			Low:     min(o, c) - 1,
			Close:   c,
			Volume:  1 + float64(i%5),
		}
	}
	return cs
}

// col is the 5m-grid column a candle closing at closeTs lands in.
func col(closeTs int64) int64 { return (closeTs + fiveMinMs - 1) / fiveMinMs }

// record is one export: its market data and a copy of its snapshot.
type record struct {
	md   proto.MarketData
	snap map[string]any
}

// recordingExporter keeps every export, and counts Init and Close.
type recordingExporter struct {
	mu      sync.Mutex
	initErr error
	records []record
	inits   int
	closes  int
}

func (x *recordingExporter) ID() string { return "recording" }

func (x *recordingExporter) Init() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.inits++
	return x.initErr
}

func (x *recordingExporter) Export(md proto.MarketData, snap *proto.Snapshot) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.records = append(x.records, record{md: md, snap: maps.Clone(snap.Data())})
	return nil
}

func (x *recordingExporter) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.closes++
	return nil
}

// byStream returns the records of the stream with the given ID, in export order.
func (x *recordingExporter) byStream(id string) []record {
	x.mu.Lock()
	defer x.mu.Unlock()
	var rs []record
	for _, r := range x.records {
		if r.md.Subscription.ID() == id {
			rs = append(rs, r)
		}
	}
	return rs
}

// lockedBuffer is a log sink safe for the lanes' concurrent writes.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// frameLog is one logged frame event.
type frameLog struct {
	Event    string   `json:"event"`
	Col      int64    `json:"col"`
	Complete bool     `json:"complete"`
	Streams  []string `json:"streams"`
}

// frames parses the frame events from a JSON log.
func frames(t *testing.T, b *lockedBuffer) []frameLog {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var fs []frameLog
	sc := bufio.NewScanner(bytes.NewReader(b.buf.Bytes()))
	for sc.Scan() {
		var f frameLog
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			t.Fatalf("log line %q: %v", sc.Text(), err)
		}
		if f.Event == "frame" {
			fs = append(fs, f)
		}
	}
	return fs
}

// newTestDataLoggerV2 builds the engine directly over the given providers, exporter (nil for none)
// and features, logging JSON at info level to the returned buffer.
func newTestDataLoggerV2(t *testing.T, cfg DataLoggerOptions, exp proto.Exporter,
	provs map[proto.Subscription]*barProvider, features ...proto.Feature) (*DataLoggerV2, *lockedBuffer) {
	t.Helper()
	chain, err := feat.NewFeatureChain(features)
	if err != nil {
		t.Fatalf("NewFeatureChain: %v", err)
	}
	m := make(map[proto.Subscription]proto.DataProvider, len(provs))
	for sub, p := range provs {
		m[sub] = p
	}
	out := &lockedBuffer{}
	return &DataLoggerV2{
		id:        "test",
		providers: m,
		chain:     chain,
		exporter:  exp,
		logger:    logger.NewLogger(logger.LoggerConfig{Level: "info", Output: out}),
		cfg:       cfg,
		stopped:   make(chan struct{}),
	}, out
}

// runWithin runs e and fails if Run does not return within limit.
func runWithin(t *testing.T, e *DataLoggerV2, ctx context.Context, limit time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("Run did not return within %v", limit)
		return nil
	}
}

func fixedTicker(d time.Duration) *TickerConfig {
	return &TickerConfig{Type: FixedInterval, Interval: config.Duration(d)}
}

var (
	sub5mDerive1h = proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "p5m",
		Derive: proto.NewTimeframeDerivationSet(proto.Timeframe1h)}
	sub5m = proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe5m, Provider: "p5m"}
	sub1h = proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1h, Provider: "p1h"}
	sub1d = proto.Subscription{Symbol: "BTCUSDT", Timeframe: proto.Timeframe1d, Provider: "p1d"}
)

// TestDataLoggerV2Factory covers construction through the registry: derivations are accepted, bad
// options and duplicate features are errors, and no provider is initialised.
func TestDataLoggerV2Factory(t *testing.T) {
	sma, err := reg.Features.Create(feat.SMAID, map[string]any{"period": 3})
	if err != nil {
		t.Fatalf("create sma: %v", err)
	}
	p := &barProvider{}
	opts := func(features []proto.Feature, options map[string]any) map[string]any {
		o := map[string]any{
			"_providers": map[proto.Subscription]proto.DataProvider{sub5mDerive1h: p},
			"_features":  features,
		}
		if options != nil {
			o[config.OptionsKey] = options
		}
		return o
	}

	if _, err := reg.Engines.Create(DataLoggerV2ID, opts([]proto.Feature{sma}, nil)); err != nil {
		t.Fatalf("derived subscription: %v", err)
	}
	for name, o := range map[string]map[string]any{
		"negative limit":     opts(nil, map[string]any{"limit": float64(-1)}),
		"invalid ticker":     opts(nil, map[string]any{"ticker": map[string]any{"type": "fixedinterval"}}),
		"duplicate features": opts([]proto.Feature{sma, sma}, nil),
	} {
		if _, err := reg.Engines.Create(DataLoggerV2ID, o); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if n := p.inits.Load(); n != 0 {
		t.Errorf("provider initialised %d times by the factory, want 0", n)
	}
}

/*
TestDataLoggerV2Exports covers a day of BTCUSDT 5m deriving 1h beside a direct 1h, the 5m bar
closing at 10:00 missing, with sma_3: every stream update is exported once under its own
subscription, in column order; each column is one frame, the 10:00 one Partial without the 5m and
rolling legs; sma_3 reads each stream's own history — the rolling one stepped by 12 columns.
*/
func TestDataLoggerV2Exports(t *testing.T) {
	sma, err := reg.Features.Create(feat.SMAID, map[string]any{"period": 3})
	if err != nil {
		t.Fatalf("create sma: %v", err)
	}
	b5m := bars(proto.Timeframe5m, day0, 288)
	b5m = slices.Delete(b5m, 119, 120) // closes at 10:00
	gapCol := col(day0 + 10*60*60*1000)

	exp := &recordingExporter{}
	e, out := newTestDataLoggerV2(t, DataLoggerOptions{}, exp, map[proto.Subscription]*barProvider{
		sub5mDerive1h: {candles: b5m},
		sub1h:         {candles: bars(proto.Timeframe1h, day0, 24)},
	}, sma)
	if err := runWithin(t, e, context.Background(), 10*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}

	id5m, id1h := sub5mDerive1h.ID(), sub1h.ID()
	idRolling := "BTCUSDT:1h:derived_" + id5m
	r5m, r1h, rRolling := exp.byStream(id5m), exp.byStream(id1h), exp.byStream(idRolling)
	if len(r5m) != 287 || len(rRolling) != 287 || len(r1h) != 24 {
		t.Fatalf("exports: 5m %d, rolling 1h %d, direct 1h %d; want 287, 287, 24",
			len(r5m), len(rRolling), len(r1h))
	}
	for i := 1; i < len(exp.records); i++ {
		if exp.records[i].md.Candle.CloseTs < exp.records[i-1].md.Candle.CloseTs {
			t.Fatalf("record %d closes before record %d", i, i-1)
		}
	}

	fs := frames(t, out)
	if len(fs) != 288 {
		t.Fatalf("%d frames, want 288", len(fs))
	}
	for i, f := range fs {
		if want := col(day0+5*60*1000) + int64(i); f.Col != want {
			t.Fatalf("frame %d at column %d, want %d", i, f.Col, want)
		}
		if f.Col == gapCol {
			if f.Complete || slices.Contains(f.Streams, id5m) || slices.Contains(f.Streams, idRolling) {
				t.Errorf("10:00 frame = %+v, want Partial without the 5m and rolling legs", f)
			}
		}
	}

	// 5m: the mean of the last three 5m closes
	for i, r := range r5m {
		got, ok := r.snap["sma_3"]
		if i < 2 {
			if ok {
				t.Errorf("5m record %d: sma_3 = %v with fewer than three bars", i, got)
			}
			continue
		}
		want := (r5m[i].md.Candle.Close + r5m[i-1].md.Candle.Close + r5m[i-2].md.Candle.Close) / 3
		if !ok || got.(float64) != roundTo6(want) {
			t.Errorf("5m record %d: sma_3 = %v, want %v", i, got, roundTo6(want))
		}
	}

	// rolling 1h: the mean of the rolling closes at c, c-12 and c-24, all Fresh within its 3 kept bars
	rolling := make(map[int64]float64, len(rRolling))
	for _, r := range rRolling {
		rolling[col(r.md.Candle.CloseTs)] = r.md.Candle.Close
	}
	checked := 0
	for _, r := range rRolling {
		c := col(r.md.Candle.CloseTs)
		got, ok := r.snap["sma_3"]
		c0, ok0 := rolling[c-12]
		c1, ok1 := rolling[c-24]
		if !ok0 || !ok1 {
			if ok {
				t.Errorf("rolling at column %d: sma_3 = %v without three stepped bars", c, got)
			}
			continue
		}
		want := roundTo6((rolling[c] + c0 + c1) / 3)
		if !ok || got.(float64) != want {
			t.Errorf("rolling at column %d: sma_3 = %v, want %v", c, got, want)
		}
		checked++
	}
	if checked == 0 {
		t.Error("no rolling record had three stepped bars")
	}
}

// roundTo6 rounds to 6 decimals, as the features do.
func roundTo6(v float64) float64 { return math.Round(v*1e6) / 1e6 }

/*
TestDataLoggerV2FixedPacerSkew covers the original skew problem: two days of 5m, 1h and 1d, every
stream with a bar at the first column, the 5m lane delayed per candle while 1h and 1d send at once,
under a fixed pacer. Every column is released once, in order and Complete, and every candle exported
once.
*/
func TestDataLoggerV2FixedPacerSkew(t *testing.T) {
	const hourMs, dayMs = 60 * 60 * 1000, 24 * 60 * 60 * 1000
	provs := map[proto.Subscription]*barProvider{
		sub5m: {candles: bars(proto.Timeframe5m, day0-fiveMinMs, 577), delay: 100 * time.Microsecond},
		sub1h: {candles: bars(proto.Timeframe1h, day0-hourMs, 49)},
		sub1d: {candles: bars(proto.Timeframe1d, day0-dayMs, 3)},
	}
	exp := &recordingExporter{}
	e, out := newTestDataLoggerV2(t, DataLoggerOptions{Ticker: fixedTicker(time.Millisecond)}, exp, provs)
	if err := runWithin(t, e, context.Background(), 20*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}

	fs := frames(t, out)
	if len(fs) != 577 {
		t.Fatalf("%d frames, want 577", len(fs))
	}
	for i, f := range fs {
		if f.Col != col(day0)+int64(i) || !f.Complete {
			t.Fatalf("frame %d = column %d complete %v, want column %d complete", i, f.Col, f.Complete,
				col(day0)+int64(i))
		}
	}
	for sub, p := range provs {
		rs := exp.byStream(sub.ID())
		if len(rs) != len(p.candles) {
			t.Errorf("%s: %d exports, want %d", sub.ID(), len(rs), len(p.candles))
			continue
		}
		for i, r := range rs {
			if r.md.Candle != p.candles[i] {
				t.Errorf("%s: export %d = %+v, want %+v", sub.ID(), i, r.md.Candle, p.candles[i])
				break
			}
		}
	}
}

// TestDataLoggerV2Limit covers limit: the run stops promptly after exactly limit frames, exporting
// only their updates, with every provider and the exporter closed once.
func TestDataLoggerV2Limit(t *testing.T) {
	provs := map[proto.Subscription]*barProvider{
		sub5m: {candles: bars(proto.Timeframe5m, day0, 288)},
		sub1h: {candles: bars(proto.Timeframe1h, day0, 24)},
	}
	exp := &recordingExporter{}
	e, out := newTestDataLoggerV2(t, DataLoggerOptions{Limit: 10, Ticker: fixedTicker(5 * time.Millisecond)},
		exp, provs)
	if err := runWithin(t, e, context.Background(), time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}

	fs := frames(t, out)
	if len(fs) != 10 {
		t.Fatalf("%d frames, want 10", len(fs))
	}
	updates := 0
	for _, f := range fs {
		updates += len(f.Streams)
	}
	if len(exp.records) != updates {
		t.Errorf("%d exports, want %d — the updates of the logged frames", len(exp.records), updates)
	}
	for sub, p := range provs {
		if n := p.closes.Load(); n != 1 {
			t.Errorf("%s closed %d times, want 1", sub.ID(), n)
		}
	}
	if exp.inits != 1 || exp.closes != 1 {
		t.Errorf("exporter initialised %d and closed %d times, want 1 and 1", exp.inits, exp.closes)
	}
}

// TestDataLoggerV2Stop covers stopping a run: cancelling ctx, Close from another goroutine, and
// Close before Run.
func TestDataLoggerV2Stop(t *testing.T) {
	long := func() map[proto.Subscription]*barProvider {
		return map[proto.Subscription]*barProvider{sub5m: {candles: bars(proto.Timeframe5m, day0, 30*288)}}
	}
	cfg := DataLoggerOptions{Ticker: fixedTicker(10 * time.Millisecond)}

	t.Run("cancel", func(t *testing.T) {
		provs, exp := long(), &recordingExporter{}
		e, _ := newTestDataLoggerV2(t, cfg, exp, provs)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		if err := runWithin(t, e, ctx, time.Second); err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		if n := provs[sub5m].closes.Load(); n != 1 {
			t.Errorf("provider closed %d times, want 1", n)
		}
		if exp.closes != 1 {
			t.Errorf("exporter closed %d times, want 1", exp.closes)
		}
	})

	t.Run("close", func(t *testing.T) {
		provs, exp := long(), &recordingExporter{}
		e, _ := newTestDataLoggerV2(t, cfg, exp, provs)
		ran := make(chan error, 1)
		go func() { ran <- e.Run(context.Background()) }()
		time.Sleep(50 * time.Millisecond)

		closed := make(chan struct{})
		go func() { e.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("Close did not return within 1s")
		}
		// Close returns once Run's cleanup is done
		if n := provs[sub5m].closes.Load(); n != 1 {
			t.Errorf("provider closed %d times, want 1", n)
		}
		if exp.closes != 1 {
			t.Errorf("exporter closed %d times, want 1", exp.closes)
		}
		select {
		case err := <-ran:
			if err != nil {
				t.Errorf("Run = %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Run did not return after Close")
		}
		e.Close() // no-op
	})

	t.Run("close before run", func(t *testing.T) {
		provs := long()
		e, _ := newTestDataLoggerV2(t, cfg, nil, provs)
		e.Close()
		if err := runWithin(t, e, context.Background(), time.Second); err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		if n := provs[sub5m].inits.Load(); n != 0 {
			t.Errorf("provider initialised %d times, want 0", n)
		}
	})
}

// TestDataLoggerV2Failures covers exporter and provider failures before the first frame.
func TestDataLoggerV2Failures(t *testing.T) {
	refused := errors.New("refused")
	two := func() map[proto.Subscription]*barProvider {
		return map[proto.Subscription]*barProvider{
			sub5m: {candles: bars(proto.Timeframe5m, day0, 12)},
			sub1h: {candles: bars(proto.Timeframe1h, day0, 1)},
		}
	}

	t.Run("exporter init", func(t *testing.T) {
		provs := two()
		e, _ := newTestDataLoggerV2(t, DataLoggerOptions{}, &recordingExporter{initErr: refused}, provs)
		if err := runWithin(t, e, context.Background(), time.Second); !errors.Is(err, refused) {
			t.Fatalf("Run = %v, want the exporter's error", err)
		}
		for sub, p := range provs {
			if n := p.inits.Load(); n != 0 {
				t.Errorf("%s initialised %d times, want 0", sub.ID(), n)
			}
		}
	})

	t.Run("provider init", func(t *testing.T) {
		provs, exp := two(), &recordingExporter{}
		provs[sub1h].initErr = refused
		e, _ := newTestDataLoggerV2(t, DataLoggerOptions{}, exp, provs)
		err := runWithin(t, e, context.Background(), time.Second)
		if !errors.Is(err, refused) || !strings.Contains(err.Error(), sub1h.ID()) {
			t.Fatalf("Run = %v, want an init error naming %s", err, sub1h.ID())
		}
		for sub, p := range provs {
			if n := p.starts.Load(); n != 0 {
				t.Errorf("%s started %d times, want 0", sub.ID(), n)
			}
		}
		if exp.closes != 1 {
			t.Errorf("exporter closed %d times, want 1", exp.closes)
		}
	})

	t.Run("provider start", func(t *testing.T) {
		provs, exp := two(), &recordingExporter{}
		provs[sub5m].startErr = refused // its lane starts after the 1h lane's
		e, _ := newTestDataLoggerV2(t, DataLoggerOptions{}, exp, provs)
		err := runWithin(t, e, context.Background(), time.Second)
		if !errors.Is(err, refused) || !strings.Contains(err.Error(), sub5m.ID()) {
			t.Fatalf("Run = %v, want a start error naming %s", err, sub5m.ID())
		}
		if n := provs[sub1h].closes.Load(); n != 1 {
			t.Errorf("1h provider closed %d times, want 1", n)
		}
		if n := provs[sub5m].closes.Load(); n != 0 {
			t.Errorf("5m provider closed %d times, want 0 — it never started", n)
		}
		if exp.closes != 1 {
			t.Errorf("exporter closed %d times, want 1", exp.closes)
		}
	})
}
