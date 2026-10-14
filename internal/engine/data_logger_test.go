package engine

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"whatever/internal/config"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

const fiveMinMs = int64(5 * 60 * 1000)

// fakeProvider emits 5m candles with Binance-style CloseTs, forever when
// total is 0.
type fakeProvider struct {
	total  int
	data   chan proto.MarketData
	errs   chan error
	done   chan struct{}
	sub    proto.Subscription
	once   sync.Once
	mu     sync.Mutex
	closed bool
}

func (p *fakeProvider) ID() string { return "fake" }

func (p *fakeProvider) Init(sub proto.Subscription) error {
	p.sub = sub
	p.data = make(chan proto.MarketData, 100)
	p.errs = make(chan error, 1)
	p.done = make(chan struct{})
	return nil
}

func (p *fakeProvider) Streams() (<-chan proto.MarketData, <-chan error) {
	return p.data, p.errs
}

func (p *fakeProvider) Start() error {
	go func() {
		defer close(p.data)
		defer close(p.errs)
		for i := 0; p.total == 0 || i < p.total; i++ {
			open := int64(i) * fiveMinMs
			md := proto.MarketData{
				Subscription: p.sub,
				Candle:       proto.Candle{OpenTs: open, CloseTs: open + fiveMinMs - 1},
			}
			select {
			case p.data <- md:
			case <-p.done:
				return
			}
		}
	}()
	return nil
}

func (p *fakeProvider) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		close(p.done)
	})
}

func (p *fakeProvider) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

type countingExporter struct {
	mu sync.Mutex
	n  int
}

func (x *countingExporter) ID() string   { return "counting" }
func (x *countingExporter) Init() error  { return nil }
func (x *countingExporter) Close() error { return nil }

func (x *countingExporter) Export(proto.MarketData, *proto.Snapshot) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.n++
	return nil
}

func (x *countingExporter) count() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.n
}

func newTestDataLogger(cfg DataLoggerOptions, exp proto.Exporter, provs map[proto.Subscription]*fakeProvider) *DataLogger {
	subs := make([]proto.Subscription, 0, len(provs))
	providers := make(map[proto.Subscription]proto.DataProvider, len(provs))
	for sub, p := range provs {
		subs = append(subs, sub)
		providers[sub] = p
	}
	return &DataLogger{
		id:            "test",
		subscriptions: subs,
		providers:     providers,
		exporter:      exp,
		logger:        logger.NewLogger(logger.LoggerConfig{Level: "error", Output: io.Discard}),
		cfg:           cfg,
		done:          make(chan struct{}),
		limitHit:      make(chan struct{}),
	}
}

// runGuarded runs the engine and fails if it does not return within guard.
func runGuarded(t *testing.T, e *DataLogger, guard time.Duration) time.Duration {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), guard)
	defer cancel()

	start := time.Now()
	if err := e.Run(ctx); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("Run did not stop before the %v guard", guard)
	}
	return time.Since(start)
}

func sub(symbol string) proto.Subscription {
	return proto.Subscription{Symbol: symbol, Timeframe: proto.Timeframe5m, Provider: "fake"}
}

func TestDataLoggerLimitSingleSub(t *testing.T) {
	const limit = 25
	p := &fakeProvider{}
	exp := &countingExporter{}
	e := newTestDataLogger(DataLoggerOptions{Limit: limit}, exp,
		map[proto.Subscription]*fakeProvider{sub("BTCUSDT"): p})

	runGuarded(t, e, 5*time.Second)

	if got := exp.count(); got != limit {
		t.Errorf("exports = %d, want %d", got, limit)
	}
	if !p.isClosed() {
		t.Error("provider not closed")
	}
}

func TestDataLoggerLimitMultiSub(t *testing.T) {
	const limit = 25
	a, b := &fakeProvider{}, &fakeProvider{}
	exp := &countingExporter{}
	e := newTestDataLogger(DataLoggerOptions{Limit: limit}, exp,
		map[proto.Subscription]*fakeProvider{sub("BTCUSDT"): a, sub("ETHUSDT"): b})

	runGuarded(t, e, 5*time.Second)

	// Both legs close at every frame time.
	if got := exp.count(); got != 2*limit {
		t.Errorf("exports = %d, want %d", got, 2*limit)
	}
	if !a.isClosed() || !b.isClosed() {
		t.Error("not every provider closed")
	}
}

func TestDataLoggerNoLimitRunsToEnd(t *testing.T) {
	const total = 40
	exp := &countingExporter{}
	e := newTestDataLogger(DataLoggerOptions{}, exp,
		map[proto.Subscription]*fakeProvider{sub("BTCUSDT"): {total: total}})

	runGuarded(t, e, 5*time.Second)

	if got := exp.count(); got != total {
		t.Errorf("exports = %d, want %d", got, total)
	}
}

func TestDataLoggerLimitStopsFixedTickerPromptly(t *testing.T) {
	const limit = 3
	interval := 20 * time.Millisecond
	exp := &countingExporter{}
	e := newTestDataLogger(DataLoggerOptions{
		Limit:  limit,
		Ticker: &TickerConfig{Type: FixedInterval, Interval: config.Duration(interval)},
	}, exp, map[proto.Subscription]*fakeProvider{sub("BTCUSDT"): {}})

	// Draining the 100-slot provider buffer per tick would take ~2s.
	if took := runGuarded(t, e, 5*time.Second); took > time.Second {
		t.Errorf("shutdown took %v, want well under the buffer drain time", took)
	}
	if got := exp.count(); got != limit {
		t.Errorf("exports = %d, want %d", got, limit)
	}
}
