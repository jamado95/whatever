package data

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"whatever/internal/domains/provider"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// fakeBuffer is a fake provider's output capacity, matching binance_csv.
const fakeBuffer = 100

// fakeProvider is a DataProvider over a candle slice: once started it sends its errors, then its
// candles, from a goroutine that stops on Close.
type fakeProvider struct {
	candles   []proto.Candle
	errs      []error
	throttle  bool          // pause every 50 candles
	delay     time.Duration // Start waits this long first
	startErr  error
	sub       proto.Subscription
	data      chan proto.MarketData
	errc      chan error
	done      chan struct{}
	sent      atomic.Int64 // candles delivered to data
	starts    atomic.Int32
	closes    atomic.Int32
	closeOnce sync.Once
}

func (p *fakeProvider) ID() string { return "fake" }

func (p *fakeProvider) Subscription() proto.Subscription { return p.sub }

func (p *fakeProvider) Init(sub proto.Subscription) error {
	p.sub = sub
	p.data, p.errc, p.done = make(chan proto.MarketData, fakeBuffer), make(chan error, 10), make(chan struct{})
	return nil
}

func (p *fakeProvider) Streams() (<-chan proto.MarketData, <-chan error) { return p.data, p.errc }

func (p *fakeProvider) Start() error {
	p.starts.Add(1)
	time.Sleep(p.delay)
	if p.startErr != nil {
		return p.startErr
	}
	go func() {
		defer close(p.data)
		defer close(p.errc)
		for _, err := range p.errs {
			select {
			case p.errc <- err:
			case <-p.done:
				return
			}
		}
		for i, c := range p.candles {
			if p.throttle && i%50 == 0 {
				time.Sleep(time.Millisecond)
			}
			select {
			case p.data <- proto.MarketData{Subscription: p.sub, Candle: c}:
				p.sent.Add(1)
			case <-p.done:
				return
			}
		}
	}()
	return nil
}

func (p *fakeProvider) Close() {
	p.closes.Add(1)
	p.closeOnce.Do(func() {
		if p.done != nil {
			close(p.done)
		}
	})
}

// fakes returns one fake provider per direct stream of st, initialised for its subscription, by
// stream index and keyed by subscription.
func fakes(st *Streams) ([]*fakeProvider, map[proto.Subscription]proto.DataProvider) {
	fs := make([]*fakeProvider, st.Directs)
	m := make(map[proto.Subscription]proto.DataProvider, st.Directs)
	for s := range fs {
		fs[s] = &fakeProvider{}
		_ = fs[s].Init(st.All[s].Sub)
		m[st.All[s].Sub] = fs[s]
	}
	return fs, m
}

// startLayer builds a layer over testSubs with fake providers fed by feed, and starts it.
func startLayer(t *testing.T, headroom int64, log *logger.Logger, feed func(*Streams, []*fakeProvider)) (*Layer, []*fakeProvider) {
	t.Helper()
	st := NewStreams(testSubs())
	fs, provs := fakes(st)
	feed(st, fs)
	l, err := newLayer(provs, 20, headroom, log)
	if err != nil {
		t.Fatalf("newLayer: %v", err)
	}
	if err := l.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(l.Close)
	return l, fs
}

// nextAll calls Next until the run ends, passing each frame to fn and collecting it; a stall of
// 10 s fails.
func nextAll(t *testing.T, l *Layer, fn func(*View)) []View {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var got []View
	for {
		v, err := l.Next(ctx)
		if err != nil {
			t.Fatalf("Next after %d frames: %v", len(got), err)
		}
		if v == nil {
			return got
		}
		fn(v)
		got = append(got, *v)
	}
}

// within runs fn and fails if it does not return within 10 s.
func within(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

/*
model computes the frames expected from each stream's accepted columns by brute force: every column
from the earliest first candle to the latest candle, Complete when every stream that has started and
not ended has a candle at its latest due column.
*/
func model(buf *FrameBuffer, accepted map[int][]int64) []View {
	st := buf.streams
	first, last := int64(math.MaxInt64), int64(math.MinInt64)
	sets := make(map[int]map[int64]bool)
	for s, cs := range accepted {
		if len(cs) == 0 {
			continue
		}
		first, last = min(first, cs[0]), max(last, cs[len(cs)-1])
		sets[s] = make(map[int64]bool)
		for _, c := range cs {
			sets[s][c] = true
		}
	}

	var vs []View
	for col := first; col <= last; col++ {
		complete := true
		for s, cs := range accepted {
			if len(cs) == 0 {
				continue
			}
			p := st.All[s].Period
			switch {
			case col < cs[0]: // not started
				complete = false
			case col >= cs[len(cs)-1]+p: // ended
			case !sets[s][col-col%p]:
				complete = false
			}
		}
		vs = append(vs, buf.view(col, complete))
	}
	return vs
}

/*
TestNew covers construction from the provider map: a nil provider, one provider instance for two
subscriptions and the 365-day cap are errors; the stream table comes from the keys, derived streams
included; nothing runs.
*/
func TestNew(t *testing.T) {
	st := NewStreams(testSubs())
	s5m := mustIndex(t, st, idBTC5m)

	t.Run("a nil provider", func(t *testing.T) {
		_, provs := fakes(st)
		provs[st.All[s5m].Sub] = nil
		if _, err := New(provs, 20, quiet()); err == nil || !strings.Contains(err.Error(), idBTC5m) {
			t.Fatalf("err = %v, want one naming %s", err, idBTC5m)
		}
	})

	t.Run("one provider instance for two subscriptions", func(t *testing.T) {
		fs, provs := fakes(st)
		provs[st.All[s5m].Sub] = fs[mustIndex(t, st, idBTC1h)]
		if _, err := New(provs, 20, quiet()); err == nil || !strings.Contains(err.Error(), idBTC5m) {
			t.Fatalf("err = %v, want one naming %s", err, idBTC5m)
		}
	})

	t.Run("the 365-day cap", func(t *testing.T) {
		_, provs := fakes(st)
		if _, err := New(provs, 400, quiet()); err == nil || !strings.Contains(err.Error(), "365-day") {
			t.Fatalf("err = %v, want the 365-day cap", err)
		}
	})

	t.Run("built, not run", func(t *testing.T) {
		fs, provs := fakes(st)
		l, err := New(provs, 20, quiet())
		if err != nil {
			t.Fatal(err)
		}
		for i, s := range st.All {
			if j, ok := l.Streams().Index(s.Sub.ID()); !ok || j != i {
				t.Errorf("stream %s at %d, %v; want %d", s.Sub.ID(), j, ok, i)
			}
		}
		for s, f := range fs {
			if f.starts.Load()+f.closes.Load() != 0 {
				t.Errorf("provider of %s started or closed", st.All[s].Sub.ID())
			}
		}
		if _, err := l.Next(context.Background()); !errors.Is(err, ErrNotStarted) {
			t.Errorf("Next = %v, want ErrNotStarted", err)
		}
	})
}

/*
TestLayerRun covers a run through Next with headroom 4: the TestPipelineRun input releases each
column once, in order, with the model's completeness, and the derived legs match the brute-force
aggregation; then Next returns nil, nil, again on the next call.
*/
func TestLayerRun(t *testing.T) {
	const d = day0
	var s5m, s1h, s1d int
	var fiveMin []int64
	l, _ := startLayer(t, 4, quiet(), func(st *Streams, fs []*fakeProvider) {
		s5m, s1h, s1d = mustIndex(t, st, idBTC5m), mustIndex(t, st, idBTC1h), mustIndex(t, st, idETH1d)
		fiveMin = slices.DeleteFunc(span(d+1, d+288, 1), func(c int64) bool { return c == d+50 })
		fs[s5m].candles = bars(st, s5m, fiveMin...)
		fs[s5m].candles[slices.Index(fiveMin, d+60)].Volume = -1 // invalid: dropped by normalise, then a Gap
		fs[s1h].candles = bars(st, s1h, span(d+12, d+132, 12)...)
		fs[s1d].candles = bars(st, s1d, d, d+288)
	})

	st := l.Streams()
	accepted := map[int][]int64{
		s5m: slices.DeleteFunc(slices.Clone(fiveMin), func(c int64) bool { return c == d+60 }),
		s1h: span(d+12, d+132, 12),
		s1d: {d, d + 288},
	}
	r1h, r1d := mustIndex(t, st, idRoll1h), mustIndex(t, st, idRoll1d)
	sources := map[int]int{r1h: s5m, r1d: s1h}

	got := nextAll(t, l, func(v *View) {
		if v.Status() != Current {
			t.Errorf("col d+%d: status %d, want Current", v.Col()-d, v.Status())
		}
		for r, s := range sources {
			want := expect(st, r, staged{fresh: accepted[s], ended: true},
				func(x int64) proto.Candle { return bar(st, s, x) }, v.Col())
			if g := readLeg(*v, r); !sameLeg(g, want) {
				t.Errorf("col d+%d, %s: %+v, want %+v", v.Col()-d, st.All[r].Sub.ID(), g, want)
			}
		}
	})
	if want := model(l.buf, accepted); !slices.Equal(got, want) {
		t.Fatalf("%d frames, want %d", len(got), len(want))
	}
	for range 2 {
		if v, err := l.Next(context.Background()); v != nil || err != nil {
			t.Fatalf("Next after the run = %v, %v; want nil, nil", v, err)
		}
	}
}

// TestLayerCancel covers a cancelled context: Next returns its error, and Close still returns.
func TestLayerCancel(t *testing.T) {
	l, _ := startLayer(t, 4, quiet(), func(st *Streams, fs []*fakeProvider) {
		s := mustIndex(t, st, idBTC5m)
		fs[s].candles = bars(st, s, span(day0, day0+1000, 1)...)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if v, err := l.Next(ctx); v != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Next = %v, %v; want nil, context.Canceled", v, err)
	}
	within(t, "Close", l.Close)
}

/*
TestLayerClose covers Close mid-run after one frame: with headroom 4 the align stages wait on the
gate, with headroom 600 readiness blocks on its full channel and align on its evaluates. Close
returns with every provider closed and every channel drained; the frames left are never returned,
and a second Close is a no-op. After a run that ended on its own, Close still returns with every
provider closed once.
*/
func TestLayerClose(t *testing.T) {
	const d = day0
	for _, headroom := range []int64{4, 600} {
		t.Run(strconv.FormatInt(headroom, 10), func(t *testing.T) {
			l, fs := startLayer(t, headroom, quiet(), func(st *Streams, fs []*fakeProvider) {
				for s, cols := range map[string][]int64{
					idBTC5m: span(d, d+2000, 1), idBTC1h: span(d, d+2000, 12), idETH1d: span(d, d+2000, 288),
				} {
					i := mustIndex(t, st, s)
					fs[i].candles = bars(st, i, cols...)
				}
			})
			if v, err := l.Next(context.Background()); v == nil || err != nil {
				t.Fatalf("first Next = %v, %v", v, err)
			}

			within(t, "Close", l.Close)
			for s, f := range fs {
				if n := f.closes.Load(); n != 1 {
					t.Errorf("provider %d closed %d times, want 1", s, n)
				}
			}
			if _, ok := <-l.views; ok {
				t.Error("event channel not drained")
			}
			select {
			case <-l.errsDone:
			default:
				t.Error("error channels not drained")
			}
			if v, err := l.Next(context.Background()); v != nil || !errors.Is(err, ErrClosed) {
				t.Errorf("Next after Close = %v, %v; want nil, ErrClosed", v, err)
			}

			l.Close()
			for s, f := range fs {
				if n := f.closes.Load(); n != 1 {
					t.Errorf("second Close: provider %d closed %d times, want 1", s, n)
				}
			}
		})
	}

	t.Run("after the run ends", func(t *testing.T) {
		l, fs := startLayer(t, 4, quiet(), func(st *Streams, fs []*fakeProvider) {
			for s := range fs {
				fs[s].candles = bars(st, s, span(d+288, d+576, st.All[s].Period)...)
			}
		})
		if frames := nextAll(t, l, func(*View) {}); len(frames) == 0 {
			t.Fatal("no frames")
		}
		l.Close()
		for s, f := range fs {
			if n := f.closes.Load(); n != 1 {
				t.Errorf("provider %d closed %d times, want 1", s, n)
			}
		}
	})
}

/*
TestLayerStart covers Start: a provider failing to start closes the layer — the lanes already
running included, even one whose align stage waits on ring capacity — and leaves nothing for Close;
a layer starts once, and not after Close.
*/
func TestLayerStart(t *testing.T) {
	st := NewStreams(testSubs())
	build := func(t *testing.T, fail func([]*fakeProvider)) (*Layer, []*fakeProvider) {
		fs, provs := fakes(st)
		fail(fs)
		l, err := New(provs, 20, quiet())
		if err != nil {
			t.Fatal(err)
		}
		return l, fs
	}
	// direct streams in table order: BTCUSDT 1h, BTCUSDT 5m, ETHUSDT 1d
	s1h, s5m, s1d := mustIndex(t, st, idBTC1h), mustIndex(t, st, idBTC5m), mustIndex(t, st, idETH1d)

	t.Run("a Start failure", func(t *testing.T) {
		l, fs := build(t, func(fs []*fakeProvider) { fs[s5m].startErr = errors.New("refused") })
		if err := l.Start(); err == nil || !strings.Contains(err.Error(), idBTC5m) {
			t.Fatalf("Start = %v, want an error naming %s", err, idBTC5m)
		}
		closes := []int32{fs[0].closes.Load(), fs[1].closes.Load(), fs[2].closes.Load()}
		want := make([]int32, 3)
		want[s1h] = 1 // running before 5m failed
		if !slices.Equal(closes, want) {
			t.Fatalf("provider closes %v, want %v", closes, want)
		}
		if n := fs[s1d].starts.Load(); n != 0 {
			t.Errorf("1d provider started %d times, want 0", n)
		}
		if _, err := l.Next(context.Background()); !errors.Is(err, ErrClosed) {
			t.Errorf("Next = %v, want ErrClosed", err)
		}
		if err := l.Start(); !errors.Is(err, ErrClosed) {
			t.Errorf("Start again = %v, want ErrClosed", err)
		}
		l.Close()
		if again := []int32{fs[0].closes.Load(), fs[1].closes.Load(), fs[2].closes.Load()}; !slices.Equal(again, closes) {
			t.Errorf("Close touched providers: closes %v, want %v", again, closes)
		}
	})

	t.Run("a Start failure after a lane fills its ring", func(t *testing.T) {
		// readiness releases nothing while a lane never started, so only Close frees the 1h lane
		l, fs := build(t, func(fs []*fakeProvider) {
			fs[s1h].candles = bars(st, s1h, span(day0, day0+12*1000, 12)...)
			fs[s5m].startErr, fs[s5m].delay = errors.New("refused"), 100*time.Millisecond
		})
		within(t, "Start", func() {
			if err := l.Start(); err == nil {
				t.Error("Start succeeded")
			}
		})
		if n := fs[s1h].closes.Load(); n != 1 {
			t.Errorf("1h provider closed %d times, want 1", n)
		}
	})

	t.Run("once, and not after Close", func(t *testing.T) {
		l, _ := build(t, func([]*fakeProvider) {})
		if err := l.Start(); err != nil {
			t.Fatal(err)
		}
		if err := l.Start(); err == nil {
			t.Error("second Start succeeded")
		}
		l.Close()
		if err := l.Start(); !errors.Is(err, ErrClosed) {
			t.Errorf("Start after Close = %v, want ErrClosed", err)
		}
	})
}

// lockedBuffer is a bytes.Buffer safe for the concurrent writes of the stages' loggers.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// TestLayerProviderErrors covers provider errors: more than a provider's buffer holds are drained
// and each is logged, and the run completes.
func TestLayerProviderErrors(t *testing.T) {
	const n = 25
	var out lockedBuffer
	l, _ := startLayer(t, 4, logger.NewLogger(logger.LoggerConfig{Level: "debug", Output: &out}),
		func(st *Streams, fs []*fakeProvider) {
			s := mustIndex(t, st, idBTC5m)
			fs[s].candles = bars(st, s, span(day0, day0+100, 1)...)
			for range n {
				fs[s].errs = append(fs[s].errs, errors.New("bad record"))
			}
		})
	if frames := nextAll(t, l, func(*View) {}); len(frames) == 0 {
		t.Fatal("no frames")
	}
	l.Close()
	if got := strings.Count(out.buf.String(), "bad record"); got != n {
		t.Errorf("logged %d provider errors, want %d", got, n)
	}
}

/*
TestLayerSkew covers per-stream blocking through the layer with headroom 4: ten days of 5m, 1h and
1d bars, the 5m provider throttled, then the coarse ones. Both runs return the model's frames, and
every stream's legs and history are read at each frame while the align stages stage ahead —
meaningful under -race.
*/
func TestLayerSkew(t *testing.T) {
	const d = day0
	st := NewStreams(testSubs())
	cols := map[int][]int64{
		mustIndex(t, st, idBTC5m): span(d+1, d+2880, 1),
		mustIndex(t, st, idBTC1h): span(d+12, d+2880, 12),
		mustIndex(t, st, idETH1d): span(d+288, d+2880, 288),
	}

	for _, slow := range [][]string{{idBTC5m}, {idBTC1h, idETH1d}} {
		l, _ := startLayer(t, 4, quiet(), func(st *Streams, fs []*fakeProvider) {
			for s, cs := range cols {
				fs[s].candles = bars(st, s, cs...)
				fs[s].throttle = slices.Contains(slow, st.All[s].Sub.ID())
			}
		})
		dst := make([]proto.Candle, 0, 20)
		got := nextAll(t, l, func(v *View) {
			for s := range st.All {
				v.Leg(s)
				v.Count(s)
				dst = v.Candles(s, 20, dst)
			}
		})
		if want := model(l.buf, cols); !slices.Equal(got, want) {
			t.Fatalf("slow %v: %d frames, want %d", slow, len(got), len(want))
		}
	}
}

// sampleCSV copies the rows of src whose OpenTs (ms, first column) lies in [from, to) to dst.
func sampleCSV(t *testing.T, src, dst string, from, to int64) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	var out bytes.Buffer
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		line := sc.Text()
		ts, err := strconv.ParseInt(line[:strings.IndexByte(line, ',')], 10, 64)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if ts >= from && ts < to {
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, out.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

/*
TestLayerReference compares rolling 1h and 1d legs over BTCUSDT 5m with the direct 1h and 1d bars of
the reference dataset — all of 2024, agreeing exactly in decimal — over four months sampled at random
each run. Every hour and day of the sample is compared: timestamps and OHLC exactly, volume within
1e-9 relative. Rolling 1d over 5m equals rolling 1d over the rolling 1h, so a 1h → 1d derivation's
match follows. Skipped without data/BTCUSDT.
*/
func TestLayerReference(t *testing.T) {
	const dir = "../../../data/BTCUSDT"
	tfs := []proto.Timeframe{proto.Timeframe5m, proto.Timeframe1h, proto.Timeframe1d}
	for _, tf := range tfs {
		if _, err := os.Stat(filepath.Join(dir, "BTCUSDT-"+string(tf)+"-2024-01.csv")); err != nil {
			t.Skipf("reference dataset missing: %v", err)
		}
	}

	year := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	start := year.AddDate(0, 0, rand.IntN(245)) // 2024-01-01 … 2024-09-01
	end := start.AddDate(0, 4, 0)
	t.Logf("sample %s … %s", start.Format(time.DateOnly), end.Format(time.DateOnly))

	subs := []proto.Subscription{
		sub("BTCUSDT", proto.Timeframe5m, "btc_5m", proto.Timeframe1h, proto.Timeframe1d),
		sub("BTCUSDT", proto.Timeframe1h, "btc_1h"),
		sub("BTCUSDT", proto.Timeframe1d, "btc_1d"),
	}
	provs := make(map[proto.Subscription]proto.DataProvider, len(subs))
	for i, tf := range tfs {
		file := filepath.Join(t.TempDir(), string(tf)+".csv")
		sampleCSV(t, filepath.Join(dir, "BTCUSDT-"+string(tf)+"-2024-01.csv"), file,
			start.UnixMilli(), end.UnixMilli())
		p, err := reg.Providers.Create(provider.BinanceCSVID, map[string]any{"file": file})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Init(subs[i]); err != nil { // the engine's part
			t.Fatal(err)
		}
		provs[subs[i]] = p
	}

	l, err := New(provs, 1, quiet())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	st := l.Streams()
	pairs := []struct {
		direct, rolling string
		count           int
		seen            int
	}{
		{"BTCUSDT:1h:btc_1h", idRoll1h, 12, 0},
		{"BTCUSDT:1d:btc_1d", idRoll1dOf5m, 288, 0},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for {
		v, err := l.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if v == nil {
			break
		}
		for i := range pairs {
			p := &pairs[i]
			dc, state, age := v.Leg(mustIndex(t, st, p.direct))
			if state != Fresh || age != 0 {
				continue
			}
			want := leg{c: dc, state: Fresh, count: p.count}
			if got := readLeg(*v, mustIndex(t, st, p.rolling)); !sameLeg(got, want) {
				t.Fatalf("%s: %s %+v,\nwant %+v", time.UnixMilli(v.Boundary()).UTC(), p.rolling, got, want)
			}
			p.seen++
		}
	}

	days := int(end.Sub(start).Hours() / 24)
	if pairs[0].seen != 24*days || pairs[1].seen != days {
		t.Errorf("compared %d hours and %d days, want %d and %d", pairs[0].seen, pairs[1].seen, 24*days, days)
	}
}
