package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"whatever/internal/config"
	feat "whatever/internal/domains/features"
	"whatever/internal/engine/data"
	"whatever/internal/flow/timing"
	"whatever/internal/idgen"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const DataLoggerV2ID = "data_logger_v2"

func init() {
	reg.Engines.Register(DataLoggerV2ID, func(opts map[string]any) (reg.Runnable, error) {
		id := idgen.GenerateID(DataLoggerV2ID)
		log := logger.NewLogger(logger.DefaultLoggerConfig()).
			With("engine", id)

		providers, err := config.Dep[map[proto.Subscription]proto.DataProvider](opts, "_providers")
		if err != nil {
			return nil, err
		}

		features, err := config.Dep[[]proto.Feature](opts, "_features")
		if err != nil {
			return nil, err
		}

		exporter, _, err := config.OptDep[proto.Exporter](opts, "_exporter")
		if err != nil {
			return nil, err
		}

		cfg, err := parseDataLoggerOptions(opts)
		if err != nil {
			return nil, err
		}

		// no I/O: duplicate and cyclic features fail validation
		chain, err := feat.NewFeatureChain(features)
		if err != nil {
			return nil, err
		}

		return &DataLoggerV2{
			id:        id,
			providers: providers,
			chain:     chain,
			exporter:  exporter,
			logger:    log,
			cfg:       cfg,
			stopped:   make(chan struct{}),
		}, nil
	})
}

/*
DataLoggerV2 runs the data layer in replay and exports every stream update with its features. Run
initialises each provider with its subscription, then builds and starts the data layer; each frame
is paced, every stream updated at its column is evaluated by the feature chain over its history and
exported, and the frame is logged. The run ends with the data, at limit frames, or when ctx is done
or Close is called; the data layer is then closed, then the exporter.
*/
type DataLoggerV2 struct {
	id        string
	providers map[proto.Subscription]proto.DataProvider
	chain     *feat.FeatureChain
	exporter  proto.Exporter // nil: frames are logged only
	logger    *logger.Logger
	cfg       DataLoggerOptions

	mu      sync.Mutex
	cancel  context.CancelFunc // Run's; nil until Run
	closed  bool
	stopped chan struct{} // closed once Run returns
}

func (e *DataLoggerV2) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.cancel = cancel
	e.mu.Unlock()
	defer close(e.stopped)

	if e.exporter != nil {
		if err := e.exporter.Init(); err != nil {
			return fmt.Errorf("exporter: init: %w", err)
		}
		defer func() {
			if err := e.exporter.Close(); err != nil {
				e.logger.Error(err, "exporter close error")
			}
		}()
	}

	// in ID order, so the first failure reported is deterministic
	subs := slices.SortedFunc(maps.Keys(e.providers), func(a, b proto.Subscription) int {
		return strings.Compare(a.ID(), b.ID())
	})
	for _, sub := range subs {
		if err := e.providers[sub].Init(sub); err != nil {
			return fmt.Errorf("provider of %s: init: %w", sub.ID(), err)
		}
	}

	dp, err := data.New(e.providers, e.chain.Lookback(), e.logger)
	if err != nil {
		return err
	}
	// a failed Start has closed the pipeline
	if err := dp.Start(); err != nil {
		return err
	}
	defer dp.Close()

	pacer, err := newPacer(e.cfg.Ticker)
	if err != nil {
		return err
	}

	if err := e.consume(ctx, dp, pacer); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

/*
consume takes frames until the data ends, limit frames are done or ctx is done. Each frame is paced,
then every stream updated at its column — a Fresh leg at age 0 — is evaluated over its history and
exported, and the frame is logged.
*/
func (e *DataLoggerV2) consume(ctx context.Context, dp *data.DataPipeline, pacer timing.Pacer) error {
	streams := dp.Streams().All
	lookback := e.chain.Lookback()
	history := make([]proto.Candle, 0, lookback)
	updated := make([]string, 0, len(streams))

	count := 0
	for e.cfg.Limit == 0 || count < e.cfg.Limit {
		view, err := dp.Next(ctx)
		if err != nil {
			return err
		}
		if view == nil {
			e.logger.With("count", count).Info("data logger v2 stream complete")
			return nil
		}
		if err := pacer.Wait(ctx, view.Boundary()); err != nil {
			return err
		}

		updated = updated[:0]
		for s, st := range streams {
			c, state, age := view.Leg(s)
			// skip non-Fresh data
			if state != data.Fresh || age != 0 {
				continue
			}

			updated = append(updated, st.Sub.ID())
			if e.exporter == nil {
				continue
			}

			snap := e.chain.Evaluate(st.Sub, view.Candles(s, lookback, history))
			if err := e.exporter.Export(proto.MarketData{Subscription: st.Sub, Candle: c}, &snap); err != nil {
				e.logger.Error(err, "exporter error")
			}
		}
		e.logger.View(view.Boundary(), view.Col(), int(view.Status()), view.Complete(), updated, len(streams))
		count++
	}

	e.logger.With("count", count).Info("limit reached")
	return nil
}

// Close stops Run and waits for it to return; before Run, Run returns at once. Idempotent.
func (e *DataLoggerV2) Close() {
	e.mu.Lock()
	e.closed = true
	cancel := e.cancel
	e.mu.Unlock()

	if cancel != nil {
		cancel()
		<-e.stopped
	}
}
