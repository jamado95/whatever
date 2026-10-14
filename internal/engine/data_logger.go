package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"whatever/internal/config"
	feat "whatever/internal/domains/features"
	"whatever/internal/flow/align"
	"whatever/internal/flow/pipe"
	"whatever/internal/flow/timing"
	"whatever/internal/idgen"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const DataLoggerID = "data_logger"

func init() {
	reg.Engines.Register(DataLoggerID, func(opts map[string]any) (reg.Runnable, error) {
		id := idgen.GenerateID(DataLoggerID)
		log := logger.NewLogger(logger.DefaultLoggerConfig()).
			With("engine", id)

		subscriptions, err := config.Dep[[]proto.Subscription](opts, "_subscriptions")
		if err != nil {
			return nil, err
		}

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

		// validate every sub has a matching provider
		for _, sub := range subscriptions {
			if _, ok := providers[sub]; !ok {
				return nil, fmt.Errorf(
					"data_logger: subscription %q has no provider injected", sub.ID())
			}
		}

		return &DataLogger{
			id:            id,
			subscriptions: subscriptions,
			providers:     providers,
			features:      features,
			exporter:      exporter,
			logger:        log,
			cfg:           cfg,
			done:          make(chan struct{}),
			limitHit:      make(chan struct{}),
		}, nil
	})
}

type DataLogger struct {
	id            string
	subscriptions []proto.Subscription
	providers     map[proto.Subscription]proto.DataProvider
	features      []proto.Feature
	exporter      proto.Exporter
	logger        *logger.Logger
	cfg           DataLoggerOptions

	done      chan struct{}
	limitHit  chan struct{} // closed once Limit frames are drained
	closeOnce sync.Once

	gateMu      sync.Mutex
	cancelGates context.CancelFunc // stops ticker gates; set by Run
	wg          sync.WaitGroup
}

type DataLoggerOptions struct {
	// Limit caps synchronizer frames; reaching it closes providers and shuts
	// down. 0 = unlimited.
	Limit  int           `json:"limit"`
	Ticker *TickerConfig `json:"ticker"`
}

func (o *DataLoggerOptions) Validate() error {
	if o.Limit < 0 {
		return fmt.Errorf("limit must not be negative")
	}
	if o.Ticker != nil {
		return o.Ticker.Validate()
	}
	return nil
}

func parseDataLoggerOptions(opts map[string]any) (DataLoggerOptions, error) {
	cfg := DataLoggerOptions{}
	if err := config.DecodeOptions(opts, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (e *DataLogger) Run(ctx context.Context) error {
	// Cancelled on shutdown so gates stop instead of draining buffers per tick.
	gateCtx, cancelGates := context.WithCancel(ctx)
	defer cancelGates()
	e.gateMu.Lock()
	e.cancelGates = cancelGates
	e.gateMu.Unlock()

	if e.exporter != nil {
		if err := e.exporter.Init(); err != nil {
			return fmt.Errorf("failed to initialize exporter: %w", err)
		}
	}

	// Per-sub lanes: provider → ticker → feature chain.
	streams := make(map[proto.Subscription]<-chan proto.ExtendedMarketData, len(e.subscriptions))
	errChans := make([]<-chan error, 0, len(e.subscriptions))
	tickers := make([]timing.Ticker[proto.MarketData], 0, len(e.subscriptions))

	for _, sub := range e.subscriptions {
		prov := e.providers[sub]

		if err := prov.Init(sub); err != nil {
			return fmt.Errorf("failed to init provider %s: %w", sub.ID(), err)
		}

		data, dataErrs := prov.Streams()
		errChans = append(errChans, dataErrs)

		ticker, err := newTicker(e.cfg.Ticker)
		if err != nil {
			return fmt.Errorf("failed to create ticker for %s: %w", sub.ID(), err)
		}
		tickers = append(tickers, ticker)

		chain, err := feat.NewFeatureChain(e.features)
		if err != nil {
			return fmt.Errorf("failed to create feature chain for %s: %w", sub.ID(), err)
		}

		streams[sub] = chain.Process(ticker.Gate(gateCtx, data))
	}

	syncOut := align.NewOrderedSynchronizer(nil).Sync(streams)

	// Collect all provider errors.
	e.wg.Go(func() {
		for err := range pipe.FanIn(errChans...) {
			e.logger.Error(err, "provider error")
		}
		e.logger.Debug("data logger error stream complete")
	})

	// Drain synchronizer: log every frame and export each leg that closed at
	// its time, so every candle is exported once. Past Limit, discard the rest
	// so upstream can unwind after providers close.
	e.wg.Go(func() {
		count := 0
		for ev := range syncOut {
			if e.cfg.Limit > 0 && count >= e.cfg.Limit {
				continue
			}
			e.logger.SyncEvent(ev)
			for _, emd := range ev.Data() {
				if emd.Timestamp() != ev.Time() {
					continue // forward-filled leg
				}
				if e.exporter != nil {
					if err := e.exporter.Export(emd.MarketData, emd.Indicators); err != nil {
						e.logger.Error(err, "exporter error")
					}
				}
			}
			count++
			if e.cfg.Limit > 0 && count == e.cfg.Limit {
				e.logger.With("count", count).Info("limit reached")
				close(e.limitHit)
			}
		}
		e.logger.With("count", count).Info("data logger stream complete")
	})

	// Start providers and tickers.
	for _, sub := range e.subscriptions {
		if err := e.providers[sub].Start(); err != nil {
			e.logger.Error(err, "failed to start provider")
			e.Close()
			return err
		}
	}
	for _, t := range tickers {
		t.Start()
	}

	wait := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(wait)
	}()

	select {
	case <-ctx.Done():
	case <-wait:
	case <-e.limitHit:
	}

	e.Close()

	if err := ctx.Err(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func (e *DataLogger) Close() {
	e.closeOnce.Do(func() {
		e.logger.Info("data engine close")
		close(e.done)

		e.gateMu.Lock()
		if e.cancelGates != nil {
			e.cancelGates()
		}
		e.gateMu.Unlock()

		for _, sub := range e.subscriptions {
			e.providers[sub].Close()
		}

		e.wg.Wait()

		if e.exporter != nil {
			if err := e.exporter.Close(); err != nil {
				e.logger.Error(err, "exporter close error")
			}
		}
	})
}

// RunFor runs the data engine for a specified duration then stops.
func (e *DataLogger) RunFor(duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	return e.Run(ctx)
}
