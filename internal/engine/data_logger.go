package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"whatever/internal/config"
	feat "whatever/internal/domains/features"
	"whatever/internal/idgen"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
	"whatever/internal/timing"
)

const DataLoggerID = "data_logger"

func init() {
	reg.Engines.Register(DataLoggerID, func(opts map[string]any) (reg.Runnable, error) {
		id := idgen.GenerateID(DataLoggerID)
		log := logger.NewLogger(logger.DefaultLoggerConfig()).
			With("engine", id)

		provider, err := config.Dep[proto.DataProvider](opts, "_provider")
		if err != nil {
			return nil, err
		}

		subscription, err := config.Dep[proto.Subscription](opts, "_subscription")
		if err != nil {
			return nil, err
		}

		features, err := config.Dep[[]proto.Feature](opts, "_features")
		if err != nil {
			return nil, err
		}

		// exporter is optional
		exporter, _, err := config.OptDep[proto.Exporter](opts, "_exporter")
		if err != nil {
			return nil, err
		}

		featuresChain, err := feat.NewFeatureChain(features)
		if err != nil {
			log.Error(err, fmt.Sprintf("%s failed to create feature chain", id))
			return nil, err
		}

		cfg, err := parseDataLoggerOptions(opts)
		if err != nil {
			return nil, err
		}

		ticker, err := newTicker(cfg.Ticker)
		if err != nil {
			return nil, err
		}

		return &DataLogger{
			id:           id,
			provider:     provider,
			subscription: subscription,
			features:     featuresChain,
			ticker:       ticker,
			exporter:     exporter,
			logger:       log,
			cfg:          cfg,
			done:         make(chan struct{}),
		}, nil
	})
}

type DataLogger struct {
	id           string
	provider     proto.DataProvider
	subscription proto.Subscription
	features     *feat.FeatureChain
	ticker       timing.Ticker[proto.MarketData]
	exporter     proto.Exporter
	logger       *logger.Logger
	cfg          DataLoggerOptions

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type DataLoggerOptions struct {
	Limit  int           `json:"limit"` // 0 = unlimited
	Ticker *TickerConfig `json:"ticker"`
}

// Validate distinguishes an absent ticker block (nil, meaning realtime) from a
// present but incomplete one, which is a config mistake.
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
	// The caller names the engine; adding it here would duplicate the prefix.
	if err := config.DecodeOptions(opts, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (e *DataLogger) Run(ctx context.Context) error {
	if err := e.provider.Init(e.subscription, e.cfg.Limit); err != nil {
		return err
	}

	// initialize exporter if configured
	if e.exporter != nil {
		if err := e.exporter.Init(); err != nil {
			return fmt.Errorf("failed to initialize exporter: %w", err)
		}
	}

	data, dataErrs := e.provider.Streams()
	// gate data through ticker
	gatedData := e.ticker.Gate(ctx, data)
	// feature extraction pipeline
	processedData := e.features.Process(gatedData)

	// log errors
	e.wg.Go(func() {
		for err := range dataErrs {
			e.logger.Error(err, "provider error")
		}
		e.logger.Debug("data logger error stream complete")
	})

	// log market data
	e.wg.Go(func() {
		count := 0
		for emd := range processedData {
			e.logger.ExtendedMarketData(emd)
			// export data if exporter is configured
			if e.exporter != nil {
				if err := e.exporter.Export(emd.MarketData, emd.Indicators); err != nil {
					e.logger.Error(err, "exporter error")
				}
			}
			count++
		}
		e.logger.With("count", count).Info("data logger stream complete")
	})

	if err := e.provider.Start(); err != nil {
		e.logger.Error(err, "failed to start provider")
		e.Close()
		return err
	}

	// start ticker
	e.ticker.Start()

	// close once logging goroutines complete
	wait := make(chan struct{})
	go func() {
		e.wg.Wait()
		wait <- struct{}{}
	}()

	select {
	// handle external cancellations
	case <-ctx.Done():
	case <-wait:
	}

	e.Close()

	// absorb ctrl+c context cancellation errors
	if err := ctx.Err(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}

func (e *DataLogger) Close() {
	e.closeOnce.Do(func() {
		e.logger.Info("data engine close")
		close(e.done)

		if e.ticker != nil {
			e.ticker.Stop()
		}
		if e.provider != nil {
			e.provider.Close()
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
