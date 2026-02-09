package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

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

		provider, ok := opts["_provider"].(proto.DataProvider)
		if !ok {
			return nil, fmt.Errorf("missing or invalid _provider")
		}

		// exporter is optional
		var exporter proto.Exporter
		if exp, ok := opts["_exporter"].(proto.Exporter); ok {
			exporter = exp
		}

		features, ok := opts["_features"].([]proto.Feature)
		if !ok {
			err := fmt.Errorf("%s missing or invalid _features", id)
			log.Error(err, err.Error())
			return nil, err
		}

		featuresChain, err := feat.NewFeatureChain(features)
		if err != nil {
			log.Error(err, fmt.Sprintf("%s failed to create feature chain", id))
			return nil, err
		}

		cfg, err := parseDataLoggerConfig(opts)
		if err != nil {
			return nil, err
		}

		var ticker timing.Ticker[proto.MarketData]
		switch cfg.Ticker.Type {
		case FixedInterval:
			ticker = timing.FixedInterval[proto.MarketData](cfg.Ticker.TickInterval)
		case Realtime:
			ticker = timing.Realtime[proto.MarketData]()
		default:
			ticker = timing.Realtime[proto.MarketData]()
		}

		return &DataLogger{
			id:       id,
			provider: provider,
			features: featuresChain,
			ticker:   ticker,
			exporter: exporter,
			logger:   log,
			cfg:      cfg,
			done:     make(chan struct{}),
		}, nil
	})
}

type DataLogger struct {
	id       string
	provider proto.DataProvider
	features *feat.FeatureChain
	ticker   timing.Ticker[proto.MarketData]
	exporter proto.Exporter
	logger   *logger.Logger
	cfg      DataLoggerConfig

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type DataLoggerConfig struct {
	Subscription proto.Subscription
	Limit        int // 0 = unlimited
	Ticker       TickerConfig
	BufferSize   int
}

func parseDataLoggerConfig(opts map[string]any) (DataLoggerConfig, error) {
	cfg := DataLoggerConfig{}

	if sub, ok := opts["subscription"].(map[string]any); ok {
		symbol, _ := sub["symbol"].(string)
		timeframe, _ := sub["timeframe"].(string)
		cfg.Subscription = proto.Subscription{
			Symbol:    symbol,
			Timeframe: proto.Timeframe(timeframe),
		}
	}

	if limit, ok := opts["limit"].(float64); ok {
		cfg.Limit = int(limit)
	}

	if bufferSize, ok := opts["bufferSize"].(float64); ok {
		cfg.BufferSize = int(bufferSize)
	}

	if ticker, ok := opts["ticker"].(map[string]any); ok {
		tickerType, _ := ticker["type"].(string)
		if tickerType == "fixed" {
			cfg.Ticker.Type = FixedInterval
		} else {
			cfg.Ticker.Type = Realtime
		}
		if interval, ok := ticker["interval"].(string); ok {
			dur, err := time.ParseDuration(interval)
			if err != nil {
				return cfg, fmt.Errorf("invalid ticker interval: %w", err)
			}
			cfg.Ticker.TickInterval = dur
		}
	}

	return cfg, nil
}

func (e *DataLogger) Run(ctx context.Context) error {
	if err := e.provider.Init(e.cfg.Subscription, e.cfg.Limit); err != nil {
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
	go func() {
		e.wg.Wait()
		e.Close()
	}()

	select {
	// handle external cancellations
	case <-ctx.Done():
		e.Close()
	case <-e.done:
	}

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
