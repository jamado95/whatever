package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"whatever/domains/provider"
	"whatever/types"
	"whatever/utils/idgen"
	"whatever/utils/logger"
	"whatever/utils/timing"
)

const DataLoggerID = "data_logger"

type DataLogger struct {
	id       string
	provider provider.DataProvider
	ticker   timing.Ticker[types.MarketData]
	logger   *logger.Logger
	cfg      DataLoggerConfig

	done chan struct{}
	wg   sync.WaitGroup
}

type DataLoggerConfig struct {
	Subscription types.Subscription
	Limit        int // 0 = unlimited
	Ticker       TickerConfig
	BufferSize   int
}

func NewDataLogger(opts map[string]any) (*DataLogger, error) {
	id := idgen.GenerateID(DataLoggerID)
	log := logger.NewLogger(logger.DefaultLoggerConfig()).
		With("domain", "core").
		With("engine", id)

	provider, ok := opts["_provider"].(provider.DataProvider)
	if !ok {
		return nil, fmt.Errorf("missing or invalid _provider")
	}

	cfg, err := parseDataLoggerConfig(opts)
	if err != nil {
		return nil, err
	}

	var ticker timing.Ticker[types.MarketData]
	switch cfg.Ticker.Type {
	case FixedInterval:
		ticker = timing.FixedInterval[types.MarketData](cfg.Ticker.TickInterval)
	case Realtime:
		ticker = timing.Realtime[types.MarketData]()
	default:
		ticker = timing.Realtime[types.MarketData]()
	}

	return &DataLogger{
		id:       id,
		provider: provider,
		ticker:   ticker,
		logger:   log,
		cfg:      cfg,
		done:     make(chan struct{}),
	}, nil
}

func parseDataLoggerConfig(opts map[string]any) (DataLoggerConfig, error) {
	cfg := DataLoggerConfig{}

	if sub, ok := opts["subscription"].(map[string]any); ok {
		symbol, _ := sub["symbol"].(string)
		timeframe, _ := sub["timeframe"].(string)
		cfg.Subscription = types.Subscription{
			Symbol:    symbol,
			Timeframe: types.Timeframe(timeframe),
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

	data, dataErrs := e.provider.Streams()
	// gate data through ticker
	gatedData := e.ticker.Gate(data)

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
		for md := range gatedData {
			e.logger.MarketData(md)
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

	// handle external cancellation
	go func() {
		e.wg.Wait()
		close(e.done)
	}()

	select {
	case <-ctx.Done():
	case <-e.done:
	}

	e.Close()
	return nil
}

func (e *DataLogger) Close() {
	e.logger.Info("data engine close")
	select {
	case <-e.done:
		return
	default:
		close(e.done)
	}

	if e.ticker != nil {
		e.ticker.Stop()
	}

	if e.provider != nil {
		e.provider.Close()
	}

	e.wg.Wait()
}

// RunFor runs the data engine for a specified duration then stops.
func (e *DataLogger) RunFor(duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	return e.Run(ctx)
}
