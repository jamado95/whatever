package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"whatever/internal/idgen"
	"whatever/internal/logger"
	"whatever/internal/pipeline"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
	"whatever/internal/timing"
)

const SignalLoggerID = "signal_logger"

func init() {
	reg.Engines.Register(SignalLoggerID, func(opts map[string]any) (reg.Runnable, error) {
		id := idgen.GenerateID(SignalLoggerID)
		log := logger.NewLogger(logger.DefaultLoggerConfig()).
			With("domain", "core").
			With("engine", id)

		prov, ok := opts["_provider"].(proto.DataProvider)
		if !ok {
			return nil, fmt.Errorf("missing or invalid _provider")
		}

		strats, ok := opts["_strategies"].([]proto.Strategy)
		if !ok {
			return nil, fmt.Errorf("missing or invalid _strategies")
		}

		cfg, err := parseSignalLoggerConfig(opts)
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

		return &SignalLogger{
			id:         id,
			provider:   prov,
			strategies: strats,
			ticker:     ticker,
			logger:     log,
			cfg:        cfg,
			done:       make(chan struct{}),
		}, nil
	})
}

func parseSignalLoggerConfig(opts map[string]any) (SignalLoggerConfig, error) {
	cfg := SignalLoggerConfig{}

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

type SignalLogger struct {
	id         string
	provider   proto.DataProvider
	strategies []proto.Strategy
	ticker     timing.Ticker[proto.MarketData]
	logger     *logger.Logger
	cfg        SignalLoggerConfig

	done chan struct{}
	wg   sync.WaitGroup
}

type SignalLoggerConfig struct {
	Subscription proto.Subscription
	Limit        int // 0 = unlimited
	Ticker       TickerConfig
	BufferSize   int
}

func (e *SignalLogger) Run(ctx context.Context) error {
	if err := e.provider.Init(e.cfg.Subscription, e.cfg.Limit); err != nil {
		return err
	}
	data, dataErrs := e.provider.Streams()
	// gate data through ticker
	gatedData := e.ticker.Gate(ctx, data)

	// init strats
	signals := make([]<-chan proto.Signal, len(e.strategies))
	stratErrs := make([]<-chan error, len(e.strategies))
	for i, strategy := range e.strategies {
		if err := strategy.Init(gatedData); err != nil {
			e.logger.Error(err, "failed to init strategy")
			return err
		} else if err := strategy.Start(); err != nil {
			e.logger.Error(err, "failed to start strategy")
			return err
		}
		signals[i], stratErrs[i] = strategy.Streams()
	}
	// agreagre errors
	eErrs := make([]<-chan error, len(e.strategies)+1)
	eErrs = append(eErrs, dataErrs)
	eErrs = append(eErrs, stratErrs...)

	// log errors
	go func() {
		for err := range pipeline.FanIn(eErrs...) {
			e.logger.Error(err, "provider error")
		}
		e.logger.Debug("data logger error stream complete")
	}()

	// log market data
	e.wg.Go(func() {
		count := 0
		for sig := range pipeline.FanIn(signals...) {
			e.logger.Signal(sig)
			count++
		}
		e.logger.With("count", count).Info("strategy logger stream complete")
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

func (e *SignalLogger) Close() {
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
func (e *SignalLogger) RunFor(duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	return e.Run(ctx)
}
