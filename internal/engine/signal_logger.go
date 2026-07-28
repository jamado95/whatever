package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"whatever/internal/config"
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

		prov, err := config.Dep[proto.DataProvider](opts, "_provider")
		if err != nil {
			return nil, err
		}

		subscription, err := config.Dep[proto.Subscription](opts, "_subscription")
		if err != nil {
			return nil, err
		}

		strats, err := config.Dep[[]proto.Strategy](opts, "_strategies")
		if err != nil {
			return nil, err
		}

		cfg, err := parseSignalLoggerOptions(opts)
		if err != nil {
			return nil, err
		}

		ticker, err := newTicker(cfg.Ticker)
		if err != nil {
			return nil, err
		}

		return &SignalLogger{
			id:           id,
			provider:     prov,
			subscription: subscription,
			strategies:   strats,
			ticker:       ticker,
			logger:       log,
			cfg:          cfg,
			done:         make(chan struct{}),
		}, nil
	})
}

func parseSignalLoggerOptions(opts map[string]any) (SignalLoggerOptions, error) {
	cfg := SignalLoggerOptions{}
	// The caller names the engine; adding it here would duplicate the prefix.
	if err := config.DecodeOptions(opts, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

type SignalLogger struct {
	id           string
	provider     proto.DataProvider
	subscription proto.Subscription
	strategies   []proto.Strategy
	ticker       timing.Ticker[proto.MarketData]
	logger       *logger.Logger
	cfg          SignalLoggerOptions

	done chan struct{}
	wg   sync.WaitGroup
}

type SignalLoggerOptions struct {
	Limit  int           `json:"limit"` // 0 = unlimited
	Ticker *TickerConfig `json:"ticker"`
}

func (o *SignalLoggerOptions) Validate() error {
	if o.Limit < 0 {
		return fmt.Errorf("limit must not be negative")
	}
	if o.Ticker != nil {
		return o.Ticker.Validate()
	}
	return nil
}

func (e *SignalLogger) Run(ctx context.Context) error {
	if err := e.provider.Init(e.subscription, e.cfg.Limit); err != nil {
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
