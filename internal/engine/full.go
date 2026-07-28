package engine

import (
	"context"
	"fmt"
	"sync"

	"whatever/internal/config"
	"whatever/internal/logger"
	"whatever/internal/pipeline"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
	"whatever/internal/timing"
)

const FullEngineID = "full"

func init() {
	reg.Engines.Register(FullEngineID, func(opts map[string]any) (reg.Runnable, error) {
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

		riskMgr, err := config.Dep[proto.RiskManager](opts, "_risk")
		if err != nil {
			return nil, err
		}

		execs, err := config.Dep[[]proto.Executor](opts, "_executors")
		if err != nil {
			return nil, err
		}

		cfg, err := parseFullEngineOptions(opts)
		if err != nil {
			return nil, err
		}

		return NewEngine(prov, subscription, riskMgr, strats, execs, cfg)
	})
}

type Engine struct {
	provider     proto.DataProvider
	subscription proto.Subscription
	strategies   []proto.Strategy
	risk         proto.RiskManager
	executors    []proto.Executor
	ticker       timing.Ticker[proto.MarketData]
	logger       *logger.Logger
	cfg          FullEngineOptions

	done chan struct{}
	wg   sync.WaitGroup
}

type FullEngineOptions struct {
	Limit          int           `json:"limit"`          // 0 = unlimited
	BufferSize     int           `json:"bufferSize"`     // channel buffer size
	ErrorThreshold int           `json:"errorThreshold"` // errors tolerated before shutdown
	Ticker         *TickerConfig `json:"ticker"`
}

func (o *FullEngineOptions) Validate() error {
	if o.Limit < 0 {
		return fmt.Errorf("limit must not be negative")
	}
	if o.BufferSize < 0 {
		return fmt.Errorf("bufferSize must not be negative")
	}
	if o.ErrorThreshold <= 0 {
		return fmt.Errorf("errorThreshold is required and must be positive")
	}
	if o.Ticker != nil {
		return o.Ticker.Validate()
	}
	return nil
}

func parseFullEngineOptions(opts map[string]any) (FullEngineOptions, error) {
	cfg := FullEngineOptions{}
	// The caller names the engine; adding it here would duplicate the prefix.
	if err := config.DecodeOptions(opts, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func NewEngine(
	prov proto.DataProvider,
	sub proto.Subscription,
	riskMgr proto.RiskManager,
	strats []proto.Strategy,
	execs []proto.Executor,
	cfg FullEngineOptions,
) (*Engine, error) {
	if prov == nil {
		return nil, fmt.Errorf("provider is required")
	}
	if len(strats) == 0 {
		return nil, fmt.Errorf("at least one Strategy is required")
	}
	if riskMgr == nil {
		return nil, fmt.Errorf("risk manager is required")
	}
	if len(execs) == 0 {
		return nil, fmt.Errorf("executor is required")
	}

	ticker, err := newTicker(cfg.Ticker)
	if err != nil {
		return nil, err
	}

	// init engine logger
	logger := logger.NewLogger(logger.DefaultLoggerConfig()).With("domain", "engine")

	return &Engine{
		provider:     prov,
		subscription: sub,
		strategies:   strats,
		risk:         riskMgr,
		executors:    execs,
		cfg:          cfg,
		ticker:       ticker,
		logger:       logger,
		done:         make(chan struct{}),
	}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	// declare engine error channels: strats + execs + risk + provider
	var eErrors []<-chan error

	/*
		Init data provider layer
	*/
	if err := e.provider.Init(e.subscription, e.cfg.Limit); err != nil {
		e.Close()
		return fmt.Errorf("failed to init provider %s: %w", e.provider.ID(), err)
	}

	data, providerErrs := e.provider.Streams()
	eErrors = append(eErrors, providerErrs)
	// gate data through e.ticker and broadcast to strats and riskmgr
	dataOuts := pipeline.FanOut(e.ticker.Gate(ctx, data), len(e.strategies)+1, e.cfg.BufferSize)

	/*
		Init strategy modules
	*/
	stratSignals := make([]<-chan proto.Signal, len(e.strategies))
	stratErrs := make([]<-chan error, len(e.strategies))
	for i, strat := range e.strategies {
		if err := strat.Init(dataOuts[i]); err != nil {
			e.Close()
			return fmt.Errorf("failed to init strategy %s: %w", strat.ID(), err)
		}

		stratSignals[i], stratErrs[i] = strat.Streams()
	}

	// aggregate strat signals
	signalsIn := pipeline.FanIn(stratSignals...)
	eErrors = append(eErrors, stratErrs...)

	/*
		Init risk manager
	*/
	fillsRelay := make(chan proto.Fill, e.cfg.BufferSize)
	if err := e.risk.Init(dataOuts[len(e.strategies)], signalsIn, fillsRelay); err != nil {
		e.Close()
		return fmt.Errorf("failed to init risk manager: %w", err)
	}

	orders, orderErrs := e.risk.Streams()
	eErrors = append(eErrors, orderErrs)
	// broadcast to execution layer
	orderOuts := pipeline.FanOut(orders, len(e.executors), e.cfg.BufferSize)

	/*
		Init execution layer
	*/
	fills := make([]<-chan proto.Fill, len(e.executors))
	fillErrs := make([]<-chan error, len(e.executors))
	for i, exec := range e.executors {
		if err := exec.Init(orderOuts[i]); err != nil {
			e.Close()
			return fmt.Errorf("failed to init executor %s: %w", exec.ID(), err)
		}
		fills[i], fillErrs[i] = exec.Streams()
	}

	// merge exec fills into fillsRelay (input to riskmgr)
	fillsRelayWg := pipeline.FanInTo(fillsRelay, fills...)
	eErrors = append(eErrors, fillErrs...)

	// finalize plumbing: close fillsRelay when all execs close
	go func() {
		fillsRelayWg.Wait()
		close(fillsRelay)
	}()

	// error handling: log and close engine after threshold
	go func() {
		errCh := pipeline.FanIn(eErrors...)
		errorCount := 0

		for err := range errCh {
			e.logger.Error(err, "engine error")
			errorCount++
			if errorCount >= e.cfg.ErrorThreshold {
				e.logger.Warn("engine error threshold reached, shutting down")
				e.Close()
			}
		}
	}()

	/*
		Run engine. Start all components in separate goroutines.
	*/

	err := e.risk.Start()
	if err != nil {
		e.Close()
		return fmt.Errorf("failed to start risk manager: %w", err)
	}

	for _, strat := range e.strategies {
		err := strat.Start()
		if err != nil {
			e.Close()
			return fmt.Errorf("failed to start strategy %s: %w", strat.ID(), err)
		}
	}

	for _, exec := range e.executors {
		err := exec.Start()
		if err != nil {
			e.Close()
			return fmt.Errorf("failed to start executor %s: %w", exec.ID(), err)
		}
	}

	if err := e.provider.Start(); err != nil {
		e.Close()
		return fmt.Errorf("failed to start provider %s: %w", e.provider.ID(), err)
	}

	// start ticker
	e.ticker.Start()

	// wait for context cancellation
	<-ctx.Done()
	e.Close()
	return ctx.Err()
}

func (e *Engine) Close() {
	select {
	case <-e.done:
		return // already closed
	default:
		close(e.done)
	}

	// Stop ticker
	if e.ticker != nil {
		e.ticker.Stop()
	}

	// Close provider (stops data flow)
	if e.provider != nil {
		e.provider.Close()
	}

	// Close strategies
	for _, strat := range e.strategies {
		strat.Close()
	}

	// Close risk manager
	if e.risk != nil {
		e.risk.Close()
	}

	// Close executor
	for _, exec := range e.executors {
		exec.Close()
	}

	// Wait for all goroutines
	e.wg.Wait()
}
