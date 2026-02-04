package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"whatever/domains/execution"
	"whatever/domains/provider"
	"whatever/domains/risk"
	"whatever/domains/strategy"
	"whatever/types"
	"whatever/utils/logger"
	"whatever/utils/pipeline"
	"whatever/utils/timing"
)

type TickerType int

const (
	Realtime TickerType = iota
	FixedInterval
)

type TickerConfig struct {
	Type         TickerType
	TickInterval time.Duration
}

type Engine struct {
	provider   provider.DataProvider
	strategies []strategy.Strategy
	risk       risk.RiskManager
	executors  []execution.Executor
	ticker     timing.Ticker[types.MarketData]
	logger     *logger.Logger
	cfg        Config

	done chan struct{}
	wg   sync.WaitGroup
}

type Config struct {
	Subscription   types.Subscription
	Limit          int // 0 = unlimited
	Ticker         TickerConfig
	BufferSize     int // channel buffer size
	ErrorThreshold int // number of errors to log before shutting down
}

func NewEngine(
	prov provider.DataProvider,
	riskMgr risk.RiskManager,
	strats []strategy.Strategy,
	execs []execution.Executor,
	cfg Config,
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

	// initiate ticker based on engine config
	var ticker timing.Ticker[types.MarketData]
	switch cfg.Ticker.Type {
	case FixedInterval:
		ticker = timing.FixedInterval[types.MarketData](cfg.Ticker.TickInterval)
	case Realtime:
		ticker = timing.Realtime[types.MarketData]()
	default:
		return nil, fmt.Errorf("unsupported ticker type: %d", cfg.Ticker.Type)
	}

	// init engine logger
	logger := logger.NewLogger(logger.DefaultLoggerConfig()).With("domain", "engine")

	return &Engine{
		provider:   prov,
		strategies: strats,
		risk:       riskMgr,
		executors:  execs,
		cfg:        cfg,
		ticker:     ticker,
		logger:     logger,
		done:       make(chan struct{}),
	}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	// declare engine error channels: strats + execs + risk + provider
	var eErrors []<-chan error

	/*
		Init data provider layer
	*/
	if err := e.provider.Init(e.cfg.Subscription, e.cfg.Limit); err != nil {
		e.Close()
		return fmt.Errorf("failed to init provider %s: %w", e.provider.ID(), err)
	}

	data, providerErrs := e.provider.Streams()
	eErrors = append(eErrors, providerErrs)
	// gate data through e.ticker and broadcast to strats and riskmgr
	dataOuts := pipeline.FanOut(e.ticker.Gate(data), len(e.strategies)+1, e.cfg.BufferSize)

	/*
		Init strategy modules
	*/
	stratSignals := make([]<-chan types.Signal, len(e.strategies))
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
	fillsRelay := make(chan types.Fill, e.cfg.BufferSize)
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
	fills := make([]<-chan types.Fill, len(e.executors))
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
