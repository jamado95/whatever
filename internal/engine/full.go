package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"whatever/internal/logger"
	"whatever/internal/pipeline"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
	"whatever/internal/timing"
)

const FullEngineID = "full"

func init() {
	reg.Engines.Register(FullEngineID, func(opts map[string]any) (reg.Runnable, error) {
		prov, ok := opts["_provider"].(proto.DataProvider)
		if !ok {
			return nil, fmt.Errorf("missing or invalid _provider")
		}

		strats, ok := opts["_strategies"].([]proto.Strategy)
		if !ok {
			return nil, fmt.Errorf("missing or invalid _strategies")
		}

		riskMgr, ok := opts["_risk"].(proto.RiskManager)
		if !ok {
			return nil, fmt.Errorf("missing or invalid _risk")
		}

		execs, ok := opts["_executors"].([]proto.Executor)
		if !ok {
			return nil, fmt.Errorf("missing or invalid _executors")
		}

		cfg, err := parseFullEngineConfig(opts)
		if err != nil {
			return nil, err
		}

		return NewEngine(prov, riskMgr, strats, execs, cfg)
	})
}

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
	provider   proto.DataProvider
	strategies []proto.Strategy
	risk       proto.RiskManager
	executors  []proto.Executor
	ticker     timing.Ticker[proto.MarketData]
	logger     *logger.Logger
	cfg        Config

	done chan struct{}
	wg   sync.WaitGroup
}

type Config struct {
	Subscription   proto.Subscription
	Limit          int // 0 = unlimited
	Ticker         TickerConfig
	BufferSize     int // channel buffer size
	ErrorThreshold int // number of errors to log before shutting down
}

func NewFullEngine(opts map[string]any) (*Engine, error) {
	prov, ok := opts["_provider"].(proto.DataProvider)
	if !ok {
		return nil, fmt.Errorf("missing or invalid _provider")
	}

	strats, ok := opts["_strategies"].([]proto.Strategy)
	if !ok {
		return nil, fmt.Errorf("missing or invalid _strategies")
	}

	riskMgr, ok := opts["_risk"].(proto.RiskManager)
	if !ok {
		return nil, fmt.Errorf("missing or invalid _risk")
	}

	execs, ok := opts["_executors"].([]proto.Executor)
	if !ok {
		return nil, fmt.Errorf("missing or invalid _executors")
	}

	cfg, err := parseFullEngineConfig(opts)
	if err != nil {
		return nil, err
	}

	return NewEngine(prov, riskMgr, strats, execs, cfg)
}

func parseFullEngineConfig(opts map[string]any) (Config, error) {
	cfg := Config{}

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

	if errorThreshold, ok := opts["errorThreshold"].(float64); ok {
		cfg.ErrorThreshold = int(errorThreshold)
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

func NewEngine(
	prov proto.DataProvider,
	riskMgr proto.RiskManager,
	strats []proto.Strategy,
	execs []proto.Executor,
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
	var ticker timing.Ticker[proto.MarketData]
	switch cfg.Ticker.Type {
	case FixedInterval:
		ticker = timing.FixedInterval[proto.MarketData](cfg.Ticker.TickInterval)
	case Realtime:
		ticker = timing.Realtime[proto.MarketData]()
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
