package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"whatever/internal/config"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"

	// side-effect imports
	_ "whatever/internal/domains/execution"
	_ "whatever/internal/domains/exporter"
	_ "whatever/internal/domains/features"
	_ "whatever/internal/domains/market"
	_ "whatever/internal/domains/provider"
	_ "whatever/internal/domains/risk"
	_ "whatever/internal/domains/strategy"
	_ "whatever/internal/engine"
)

const defaultConfigPath = "config.json"

func main() {
	log := logger.NewLogger(logger.DefaultLoggerConfig())

	configPath := flag.String("config", defaultConfigPath, "path to the config file")
	validateOnly := flag.Bool("validate-only", false,
		"validate the config and exit without running the engine")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error(err, fmt.Sprintf("%s: %v", *configPath, err))
		os.Exit(1)
	}

	// Construction is side-effect free — no factory performs I/O, which is what
	// makes building everything a genuine dry run. Errors are collected rather
	// than fatal so that one pass reports every problem in the file.
	eng, errs := build(cfg, log)
	if len(errs) > 0 {
		for _, e := range errs {
			log.Error(e, fmt.Sprintf("%s: %v", *configPath, e))
		}
		log.Error(fmt.Errorf("%d config error(s)", len(errs)),
			fmt.Sprintf("%s: %d config error(s)", *configPath, len(errs)))
		os.Exit(1)
	}

	if *validateOnly {
		log.Info(fmt.Sprintf("%s: config valid", *configPath))
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info(fmt.Sprintf("whtv starting with engine: %s", cfg.Engine.Type))

	if err := eng.Run(ctx); err != nil {
		log.Error(err, "engine stopped with error")
		os.Exit(1)
	}

	log.Info("whtv shutdown complete")
}

// build constructs every enabled component and wires the engine, accumulating
// errors instead of exiting on the first. The returned engine is nil whenever
// any error was recorded.
func build(cfg *config.Config, log *logger.Logger) (reg.Runnable, []error) {
	var errs []error

	if !reg.Engines.Has(cfg.Engine.Type) {
		errs = append(errs, fmt.Errorf("engine %q not registered", cfg.Engine.Type))
	}

	// 1. Instantiate enabled providers
	providers := make(map[string]proto.DataProvider)
	for _, pc := range cfg.Providers {
		if pc.Disabled {
			log.Info(fmt.Sprintf("skipping disabled provider: %s", pc.Type))
			continue
		}
		if !reg.Providers.Has(pc.Type) {
			errs = append(errs, fmt.Errorf("provider %q not registered", pc.Type))
			continue
		}

		prov, err := reg.Providers.Create(pc.Type, pc.Opts)
		if err != nil {
			errs = append(errs, fmt.Errorf("provider %q: %w", pc.Type, err))
			continue
		}
		providers[pc.Type] = prov
		log.Info(fmt.Sprintf("created provider: %s", pc.Type))
	}

	// 2. Instantiate enabled strategies
	strategies := make(map[string]proto.Strategy)
	for _, sc := range cfg.Strategies {
		if sc.Disabled {
			log.Info(fmt.Sprintf("skipping disabled strategy: %s", sc.Type))
			continue
		}
		if !reg.Strategies.Has(sc.Type) {
			errs = append(errs, fmt.Errorf("strategy %q not registered", sc.Type))
			continue
		}

		strat, err := reg.Strategies.Create(sc.Type, sc.Opts)
		if err != nil {
			errs = append(errs, fmt.Errorf("strategy %q: %w", sc.Type, err))
			continue
		}
		strategies[sc.Type] = strat
		log.Info(fmt.Sprintf("created strategy: %s", sc.Type))
	}

	// 3. Instantiate features (one per variant)
	features := make([]proto.Feature, 0)
	for _, fc := range cfg.Features {
		if fc.Disabled {
			log.Info(fmt.Sprintf("skipping disabled feature: %s", fc.Type))
			continue
		}
		if !reg.Features.Has(fc.Type) {
			errs = append(errs, fmt.Errorf("feature %q not registered", fc.Type))
			continue
		}

		// A feature with no variants is a single instance with no options.
		variants := fc.Variants
		if len(variants) == 0 {
			variants = []map[string]any{nil}
		}

		for i, opts := range variants {
			feature, err := reg.Features.Create(fc.Type, opts)
			if err != nil {
				errs = append(errs, fmt.Errorf("feature %q variant %d: %w", fc.Type, i, err))
				continue
			}
			features = append(features, feature)
			log.Info(fmt.Sprintf("created feature: %s", feature.ID().Name))
		}
	}

	// 4. Resolve engine dependencies. The engine block's own keys stay untouched
	// under "options"; only resolved instances are injected here.
	engineOpts := cfg.Engine.Opts()
	engineOpts["_features"] = features
	engineOpts["_subscription"] = cfg.Subscription

	depsOK := true

	if prov, exists := providers[cfg.Engine.Provider]; exists {
		engineOpts["_provider"] = prov
	} else {
		errs = append(errs, fmt.Errorf(
			"engine references provider %q, which was not instantiated (disabled or missing)",
			cfg.Engine.Provider))
		depsOK = false
	}

	engineStrategies := make([]proto.Strategy, 0, len(cfg.Engine.Strategies))
	for _, name := range cfg.Engine.Strategies {
		strat, exists := strategies[name]
		if !exists {
			errs = append(errs, fmt.Errorf(
				"engine references strategy %q, which was not instantiated (disabled or missing)",
				name))
			depsOK = false
			continue
		}
		engineStrategies = append(engineStrategies, strat)
	}
	engineOpts["_strategies"] = engineStrategies

	// 5. Resolve exporter (optional)
	if exporter, ok, err := buildExporter(cfg, log); err != nil {
		errs = append(errs, err)
		depsOK = false
	} else if ok {
		engineOpts["_exporter"] = exporter
	}

	// 6. Create engine. With a dependency unresolved this is a guaranteed
	// failure, so it is skipped rather than attempted — the resulting "missing
	// _provider" would restate an error already reported. The skip is reported
	// as a consequence, not counted as a config error of its own.
	if !depsOK || !reg.Engines.Has(cfg.Engine.Type) {
		log.Warn(fmt.Sprintf(
			"engine %q not constructed: unresolved dependencies; errors inside its "+
				"'options' block will surface once the above are fixed", cfg.Engine.Type))
		return nil, errs
	}

	eng, err := reg.Engines.Create(cfg.Engine.Type, engineOpts)
	if err != nil {
		errs = append(errs, fmt.Errorf("engine %q: %w", cfg.Engine.Type, err))
		return nil, errs
	}

	if len(errs) > 0 {
		return nil, errs
	}
	return eng, nil
}

// buildExporter reports (exporter, configured, error). A missing or disabled
// exporter block is not an error — the engine treats it as optional.
func buildExporter(cfg *config.Config, log *logger.Logger) (proto.Exporter, bool, error) {
	if cfg.Engine.Exporter == nil {
		return nil, false, nil
	}

	exporterCfg := cfg.Engine.Exporter

	exporterType, _ := exporterCfg["type"].(string)
	if exporterType == "" {
		return nil, false, fmt.Errorf("exporter: missing required 'type' field")
	}

	if disabled, ok := exporterCfg["disabled"].(bool); ok && disabled {
		log.Info(fmt.Sprintf("skipping disabled exporter: %s", exporterType))
		return nil, false, nil
	}

	if !reg.Exporters.Has(exporterType) {
		return nil, false, fmt.Errorf("exporter %q not registered", exporterType)
	}

	// Runtime context for filename generation. These are dependency keys, so
	// config.Decode strips them rather than rejecting them as unknown fields.
	opts := make(map[string]any, len(exporterCfg)+3)
	for k, v := range exporterCfg {
		opts[k] = v
	}
	opts["_symbol"] = cfg.Subscription.Symbol
	opts["_timeframe"] = string(cfg.Subscription.Timeframe)
	opts["_engine"] = cfg.Engine.Type

	exporter, err := reg.Exporters.Create(exporterType, opts)
	if err != nil {
		return nil, false, fmt.Errorf("exporter %q: %w", exporterType, err)
	}

	log.Info(fmt.Sprintf("created exporter: %s", exporterType))
	return exporter, true, nil
}
