package main

import (
	"context"
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

	cfg, err := config.Load(defaultConfigPath)
	if err != nil {
		log.Error(err, "Error loading config: %v\n")
		os.Exit(1)
	}

	// 0. check engine type is registered
	if !reg.Engines.Has(cfg.Engine.Type) {
		err := fmt.Errorf("engine %s not registered", cfg.Engine.Type)
		log.Error(err, err.Error())
		os.Exit(1)
	}

	// 1. Instantiate enabled providers
	providers := make(map[string]proto.DataProvider)
	for _, pc := range cfg.Providers {
		if !pc.Enabled && reg.Providers.Has(pc.Type) {
			log.Info(fmt.Sprintf("skipping disabled provider: %s", pc.Type))
			continue
		}
		prov, err := reg.Providers.Create(pc.Type, pc.Opts)
		if err != nil {
			log.Error(err, fmt.Sprintf("Error creating provider %s: %v\n", pc.Type, err))
			os.Exit(1)
		}
		providers[pc.Type] = prov
		log.Info(fmt.Sprintf("created provider: %s", pc.Type))
	}

	// 2. Instantiate enabled strategies
	strategies := make(map[string]proto.Strategy)
	for _, sc := range cfg.Strategies {
		if !sc.Enabled && reg.Strategies.Has(sc.Type) {
			log.Info(fmt.Sprintf("skipping disabled strategy: %s", sc.Type))
			continue
		}
		strat, err := reg.Strategies.Create(sc.Type, sc.Opts)
		if err != nil {
			log.Error(err, fmt.Sprintf("error creating strategy %s: %v\n", sc.Type, err))
			os.Exit(1)
		}
		strategies[sc.Type] = strat
		log.Info(fmt.Sprintf("created strategy: %s", sc.Type))
	}

	// 3. Instantiate features (one per variant)
	features := make([]proto.Feature, 0)
	for _, fc := range cfg.Features {
		if !reg.Features.Has(fc.Type) {
			log.Info(fmt.Sprintf("skipping unregistered feature: %s", fc.Type))
			continue
		}
		for _, opts := range fc.Variants {
			feat, err := reg.Features.Create(fc.Type, opts)
			if err != nil {
				log.Error(err, fmt.Sprintf("error creating feature %s: %v", fc.Type, err))
				os.Exit(1)
			}
			features = append(features, feat)
			log.Info(fmt.Sprintf("created feature: %s", feat.ID()))
		}
	}

	// 4. Resolve engine dependencies
	engineOpts := cfg.Engine.Opts

	// pass features to engine
	engineOpts["_features"] = features

	// resolve provider
	if provType, ok := engineOpts["provider"].(string); ok && reg.Providers.Has(provType) {
		engineOpts["_provider"] = providers[provType]
	} else {
		err := fmt.Errorf("engine config references unknown provider: %s", provType)
		log.Error(err, err.Error())
		os.Exit(1)
	}

	// resolve strategies
	engineOpts["_strategies"] = make([]proto.Strategy, 0)
	if stratTypes, ok := engineOpts["strategies"].([]any); ok {
		for _, t := range stratTypes {
			stratType := t.(string)
			if reg.Strategies.Has(stratType) {
				engineOpts["_strategies"] = append(
					engineOpts["_strategies"].([]proto.Strategy),
					strategies[stratType],
				)
			} else {
				err := fmt.Errorf("engine config references unknown strategy: %s", stratType)
				log.Error(err, err.Error())
				os.Exit(1)
			}
		}
	}

	// 5. Create engine
	eng, err := reg.Engines.Create(cfg.Engine.Type, engineOpts)
	if err != nil {
		log.Error(err, fmt.Sprintf("error creating engine %s: %v", cfg.Engine.Type, err))
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info(fmt.Sprintf("whtv starting with engine: %s", cfg.Engine.Type))

	// 6. Run engine (blocks until context cancelled or engine completes)
	if err := eng.Run(ctx); err != nil {
		log.Error(err, "engine stopped with error")
		os.Exit(1)
	}

	log.Info("whtv shutdown complete")
}
