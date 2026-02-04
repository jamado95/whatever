package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"whatever/internal/config"
	"whatever/internal/domains/strategy"
	"whatever/internal/engine"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
)

const defaultConfigPath = "config.json"

func main() {
	log := logger.NewLogger(logger.DefaultLoggerConfig())

	cfg, err := config.Load(defaultConfigPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// 1. Instantiate enabled providers
	providers := make(map[string]proto.DataProvider)
	for _, pc := range cfg.Providers {
		if !pc.Enabled || !proto.ProvidersRegistry.Has(pc.Type) {
			log.Info(fmt.Sprintf("skipping disabled/unregistered provider: %s", pc.Type))
			continue
		}
		prov, err := proto.ProvidersRegistry.Create(pc.Type, pc.Opts)
		if err != nil {
			fmt.Printf("Error creating provider %s: %v\n", pc.Type, err)
			os.Exit(1)
		}
		providers[pc.Type] = prov
		log.Info(fmt.Sprintf("created provider: %s", pc.Type))
	}

	// 2. Instantiate enabled strategies
	strategies := make(map[string]proto.Strategy)
	for _, sc := range cfg.Strategies {
		if !sc.Enabled || !proto.StrategiesRegistry.Has(sc.Type) {
			log.Info(fmt.Sprintf("skipping disabled strategy: %s", sc.Type))
			continue
		}
		strat, err := proto.StrategiesRegistry.Create(sc.Type, sc.Opts)
		if err != nil {
			fmt.Printf("Error creating strategy %s: %v\n", sc.Type, err)
			os.Exit(1)
		}
		strategies[sc.Type] = strat
		log.Info(fmt.Sprintf("created strategy: %s", sc.Type))
	}

	// 3. Resolve engine dependencies
	engineOpts := cfg.Engine.Opts
	if provType, ok := engineOpts["provider"].(string); ok {
		if prov, exists := providers[provType]; exists {
			engineOpts["_provider"] = prov
		} else {
			fmt.Printf("Error: engine references unknown provider %s\n", provType)
			os.Exit(1)
		}
	}
	if stratTypes, ok := engineOpts["strategies"].([]any); ok {
		strats := make([]strategy.Strategy, 0, len(stratTypes))
		for _, t := range stratTypes {
			typeName := t.(string)
			if strat, exists := strategies[typeName]; exists {
				strats = append(strats, strat)
			} else {
				fmt.Printf("Error: engine references unknown strategy %s\n", typeName)
				os.Exit(1)
			}
		}
		engineOpts["_strategies"] = strats
	}

	// 4. Create engine
	eng, err := engine.NewDataLogger(engineOpts)
	if err != nil {
		fmt.Printf("Error creating engine %s: %v\n", cfg.Engine.Type, err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Info(fmt.Sprintf("whtv starting with engine: %s", cfg.Engine.Type))

	// 5. Run engine (blocks until context cancelled or engine completes)
	if err := eng.Run(ctx); err != nil {
		log.Error(err, "engine stopped with error")
		os.Exit(1)
	}

	log.Info("whtv shutdown complete")
}
