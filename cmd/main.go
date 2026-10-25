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

const defaultConfigPath = "configs/data_logger_v2.json"

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

// build instantiates every enabled component and wires the engine. Errors from
// both phases are merged; the engine is nil whenever any error was recorded.
func build(cfg *config.Config, log *logger.Logger) (reg.Runnable, []error) {
	c, errs := instantiate(cfg, log)
	eng, wireErrs := wire(cfg, c, log)
	errs = append(errs, wireErrs...)
	if len(errs) > 0 {
		return nil, errs
	}
	return eng, nil
}
