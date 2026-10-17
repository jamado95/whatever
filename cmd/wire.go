package main

import (
	"fmt"

	"whatever/internal/config"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// wire validates references between instantiated components, resolves the
// engine's dependencies and constructs it. Errors are accumulated rather than
// fatal; the engine is nil whenever any error was recorded.
func wire(cfg *config.Config, c *components, log *logger.Logger) (reg.Runnable, []error) {
	var errs []error

	if !reg.Engines.Has(cfg.Engine.Type) {
		errs = append(errs, fmt.Errorf("engine %q not registered", cfg.Engine.Type))
	}

	// 1. Validate subscription and strategy references. These checks run for
	// every subscription regardless of count.
	subNames := make(map[string]bool, len(cfg.Subscriptions))
	for _, sub := range cfg.Subscriptions {
		subNames[sub.ID()] = true
	}
	for i, sub := range cfg.Subscriptions {
		if _, exists := c.providers[sub.Provider]; !exists {
			errs = append(errs, fmt.Errorf(
				"subscriptions[%d] (%s) references provider %q, which was not instantiated (disabled or missing)",
				i, sub.ID(), sub.Provider))
		}
	}
	for _, sc := range cfg.Strategies {
		if sc.Disabled {
			continue
		}
		for _, ref := range sc.Subscriptions {
			if !subNames[ref] {
				errs = append(errs, fmt.Errorf(
					"strategy %q references subscription %q, which is not declared", sc.Type, ref))
			}
		}
	}

	// 2. Resolve engine dependencies. The engine block's own keys stay untouched
	// under "options"; only resolved instances are injected here.
	engineOpts := cfg.Engine.Opts()
	engineOpts["_features"] = c.features

	depsOK := !c.exporterFailed // exporter error already recorded by instantiate

	// one provider instance per subscription
	subProviders := make(map[proto.Subscription]proto.DataProvider, len(cfg.Subscriptions))
	for _, sub := range cfg.Subscriptions {
		if prov, exists := c.providers[sub.Provider]; exists {
			subProviders[sub] = prov
		} else {
			depsOK = false // reference error already recorded above
		}
	}

	if len(cfg.Subscriptions) == 0 {
		depsOK = false // already reported by validate()
	}
	engineOpts["_subscriptions"] = cfg.Subscriptions
	engineOpts["_providers"] = subProviders

	engineStrategies := make([]proto.Strategy, 0, len(cfg.Engine.Strategies))
	for _, name := range cfg.Engine.Strategies {
		strat, exists := c.strategies[name]
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

	if c.exporter != nil {
		engineOpts["_exporter"] = c.exporter
	}

	// 3. Create engine. With a dependency unresolved this is a guaranteed
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
