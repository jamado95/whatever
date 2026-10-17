package main

import (
	"fmt"

	"whatever/internal/config"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// components holds every instantiated component, keyed the way wiring resolves them.
type components struct {
	providers      map[string]proto.DataProvider // by config name
	strategies     map[string]proto.Strategy     // by type
	features       []proto.Feature
	exporter       proto.Exporter // nil when absent or disabled
	exporterFailed bool           // configured but failed to build; blocks engine construction
}

// instantiate constructs every enabled component from the registries,
// accumulating errors instead of stopping at the first. The returned
// components are never nil, so wiring can still report reference errors in
// the same pass.
func instantiate(cfg *config.Config, log *logger.Logger) (*components, []error) {
	var errs []error
	c := &components{
		providers:  make(map[string]proto.DataProvider),
		strategies: make(map[string]proto.Strategy),
		features:   make([]proto.Feature, 0),
	}

	// 1. Providers
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
		c.providers[pc.Name] = prov
		log.Info(fmt.Sprintf("created provider: %s", pc.Name))
	}

	// 2. Strategies
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
		c.strategies[sc.Type] = strat
		log.Info(fmt.Sprintf("created strategy: %s", sc.Type))
	}

	// 3. Features (one per variant)
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
			c.features = append(c.features, feature)
			log.Info(fmt.Sprintf("created feature: %s", feature.ID().Name))
		}
	}

	// 4. Exporter (optional)
	if exporter, ok, err := instantiateExporter(cfg, log); err != nil {
		errs = append(errs, err)
		c.exporterFailed = true
	} else if ok {
		c.exporter = exporter
	}

	return c, errs
}

// instantiateExporter reports (exporter, configured, error). A missing or
// disabled exporter block is not an error — the engine treats it as optional.
func instantiateExporter(cfg *config.Config, log *logger.Logger) (proto.Exporter, bool, error) {
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
	// Multi-sub: inject a non-sub-specific label so the filename does not
	// carry a single subscription's identity.
	opts["_symbol"] = "multi"
	opts["_timeframe"] = "multi"
	opts["_engine"] = cfg.Engine.Type

	exporter, err := reg.Exporters.Create(exporterType, opts)
	if err != nil {
		return nil, false, fmt.Errorf("exporter %q: %w", exporterType, err)
	}

	log.Info(fmt.Sprintf("created exporter: %s", exporterType))
	return exporter, true, nil
}
