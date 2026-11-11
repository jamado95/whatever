package main

import (
	"fmt"
	"maps"

	"whatever/internal/config"
	"whatever/internal/engine/features"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// components holds every instantiated component, keyed the way wiring resolves them.
type components struct {
	streams    *proto.Streams                // the stream table, built once from the subscriptions
	providers  map[string]proto.DataProvider // by config name
	strategies map[string]proto.Strategy     // by type
	features   []proto.Feature
	featuresV2 []features.Spec            // the feature engine's variants, from reg.FeatureTypes
	universes  map[string]config.Resolved // the universes resolved without error, by name
	exporter   proto.Exporter             // nil when absent or disabled
}

// instantiate constructs every enabled component from the registries,
// accumulating errors instead of stopping at the first. The returned
// components are never nil, so wiring can still report reference errors in
// the same pass.
func instantiate(cfg *config.Config, log *logger.Logger) (*components, []error) {
	var errs []error
	c := &components{
		streams:    proto.NewStreams(cfg.Subscriptions),
		universes:  make(map[string]config.Resolved),
		providers:  make(map[string]proto.DataProvider),
		strategies: make(map[string]proto.Strategy),
		features:   make([]proto.Feature, 0),
	}

	// 0. Universes, resolved over the table; feature specs use only those that resolve
	for _, uc := range cfg.Universes {
		errs = append(errs, c.addUniverse(uc, cfg.Features)...)
	}

	// 1. Providers
	for _, pc := range cfg.Providers {
		if err := c.addProvider(pc, log); err != nil {
			errs = append(errs, err)
		}
	}

	// 2.1 Features: the originals, one per variant, for data_logger and data_logger_v2's bridge
	for _, fc := range cfg.Features {
		if ferrs := c.addFeature(fc, log); len(ferrs) != 0 {
			errs = append(errs, ferrs...)
		}
	}

	// 2.2 Feature specs: the feature engine's variants, from the same entries. Until the originals
	// retire, an unregistered type or a bad option is reported by both registries.
	for _, fc := range cfg.Features {
		if ferrs := c.addFeatureV2(fc, log); len(ferrs) != 0 {
			errs = append(errs, ferrs...)
		}
	}

	// 3. Strategies
	for _, sc := range cfg.Strategies {
		if serr := c.addStrategy(sc, log); serr != nil {
			errs = append(errs, serr)
		}
	}

	// 4. Exporter (optional)
	if err := c.addExporter(cfg.Engine, log); err != nil {
		errs = append(errs, err)
	}

	return c, errs
}

/*
addUniverse resolves cfg over the stream table and keeps it in c.universes when it resolves
without error. A universe an enabled feature of fcfg names that selects no stream is an error. Errors
are prefixed universe "name": Load has validated the name.
*/
func (c *components) addUniverse(cfg config.Universe, fcfg []config.FeatureConfig) []error {
	var errs []error

	ru, rerrs := config.Resolve(cfg, c.streams)
	for _, err := range rerrs {
		errs = append(errs, fmt.Errorf("universe %q: %w", cfg.Name, err))
	}
	if len(errs) > 0 {
		return errs
	}

	// an unmatched name already explains an empty universe
	for _, f := range fcfg {
		if !f.Disabled && f.Universe == cfg.Name && len(ru.Streams) == 0 {
			return []error{fmt.Errorf("universe %q: selects no streams", cfg.Name)}
		}
	}

	c.universes[cfg.Name] = ru
	return nil
}

// addProvider builds an enabled provider, keyed by its instance name.
func (c *components) addProvider(cfg config.ComponentConfig, log *logger.Logger) error {
	if cfg.Disabled {
		log.Info(fmt.Sprintf("skipping disabled provider: %s", cfg.Type))
		return nil
	}
	if !reg.Providers.Has(cfg.Type) {
		return fmt.Errorf("provider %q not registered", cfg.Type)
	}

	prov, err := reg.Providers.Create(cfg.Type, cfg.Opts)
	if err != nil {
		return fmt.Errorf("provider %q: %w", cfg.Type, err)
	}

	c.providers[cfg.Name] = prov
	log.Info(fmt.Sprintf("created provider: %s", cfg.Name))
	return nil
}

/*
addFeature builds an enabled entry's variants from reg.Features: the originals, which
data_logger and data_logger_v2's bridge run. An entry without variants is one without options.

Deprecated: the feature engine's variants come from addFeatureV2. This goes when the
originals retire.
*/
func (c *components) addFeature(cfg config.FeatureConfig, log *logger.Logger) []error {
	if cfg.Disabled {
		log.Info(fmt.Sprintf("skipping disabled feature: %s", cfg.Type))
		return nil
	}
	if !reg.Features.Has(cfg.Type) {
		return []error{fmt.Errorf("feature %q not registered", cfg.Type)}
	}

	var errs []error

	variants := []map[string]any{nil}
	if len(cfg.Variants) != 0 {
		variants = cfg.Variants
	}

	for i, opts := range variants {
		feature, err := reg.Features.Create(cfg.Type, opts)
		if err != nil {
			errs = append(errs, fmt.Errorf("feature %q variant %d: %w", cfg.Type, i, err))
			continue
		}

		c.features = append(c.features, feature)
	}

	return errs
}

/*
addFeatureV2 builds an enabled entry's variants from reg.FeatureTypes, each validated, as
specs on the entry's universe. An entry without variants is one without options. A spec on a
universe that failed to resolve is dropped, its error already reported.
*/
func (c *components) addFeatureV2(cfg config.FeatureConfig, log *logger.Logger) []error {
	if cfg.Disabled {
		return nil
	}
	if !reg.FeatureTypes.Has(cfg.Type) {
		return []error{fmt.Errorf("feature %q not registered", cfg.Type)}
	}

	var errs []error
	variants := []map[string]any{nil}
	if len(cfg.Variants) != 0 {
		variants = cfg.Variants
	}

	for i, opts := range variants {
		f, err := reg.FeatureTypes.Create(cfg.Type, opts)
		if err == nil {
			err = f.Validate()
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("feature %q variant %d: %w", cfg.Type, i, err))
			continue
		}
		// a universe that failed to resolve is already reported
		if _, ok := c.universes[cfg.Universe]; cfg.Universe != "" && !ok {
			continue
		}

		c.featuresV2 = append(c.featuresV2, features.Spec{Type: cfg.Type, Universe: cfg.Universe, Feature: f})
		log.Info(fmt.Sprintf("created feature: %s", f.ID))
	}

	return errs
}

// addStrategy builds an enabled strategy, keyed by its type.
func (c *components) addStrategy(cfg config.ComponentConfig, log *logger.Logger) error {
	if cfg.Disabled {
		log.Info(fmt.Sprintf("skipping disabled strategy: %s", cfg.Type))
		return nil
	}
	if !reg.Strategies.Has(cfg.Type) {
		return fmt.Errorf("strategy %q not registered", cfg.Type)
	}

	strat, err := reg.Strategies.Create(cfg.Type, cfg.Opts)
	if err != nil {
		return fmt.Errorf("strategy %q: %w", cfg.Type, err)
	}

	c.strategies[cfg.Type] = strat
	log.Info(fmt.Sprintf("created strategy: %s", cfg.Type))
	return nil
}

// addExporter builds the engine's exporter into c.exporter. A missing or disabled exporter
// block is not an error: the engine treats it as optional, and c.exporter stays nil.
func (c *components) addExporter(cfg config.EngineConfig, log *logger.Logger) error {
	if cfg.Exporter == nil {
		return nil
	}

	exporterCfg := cfg.Exporter

	exporterType, _ := exporterCfg["type"].(string)
	if exporterType == "" {
		return fmt.Errorf("exporter: missing required 'type' field")
	}

	if disabled, ok := exporterCfg["disabled"].(bool); ok && disabled {
		log.Info(fmt.Sprintf("skipping disabled exporter: %s", exporterType))
		return nil
	}

	if !reg.Exporters.Has(exporterType) {
		return fmt.Errorf("exporter %q not registered", exporterType)
	}

	// Runtime context for filename generation. These are dependency keys, so
	// config.Decode strips them rather than rejecting them as unknown fields.
	opts := make(map[string]any, len(exporterCfg)+3)
	maps.Copy(opts, exporterCfg)

	// Multi-sub: inject a non-sub-specific label so the filename does not
	// carry a single subscription's identity.
	opts["_symbol"] = "multi"
	opts["_timeframe"] = "multi"
	opts["_engine"] = cfg.Type

	exporter, err := reg.Exporters.Create(exporterType, opts)
	if err != nil {
		return fmt.Errorf("exporter %q: %w", exporterType, err)
	}

	log.Info(fmt.Sprintf("created exporter: %s", exporterType))
	c.exporter = exporter
	return nil
}
