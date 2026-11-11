package features

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"whatever/internal/config"
	proto "whatever/internal/protocol"
	"whatever/internal/protocol/feature"
	reg "whatever/internal/registry"
)

// Spec is one configured variant: what the composition root builds from a feature entry.
type Spec struct {
	Type     string          // the entry's type: the base ID other types' Deps name
	Universe string          // the entry's universe; "" runs a single-stream feature on every stream
	Feature  feature.Feature // the variant reg.FeatureTypes built from the entry's options
}

/*
Expand turns specs into a validated Graph over st. Each variant is composed with every variant of its
dependency types and placed on its universe's streams, every stream without one, or its universe's
pairs; its dependencies are pulled onto the streams it runs on, both ends for a pair. newGraph then
sizes and binds them. Specs are assumed to have passed Feature.Validate.

Its checks run in stages: grouping, dependencies, cycles, placement, then newGraph's binding. The first
stage with errors stops it, returning all of that stage's errors, joined; each stage relies on those
before it.
*/
func Expand(st *proto.Streams, universes map[string]config.Resolved, specs []Spec) (*Graph, error) {
	instances, err := expand(st, universes, specs)
	if err != nil {
		return nil, err
	}
	return newGraph(st, instances)
}

// expand returns the instances of specs, or the errors of its first check stage that finds any.
func expand(st *proto.Streams, universes map[string]config.Resolved, specs []Spec) ([]instance, error) {
	x := &expansion{
		variants: make(map[string][]Spec),
		comps:    make(map[string][]*composition),
	}
	for _, stage := range []func(){
		func() { x.group(specs) },
		x.checkDeps,
		x.checkCycles,
	} {
		if stage(); len(x.errs) > 0 {
			return nil, errors.Join(x.errs...)
		}
	}

	instances := x.place(st, universes)
	if len(x.errs) > 0 {
		return nil, errors.Join(x.errs...)
	}
	return instances, nil
}

// expansion is Expand's state: the specs by type and the errors found.
type expansion struct {
	specs    []Spec                    // each variant ID once, in order
	types    []string                  // in order of first appearance
	variants map[string][]Spec         // by type
	comps    map[string][]*composition // by type, memoised
	errs     []error
}

// composition is one composed ID: a variant over one composition of each of its dependency types.
type composition struct {
	id      string
	feature feature.Feature
	deps    []*composition // in the variant's Deps order
}

func (x *expansion) fail(format string, args ...any) {
	x.errs = append(x.errs, fmt.Errorf("features: "+format, args...))
}

// deps is a type's dependency types, as its first variant declares them.
func (x *expansion) deps(typ string) []string { return x.variants[typ][0].Feature.Deps }

// group sorts specs by type, reporting a variant ID seen before and a variant declaring Pair or Deps
// unlike its type's first.
func (x *expansion) group(specs []Spec) {
	seen := make(map[string]bool, len(specs))
	for _, s := range specs {
		id := s.Feature.ID
		if seen[id] {
			x.fail("%s is configured twice", id)
			continue
		}
		seen[id] = true

		vs, ok := x.variants[s.Type]
		if !ok {
			x.types = append(x.types, s.Type)
		} else if first := vs[0].Feature; first.Pair != s.Feature.Pair || !slices.Equal(first.Deps, s.Feature.Deps) {
			x.fail("%s: %s declares Pair or Deps unlike %s", s.Type, id, first.ID)
		}
		x.variants[s.Type] = append(vs, s)
		x.specs = append(x.specs, s)
	}
}

// checkDeps reports a dependency type that is not registered, has no configured variant or is a pair
// feature.
func (x *expansion) checkDeps() {
	for _, typ := range x.types {
		for _, d := range x.deps(typ) {
			switch vs, ok := x.variants[d]; {
			case !ok && !reg.FeatureTypes.Has(d):
				x.fail("%s: dependency type %q is not registered", typ, d)
			case !ok:
				x.fail("%s: no variant of dependency type %q is configured", typ, d)
			case vs[0].Feature.Pair:
				x.fail("%s: dependency type %q is a pair feature, not supported yet", typ, d)
			}
		}
	}
}

// checkCycles walks the types' dependencies with DFS, reporting each cycle once, as its path. Every
// dependency type is configured: checkDeps passed.
func (x *expansion) checkCycles() {
	const onPath, done = 1, 2
	state := make(map[string]int, len(x.types))
	var path []string

	var walk func(typ string)
	walk = func(typ string) {
		state[typ] = onPath
		path = append(path, typ)
		for _, d := range x.deps(typ) {
			switch state[d] {
			case 0:
				walk(d)
			case onPath:
				cycle := append(slices.Clone(path[slices.Index(path, d):]), d)
				x.fail("dependency cycle: %s", strings.Join(cycle, " -> "))
			}
		}

		path = path[:len(path)-1]
		state[typ] = done
	}

	for _, typ := range x.types {
		if state[typ] == 0 {
			walk(typ)
		}
	}
}

/*
compose returns a type's compositions: each variant crossed with every composition of each dependency
type, in Deps order, its composed ID their IDs joined with '-'. checkCycles passed, so the walk is
acyclic.
*/
func (x *expansion) compose(typ string) []*composition {
	if comps, ok := x.comps[typ]; ok {
		return comps
	}

	var comps []*composition
	for _, s := range x.variants[typ] {
		combos := [][]*composition{nil}
		for _, d := range s.Feature.Deps {
			var next [][]*composition
			for _, combo := range combos {
				for _, c := range x.compose(d) {
					next = append(next, append(slices.Clone(combo), c))
				}
			}
			combos = next
		}

		for _, combo := range combos {
			ids := []string{s.Feature.ID}
			for _, c := range combo {
				ids = append(ids, c.id)
			}
			comps = append(comps, &composition{id: strings.Join(ids, "-"), feature: s.Feature, deps: combo})
		}
	}
	x.comps[typ] = comps
	return comps
}

/*
place returns every spec's compositions at its coordinates, each composition's dependencies pulled
onto the streams it runs on, each composed ID once per coordinate. A spec with a wrong universe is
reported and places nothing; expand then discards what was placed.
*/
func (x *expansion) place(st *proto.Streams, universes map[string]config.Resolved) []instance {
	type placed struct {
		id string
		at [2]int
	}
	seen := make(map[placed]bool)
	var out []instance

	var put func(c *composition, at [2]int)
	put = func(c *composition, at [2]int) {
		if seen[placed{c.id, at}] {
			return
		}
		seen[placed{c.id, at}] = true

		var deps map[string]string
		if len(c.deps) > 0 {
			deps = make(map[string]string, len(c.deps))
		}
		for i, d := range c.deps {
			deps[c.feature.Deps[i]] = d.id
		}
		out = append(out, instance{feature: c.feature, id: c.id, at: at, deps: deps})

		for _, d := range c.deps {
			for _, end := range at {
				if end >= 0 {
					put(d, [2]int{end, -1})
				}
			}
		}
	}

	for _, s := range x.specs {
		ats, err := coordinates(st, universes, s)
		if err != nil {
			x.errs = append(x.errs, err)
			continue
		}
		for _, c := range x.compose(s.Type) {
			if c.feature.ID != s.Feature.ID {
				continue // another variant's
			}
			for _, at := range ats {
				put(c, at)
			}
		}
	}
	return out
}

// coordinates returns where s runs: its universe's pairs for a pair feature, otherwise its universe's
// streams, or every stream without one.
func coordinates(st *proto.Streams, universes map[string]config.Resolved, s Spec) ([][2]int, error) {
	id := s.Feature.ID
	if s.Universe == "" {
		if s.Feature.Pair {
			return nil, fmt.Errorf("features: %s: a pair feature needs a universe", id)
		}
		ats := make([][2]int, len(st.All))
		for i := range ats {
			ats[i] = [2]int{i, -1}
		}
		return ats, nil
	}

	u, ok := universes[s.Universe]
	if !ok {
		return nil, fmt.Errorf("features: %s: unknown universe %q", id, s.Universe)
	}
	if s.Feature.Pair {
		if len(u.Pairs) == 0 {
			return nil, fmt.Errorf("features: %s: universe %q has no pairs", id, s.Universe)
		}
		return u.Pairs, nil
	}
	ats := make([][2]int, len(u.Streams))
	for i, s := range u.Streams {
		ats[i] = [2]int{s, -1}
	}
	return ats, nil
}
