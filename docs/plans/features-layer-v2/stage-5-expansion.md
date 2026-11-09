# Stage 5 — expansion (features layer v2, iteration 1)

## Context

[main.md](main.md) stage 5. Stage 3's `newGraph` sizes and binds a graph from hand-built instances;
stage 4 registered the five ported types in `reg.FeatureTypes`; stage 2 resolves universes in the
composition root and discards them. Nothing turns configuration into instances.

This stage adds `features.Expand`: specs, one per configured variant, become instances, each a
composed ID at a stream or a pair, handed to `newGraph`. The composition root builds the specs and
injects them, with the resolved universes, into the engine factory; `data_logger_v2`'s factory calls
`Expand` and keeps the graph, which runs from stage 7.

```
config        features[i]: type, universe, variants
cmd           reg.FeatureTypes.Create ─▶ Feature.Validate ─▶ Spec{Type, Universe, Feature}
              cfg.ResolveUniverses ─▶ the universes resolved without error, by name
wire          _feature_specs, _universes ─▶ engine factory; a joined factory error split per part
factory       features.Expand(streams, universes, specs)
                types ─▶ dependencies, cycles ─▶ compositions ─▶ placement ─▶ newGraph ─▶ *Graph
```

The plan, with the decisions it records in [main.md](main.md),
[features.md](../../discussions/features.md) and [config.md](../../discussions/config.md), is
committed before step 1. The stage is then implemented in four steps, in order. Each step is a
review checkpoint: its changes stay unstaged for review, are committed once approved, and the next
step starts from that commit. Every step leaves the tree building, tested and `make check` clean.

| Step | Adds | Commit |
|---|---|---|
| 1 | `Expand` | `feat(features): expand feature specs into the graph` |
| 2 | Joined engine errors reported per part | `feat(cmd): report each part of a joined engine error` |
| 3 | Specs and universes from the composition root | `feat(cmd): build and inject the feature specs` |
| 4 | `Expand` in `data_logger_v2`'s factory; the stage's documentation | `feat(engine): expand the feature graph in data_logger_v2's factory` |

## Decisions

- **A spec is one configured variant.** `Spec{Type, Universe, Feature}`: the config entry's type (the
  base ID other types' `Deps` name), its universe's name (`""` for none) and the variant
  `reg.FeatureTypes` built from its options. It lives in `internal/engine/features`, beside `Expand`.
  The resolved universes are injected beside the specs, keyed by name: `Expand(streams, universes,
  specs)`, as main.md fixes it.
- **Checks on one variant belong to the contract.** `Feature.Validate` in `internal/protocol/feature`
  rejects an empty ID, an ID containing `-`, and an empty or repeated `Deps` entry. The composition
  root calls it after each `Create`, reporting it like a factory error. `Expand` assumes specs passed
  it. With `variantID`, a `-` can only come from a negative value, which every port's config rejects,
  so the check guards future types.
- **Graph checks belong to `Expand`:** everything that needs more than one variant.
  - Variants of one type declare the same `Pair` and `Deps`; a variant that does not fails its type.
  - A variant ID is configured once, across every entry and type; the later spec is dropped. Since
    variant IDs hold no `-` and composition is fixed per type, unique variant IDs make composed IDs
    unique: main.md's "duplicate composed IDs".
  - A dependency type is registered (`reg.FeatureTypes.Has`), has a configured variant and is not a
    pair feature; otherwise its dependent type fails.
  - Type-level cycles, each reported once with its path; the types on it fail.
  - A pair spec names a universe, and that universe has pairs; a spec's universe is in the map.
- **A type may repeat across entries,** each entry with its own universe: `rsi_14` on one universe and
  `rsi_21` on another. A variant ID in two entries is an error, not a union of universes: a duplicate
  ID is an error ([config.md](../../discussions/config.md)). A dependent composes with every variant
  of its dependency type, across entries.
- **A pair feature is not a dependency yet.** Pair features over pair features are an open question
  ([features.md](../../discussions/features.md)), and supporting them changes the binder, `newGraph`'s
  dependency check and stage 6's ordering of pair nodes. `Expand` rejects any dependency type whose
  variants are pair features, naming it.
- **Unknown and unconfigured dependency types are told apart through the registry.** `Expand` reads
  `reg.FeatureTypes.Has`, a lookup over registrations fixed after `init`. The engine's tests register
  their test-only types, so `TestFeatureTypes` (stage 4), which required the registry to hold exactly
  the ported types, is removed: a registered type whose value type has no case in the switch is then
  caught when a config enables it, by `Expand` under `--validate-only`.
- **Any error fails the build.** `Expand` returns a graph only when it found no error; otherwise the
  factory fails, the engine is not constructed and nothing runs. No configured feature or dependency
  is ever left out of a run that starts.
- **Each error is reported once, at its cause.** Within a failing pass, checking goes on past an
  error so every problem is reported: a type whose dependency type failed is set aside without
  restating it, and a spec whose universe is wrong is not placed but still counts as its type's
  variant, so its dependents are not reported unconfigured. `newGraph` runs on what is left, so value
  types and bind errors are reported in the same pass.
- **Composed IDs are a depth-first walk.** A variant's ID, then each dependency's composed ID in `Deps`
  order: `ratio_1` over `zscore` and `vwap`, `zscore` over `rsi`, gives `ratio_1-zscore_50-rsi_14-vwap_20`.
  One composition per combination of dependency variants, memoised per type.
- **Dependencies follow their dependents.** A composition placed at a stream places its dependencies
  there; at a pair, on both ends. Placements are deduplicated by composed ID and coordinate, so a
  dependency configured on its own universe and pulled elsewhere runs once per stream.
- **Deterministic.** `Expand` walks specs, `Deps` and coordinates as slices, never maps: the same
  config gives the same instances and errors in the same order.
- **The graph holds the lookback.** `newGraph` computes the longest `Lookback`, at least 1, once, and
  `Graph.Lookback` returns it: `data.New`'s lookback from stage 7. The instances stay out of the
  `Graph` until stage 6's schedules read them.
- **Universes that fail are left out.** `ResolveUniverses` returns only the universes resolved
  without error; its errors cover the rest, and fail the build. The composition root drops a spec
  whose universe is missing, its error already reported. A dependent of a dropped type's last variant
  would then also report it unconfigured, reachable only once a type with dependencies is registered
  (iteration 3).
- **`wire` splits a joined factory error,** prefixing each part with `engine "<type>":`, so
  `reportErrors` logs and counts each. `reportErrors` and `wire` share the split.
- **`Expand` runs before the feature chain** in `data_logger_v2`'s factory and its error is returned
  unwrapped. A duplicate variant is then reported once, by `Expand`, not again by the chain.
- **Variants are built in both registries until the originals retire.** The composition root builds
  `_features` from `reg.Features` (`data_logger`, and `data_logger_v2`'s bridge until stage 7) and the
  specs from `reg.FeatureTypes`, from the same entries. Their factories decode the same options, so a
  bad option is reported twice. Accepted until `data_logger` and the originals retire. Each registry
  reports a type it does not have, so a type missing from `reg.FeatureTypes` fails the build rather
  than running without it, and a type in neither is reported twice.

## Step 1 — `Expand`

### Changes — new `internal/engine/features/expand.go`

```go
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
sizes and binds them. Specs are assumed to have passed Feature.Validate. Every problem is reported,
joined.
*/
func Expand(st *proto.Streams, universes map[string]config.Resolved, specs []Spec) (*Graph, error)
```

In order:

1. **Types.** Specs grouped by `Type`, in order of first appearance. A variant ID seen before, in any
   type, is reported and its spec dropped. A variant declaring a `Pair` or `Deps` unlike its type's
   first fails the type.
2. **Dependencies.** For each type, each `Deps` entry in order: not registered, registered with no
   spec, or a pair type fails the dependent.
3. **Cycles.** A depth-first walk over the configured types' `Deps`, each back edge reported as its
   path; the types on it fail.
4. **Propagation.** A type depending on a failed type fails, with no error of its own.
5. **Compositions.** Per surviving type, memoised: each variant crossed with every composition of each
   dependency type, in `Deps` order; the composed ID joins their IDs with `-`.
6. **Placement.** Per surviving spec: a pair spec with no universe, a universe without pairs, or a
   universe not in the map is reported, and the spec places nothing. Otherwise each of its variant's
   compositions goes on each coordinate, ascending, and recursively each dependency's at the stream,
   or each end. Placements are deduplicated by composed ID and coordinate.
7. **Graph.** `newGraph(st, instances)`; its error joins `Expand`'s.

Errors, each prefixed `features:`:

| Problem | Error |
|---|---|
| A variant ID twice | `features: rsi_14 is configured twice` |
| Variants of one type unlike | `features: rsi: rsi_21 declares Pair or Deps unlike rsi_14` |
| An unregistered dependency type | `features: zscore: dependency type "rsi" is not registered` |
| A dependency type with no variant | `features: zscore: no variant of dependency type "rsi" is configured` |
| A pair dependency type | `features: zscore: dependency type "corr" is a pair feature, not supported yet` |
| A cycle | `features: dependency cycle: a -> b -> a` |
| A pair feature without a universe | `features: corr_100: a pair feature needs a universe` |
| A universe without pairs | `features: corr_100: universe "usdt" has no pairs` |
| A universe not in the map | `features: rsi_14: unknown universe "usdt"` |

### Changes — `internal/engine/features/graph.go`

- `Graph` gains `lookback`, the longest `Lookback` across the instances `newGraph` sizes, at least 1,
  computed once in `newGraph` and read by `shapes`; `func (g *Graph) Lookback() int` returns it.

### Tests

**`internal/engine/features/features_test.go`** — the test-only types register in `reg.FeatureTypes`
in `init`, each factory returning its base variant, so `Expand` finds them. New types: `ratio`
(`Deps` `sum` then `last`), `ddiff` (over `diff`), and `loop_a` / `loop_b` (over each other) and
`loop_self` (over itself) for cycles. A helper builds specs.

**New `internal/engine/features/expand_test.go`** — `Expand` over `testStreams()` and hand-written
`config.Resolved` universes:

- **`TestExpandPlacement`** — a single-stream type without a universe runs on every stream, with one
  on its universe's streams only; a pair type on its universe's pairs, the pair table equal to them.
- **`TestExpandDependencies`** — `diff` pulls `last` onto its streams, beyond `last`'s own universe,
  and `last` runs once per stream; `ddiff` composes `ddiff-diff-last`; `pdiff` pulls `last` onto both
  ends, `pdiff-last`.
- **`TestExpandVariants`** — `ratio` over `sum_2`, `sum_3` and `last` gives `ratio-sum_2-last` and
  `ratio-sum_3-last`, in `Deps` order; a type in two specs with two universes runs each variant on its
  own, and a dependent composes with both.
- **`TestExpandLookback`** — the longest `Lookback` across placed instances, dependencies included; 1
  with no specs.
- **`TestExpandErrors`** — each row of the error table, by message; a dependent of a failed type adds
  no error; a spec with a wrong universe still serves its dependents.
- **`TestExpandOnePass`** — one call with an `Expand` error, an unsupported value type (`label`) and a
  bind error (`diff` over `sign`) reports all three; two calls give the same errors in the same order.

**`internal/engine/features/ported_test.go`** — `TestFeatureTypes` removed, with what only it used.

### Acceptance

- `go test -race ./internal/engine/features/` passes; `make check` clean.

## Step 2 — joined engine errors

### Changes — `cmd`

- `main.go`: the split `reportErrors` does moves into a helper `reportErrors` and `wire` both use.
- `wire.go`: a joined error from the engine factory is split, each part prefixed `engine %q:`.

No engine returns a joined error yet: `Expand`'s arrive in step 4.

### Tests

- `cmd/wire_test.go` — **`TestWireSplitsEngineErrors`**: a stub engine returning a joined error gives
  one error per part, each prefixed with the engine's type; a single error stays one.
- `cmd/main_test.go` — `TestReportErrorsSplitsJoined` stays.

### Acceptance

- `go test -race ./cmd/...` passes; `make check` clean.

## Step 3 — specs and universes from the composition root

### Changes — `internal/protocol/feature/feature.go`

```go
// Validate checks what the contract requires of any variant: an ID, without '-', and Deps naming
// each type once.
func (f Feature) Validate() error
```

Errors: `feature: empty ID`, `feature: ID "x-1" contains '-'`, `feature: x_1: empty dependency type`,
`feature: x_1: dependency type "rsi" listed twice`.

### Changes — `internal/config/universe.go`

- `ResolveUniverses` keeps a universe in its map only when it resolved without error and, if used,
  selects a stream. Its doc says so.

### Changes — `cmd`

- `instantiate.go`: `components` gains `specs []features.Spec` and `universes map[string]config.Resolved`,
  the latter from `ResolveUniverses`. For each enabled entry, a type `reg.FeatureTypes` does not have
  is reported `feature %q not registered`; otherwise each variant is created and validated, errors
  reported as `feature %q variant %d: %w`, and a spec whose universe is not in `universes` is dropped.
  Nothing is logged per spec: the original's line stays.
- `main.go`: blank-imports `internal/domains/featuresv2`, so `reg.FeatureTypes` holds the ported
  types.
- `wire.go`: injects `_feature_specs` and `_universes`. Nothing reads them until step 4.

### Tests

- `internal/protocol/feature/feature_test.go` — **`TestValidate`**: each error above; a valid
  variant, with and without `Deps`, passes.
- `internal/config/config_test.go` — **`TestResolveUniverses`**: the map holds `btc`, `unused` and
  `disabled_only`, not `typo` and `empty`.
- `cmd/instantiate_test.go` — **`TestInstantiateSpecs`**: one spec per enabled variant, with its type
  and universe; disabled entries build none; a spec on a universe that failed is dropped; a type
  missing from `reg.FeatureTypes` (a test type registered in `reg.Features` only) is reported; a
  variant failing `Validate` (a test type registered with a `-` in its ID) is reported, path-named.
- `cmd/wire_test.go` — **`TestWireInjectsSpecs`**: a stub engine recording its options receives the
  components' specs as `_feature_specs` and universes as `_universes`.

### Acceptance

- `go test -race ./internal/protocol/... ./internal/config/... ./cmd/...` passes; `make check` clean.

## Step 4 — `Expand` in `data_logger_v2`'s factory

### Changes — `internal/engine/data_logger_v2.go`

- The factory reads `_feature_specs` (`[]features.Spec`) and `_universes`
  (`map[string]config.Resolved`), both required, and calls `features.Expand` before
  `feat.NewFeatureChain`, returning its error unwrapped. `DataLoggerV2` gains `graph *features.Graph`.
  `Run` does not read it: stage 7 runs it.

### Tests

- `internal/engine/data_logger_v2_test.go` — **`TestDataLoggerV2Factory`**: options carry specs and
  universes; missing `_feature_specs` or `_universes` is an error; a variant configured twice is
  `Expand`'s error; a valid spec's graph has its `Lookback`.
- `cmd/main_test.go` — **`TestBuildReportsExpandErrors`**: a `data_logger_v2` config with `rsi` 14 in
  two entries and `sma` 20 twice in one reports two errors, `engine "data_logger_v2": features:
  rsi_14 is configured twice` and the `sma_20` one, each its own entry.

### Documentation

- [features.md](../../discussions/features.md):
  - WIP: expansion implemented; `data_logger_v2`'s factory builds the graph, which nothing runs yet.
  - Solution: references this stage.
- [config.md](../../discussions/config.md) — WIP, universes: consumed by `data_logger_v2`'s factory
  through `Expand`; `ResolveUniverses` returns only the universes that resolved.
- [main.md](main.md) — stage 5 marked implemented.
- `docs/Architecture.md` — `internal/engine/features`: `Expand`, from the composition root's specs and
  universes; `internal/protocol/feature`: `Feature.Validate`.
- `configs/README.md`:
  - § Universes: used by `data_logger_v2`, whose factory expands the features over them; not yet
    run.
  - § Binding: a pair feature needs a universe with pairs; dependencies run on every stream their
    dependents do, and on both ends of their pairs; a type may appear in several entries, each with
    its own universe, and an ID configured twice is an error.
  - § Examples: `Expand`'s errors reachable from config; the duplicate variant row becomes
    `engine "data_logger_v2": features: rsi_14 is configured twice`; an unregistered type and a bad
    option are reported once per registry until the original features retire.

### Acceptance

- `go test -race ./internal/engine/... ./cmd/...` passes; `make check` clean.
- `make validate` passes on `configs/data_logger_v2.json` and `configs/data_logger.json`.
- Stage acceptance ([main.md](main.md), as amended): expansion over chains, several dependency types
  and pairs; each error of the table, `Feature.Validate`'s, an unsupported value type and a bind
  error reported in one `--validate-only` pass, one line each; `data_logger`'s behaviour unchanged
  but for an unregistered type or a bad option reported twice.

## Documentation recorded at planning

Committed with this plan, before step 1:

- [main.md](main.md):
  - Decisions, *One feature type*: a missing case or a malformed `Bind` is caught by `Expand`, the
    registered-types test removed.
  - Stage 5: touches and acceptance as this plan amends them — `Feature.Validate` in the contract,
    called by the composition root; repeated types; a pair dependency type an error; `ResolveUniverses`
    returning the valid universes; `wire` splitting joined errors; the graph's lookback, the
    instances left to stage 6.
  - Open items: variants built in both registries, an unregistered type or a bad option reported
    twice, until the originals retire.
- [features.md](../../discussions/features.md):
  - § Universes: a type may repeat across entries, each with its own universe; a variant ID
    configured twice is an error.
  - § Feature buffer, *Value types are the engine's switch*: caught by expansion, not a test over the
    registered types.
  - Open Questions: *Parameters in IDs* — `Feature.Validate` rejects `-`; *Pair features over pair
    features* — `Expand` rejects a pair dependency type until then.
- [config.md](../../discussions/config.md) — Decisions: a feature type may appear in several entries;
  an ID configured twice is still an error.

## Out of scope

- Pair features as dependencies, and asymmetric pair order.
- Collapsing repeated bind errors across coordinates.
- Building variants in one registry only — when the originals retire.
- Running the graph: `New`, schedules and `Step` (stage 6); `data_logger_v2`'s run, `data.New` sized
  from the graph, exports (stage 7).
- The instances on the `Graph` (stage 6).

## Verification

```bash
# step 1
go test -race ./internal/engine/features/
# step 2
go test -race ./cmd/...
# step 3
go test -race ./internal/protocol/... ./internal/config/... ./cmd/...
# step 4
go test -race ./internal/engine/... ./cmd/...
make validate
make validate CONFIG=configs/data_logger.json
# every step
make check
```
