# Split composition root: instantiation vs wiring

## Context

`cmd/main.go` `build()` does two jobs in one 170-line function:
- **Instantiation:** create providers, strategies, features and the exporter from the registries (steps 1–3 and 6).
- **Wiring:** check cross-references (subscription → provider, strategy → subscription, engine → strategy), build the engine's `_`-prefixed dependency map, and create the engine (steps 4, 5 and 7).

The goal is a clear split: two functions in separate files, linked by one explicit struct. `build()` stays as a thin composition of the two, so `main()`, `--validate-only` and the existing tests keep working unchanged.

Behaviour is preserved: every error is still collected in one pass, and the engine is still skipped when a dependency is unresolved.

## Design

### `cmd/instantiate.go` (new)

```go
// components holds every instantiated component, keyed the way wiring resolves them.
type components struct {
    providers      map[string]proto.DataProvider // by config name
    strategies     map[string]proto.Strategy     // by type
    features       []proto.Feature
    exporter       proto.Exporter // nil when absent or disabled
    exporterFailed bool           // configured but failed to build; blocks engine construction
}

func instantiate(cfg *config.Config, log *logger.Logger) (*components, []error)
```
- Moves steps 1–3 over verbatim: registry checks, disabled skips, per-variant features and logging.
- Moves step 6 here. `buildExporter` is renamed to `instantiateExporter` and its code is unchanged. On error it appends the error and sets `exporterFailed`.
- Always returns a non-nil `*components`, even when errors were recorded, so wiring can still report reference errors in the same pass.

### `cmd/wire.go` (new)

```go
// wire validates references between instantiated components, resolves the
// engine's dependencies and constructs it. The engine is nil whenever any
// error was recorded.
func wire(cfg *config.Config, c *components, log *logger.Logger) (reg.Runnable, []error)
```
- The `engine %q not registered` check moves here, since the engine is what wiring produces.
- Moves steps 4, 5 and 7 over verbatim, reading from `c.*` instead of local maps.
- `depsOK` starts as `!c.exporterFailed`. The rest of the `depsOK` logic is unchanged. Feature failures still don't block engine construction, matching current behaviour.
- `_exporter` is injected only when `c.exporter != nil`.

### `cmd/main.go`

`main()` is unchanged. `build()` becomes:

```go
// build instantiates every enabled component and wires the engine. Errors from
// both phases are merged; the engine is nil whenever any error was recorded.
func build(cfg, log) (reg.Runnable, []error) {
    c, errs := instantiate(cfg, log)
    eng, wireErrs := wire(cfg, c, log)
    errs = append(errs, wireErrs...)
    if len(errs) > 0 { return nil, errs }
    return eng, nil
}
```
The final `len(errs) > 0 → nil` check moves out of the old body into `build()`, because `wire` only sees its own errors. To stay self-contained, `wire` still returns nil when it records errors of its own.

The side-effect imports stay in `main.go`; its `proto` import is dropped (no longer used).

**Known difference:** "engine not registered" now appears after the instantiation errors instead of first. Only the error order changes; the set of errors is the same.

## Tests

- `cmd/main_test.go`: the existing `TestBuild*` tests stay as they are and act as the regression check for the composition.
- New `cmd/wire_test.go`: unit tests that pass a hand-built `*components` (no registry provider creation needed):
  - an unresolved subscription provider → error, nil engine
  - `exporterFailed` → engine skipped, no extra error
  - a missing engine strategy → error
- New `cmd/instantiate_test.go`:
  - an unregistered provider, strategy and feature in one config → all three errors reported, `components` non-nil
  - disabled components are skipped and not validated

## Docs

- On execution, save this plan as `docs/plans/composition-root-split.md`.
- `docs/discussions/engine.md` § Dependency injection: describe the instantiate/wire split, and fix the stale key list (`_provider` → `_subscriptions`/`_providers`). Add a reference to the plan.
- `Makefile`: `run`/`validate` used `go run ./cmd/main.go` (single file), which breaks once `package main` spans files; changed to `go run ./cmd`. Command names and flags are unchanged, so `docs/Commands.md` needs no update.
- `docs/Architecture.md`: the entry point is still `cmd/`, so no update.

## Verification (per docs/Commands.md)

- `make test`: the existing build tests plus the new instantiate and wire tests.
- `make lint`, `go vet ./...`, `make fmt`
- `make validate` on `config.json` → "config valid"
- `make validate CONFIG=<broken copy>` with a bad provider ref plus an unknown feature → both errors reported in one run, and the engine-skipped warning is printed
