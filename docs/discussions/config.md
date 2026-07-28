# Config

## Topic

`config.json` is the single input that determines what the program runs: which providers, features
and strategies are constructed, which engine wires them together, and how each is parameterised.
Every component receives its slice of that file as `map[string]any` through the registry factory
signature.

Objective of this discussion: record how config reaches components, why the untyped map produced a
family of silent failures, and the decoding contract that replaced it.

Implemented 2026-07-26 — see Solution for the executed plan.

## Current Understanding

### Path from file to component

`internal/config.Load` decodes the file with `DisallowUnknownFields`, then `cmd/main.go` walks the
blocks, constructs each enabled component from its registry, and injects the resolved instances into
the engine's option map under `_`-prefixed keys. Each factory decodes its own slice of the map into
a struct it declares itself.

```
config.json ──Load──▶ config.Config ──main.go──▶ registry factories ──config.Decode──▶ typed struct
                                          └── resolved instances injected as _provider, _features, …
```

### The decoding contract

`config.Decode(opts, dst)` strips `_`-prefixed keys, re-marshals what remains, and decodes it with
unknown fields rejected, then runs `dst.Validate()` if implemented.

Round-tripping through `encoding/json` is what does the work. The map originated as JSON, so the
trip is lossless, and the decoder handles number widening, type mismatches and unknown keys in one
pass — the three things hand-written assertions got wrong. Stripping `_*` keys first is what keeps
injected interface values (holding mutexes, `*os.File`, channels) away from the decoder, which is
why dependency injection and strict decoding can share one map.

Source of truth sits at two levels:

- **File shape** — which blocks exist, that features carry `variants[]` — is central, in
  `config.Config`.
- **Component fields** — the struct declared beside each factory. Its json tags are the accepted
  keys, its field types the accepted value types, an enum's `UnmarshalJSON` the accepted value set,
  the factory's literal the defaults, `Validate()` the cross-field rules.

The validator and the factory decode into the same struct, so validation can neither describe a
shape the code does not consume nor miss a key it does.

### Requiredness

Requiredness does not fall out of decoding. An absent field keeps whatever the defaults literal
held, so "user omitted it" and "zero value" are indistinguishable. It must be stated explicitly,
either as a pointer field (nil means absent — used for the ticker block, where a missing block and
an incomplete one mean different things) or in `Validate()`.

### Two readers of the engine block

`main.go` resolves `provider`, `strategies` and `exporter` into instances; the engine factory reads
its own tuning. Nesting the latter under `options` keeps the two key sets disjoint so each is
validated strictly by exactly one reader. This mirrors the feature block, where `type`/`disabled`/
`variants` belong to `config` and the variant maps belong to the factory.

`subscription` sits at top level rather than in the engine block because it is shared by the
provider (`Init`) and the exporter (export filenames), and is not engine tuning. It reaches the
engine as an injected `_subscription` dependency.

### Dry run

`--validate-only` constructs every enabled component and exits before `Run`. It needs no separate
validation machinery because `main.go` already builds everything before running anything, and no
factory performs I/O. Errors are accumulated rather than fatal, so one pass reports every
independent problem.

## Decisions

- **Unknown config keys are a hard startup error**, not a warning. Silent failure was the defect
  class this layer exists to close.
- **Config enums accept only known values**, via validating `UnmarshalJSON` (`proto.Timeframe`,
  `TickerType`). Enum types are strings, not iota — an integer zero value is a legal selection, so
  "absent" and "explicitly chosen" become indistinguishable.
- **Schemas live with their component, not in a central table.** Preserves locality and avoids a
  table that can drift out of agreement with the factory. Rules out validating a component's options
  without constructing it.
- **Factories must not perform I/O.** Long true in practice, now load-bearing: it is what makes
  `--validate-only` a genuine dry run.
- **Channel capacity is not config.** Capacity belongs to whoever owns the channel, so each owner
  carries its own default rather than exposing `bufferSize`.
- **Feature IDs encode every identity-bearing field**, omitting defaulted ones so common IDs
  (`ema_20`) stay stable in exports. A duplicate ID is therefore an error, not a deduplication.
- **Disabled blocks are not validated**, keeping them usable as scratch space.
- **An engine with an unresolved dependency is skipped, not constructed.** Constructing it would
  only restate an error already reported. Accepted cost: errors inside that engine's own options
  surface on the next run.

## Solution

Executed implementation plans:

- [`docs/plans/config-strict-parsing.md`](../plans/config-strict-parsing.md) — strict config parsing
  and validation. Executed 2026-07-26.

As implemented:

| Piece | Location |
|---|---|
| `Decode`, `DecodeOptions`, `Dep`, `OptDep`, `Duration`, `Validator` | `internal/config/decode.go` |
| File shape, strict `Load`, `EngineConfig.Opts()` | `internal/config/config.go` |
| `Timeframe.UnmarshalJSON` | `internal/protocol/provider.go` |
| `TickerType`, `TickerConfig`, `newTicker` | `internal/engine/ticker.go` |
| Per-component config structs | beside each factory |
| `--config`, `--validate-only`, error accumulation | `cmd/main.go` |

Resolved by this work: silent ticker fallback, triplicated engine parsers, untyped config
([[engine]] defects 1, 4, 6), and the `binance_historical` time window ([[data-layer]]).

### Failure modes this closed

| Mode | Was | Now |
|---|---|---|
| A. Enum falls back | `tickerType, _ := ticker["type"].(string)`, else realtime — in all three engines | `UnmarshalJSON` rejects any unrecognised token; no realtime fallback path exists |
| B. Discarded assertion → zero | `symbol, _ := sub["symbol"].(string)`; `file`; `output_dir` | Required fields error via `Validate()`; `Timeframe` validates its value set |
| C. Assertion that can never succeed | `opts["startTime"].(int64)` against a `float64` — the window was *always* 0 | `encoding/json` widens into the declared `int64`; regression test in `binance_historical_test.go` |
| D. Optional block skipped whole | misspelling `ticker` skipped every field inside it | `tickers` is an unknown key and errors |
| E. Unknown key accepted | `ema.smoothing` did nothing; `bufferSize` was parsed and never read | Unknown keys error; `smoothing` is now a real field, `bufferSize` removed from config entirely |

## Open Questions

- Generating a config reference from the component structs. Would need that same registry. Cheap
  once it exists; not worth it on its own.
- Should `exporter` become a top-level component block referenced by name, like providers, features
  and strategies? More consistent and would allow several exporters. Deferred as a separate format
  change.
- Multi-subscription. Top-level singular `subscription` bakes in the one-provider-per-engine
  assumption ([[engine]] defect 9). It makes the limit visible rather than burying it in the engine
  block, but multi-symbol work will revisit this key.
- Secrets. `config.json` is checked in, so credentials cannot live there — unresolved and needed
  before any live provider or executor ([[execution-layer]]).

## WIP

- Implemented and verified: all five failure modes fail at startup naming the path; `make run`
  behaviour unchanged (120 candles in 12s at the configured 100ms, export written); tests in
  `internal/config`, `internal/engine`, `internal/domains/provider`.
- `buy_the_dip` still ignores its options entirely and its `config.json` keys match no struct field
  ([[strategy-layer]] defect 4). Left out of this work by decision — wiring it changes strategy
  behaviour, which belongs with that layer's own fixes. Its block is disabled, so it is not
  validated today; enabling it will surface the mismatch.
- `swing_highlow` was updated but sits behind a `//go:build wip` tag, so it is not in the default
  build and its `Validate()` (even period) is unexercised.

## Resources

- `internal/config/`, `cmd/main.go`, `config.json`
- `docs/Commands.md` § Config validation, `docs/Architecture.md` § Design Rules
- Related: [[engine]], [[data-layer]], [[features]], [[strategy-layer]]
