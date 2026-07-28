# Strict config parsing and validation

## Context

Every component in this repo is constructed from an untyped `map[string]any` decoded from
`config.json`, with per-field type assertions whose errors are discarded. A survey of all config
consumers found five distinct silent-failure modes:

| Mode | Instance |
|---|---|
| A. Enum falls back silently | `tickerType, _ := ticker["type"].(string)` then `if == "fixed" … else Realtime` — triplicated across the three engines. Typo, missing key and wrong type are indistinguishable from "user wants realtime". |
| B. Discarded assertion → zero value | `symbol, _ := sub["symbol"].(string)`; `file` in `binance_csv`; `output_dir` in `jsonl`. `proto.Timeframe(timeframe)` accepts any string. |
| C. Assertion that can never succeed | `opts["startTime"].(int64)` in `binance_historical.go:27` — `encoding/json` decodes numbers as `float64`, so the time window is *always* `0`. |
| D. Optional block skipped whole | `if ticker, ok := opts["ticker"].(map[string]any); ok` — misspelling the block key skips every field in it. No engine field is required. |
| E. Unknown key accepted | `ema.variants[].smoothing: 2.0` (EMA hardcodes `2.0/(period+1)`); `bufferSize: 64` (parsed by `data_logger`/`signal_logger`, read only by `full`). Both live in `config.json` today. |

The common root is that **a failed assertion is indistinguishable from an absent value, and both
yield a usable zero**. Consequence: the engine runs to completion producing wrong output rather than
refusing to start. This blocks the backtesting work, where a silently-wrong config invalidates
results without any signal.

Outcome intended: `config.json` is validated strictly against the field keys, value types and enum
value sets each component declares; anything unrecognised is a startup error naming the path; and a
config can be checked without executing anything.

Full discussion: [docs/discussions/engine.md](../discussions/engine.md)
§ Config parsing.

## Decisions carried in

- Unknown config keys are a **hard startup error**, not a warning.
- **Disabled blocks are not validated** — a switched-off component need not be well-formed.
- All config enums accept **only known values** (`proto.Timeframe`, ticker type, any future enum).
- `bufferSize` **removed** from `data_logger`/`signal_logger`; stays in `full`, which uses it.
- `ema.smoothing` becomes a **real optional field** (default `2.0`), used as the multiplier numerator.
- Feature IDs must derive from **all identity-bearing fields**, not just `period`.
- `removeDuplicates` **returns an error** on ID collision instead of dropping silently.
- Dry-run is a **flag, not a module** — no prototype registration, no reflection, no `registry`
  signature change. Rests on the invariant that **factories perform no I/O**; verified true across
  all registration sites today, and to be stated as a rule.
- `buy_the_dip` is **out of scope** — its factory ignores `opts` entirely and its `config.json` keys
  match no struct field. Deferred to [[strategy-layer]], where it is already recorded as defect 4.

## Target config.json shape

The engine block is read by two consumers: `main.go` resolves `provider`/`strategies`/`exporter`
into instances, the engine factory reads its own tuning keys. Nesting the latter under `options`
makes the two key sets disjoint, mirroring how features already split block keys from `variants[]`.
`subscription` moves to top level because it is shared by the provider (`Init`) and the exporter
(export filenames), and is not engine tuning.

```json
{
  "providers": [ ... ],
  "features":  [ ... ],
  "strategies":[ ... ],
  "subscription": { "symbol": "BTCUSDT", "timeframe": "1h" },
  "engine": {
    "type": "data_logger",
    "provider": "binance_csv",
    "strategies": [],
    "exporter": { "type": "jsonl", "output_dir": "data/exports" },
    "options": { "ticker": { "type": "fixed", "interval": "100ms" }, "limit": 1000 }
  }
}
```

The engine factory decodes `opts["options"]` only. `main.go` owns everything else in the block and
injects resolved instances as `_provider`, `_features`, `_exporter`, `_subscription`.

---

## Phase 1 — `internal/config` primitives

**`internal/config/decode.go`** (new)

```go
type Validator interface{ Validate() error }

// Decode strictly decodes opts into dst: strips _-prefixed dependency keys,
// round-trips through encoding/json with DisallowUnknownFields, then runs
// dst.Validate() if implemented.
func Decode(opts map[string]any, dst any) error

// Dep extracts an injected dependency with a typed error instead of a silent zero.
func Dep[T any](opts map[string]any, key string) (T, error)

// Duration decodes Go duration strings ("100ms") into time.Duration.
type Duration time.Duration
```

`Decode` stripping `_*` keys before marshalling is what makes injected interface values (which hold
mutexes, `*os.File`, channels) harmless — they never reach the decoder, so unknown-key rejection and
in-place mutation of the opts map both stop mattering. Errors wrap the component/path for messages
like `feature "ema" variant 0: json: unknown field "smoothingg"`.

**`internal/config/config.go`** (modify)

- `Config` gains `Subscription proto.Subscription \`json:"subscription"\``.
- `FeatureConfig`, `ComponentConfig`, `EngineConfig` custom `UnmarshalJSON` currently accept and
  silently drop unrecognised block-level keys. Make each reject unknown keys — this is mode E at
  block level (`{"type":"rsi","periods":[…]}` vanishes today).
- `EngineConfig` gains typed `Provider`, `Strategies`, `Exporter`, and `Options map[string]any`
  (kept raw — the engine factory owns its shape).

**`internal/config/config_test.go`** (new — the repo currently has zero tests)

Unknown key rejected; `_*` keys stripped; `float64`→`int` widening; enum rejection; `Duration`
parsing; `Dep` missing/wrong-type; `Validate()` invoked and its error propagated.

## Phase 2 — Enum types

- **`proto.Timeframe.UnmarshalJSON`** — reuse the existing
  [`IsValid()`](../../internal/protocol/provider.go:20); error listing legal
  values. *This touches `internal/protocol`, which `Architecture.md` marks as change-only-when-planned
  — this plan is that authorisation.*
- **`engine.TickerType`** — change from `int`/iota to a `string` type (`"realtime"`, `"fixed"`) with
  a validating `UnmarshalJSON`. The int form makes `Realtime` the zero value, so an absent or
  unparsed field silently *is* realtime — precisely defect 1. As a string, absence is distinguishable
  from any value. The three `switch cfg.Ticker.Type` sites keep their shape.

Rule for the ticker block: absent entirely → `Realtime`; present → `type` required; unrecognised
`type` → error. No path degrades silently.

## Phase 3 — `data_logger` pilot + `main.go`

**`internal/engine/data_logger.go`**

- Replace `DataLoggerConfig` with `DataLoggerOptions` decoded from the `options` submap:
  ```go
  type DataLoggerOptions struct {
      Ticker TickerConfig `json:"ticker"`
      Limit  int          `json:"limit"`
  }
  func (o *DataLoggerOptions) Validate() error // fixed ticker requires interval > 0
  ```
- Delete `parseDataLoggerConfig` and the `bufferSize` field.
- Subscription via `config.Dep[proto.Subscription](opts, "_subscription")`; provider, features and
  exporter likewise through `Dep`.
- Drop the `default:` arm that silently selected `Realtime`.

**`cmd/main.go`**

- Add flags: `--config <path>` (currently hardcoded to `config.json` — needed so generated sweep
  configs can be validated) and `--validate-only`.
- Decode top-level `subscription`, inject as `_subscription`.
- Pass the engine block through unchanged; the factory reaches `options` itself.
- **Accumulate errors.** Today each failure calls `os.Exit(1)` immediately. Restructure construction
  to collect into `[]error` and continue, so one run reports every problem in the file. Steps whose
  prerequisite failed (engine when provider resolution failed) are *skipped, not attempted*, to avoid
  cascading "missing `_provider`" noise on top of the real error. At the end: if any errors, log all
  and exit 1; else if `--validate-only`, log OK and exit 0; else `Run`.

**`Makefile`** — add a `validate` target.

## Phase 4 — Rollout

Same pattern per component: declare a config struct with json tags beside the factory, seed it with
a defaults literal, `config.Decode` into it, add `Validate()` where a constraint exists.

| Component | Notes |
|---|---|
| `features/{ema,rsi,sma,vwap,swing_hl}.go` | **No config structs exist today** — every feature asserts inline, so these are created, not adapted. `EMAConfig` gains `Smoothing` (default `2.0`). `swing_hl`'s even-period rule moves into `Validate()`. |
| `features/engulfing.go` | Takes no options — empty struct, so any key in its block is now an error. |
| `features/feature.go` | `iDWithPeriod` → an ID derived from all identity-bearing fields, so `ema` variants differing only in smoothing no longer collide. `removeDuplicates` returns an error on collision. |
| `provider/binance_historical.go` | `StartTime`/`EndTime` as `int64` json fields — fixes mode C outright. |
| `provider/binance_csv.go` | `file` required via `Validate()` (currently detected late, in `Init`). |
| `exporter/jsonl_exporter.go` | `output_dir` from config; `_symbol`/`_timeframe`/`_engine` stay injected and are stripped by `Decode`. |
| `engine/signal_logger.go` | `SignalLoggerOptions`; delete `parseSignalLoggerConfig`; drop `bufferSize`. |
| `engine/full.go` | `FullEngineOptions` keeping `bufferSize` and `errorThreshold`; delete `parseFullEngineConfig`. Also delete `NewFullEngine` (engine.md defect 5) — dead code that duplicates the parser being removed. |

`config.json` is migrated to the new shape: `subscription` to top level, engine keys under
`options`, `bufferSize` removed, `ema.smoothing` retained (now meaningful).

This resolves engine.md defects 1 (silent ticker fallback), 4 (triplicated parsers), 6 (untyped
config), 5 (dead code, incidental) and data-layer.md defect 1 (time window).

## Phase 5 — Documentation

**`docs/Commands.md`** — "The engine takes no CLI flags" is now false. Add `--config` and
`--validate-only` to Build & run, add the `make validate` target, and show validating a config
without running it.

**`docs/Architecture.md`** — short note that `internal/config` owns strict decoding and that
factories must stay I/O-free.

**`docs/discussions/config.md`** (new) — full topic doc in the standard format, with Solution
describing the module *as implemented*. Ports in, removing from origin:

- From `engine.md`: § Config parsing, defects 1, 4, 6, the config half of Decisions/Solution, and
  the `removeDuplicates` open question. Leave one-line stubs at defects 1/4/6 pointing to
  `[[config]]` — they remain real engine defects, just resolved elsewhere.
- From `data-layer.md`: defect 1 (`binance_historical` time window), replaced by a pointer.
- Cross-reference only (do **not** move): `strategy-layer.md` defect 4 (`buy_the_dip` ignores its
  config) — deferred by decision, so moving it would orphan it from its implementation context.

**`docs/discussions/engine.md`** — trim to what remains engine-specific. It is 309 lines against a
103–159 range for its siblings, roughly half of it config. What stays: fan-out determinism, the
backtesting engine, multi-provider topology, defects 2/3/7/8/9/10/11.

**`docs/discussions/README.md`** — add the `config.md` row; rewrite cross-cutting theme 1 ("Silent
configuration failure") as resolved, pointing at `[[config]]`.

**`docs/discussions/features.md`** — record the feature-ID change and `removeDuplicates` now
erroring.

## Verification

1. `go build ./...`, `go vet ./...`, `make test` — the config tests are the repo's first.
2. `make run` against the migrated config produces the same behaviour as before: `binance_csv` →
   feature chain → console log + JSONL export, paced at the configured 100ms (a timed run emits
   ~120 candles in 12s, the measurement used to confirm the ticker previously).
3. Confirm the export file is still written to `data/exports/` and `make quick-plot` renders it —
   the exporter's injected `_symbol`/`_timeframe` path is the one most likely to break.
4. **One deliberate break per failure mode**, each expected to fail at startup naming the path:
   mode A `ticker.type: "fixedinterval"`; mode B `subscription.timeframe: "1hr"`; mode C a
   `startTime` on the historical provider (enable it and confirm the window is applied);
   mode D `tickers` instead of `ticker`; mode E `smoothingg` in an ema variant.
5. `--validate-only` on the good config exits 0 without opening the CSV or writing an export;
   on a config with several distinct errors, confirm **all** are reported in one run, not just the
   first.
6. Two `ema` variants differing only in `smoothing` produce two distinct features in the chain;
   two identical variants produce a startup error.

## Out of scope

- `buy_the_dip` config wiring — deferred to [[strategy-layer]] by decision.
- Moving `exporter` to a top-level component block — defensible for consistency with
  providers/features/strategies, but a second format change and a rework of main.go's resolution
  path. Raise separately.
- Generating a config reference from component structs — would need the prototype/reflection
  machinery this plan deliberately drops.
- Multi-subscription support. Top-level singular `subscription` bakes in the one-provider-per-engine
  assumption (engine.md defect 9). It makes the limitation visible rather than burying it in the
  engine block, but multi-symbol work will revisit this key.

---

## Execution notes

Recorded after execution; the plan above is kept as approved.

Deviations from the plan as written:

- **`ComponentConfig` does not reject unknown keys**, contrary to Phase 1. Its non-reserved keys
  *are* the component's options, so rejecting them there would be wrong — they are checked when the
  factory decodes them. `FeatureConfig` and `EngineConfig` went the other way: their custom
  `UnmarshalJSON` methods were deleted entirely, and `Load` now uses one strict decoder for the
  whole file, which reaches them because they are plain structs.
- **Feature IDs omit defaulted parameters.** The plan said an ID derives from all identity-bearing
  fields. Doing that unconditionally would have renamed `ema_20`, which appears in exports and in
  `make plot INDICATORS=`. Only non-default values extend the ID (`ema_20_s3`); uniqueness still
  holds because identical variants are now an error.
- **`removeDuplicates` became `checkDuplicates`** — the function no longer removes anything, so the
  name changed with the behaviour.
- **API grew two helpers** not in the Phase 1 sketch: `OptDep` (absence distinguished from failure,
  for the optional exporter) and `DecodeOptions` (decodes the nested engine `options` block).
- **Error wrapping is caller-side.** Factories do not prefix their own errors; `main.go` names the
  component. Both wrapping produced doubled prefixes (`provider "binance_csv": provider
  "binance_csv": …`).
- **`JSONLExporterConfig` declares `type` and `disabled`**, which the composition root reads, so
  that strict decoding of the exporter block does not reject them.
- **`--config <path>` was added** alongside `--validate-only`, with a `CONFIG=` Makefile variable.
  Validating a generated config is pointless without being able to name it.
- **Tests** landed as `internal/config/decode_test.go`, plus `internal/engine/ticker_test.go` and
  `internal/domains/provider/binance_historical_test.go` (a regression test for the time-window bug).

Decided after approval, during review:

- **An engine with an unresolved dependency is not constructed, and the skip is not counted as a
  config error.** Building it would fail for a reason already reported, so the skip is logged as a
  consequence. Counting it overstated how many independent problems the file had.
