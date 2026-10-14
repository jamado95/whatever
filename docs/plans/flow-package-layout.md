# Plan — `internal/flow` package layout

## Context

`internal/pipeline` (generic channel combinators), `internal/stream` (Subscription-aware
synchronizers) and `internal/timing` (`Ticker[T]` gating) all govern how data moves and aligns.
The wall-clock synchronizer direction in `docs/discussions/multi-subscription.md` couples ticker and
synchronizer further. Group them under one parent, `internal/flow`, keeping generic and
domain-typed code in separate subpackages. `SortedWindow` stays in `protocol` — `protocol` must not
depend on internal packages. Pure move + import rewrite; no behaviour change.

## Layout

```
internal/flow/
  pipe/     ← internal/pipeline   (pipeline.go, fanout.go)          stdlib only
  timing/   ← internal/timing     (timing.go)                        stdlib only
  align/    ← internal/stream     (synchronizer*.go, *_test.go)      → flow/pipe, protocol
```

Dependency graph: `engine → flow/{pipe,align,timing}`, `flow/align → flow/pipe, protocol`.
`protocol` imports nothing new.

## Steps

1. **Move with history** (`git mv`):
   - `internal/pipeline/{pipeline,fanout}.go` → `internal/flow/pipe/`
   - `internal/timing/timing.go` → `internal/flow/timing/`
   - `internal/stream/*.go` (5 files incl. tests) → `internal/flow/align/`
2. **Package clauses:** `package pipeline` → `package pipe`; `timing` unchanged;
   `package stream` → `package align` in all 5 align files (incl. the 3 test files — the only edit
   to tests). Package doc comments: `// Package stream …` → `// Package align …` (both
   synchronizer files carry it).
3. **Imports + qualifiers:**

   | File | Change |
   |---|---|
   | `internal/flow/align/synchronizer.go` | `"…/internal/pipeline"` → `"…/internal/flow/pipe"`; `pipeline.FanIn` → `pipe.FanIn` |
   | `internal/flow/align/synchronizer_v2.go` | `pipe "…/internal/pipeline"` → `"…/internal/flow/pipe"` (alias drops; name already `pipe`) |
   | `internal/engine/data_logger.go` | `pipeline`→`flow/pipe`, `stream`→`flow/align`, `timing`→`flow/timing`; `pipeline.`→`pipe.`, `stream.`→`align.` |
   | `internal/engine/ticker.go` | `timing` → `flow/timing` |
   | `internal/engine/full.go` (`fullengine` tag) | `pipeline`→`flow/pipe`, `timing`→`flow/timing`; `pipeline.`→`pipe.` (incl. the `pipeline.DefaultFanOutBuffer` comment) |
   | `internal/engine/signal_logger.go` (`signalengine` tag) | same as `full.go` |

   Then `goimports -w` on the touched files only (not `make fmt` — avoids reformatting unrelated
   uncommitted files). Confirm no stale refs: `grep -rn 'internal/pipeline\|internal/stream\|internal/timing\|pipeline\.\|stream\.' --include=*.go internal cmd`.
4. **Remove** the now-empty `internal/pipeline`, `internal/stream`, `internal/timing` dirs.

## Docs (notify user on completion)

- `docs/Architecture.md` — Core Architecture: replace `internal/pipeline` line with
  `internal/flow: data-flow primitives — pipe (generic combinators), align (cross-stream
  synchronizers), timing (tickers)`; Design Rules "Pipeline is the primary…" → "`flow/pipe`…";
  Constraint "Do not modify pipeline or protocol unless explicitly planned and asked for" →
  "Do not modify protocol unless explicitly planned and asked for" (flow is not named).
- `docs/Commands.md` — `make test` coverage list: `internal/stream` → `internal/flow/align`.
- `docs/discussions/multi-subscription.md` — path refs `internal/stream`→`internal/flow/align`,
  `internal/timing/timing.go`→`internal/flow/timing/timing.go`, `internal/pipeline/fanout.go`→`internal/flow/pipe/fanout.go`.
- `docs/discussions/execution-layer.md` — `internal/pipeline` ref.
- `docs/discussions/engine.md` — path refs (`internal/timing/timing.go`, `pipeline/fanout.go:41`,
  Resources list).
- Executed plans in `docs/plans/*` are left as-is (historical records).
- Store this plan as `docs/plans/flow-package-layout.md`.

## Verification

- `go build ./...` — must pass.
- `make test` — must pass; test files differ only in package clause.
- `make lint` (staticcheck) — must be clean.
- `go vet ./...`.
- Tagged engines: `go vet -tags fullengine ./internal/engine/` and `-tags signalengine` are
  **already broken on main** (`full.go:168` passes `<-chan MarketData` to `strat.Init(<-chan SyncEvent)`,
  pending multi-subscription stage 4). Acceptance there: same pre-existing type errors only, no
  import/"undefined: pipeline" errors.
- `make validate` — engine still constructs from `config.json`.
