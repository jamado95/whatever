# Instrumentation

## Topic

Metrics for benchmarking and for monitoring: one framework for replay and live, in its own package,
not specific to any layer. The data layer ([[data-layer-v2]]) is instrumented first; the rest of
the codebase follows later. The first constraint is overhead: instrumentation must add as little
processing time as possible to the components it measures.

Takes up the open question in [[engine]] — where metrics and health reporting live. Related:
[[pipeline-runtime]] § Live specifics (the frame-event queue's depth measures overrun) and the
provider drop contract in [[data-layer-v2]] (an overload signal for risk).

Opened 2026-10-25.

## Current Understanding

### Today

- Nothing measures throughput, waits or occupancy. The only counters are the normaliser's drop
  counts, summarised once its lane ends (`normaliser.summary`).
- Benchmarks rely on external timing and pprof (`docs/benchmarks/data-logger-engines.md`).

### Measurement points in the data layer

```
provider ─▶ normalise ─[100]─▶ align ─evaluate─▶ FanIn ─▶ readiness ─[views 128]─▶ Next ─▶ engine step
                              (1)(2)                      (3)(5)                   (4)(6)(7)
                               ▲                                                      │
                               └───────────────── gate.consume(col) ◀─────────────────┘
```

| # | Metric | Site | Shows | Cost |
|---|---|---|---|---|
| 1 | Gate waits per lane: count, time blocked | `aligner.write` → `gate.wait` | A stream ahead of the engine, waiting for ring capacity | Clock read only when it blocks |
| 2 | Ring lead per lane: columns written beyond the consumed column, in slots, against the ring's `spare` | align, read inside `gate.wait` (already locked) | How full each ring gets | Arithmetic |
| 3 | Head-of-line stream: which stream held `head+1`, how often | `readiness.release` | The lane holding up release | Counter |
| 4 | `Next` wait on the event queue; queue depth at dequeue | `DataPipeline.Next` | Data-layer-bound when `Next` waits | Clock read only when the queue is empty; `len` free |
| 5 | Readiness send blocked: count, time | `readiness.run` | Engine-bound: the mirror of 4 | Non-blocking send first, clock read only on block |
| 6 | Engine step (`Next` return → next call); materialise (`agg.step`) | `DataPipeline.Next` | Cost of each cycle, without engine changes | 2–3 clock reads per frame |
| 7 | Queue time: dequeue − release | Release time stamped on the `View` by readiness | How long released frames wait | 1 clock read per frame |

Buffer fill is both 2 (each ring) and 4 (the event queue).

### What a run measures

- **Production is throttled by backpressure.** When the engine is slower, lanes wait on the gate and
  readiness on a full queue, so the observed production rate equals the consumption rate. In-run
  metrics show which side bounds the run and where each cycle's time goes, not the data layer's
  capacity. Capacity needs a run with a no-op consumer.
- **`data_logger_v2` is likely engine-bound today** — the profile in
  `docs/benchmarks/data-logger-engines.md` puts ~0.2 s of CPU in the data layer against 1.73 s in the
  feature bridge. Expected: queue near full, lanes on the gate, `Next` rarely waiting. A hypothesis
  for the metrics to test.
- **Step time (6) includes pacing** in `data_logger_v2`: the pacer waits after `Next`, inside the
  engine step. Benchmarks run unpaced.

### Measured costs

Micro-benchmarks in a scratch module, 2026-10-25, Intel Core Ultra 9 275HX, WSL2 (`tsc`
clocksource), Go 1.26.5, `client_golang` v1.25.0. Indicative. Observed values spread over 1 µs – 1 ms.

| Operation | Cost |
|---|---|
| `time.Now` + `time.Since` | 47 ns |
| Atomic add, uncontended | 5.6 ns |
| Non-blocking send + receive | 26 ns |
| Own histogram, one writer, plain increments | **1.4 ns** |
| Own histogram, atomic increments (bucket, count, sum) | 12 ns |
| Prometheus counter `Inc` | 5.5 ns |
| Prometheus classic histogram `Observe`, 20 buckets | 21 ns |
| Prometheus native histogram `Observe` | 38 ns |
| Prometheus classic histogram shared by 8 goroutines | 85–120 ns per call across all 8; ~0.7–1 µs per goroutine |
| Own atomic histogram shared by 8 goroutines | 41 ns per call across all 8; ~330 ns per goroutine |
| Scrape, 1000 lanes × (20-bucket histogram + counter), custom `Collector` | 6.6 ms CPU, 8.9 MB, 92k allocs per scrape |

Against these: a `data_logger_v2` frame costs ~20 µs (2.13 s / 105,408 frames, logging off), and
readiness ~527 ns per evaluate at 1000 streams ([[data-layer-v2]] § Release).

- A few clock reads per frame are well under 1%; one per evaluate would be 5–9% of readiness.
- **Sharing an instrument across goroutines costs 30–40× per call.** Per-writer instruments matter
  more than the choice of library.
- The scrape runs on the HTTP server's goroutine: at a 15 s interval, under 0.05% of one core, plus
  its garbage. The figure includes the bench's own conversion code.

### Histograms

Durations recorded many times — a gate wait per block, a step time per frame (105k a year at 5m,
525k at 1m) — need their distribution (p50, p99, max), not a mean: a 20 µs mean step can hide 5 ms
GC spikes. Keeping every sample grows without bound live. A histogram is a fixed array of buckets,
each counting the samples in a value range: recording is an index computation and an increment, no
allocation, fixed memory, and per-lane histograms sum into one.

| Layout, 1 ns – ~69 s | Buckets | Memory | Bucket width |
|---|---|---|---|
| One per power of two | 36 | ~300 B | 2× |
| 16 linear sub-buckets per power of two (HdrHistogram style) | ~530 | ~4.2 KB | ≤ 6.25% |

The index of the second is `bits.Len64` and a shift.

### Prometheus

- **The endpoint is off the hot path;** recording through `client_golang` types is not (table
  above).
- **Cardinality.** Exported as a classic histogram, ~530 buckets is ~530 time series per histogram
  per lane. Either the adapter downsamples to bucket bounds taken from the instrument's own edges
  (exact counts), or it exports native histograms — exponential buckets, so conversion from
  log-linear ones is approximate.
- **Native histograms** are stable in the Prometheus server since v3.8 per secondary sources
  (Resources); one says the Go client still marks them experimental — unverified.

## Decisions

- **Own package, not layer-specific.** The data layer is its first user; the rest of the codebase
  follows. It gets an `Architecture.md` entry when planned.
- **One framework for replay and live.** Per-frame records are the same in both; each output —
  end-of-run summary, per-frame records, live endpoint — is opted in or out in the run's config.
- **Opt-in through an engine-level `metrics` block**, decoded strictly with `config.Decode` like any
  component config ([[config]]).
- **Scope: the data layer only.** The engine is a black box, measured from `Next` (6).
- **Buffer fill is both** the event-queue depth and each lane's ring lead.
- **Recording is separate from exposure.** Instrumented code records into the package's own
  instruments; it never imports Prometheus. Adapters read the instruments off the hot path: the
  summary at the end of the run, per-frame records, and a Prometheus `Collector` that builds metrics
  only on scrape. Adding OpenTelemetry or `expvar` later is another adapter; instrumented code does
  not change.
- **Overhead rules:**
  - Instruments are created and allocated at construction; the hot path holds direct handles — no
    lookups by name, maps, locks or allocations.
  - One writer per instrument: never shared across goroutines.
  - Disabled: a nil handle and one branch per site, no clock reads.
  - Clock reads only on blocking paths — the non-blocking operation is tried first — and once or
    twice per frame. Readiness only counts.
  - Per-frame records go into a preallocated ring, written out by their own goroutine; no I/O on
    the engine goroutine.
- **Histograms are log-linear**, 16 sub-buckets per power of two (≤ 6.25% bucket width).

## Solution

No plan yet. Shape so far:

```
instrumented code (hot path)                      adapters (off the hot path)
  lane i:    gate waits, ring lead  ─┐
  readiness: head-of-line, send     ─┼─ instruments ─▶ summary          end of run, logged
  Next:      wait, step, queue      ─┤                ─▶ per-frame records ring → writer goroutine → file
  per frame: one record             ─┘                ─▶ Prometheus     Collector, on scrape
```

## Open Questions

- **Live read path.** Atomic increments (~12 ns per histogram observation; ~8 without the separate
  count) are always readable. Plain increments (1.4 ns) cannot be read concurrently: the owning
  goroutine would copy them out periodically — stale, and a lane blocked on the gate cannot publish,
  which is when it matters.
- **Package name and placement** — e.g. `internal/metrics`.
- **Prometheus histogram export** — downsampled classic or native; check the Go client's native
  histogram status.
- **`metrics` block shape** — outputs, record file path, endpoint address.
- **Per-frame record format** (CSV / JSONL), its fields beyond those of points 4–7, and its consumer
  in `scripts/`.
- **Capacity baseline** — a no-op-consumer run over the same CSVs: a pipeline benchmark in the
  `data` package, or an engine mode.
- **Health signals** — whether the overload signal of the provider drop contract ([[data-layer-v2]])
  reads these instruments.

## WIP

- Discussion only: no plan, nothing implemented.
- Costs above come from benchmarks in a scratch module outside the repository; none is checked in.

## Resources

- `internal/engine/data/align.go` (`gate`, `aligner.write`), `internal/engine/data/readiness.go`,
  `internal/engine/data/data.go` (`Next`), `internal/engine/data/normalise.go` (`summary`),
  `internal/engine/data_logger_v2.go`, `internal/flow/timing/pacer.go`
- `docs/benchmarks/data-logger-engines.md`
- [client_golang `prometheus` package](https://pkg.go.dev/github.com/prometheus/client_golang/prometheus)
  (`Collector`, `NewConstHistogram`)
- Native histogram status:
  [Grafana](https://grafana.com/blog/2025/10/30/get-more-precision-from-your-grafana-visualizations-with-prometheus-native-histograms-in-grafana-cloud),
  [youngju.dev](https://www.youngju.dev/blog/2026-07-16-prometheus-native-histograms-stable-lts.en),
  [Logz.io](https://logz.io/blog/prometheus-metrics-native-histograms)
- Related: [[data-layer-v2]], [[pipeline-runtime]], [[engine]], [[config]]
