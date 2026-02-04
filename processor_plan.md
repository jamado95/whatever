# Processor Layer Implementation Plan

## Motivation

Strategies currently compute their own technical indicators internally. This creates:

1. **Duplicate computation** — Multiple strategies needing the same indicator compute it independently
2. **Inconsistent state** — No guarantee strategies see identical indicator values for the same candle
3. **Mixed concerns** — Strategies handle both computation and trading decisions

The **Processor Layer** solves this by:
- Computing indicators once per candle, before fan-out to strategies
- Guaranteeing all strategies see identical indicator state for each candle
- Transforming strategies into pure "confluence engines" that evaluate pre-computed indicators

---

## Architecture Overview

```
Provider → Ticker.Gate → ProcessorChain.Process → FanOut → Strategies
                                │
                                ▼
                    MarketData becomes EnrichedMarketData
                    (candle + processor snapshot)
```

Processors form a DAG based on dependencies. The chain resolves this via topological sort.

---

## Implementation Steps

### Step 1: Add Snapshot and Key Types

Create `processor/snapshot.go`:

```go
package processor

type Key[T any] struct {
    name string
}

func NewKey[T any](name string) Key[T] {
    return Key[T]{name: name}
}

func (k Key[T]) Name() string { return k.name }

type KeyRef struct {
    Name string
}

func (k Key[T]) Ref() KeyRef {
    return KeyRef{Name: k.name}
}

type Snapshot struct {
    data map[string]any
}

func NewSnapshot() *Snapshot {
    return &Snapshot{data: make(map[string]any)}
}

func Get[T any](s *Snapshot, key Key[T]) (T, bool) {
    val, ok := s.data[key.name]
    if !ok {
        var zero T
        return zero, false
    }
    return val.(T), true
}

func Set[T any](s *Snapshot, key Key[T], val T) {
    s.data[key.name] = val
}
```

---

### Step 2: Define Processor Interface and Chain

Create `processor/processor.go`:

```go
package processor

import (
    "fmt"
    "whatever/types"
)

type Processor interface {
    ID() string
    Dependencies() []KeyRef
    Update(candle types.MarketData, snap *Snapshot)
}

type Chain struct {
    processors []Processor
}

func NewChain(processors []Processor) (*Chain, error) {
    sorted, err := topologicalSort(processors)
    if err != nil {
        return nil, err
    }
    return &Chain{processors: sorted}, nil
}

func (c *Chain) Process(in <-chan types.MarketData) <-chan types.EnrichedMarketData {
    out := make(chan types.EnrichedMarketData)

    go func() {
        defer close(out)

        for candle := range in {
            snap := NewSnapshot()

            for _, proc := range c.processors {
                proc.Update(candle, snap)
            }

            out <- types.EnrichedMarketData{
                MarketData: candle,
                Indicators: snap,
            }
        }
    }()

    return out
}

func (c *Chain) AvailableKeys() []KeyRef {
    keys := make([]KeyRef, len(c.processors))
    for i, proc := range c.processors {
        keys[i] = KeyRef{Name: proc.ID()}
    }
    return keys
}

func topologicalSort(processors []Processor) ([]Processor, error) {
    idToProcessor := make(map[string]Processor)
    for _, p := range processors {
        idToProcessor[p.ID()] = p
    }

    inDegree := make(map[string]int)
    dependents := make(map[string][]string)

    for _, p := range processors {
        inDegree[p.ID()] = len(p.Dependencies())
        for _, dep := range p.Dependencies() {
            dependents[dep.Name] = append(dependents[dep.Name], p.ID())
        }
    }

    var queue []Processor
    for _, p := range processors {
        if inDegree[p.ID()] == 0 {
            queue = append(queue, p)
        }
    }

    var sorted []Processor
    for len(queue) > 0 {
        proc := queue[0]
        queue = queue[1:]
        sorted = append(sorted, proc)

        for _, depID := range dependents[proc.ID()] {
            inDegree[depID]--
            if inDegree[depID] == 0 {
                queue = append(queue, idToProcessor[depID])
            }
        }
    }

    if len(sorted) != len(processors) {
        return nil, fmt.Errorf("circular dependency detected in processors")
    }

    return sorted, nil
}
```

---

### Step 3: Add EnrichedMarketData Type

Update `types/market.go`:

```go
import "whatever/processor"

type EnrichedMarketData struct {
    MarketData
    Indicators *processor.Snapshot
}
```

---

### Step 4: Update Strategy Interface

Update `strategy/strategy.go`:

```go
import "whatever/processor"

type Strategy interface {
    ID() string
    RequiredIndicators() []processor.KeyRef
    Init(data <-chan types.EnrichedMarketData) error
    Streams() (<-chan types.Signal, <-chan error)
    Close()
}
```

---

### Step 5: Processor Registry (following your existing pattern)

Create `processor/registry.go`:

```go
package processor

import (
    "encoding/json"
    "fmt"
    "sync"
)

type Factory func(params json.RawMessage) (Processor, error)

var (
    registry = make(map[string]Factory)
    mu       sync.RWMutex
)

func Register(name string, factory Factory) {
    mu.Lock()
    defer mu.Unlock()
    registry[name] = factory
}

func Build(name string, params json.RawMessage) (Processor, error) {
    mu.RLock()
    factory, ok := registry[name]
    mu.RUnlock()

    if !ok {
        return nil, fmt.Errorf("unknown processor type: %s", name)
    }

    return factory(params)
}

func BuildChain(configs []Config) (*Chain, error) {
    processors := make([]Processor, 0, len(configs))

    for _, cfg := range configs {
        proc, err := Build(cfg.Type, cfg.Params)
        if err != nil {
            return nil, fmt.Errorf("failed to build processor %s: %w", cfg.Type, err)
        }
        processors = append(processors, proc)
    }

    return NewChain(processors)
}

type Config struct {
    Type   string          `json:"type" yaml:"type"`
    Params json.RawMessage `json:"params" yaml:"params"`
}
```

---

### Step 6: Example Processor (Trend) — Placeholder

Create `processor/trend/trend.go`:

```go
package trend

import (
    "encoding/json"

    "whatever/processor"
    "whatever/types"
)

var Key = processor.NewKey[State]("trend")

type State struct {
    Direction string
    SwingLow  float64
    SwingHigh float64
    Maturity  int
}

type Config struct {
    MaturityThreshold int `json:"maturity_threshold"`
    BufferSize        int `json:"buffer_size"`
}

type Processor struct {
    config Config
    state  State
    buffer []types.Candle
}

func init() {
    processor.Register("trend", func(params json.RawMessage) (processor.Processor, error) {
        var cfg Config
        if err := json.Unmarshal(params, &cfg); err != nil {
            return nil, err
        }
        return New(cfg), nil
    })
}

func New(cfg Config) *Processor {
    return &Processor{
        config: cfg,
        buffer: make([]types.Candle, 0, cfg.BufferSize),
    }
}

func (p *Processor) ID() string {
    return Key.Name()
}

func (p *Processor) Dependencies() []processor.KeyRef {
    return nil
}

func (p *Processor) Update(candle types.MarketData, snap *processor.Snapshot) {
    // TODO: implement trend detection logic
    processor.Set(snap, Key, p.state)
}
```

---

### Step 7: Example Processor with Dependency (Fibonacci) — Placeholder

Create `processor/fibonacci/fibonacci.go`:

```go
package fibonacci

import (
    "encoding/json"

    "whatever/processor"
    "whatever/processor/trend"
    "whatever/types"
)

var Key = processor.NewKey[State]("fibonacci")

type State struct {
    Levels      map[float64]float64
    Retracement float64
}

type Config struct {
    Levels []float64 `json:"levels"`
}

type Processor struct {
    config Config
    state  State
}

func init() {
    processor.Register("fibonacci", func(params json.RawMessage) (processor.Processor, error) {
        var cfg Config
        if err := json.Unmarshal(params, &cfg); err != nil {
            return nil, err
        }
        return New(cfg), nil
    })
}

func New(cfg Config) *Processor {
    return &Processor{config: cfg}
}

func (p *Processor) ID() string {
    return Key.Name()
}

func (p *Processor) Dependencies() []processor.KeyRef {
    return []processor.KeyRef{trend.Key.Ref()}
}

func (p *Processor) Update(candle types.MarketData, snap *processor.Snapshot) {
    t, ok := processor.Get(snap, trend.Key)
    if !ok {
        return
    }

    // TODO: implement fibonacci calculation using t (trend.State)
    _ = t
    processor.Set(snap, Key, p.state)
}
```

---

### Step 8: Import Processors for Registration

Create `processor/all.go`:

```go
package processor

import (
    // Import for side-effect registration
    _ "whatever/processor/trend"
    _ "whatever/processor/fibonacci"
)
```

Then import `processor` package in `main.go` or factory to trigger `init()` calls.

---

### Step 9: Update Engine

Update `core/engine.go`:

```go
type Engine struct {
    provider   provider.DataProvider
    processors *processor.Chain  // NEW
    strategies []strategy.Strategy
    risk       risk.RiskManager
    executors  []execution.Executor
    ticker     timing.Ticker[types.MarketData]
    cfg        Config
}

func (e *Engine) Run(ctx context.Context) error {
    if err := e.validateIndicatorRequirements(); err != nil {
        return err
    }

    // ... init provider ...

    raw, _ := e.provider.Streams()
    gated := e.ticker.Gate(raw)
    enriched := e.processors.Process(gated)  // NEW
    dataOuts := pipeline.FanOut(enriched, len(e.strategies)+1, e.cfg.BufferSize)

    // ... rest unchanged
}

func (e *Engine) validateIndicatorRequirements() error {
    available := e.processors.AvailableKeys()

    for _, strat := range e.strategies {
        for _, required := range strat.RequiredIndicators() {
            found := false
            for _, a := range available {
                if a.Name == required.Name {
                    found = true
                    break
                }
            }
            if !found {
                return fmt.Errorf("strategy %s requires processor %s which is not configured",
                    strat.ID(), required.Name)
            }
        }
    }
    return nil
}
```

---

### Step 10: Factory Builds Chain from Config

Update `factory/factory.go`:

```go
func BuildEngine(cfg EngineConfig) (*core.Engine, error) {
    prov, err := buildProvider(cfg.Provider)
    if err != nil {
        return nil, err
    }

    // Build processor chain using registry
    processors, err := processor.BuildChain(cfg.Processors)
    if err != nil {
        return nil, err
    }

    // ... rest unchanged, pass processors to engine
}
```

---

## Strategy Usage Example

```go
package buythedip

import (
    "whatever/processor"
    "whatever/processor/trend"
    "whatever/processor/fibonacci"
    "whatever/types"
)

func (s *Strategy) RequiredIndicators() []processor.KeyRef {
    return []processor.KeyRef{
        trend.Key.Ref(),
        fibonacci.Key.Ref(),
    }
}

func (s *Strategy) process(data types.EnrichedMarketData) *types.Signal {
    t, ok := processor.Get(data.Indicators, trend.Key)
    if !ok || t.Direction != "up" {
        return nil
    }

    f, ok := processor.Get(data.Indicators, fibonacci.Key)
    if !ok {
        return nil
    }

    if f.Retracement >= 0.618 {
        return &types.Signal{Side: "BUY", Source: s.ID()}
    }

    return nil
}
```

---

## Config Example

```yaml
processors:
  - type: trend
    params:
      maturity_threshold: 3
      buffer_size: 50
  - type: fibonacci
    params:
      levels: [0.382, 0.5, 0.618]

strategies:
  - type: buy_the_dip
    params:
      fib_level: 0.618
      min_trend_maturity: 5
```

---

## Directory Structure

```
trading/
├── processor/
│   ├── processor.go      # Processor interface, Chain, topologicalSort
│   ├── snapshot.go       # Snapshot, Key, KeyRef, Get, Set
│   ├── registry.go       # Register, Build, BuildChain
│   ├── all.go            # Blank imports for registration
│   ├── trend/
│   │   └── trend.go      # Placeholder with init() registration
│   └── fibonacci/
│       └── fibonacci.go  # Placeholder with init() registration
├── strategy/
│   └── strategy.go       # Updated interface
├── types/
│   └── market.go         # Add EnrichedMarketData
└── core/
    └── engine.go         # Add processor chain to pipeline
```

---

## Summary

| File | Purpose |
|------|---------|
| `processor/snapshot.go` | Type-safe indicator storage |
| `processor/processor.go` | Interface, Chain, DAG sort |
| `processor/registry.go` | Self-registration pattern matching your existing approach |
| `processor/trend/trend.go` | Placeholder, registers via `init()` |
| `processor/fibonacci/fibonacci.go` | Placeholder with dependency, registers via `init()` |
| `strategy/strategy.go` | Updated interface with `RequiredIndicators()` |
| `types/types.go` | `EnrichedMarketData` |
| `core/engine.go` | Processor chain in pipeline, validation |