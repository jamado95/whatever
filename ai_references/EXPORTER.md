# Go JSONL Exporter for Market Data Visualization

## Context
I have a Go trading system with a feature chain architecture that processes market data and calculates technical indicators. I've already built a Python visualization script (`scripts/plot_market_data.py`) that reads JSONL files and creates interactive charts. Now I need to implement a separate exporter layer that can be composed within my engines to produce the JSONL files the Python script expects.

**Important:** My `Snapshot` has a `Data() map[string]any` method that returns all stored values, making export straightforward.

## Architecture Decision

The exporter should be a **separate layer** (like providers, strategies, risk management, execution), not a feature in the feature chain. This allows:
- Engines to optionally compose it
- Reuse across different engines
- Clean separation: features calculate, exporters persist
- Independent lifecycle management

## Task: Implement Exporter Layer

### 1. Create Exporter Interface & Implementation

**File:** `internal/exporter/types.go`
```go
package exporter

import (
    "whatever/internal/protocol"
)

type Exporter interface {
    ID() string
    Init(outputPath string) error
    Export(md protocol.MarketData, snap *protocol.Snapshot) error
    Close() error
}
```

**File:** `internal/exporter/jsonl_exporter.go`
```go
package exporter

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    
    "whatever/internal/protocol"
)

type JSONLExporter struct {
    id         string
    file       *os.File
    encoder    *json.Encoder
    mu         sync.Mutex
    outputPath string
}

func NewJSONLExporter(id string) *JSONLExporter {
    return &JSONLExporter{
        id: id,
    }
}

func (e *JSONLExporter) ID() string {
    return e.id
}

func (e *JSONLExporter) Init(outputPath string) error {
    // Create directory if needed
    dir := filepath.Dir(outputPath)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return fmt.Errorf("failed to create output directory: %w", err)
    }
    
    // Create/truncate file
    file, err := os.Create(outputPath)
    if err != nil {
        return fmt.Errorf("failed to create output file: %w", err)
    }
    
    e.file = file
    e.encoder = json.NewEncoder(file)
    e.outputPath = outputPath
    
    return nil
}

func (e *JSONLExporter) Export(md protocol.MarketData, snap *protocol.Snapshot) error {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    // Build record with OHLCV data
    record := map[string]any{
        "timestamp": md.Candle.CloseTs,
        "symbol":    md.Symbol,
        "open":      md.Candle.Open,
        "high":      md.Candle.High,
        "low":       md.Candle.Low,
        "close":     md.Candle.Close,
        "volume":    md.Candle.Volume,
    }
    
    // Add all indicators from snapshot
    for key, value := range snap.Data() {
        record[key] = value
    }
    
    // Write as single JSON line
    if err := e.encoder.Encode(record); err != nil {
        return fmt.Errorf("failed to encode JSONL record: %w", err)
    }
    
    return nil
}

func (e *JSONLExporter) Close() error {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    if e.file != nil {
        return e.file.Close()
    }
    return nil
}
```

### 2. Exporter Registry (Optional but Recommended)
Create a new Exporters registry entry following current patterns at internal/registry
Use the init pattern for registering exporter instances, like seen in instances of internal/domains/...

Parse the exporter in config.json through internal/config/config.go.
Initialise exporter instances and inject dependencies in engine in cmd/main.go. Follow existing patterns for providers and features


### 3. Configuration Support

**Add to config.json:**
```json
{
  "providers": [
    {
      "type": "csv",
      "enabled": true,
      "files": {
        "BTCUSDT:1h": "data/BTCUSDT-1h.csv"
      }
    }
  ],
  "features": [
    {
      "type": "sma",
      "enabled": true,
      "period": 20
    },
    {
      "type": "rsi",
      "enabled": true,
      "period": 14
    }
  ],
  "engine": {
    "type": "data_logger",
    "provider": "csv",
    "features": ["sma", "rsi"],
    "exporter": {
      "type": "jsonl",
      "output_path": "outputs/market_data.jsonl"
    }
  }
}
```

**Update main.go to resolve exporter:**
```go
// After resolving providers, strategies, features...

// Resolve exporter (optional)
if exporterConfig, ok := engineOpts["exporter"].(map[string]any); ok {
    exporterType := exporterConfig["type"].(string)
    exporterOpts := exporterConfig
    exporterOpts["id"] = exporterType
    
    engineOpts["_exporter"] = exporterOpts
}

// Create engine (engine will create the exporter internally)
eng, err := engine.NewDataLogger(engineOpts)
```

### 5. JSON Output Format

Each line is a complete JSON object:
```json
{"timestamp":1609459200000,"symbol":"BTCUSDT","open":29000.5,"high":29100.2,"low":28950.0,"close":29050.3,"volume":123.45,"sma_20":28900.5,"rsi_14":65.2}
{"timestamp":1609459260000,"symbol":"BTCUSDT","open":29050.3,"high":29200.0,"low":29040.0,"close":29180.5,"volume":234.56,"sma_20":28920.1,"rsi_14":68.5}
```

**Field requirements:**
- `timestamp`: Unix milliseconds from `md.Candle.CloseTs`
- `symbol`: From `md.Symbol`
- `open`, `high`, `low`, `close`, `volume`: From `md.Candle`
- Dynamic indicator fields: All keys/values from `snap.Data()`

### 6. Error Handling
```go
func (e *JSONLExporter) Export(md protocol.MarketData, snap *protocol.Snapshot) error {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    record := map[string]any{
        "timestamp": md.Candle.CloseTs,
        "symbol":    md.Symbol,
        "open":      md.Candle.Open,
        "high":      md.Candle.High,
        "low":       md.Candle.Low,
        "close":     md.Candle.Close,
        "volume":    md.Candle.Volume,
    }
    
    // Safe merge - snap.Data() returns map[string]any
    for key, value := range snap.Data() {
        record[key] = value
    }
    
    // Write and return error if any
    if err := e.encoder.Encode(record); err != nil {
        return fmt.Errorf("failed to write JSONL: %w", err)
    }
    
    return nil
}
```

In the engine, handle export errors gracefully:
```go
if e.exporter != nil {
    if err := e.exporter.Export(emd.MarketData, emd.Indicators); err != nil {
        // Log but don't stop processing
        log.Printf("export error: %v", err)
    }
}
```

### 7. Directory Structure
```
domains/
├── exporter/
│   ├── types.go           # Exporter interface
│   ├── jsonl_exporter.go  # JSONL implementation
│   └── registry.go        # Factory registry
├── provider/
│   └── ...
├── strategy/
│   └── ...
└── execution/
    └── ...

core/
└── engine/
    └── data_logger.go     # Composes exporter
```

### 8. Testing

**File:** `internal/exporter/jsonl_exporter_test.go`
```go
func TestJSONLExporter(t *testing.T) {
    tmpFile := filepath.Join(t.TempDir(), "test.jsonl")
    
    exporter := NewJSONLExporter("test")
    if err := exporter.Init(tmpFile); err != nil {
        t.Fatalf("failed to init: %v", err)
    }
    defer exporter.Close()
    
    md := protocol.MarketData{
        Symbol: "BTCUSDT",
        Candle: market.Candle{
            CloseTs: 1609459200000,
            Open:    29000.5,
            High:    29100.2,
            Low:     28950.0,
            Close:   29050.3,
            Volume:  123.45,
        },
    }
    
    snap := &protocol.Snapshot{}
    snap.Set(protocol.Key[float64]{Name: "sma_20"}, 28900.5)
    snap.Set(protocol.Key[float64]{Name: "rsi_14"}, 65.2)
    
    if err := exporter.Export(md, snap); err != nil {
        t.Fatalf("failed to export: %v", err)
    }
    
    exporter.Close()
    
    // Verify output
    data, _ := os.ReadFile(tmpFile)
    var record map[string]any
    json.Unmarshal(data, &record)
    
    assert.Equal(t, float64(1609459200000), record["timestamp"])
    assert.Equal(t, "BTCUSDT", record["symbol"])
    assert.Equal(t, 29050.3, record["close"])
    assert.Equal(t, 28900.5, record["sma_20"])
}
```

## Success Criteria

1. ✅ **Separate layer:** Exporter is in `internal/exporter/`, not mixed with features
2. ✅ **Composable:** Engines can optionally include an exporter
3. ✅ **Registry pattern:** Follows same pattern as providers/strategies
4. ✅ **Valid JSONL:** Each line is valid JSON (one object per line)
5. ✅ **Includes OHLCV + indicators:** All data from MarketData and Snapshot
6. ✅ **Thread-safe:** Mutex protects concurrent exports
7. ✅ **Clean lifecycle:** Init/Export/Close pattern
8. ✅ **Configurable:** Can be enabled/disabled via config.json

## Usage Flow

After implementation:
```bash
# 1. Configure exporter in config.json
# 2. Run Go app
./whtv --config config.json
# Produces: outputs/market_data.jsonl

# 3. Visualize with Python
make plot FILE=outputs/market_data.jsonl
# Produces: outputs/market_data_chart.html
```

## Design Benefits

- **Separation of concerns:** Features calculate, exporters persist
- **Optional composition:** Engines choose whether to export
- **Reusable:** Same exporter works with different engines
- **Extensible:** Easy to add CSV exporter, database exporter, etc.
- **Testable:** Exporter can be tested independently

## Implementation Notes

- Use `json.NewEncoder(file)` for efficient streaming writes
- The encoder automatically adds newlines (proper JSONL format)
- `snap.Data()` returns `map[string]any`, making merge trivial
- Export all indicators - let Python script choose what to plot
- Consider buffering: `bufio.NewWriter(file)` for high-throughput scenarios

This should be around 150-200 lines total across all files.