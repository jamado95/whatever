package exporter

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"

	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const JSONLExporterID = "jsonl"

func init() {
	reg.Exporters.Register(JSONLExporterID, func(opts map[string]any) (proto.Exporter, error) {
		outputDir, _ := opts["output_dir"].(string)
		symbol, _ := opts["_symbol"].(string)
		timeframe, _ := opts["_timeframe"].(string)
		engine, _ := opts["_engine"].(string)
		return &JSONLExporter{
			outputDir: outputDir,
			symbol:    symbol,
			timeframe: timeframe,
			engine:    engine,
		}, nil
	})
}

type JSONLExporter struct {
	outputDir string
	symbol    string
	timeframe string
	engine    string

	file     *os.File
	tempPath string
	encoder  *json.Encoder
	mu       sync.Mutex

	startTs int64
	endTs   int64
}

func (e *JSONLExporter) ID() string {
	return JSONLExporterID
}

func (e *JSONLExporter) Init() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.outputDir == "" {
		return fmt.Errorf("output_dir is required")
	}

	// ensure directory exists
	if err := os.MkdirAll(e.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// create temp file for writing
	tempFile, err := os.CreateTemp(e.outputDir, "export_*.jsonl.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	e.file = tempFile
	e.tempPath = tempFile.Name()
	e.encoder = json.NewEncoder(tempFile)
	return nil
}

func (e *JSONLExporter) Export(md proto.MarketData, snap *proto.Snapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.encoder == nil {
		return fmt.Errorf("exporter not initialized")
	}

	ts := md.Candle.CloseTs
	if e.startTs == 0 || ts < e.startTs {
		e.startTs = ts
	}
	if ts > e.endTs {
		e.endTs = ts
	}

	record := map[string]any{
		"timestamp": ts,
		"symbol":    md.Symbol,
		"timeframe": md.Timeframe,
		"open":      md.Candle.Open,
		"high":      md.Candle.High,
		"low":       md.Candle.Low,
		"close":     md.Candle.Close,
		"volume":    md.Candle.Volume,
	}

	if snap != nil {
		maps.Copy(record, snap.Data())
	}

	return e.encoder.Encode(record)
}

func (e *JSONLExporter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.file == nil {
		return nil
	}

	if err := e.file.Close(); err != nil {
		return err
	}

	// generate final filename: {symbol}_{timeframe}_{engine}_{start}_{end}.jsonl
	filename := fmt.Sprintf("%s_%s_%s_%d_%d.jsonl",
		e.symbol, e.timeframe, e.engine, e.startTs, e.endTs)
	finalPath := filepath.Join(e.outputDir, filename)

	if err := os.Rename(e.tempPath, finalPath); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	e.file = nil
	e.encoder = nil
	return nil
}
