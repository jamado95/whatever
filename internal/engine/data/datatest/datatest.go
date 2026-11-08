// Package datatest samples the BTCUSDT reference dataset for reference tests.
package datatest

import (
	"bufio"
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"whatever/internal/domains/provider"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

// Sample is a window of the BTCUSDT reference dataset, all of 2024, written per timeframe to a temp dir.
type Sample struct {
	Start, End time.Time
	files      map[proto.Timeframe]string
}

/*
Reference samples data/BTCUSDT/BTCUSDT-<tf>-2024-01.csv for each of tfs, found from the module root:
four months starting on a random day from 2024-01-01 to 2024-09-01, logged. It skips t when a file is
missing.
*/
func Reference(t testing.TB, tfs ...proto.Timeframe) *Sample {
	t.Helper()
	dir := filepath.Join(moduleRoot(t), "data", "BTCUSDT")
	src := func(tf proto.Timeframe) string {
		return filepath.Join(dir, "BTCUSDT-"+string(tf)+"-2024-01.csv")
	}
	for _, tf := range tfs {
		if _, err := os.Stat(src(tf)); err != nil {
			t.Skipf("reference dataset missing: %v", err)
		}
	}

	year := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	start := year.AddDate(0, 0, rand.IntN(245)) // 2024-01-01 … 2024-09-01
	end := start.AddDate(0, 4, 0)
	t.Logf("sample %s … %s", start.Format(time.DateOnly), end.Format(time.DateOnly))

	s := &Sample{Start: start, End: end, files: make(map[proto.Timeframe]string, len(tfs))}
	for _, tf := range tfs {
		file := filepath.Join(t.TempDir(), string(tf)+".csv")
		sampleCSV(t, src(tf), file, start.UnixMilli(), end.UnixMilli())
		s.files[tf] = file
	}
	return s
}

// Provider is a binance_csv provider over sub's timeframe in the sample, initialised for sub.
func (s *Sample) Provider(t testing.TB, sub proto.Subscription) proto.DataProvider {
	t.Helper()
	file, ok := s.files[sub.Timeframe]
	if !ok {
		t.Fatalf("datatest: timeframe %s not sampled", sub.Timeframe)
	}
	p, err := reg.Providers.Create(provider.BinanceCSVID, map[string]any{"file": file})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Init(sub); err != nil { // the engine's part
		t.Fatal(err)
	}
	return p
}

// moduleRoot is the nearest directory at or above the working directory holding go.mod.
func moduleRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("datatest: no go.mod at or above the working directory")
		}
		dir = parent
	}
}

// sampleCSV copies the rows of src whose OpenTs (ms, first column) lies in [from, to) to dst.
func sampleCSV(t testing.TB, src, dst string, from, to int64) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()

	var out bytes.Buffer
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		line := sc.Text()
		ts, err := strconv.ParseInt(line[:strings.IndexByte(line, ',')], 10, 64)
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if ts >= from && ts < to {
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, out.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
