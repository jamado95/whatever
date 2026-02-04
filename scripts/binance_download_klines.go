package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const baseURL = "https://data.binance.vision/data/spot/monthly/klines"

func main() {
	symbol := flag.String("symbol", "", "Trading pair symbol (e.g., BTCUSDT)")
	interval := flag.String("interval", "", "Kline interval (e.g., 1h, 4h, 1d)")
	startDate := flag.String("start", "", "Start date (YYYY-MM-DD)")
	endDate := flag.String("end", "", "End date (YYYY-MM-DD)")
	outDir := flag.String("out", "./data", "Output directory")

	flag.Parse()

	if *symbol == "" || *interval == "" || *startDate == "" || *endDate == "" {
		fmt.Println("Usage: go run download_klines.go -symbol BTCUSDT -interval 1h -start 2024-01-01 -end 2024-03-01 -out ./data")
		flag.PrintDefaults()
		os.Exit(1)
	}

	start, err := time.Parse("2006-01-02", *startDate)
	if err != nil {
		fmt.Printf("Invalid start date: %v\n", err)
		os.Exit(1)
	}

	end, err := time.Parse("2006-01-02", *endDate)
	if err != nil {
		fmt.Printf("Invalid end date: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Printf("Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	months := getMonthRange(start, end)
	fmt.Printf("Downloading %d month(s) of data for %s %s\n", len(months), *symbol, *interval)

	var allData []byte

	for _, month := range months {
		fmt.Printf("Downloading %s...\n", month)

		data, err := downloadMonth(*symbol, *interval, month)
		if err != nil {
			fmt.Printf("Warning: failed to download %s: %v\n", month, err)
			continue
		}

		allData = append(allData, data...)
		fmt.Printf("  Downloaded %d bytes\n", len(data))
	}

	if len(allData) == 0 {
		fmt.Println("No data downloaded")
		os.Exit(1)
	}

	outputFile := filepath.Join(*outDir, fmt.Sprintf("%s-%s-%s.csv", *symbol, *interval, start.Format("2006-01")))
	if err := os.WriteFile(outputFile, allData, 0644); err != nil {
		fmt.Printf("Failed to write output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Saved to %s (%d bytes)\n", outputFile, len(allData))
}

func getMonthRange(start, end time.Time) []string {
	var months []string

	current := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonth := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)

	for !current.After(endMonth) {
		months = append(months, current.Format("2006-01"))
		current = current.AddDate(0, 1, 0)
	}

	return months
}

func downloadMonth(symbol, interval, month string) ([]byte, error) {
	// URL format: https://data.binance.vision/data/spot/monthly/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{YYYY-MM}.zip
	url := fmt.Sprintf("%s/%s/%s/%s-%s-%s.zip", baseURL, symbol, interval, symbol, interval, month)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	zipData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return extractCSV(zipData)
}

func extractCSV(zipData []byte) ([]byte, error) {
	reader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return nil, fmt.Errorf("failed to open zip: %w", err)
	}

	for _, file := range reader.File {
		if filepath.Ext(file.Name) != ".csv" {
			continue
		}

		rc, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("failed to open file in zip: %w", err)
		}
		defer rc.Close()

		data, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("failed to read file from zip: %w", err)
		}

		return data, nil
	}

	return nil, fmt.Errorf("no CSV file found in zip")
}
