package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const BINANCE_APIV3_URL = "https://api.binance.com/api/v3"

type ExchangeInfo struct {
	Timezone   string   `json:"timezone"`
	ServerTime int64    `json:"serverTime"`
	Symbols    []Symbol `json:"symbols"`
}

type Symbol struct {
	Symbol             string   `json:"symbol"`
	Status             string   `json:"status"`
	BaseAsset          string   `json:"baseAsset"`
	QuoteAsset         string   `json:"quoteAsset"`
	BaseAssetPrecision int      `json:"baseAssetPrecision"`
	QuotePrecision     int      `json:"quotePrecision"`
	OrderTypes         []string `json:"orderTypes"`
	Permissions        []string `json:"permissions"`
}

type Ticker24h struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	LastPrice          string `json:"lastPrice"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
}

func main() {
	cmd := flag.String("cmd", "pairs", "Command: pairs, ticker, search")
	quote := flag.String("quote", "", "Filter by quote asset (e.g., USDT, BTC)")
	symbol := flag.String("symbol", "", "Symbol for ticker command")
	search := flag.String("search", "", "Search term for symbol names")
	jsonOut := flag.Bool("json", false, "Output as JSON")

	flag.Parse()

	switch *cmd {
	case "pairs":
		listPairs(*quote, *search, *jsonOut)
	case "ticker":
		getTicker(*symbol, *jsonOut)
	case "tickers":
		getAllTickers(*quote, *jsonOut)
	default:
		fmt.Println("Unknown command. Available: pairs, ticker, tickers")
		os.Exit(1)
	}
}

func listPairs(quoteFilter, searchTerm string, jsonOut bool) {
	resp, err := http.Get(BINANCE_APIV3_URL + "/exchangeInfo")
	if err != nil {
		fmt.Printf("Failed to fetch exchange info: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("Failed to close response body: %v\n", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		os.Exit(1)
	}

	var info ExchangeInfo
	if err := json.Unmarshal(body, &info); err != nil {
		fmt.Printf("Failed to parse response: %v\n", err)
		os.Exit(1)
	}

	var filtered []Symbol
	for _, s := range info.Symbols {
		if s.Status != "TRADING" {
			continue
		}
		if quoteFilter != "" && s.QuoteAsset != strings.ToUpper(quoteFilter) {
			continue
		}
		if searchTerm != "" && !strings.Contains(strings.ToLower(s.Symbol), strings.ToLower(searchTerm)) {
			continue
		}
		filtered = append(filtered, s)
	}

	if jsonOut {
		out, _ := json.MarshalIndent(filtered, "", "  ")
		fmt.Println(string(out))
		return
	}

	fmt.Printf("Found %d trading pairs\n\n", len(filtered))
	fmt.Printf("%-15s %-10s %-10s\n", "SYMBOL", "BASE", "QUOTE")
	fmt.Println(strings.Repeat("-", 40))
	for _, s := range filtered {
		fmt.Printf("%-15s %-10s %-10s\n", s.Symbol, s.BaseAsset, s.QuoteAsset)
	}
}

func getTicker(symbol string, jsonOut bool) {
	if symbol == "" {
		fmt.Println("Please specify -symbol")
		os.Exit(1)
	}

	url := fmt.Sprintf("%s/ticker/24hr?symbol=%s", BINANCE_APIV3_URL, strings.ToUpper(symbol))
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to fetch ticker: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("Failed to close response body: %v\n", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("API error: %s\n", string(body))
		os.Exit(1)
	}

	var ticker Ticker24h
	if err := json.Unmarshal(body, &ticker); err != nil {
		fmt.Printf("Failed to parse response: %v\n", err)
		os.Exit(1)
	}

	if jsonOut {
		out, _ := json.MarshalIndent(ticker, "", "  ")
		fmt.Println(string(out))
		return
	}

	fmt.Printf("Symbol:    %s\n", ticker.Symbol)
	fmt.Printf("Price:     %s\n", ticker.LastPrice)
	fmt.Printf("Change:    %s (%s%%)\n", ticker.PriceChange, ticker.PriceChangePercent)
	fmt.Printf("High:      %s\n", ticker.HighPrice)
	fmt.Printf("Low:       %s\n", ticker.LowPrice)
	fmt.Printf("Volume:    %s\n", ticker.Volume)
	fmt.Printf("Quote Vol: %s\n", ticker.QuoteVolume)
}

func getAllTickers(quoteFilter string, jsonOut bool) {
	resp, err := http.Get(BINANCE_APIV3_URL + "/ticker/24hr")
	if err != nil {
		fmt.Printf("Failed to fetch tickers: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("Failed to close response body: %v\n", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		os.Exit(1)
	}

	var tickers []Ticker24h
	if err := json.Unmarshal(body, &tickers); err != nil {
		fmt.Printf("Failed to parse response: %v\n", err)
		os.Exit(1)
	}

	var filtered []Ticker24h
	for _, t := range tickers {
		if quoteFilter != "" && !strings.HasSuffix(t.Symbol, strings.ToUpper(quoteFilter)) {
			continue
		}
		filtered = append(filtered, t)
	}

	if jsonOut {
		out, _ := json.MarshalIndent(filtered, "", "  ")
		fmt.Println(string(out))
		return
	}

	fmt.Printf("%-15s %15s %10s %15s\n", "SYMBOL", "PRICE", "CHANGE%", "VOLUME")
	fmt.Println(strings.Repeat("-", 60))
	for _, t := range filtered {
		fmt.Printf("%-15s %15s %10s%% %15s\n", t.Symbol, t.LastPrice, t.PriceChangePercent, t.Volume)
	}
}
