package provider

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"whatever/types"
	"whatever/utils/idgen"
	"whatever/utils/logger"
)

const BinanceHistoricalID = "binance_historical"

func init() {
	Register(BinanceHistoricalID, func(opts map[string]any) (DataProvider, error) {
		id := idgen.GenerateID(BinanceHistoricalID)
		logger := logger.NewLogger(logger.DefaultLoggerConfig()).
			With("domain", "provider").
			With("provider", id)

		cfg := BinanceHistoricalConfig{}
		if v, ok := opts["startTime"].(int64); ok {
			cfg.StartTime = v
		}
		if v, ok := opts["endTime"].(int64); ok {
			cfg.EndTime = v
		}

		return &BinanceHistoricalProvider{
			id:        id,
			startTime: cfg.StartTime,
			endTime:   cfg.EndTime,
			logger:    logger,
			http:      &http.Client{},
			mu:        sync.Mutex{},
		}, nil
	})
}

const (
	binanceBaseURL  = "https://api.binance.com/api/v3"
	binanceMaxLimit = 1000
)

var timeframeMap = map[types.Timeframe]string{
	types.Timeframe1m:  "1m",
	types.Timeframe5m:  "5m",
	types.Timeframe15m: "15m",
	types.Timeframe1h:  "1h",
	types.Timeframe1d:  "1d",
}

type BinanceHistoricalConfig struct {
	StartTime int64 // Unix ms timestamp
	EndTime   int64 // Unix ms timestamp
}

type BinanceHistoricalProvider struct {
	id        string
	sub       types.Subscription
	startTime int64
	endTime   int64
	limit     int
	data      chan types.MarketData
	errs      chan error
	done      chan struct{}
	started   bool
	logger    *logger.Logger
	mu        sync.Mutex
	http      *http.Client
}

func (p *BinanceHistoricalProvider) ID() string {
	return p.id
}

func (p *BinanceHistoricalProvider) Init(sub types.Subscription, limit int) error {
	p.sub = sub
	p.limit = limit

	if !sub.Timeframe.IsValid() {
		return fmt.Errorf("invalid timeframe: %s", sub.Timeframe)
	}
	if _, ok := timeframeMap[sub.Timeframe]; !ok {
		return fmt.Errorf("unsupported timeframe for Binance: %s", sub.Timeframe)
	}

	p.data = make(chan types.MarketData, 100)
	p.errs = make(chan error, 10)
	p.done = make(chan struct{})

	return nil
}

func (p *BinanceHistoricalProvider) Streams() (<-chan types.MarketData, <-chan error) {
	return p.data, p.errs
}

func (p *BinanceHistoricalProvider) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return fmt.Errorf("provider already started")
	}

	p.logger.Info("starting provider...")
	p.started = true

	go p.run()
	return nil
}

func (p *BinanceHistoricalProvider) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		p.logger.Warn("attempting to close provider that is not started")
		return
	}

	if p.done != nil {
		close(p.done)
	}
	p.started = false
}

func (p *BinanceHistoricalProvider) run() {
	defer close(p.data)
	defer close(p.errs)

	interval := timeframeMap[p.sub.Timeframe]
	currentStart := p.startTime
	fetched := 0

	for {
		select {
		case <-p.done:
			return
		default:
		}

		if p.endTime > 0 && currentStart >= p.endTime {
			break
		}

		url := p.buildURL(p.sub.Symbol, interval, currentStart)
		candles, err := p.fetchCandlesFromBinanceAPI(url)
		if err != nil {
			p.errs <- fmt.Errorf("failed to fetch candles for %s: %w", p.sub.Symbol, err)
			return
		}
		p.logger.Debug(fmt.Sprintf("fetched %d candles for %s:%s", len(candles), p.sub.Symbol, string(p.sub.Timeframe)))

		if len(candles) == 0 {
			break
		}

		for _, candle := range candles {
			if p.endTime > 0 && candle.OpenTs > p.endTime {
				return
			}

			md := types.MarketData{
				Symbol:    p.sub.Symbol,
				Source:    p.ID(),
				Timeframe: string(p.sub.Timeframe),
				Candle:    candle,
			}

			select {
			case p.data <- md:
				fetched++
				if p.limit > 0 && fetched >= p.limit {
					p.logger.Info("limit reached, stopping provider")
					return
				}
			case <-p.done:
				return
			}
		}

		lastCandle := candles[len(candles)-1]
		currentStart = lastCandle.CloseTs + 1

		if len(candles) < binanceMaxLimit {
			break
		}
	}
}

func (p *BinanceHistoricalProvider) buildURL(symbol, interval string, startTime int64) string {
	return fmt.Sprintf("%s/klines?symbol=%s&interval=%s&startTime=%d&limit=%d",
		binanceBaseURL, symbol, interval, startTime, binanceMaxLimit)
}

func (p *BinanceHistoricalProvider) fetchCandlesFromBinanceAPI(url string) ([]types.Candle, error) {
	resp, err := p.http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var rawKlines [][]interface{}
	if err := json.Unmarshal(body, &rawKlines); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	candles := make([]types.Candle, 0, len(rawKlines))
	for _, raw := range rawKlines {
		candle, err := parseKlineResponse(raw)
		if err != nil {
			p.logger.Warn(fmt.Sprintf("failed to parse kline: %v", err))
			continue
		}
		candles = append(candles, candle)
	}

	return candles, nil
}

func parseKlineResponse(raw []interface{}) (candle types.Candle, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("failed to parse kline: %v", r)
		}
	}()

	if len(raw) < 7 {
		return types.Candle{}, fmt.Errorf("invalid kline data: expected at least 7 elements, got %d", len(raw))
	}

	open, _ := strconv.ParseFloat(raw[1].(string), 64)
	high, _ := strconv.ParseFloat(raw[2].(string), 64)
	low, _ := strconv.ParseFloat(raw[3].(string), 64)
	closePrice, _ := strconv.ParseFloat(raw[4].(string), 64)
	volume, _ := strconv.ParseFloat(raw[5].(string), 64)

	return types.Candle{
		OpenTs:  int64(raw[0].(float64)),
		CloseTs: int64(raw[6].(float64)),
		Open:    open,
		High:    high,
		Low:     low,
		Close:   closePrice,
		Volume:  volume,
	}, nil
}
