package provider

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"

	"whatever/internal/config"
	"whatever/internal/idgen"
	"whatever/internal/logger"
	proto "whatever/internal/protocol"
	reg "whatever/internal/registry"
)

const BinanceCSVID = "binance_csv"

func init() {
	reg.Providers.Register(BinanceCSVID, func(opts map[string]any) (proto.DataProvider, error) {
		id := idgen.GenerateID(BinanceCSVID)
		logger := logger.NewLogger(logger.DefaultLoggerConfig()).
			With("domain", "provider").
			With("provider", id)

		cfg := BinanceCSVConfig{}
		// The caller names the provider; adding it here would duplicate it.
		if err := config.Decode(opts, &cfg); err != nil {
			return nil, err
		}

		return &BinanceCSVProvider{
			id:     id,
			file:   cfg.File,
			logger: logger,
			mu:     sync.Mutex{},
		}, nil
	})
}

type BinanceCSVConfig struct {
	File string `json:"file"`
}

func (c *BinanceCSVConfig) Validate() error {
	if c.File == "" {
		return fmt.Errorf("'file' is required")
	}
	return nil
}

type BinanceCSVProvider struct {
	id      string
	sub     proto.Subscription
	file    string
	started bool
	limit   int
	data    chan proto.MarketData
	errs    chan error
	done    chan struct{}
	logger  *logger.Logger
	mu      sync.Mutex
}

func (p *BinanceCSVProvider) ID() string {
	return p.id
}

func (p *BinanceCSVProvider) Init(sub proto.Subscription, limit int) error {
	p.sub = sub
	p.limit = limit

	if p.file == "" {
		return fmt.Errorf("no file configured")
	}
	if _, err := os.Stat(p.file); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", p.file)
	}

	p.data = make(chan proto.MarketData, 100)
	p.errs = make(chan error, 10)
	p.done = make(chan struct{})

	return nil
}

func (p *BinanceCSVProvider) Streams() (<-chan proto.MarketData, <-chan error) {
	return p.data, p.errs
}

func (p *BinanceCSVProvider) Start() error {
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

func (p *BinanceCSVProvider) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		p.logger.Warn("attepting to close provider that is not started")
		return
	}

	if p.done != nil {
		close(p.done)
	}
	p.started = false
}

func (p *BinanceCSVProvider) run() {
	defer close(p.data)
	defer close(p.errs)

	file, err := os.Open(p.file)
	if err != nil {
		p.errs <- fmt.Errorf("failed to open file %s: %w", p.file, err)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	reader := csv.NewReader(file)
	count := 0

	for {
		if p.limit > 0 && count >= p.limit {
			break
		}

		select {
		case <-p.done:
			return
		default:
		}

		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				return
			}
			p.logger.Error(err, "failed to read CSV record")
			p.errs <- fmt.Errorf("failed to read CSV record: %w", err)
			return
		}

		candle, err := parseCsvRecordToCandle(record)
		if err != nil {
			p.logger.Warn(fmt.Sprintf("failed to parse CSV record: %v", err))
			p.errs <- fmt.Errorf("failed to parse CSV record: %w", err)
			continue
		}

		md := proto.MarketData{
			Symbol:     p.sub.Symbol,
			ProviderID: p.ID(),
			Timeframe:  string(p.sub.Timeframe),
			Candle:     candle,
		}

		select {
		case p.data <- md:
			count++
		case <-p.done:
			return
		}
	}
}

func parseCsvRecordToCandle(record []string) (proto.Candle, error) {
	// Binance kline CSV format (12 columns):
	// OpenTime,Open,High,Low,Close,Volume,CloseTime,QuoteVol,Trades,TakerBuyBase,TakerBuyQuote,Ignore
	if len(record) < 7 {
		return proto.Candle{}, fmt.Errorf("invalid record length: %d", len(record))
	}

	openTs, err := strconv.ParseInt(record[0], 10, 64)
	if err != nil {
		return proto.Candle{}, fmt.Errorf("invalid OpenTime: %w", err)
	}

	open, err := strconv.ParseFloat(record[1], 64)
	if err != nil {
		return proto.Candle{}, fmt.Errorf("invalid Open: %w", err)
	}

	high, err := strconv.ParseFloat(record[2], 64)
	if err != nil {
		return proto.Candle{}, fmt.Errorf("invalid High: %w", err)
	}

	low, err := strconv.ParseFloat(record[3], 64)
	if err != nil {
		return proto.Candle{}, fmt.Errorf("invalid Low: %w", err)
	}

	closePrice, err := strconv.ParseFloat(record[4], 64)
	if err != nil {
		return proto.Candle{}, fmt.Errorf("invalid Close: %w", err)
	}

	volume, err := strconv.ParseFloat(record[5], 64)
	if err != nil {
		return proto.Candle{}, fmt.Errorf("invalid Volume: %w", err)
	}

	closeTs, err := strconv.ParseInt(record[6], 10, 64)
	if err != nil {
		return proto.Candle{}, fmt.Errorf("invalid CloseTime: %w", err)
	}

	return proto.Candle{
		OpenTs:  openTs,
		CloseTs: closeTs,
		Open:    open,
		High:    high,
		Low:     low,
		Close:   closePrice,
		Volume:  volume,
	}, nil
}
