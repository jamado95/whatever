package logger

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"

	proto "whatever/internal/protocol"
)

type Logger struct {
	zl zerolog.Logger
}

type LoggerConfig struct {
	Level      string
	Pretty     bool
	Output     io.Writer
	TimeFormat string
}

func DefaultLoggerConfig() LoggerConfig {
	return LoggerConfig{
		Level:      "debug",
		Pretty:     true,
		Output:     os.Stdout,
		TimeFormat: time.RFC3339,
	}
}

func NewLogger(cfg LoggerConfig) *Logger {
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}
	if cfg.TimeFormat == "" {
		cfg.TimeFormat = time.RFC3339
	}

	zerolog.TimeFieldFormat = cfg.TimeFormat

	output := cfg.Output
	if cfg.Pretty {
		output = zerolog.ConsoleWriter{
			Out:        cfg.Output,
			TimeFormat: cfg.TimeFormat,
			FieldsOrder: []string{
				"event", "symbol", "side", "timeframe",
				"id", "order_id",
				"open", "high", "low", "close", "volume",
				"size", "price", "fill_price", "fill_size",
				"count", "timestamp", "open_ts", "close_ts", "filled_at", "expires_at",
				"domain", "engine", "provider", "strategy", "executor",
			},
		}
	}

	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	zl := zerolog.New(output).Level(level).With().Timestamp().Logger()

	return &Logger{zl: zl}
}

func (l *Logger) MarketData(data proto.MarketData) {
	l.zl.Info().
		Str("event", "market_data").
		Str("symbol", data.Symbol).
		Str("provider", data.Source).
		Str("timeframe", data.Timeframe).
		Float64("open", data.Candle.Open).
		Float64("high", data.Candle.High).
		Float64("low", data.Candle.Low).
		Float64("close", data.Candle.Close).
		Float64("volume", data.Candle.Volume).
		Int64("open_ts", data.Candle.OpenTs).
		Int64("close_ts", data.Candle.CloseTs).
		Msg("MarketData")
}

func (l *Logger) Signal(sig proto.Signal) {
	l.zl.Info().
		Str("event", "signal").
		Str("symbol", sig.Symbol).
		Str("side", string(sig.Side)).
		Str("provider", sig.Source).
		Int64("timestamp", sig.Timestamp).
		Int64("expires_at", sig.ExpiresAt).
		Msg("signal")
}

func (l *Logger) Order(ord proto.Order) {
	evt := l.zl.Info().
		Str("event", "order").
		Str("id", ord.ID).
		Str("symbol", ord.Symbol).
		Str("side", string(ord.Side)).
		Float64("size", ord.Size).
		Str("provider", ord.Source).
		Int64("timestamp", ord.Timestamp).
		Int64("expires_at", ord.ExpiresAt)

	if ord.Price != nil {
		evt.Float64("price", *ord.Price)
	}

	evt.Msg("order")
}

func (l *Logger) Fill(fill proto.Fill) {
	l.zl.Info().
		Str("event", "fill").
		Str("order_id", fill.Order.ID).
		Str("symbol", fill.Order.Symbol).
		Str("side", string(fill.Order.Side)).
		Float64("fill_price", fill.FillPrice).
		Float64("fill_size", fill.FillSize).
		Int64("filled_at", fill.FilledAt).
		Msg("fill")
}

func (l *Logger) Position(pos proto.Position) {
	l.zl.Info().
		Str("event", "position").
		Str("symbol", pos.Symbol).
		Str("side", string(pos.Side)).
		Float64("size", pos.Size).
		Float64("entry_price", pos.EntryPrice).
		Float64("current_price", pos.CurrentPrice).
		Float64("unrealized_pnl", pos.UnrealizedPnL).
		Msg("position")
}

func (l *Logger) Portfolio(state proto.PortfolioState) {
	l.zl.Info().
		Str("event", "portfolio").
		Float64("total_value", state.TotalValue).
		Float64("unrealized_pnl", state.UnrealizedPnL).
		Float64("realized_pnl", state.RealizedPnL).
		Int("positions", len(state.Positions)).
		Int64("timestamp", state.Timestamp).
		Msg("portfolio")
}

func (l *Logger) Error(err error, msg string) {
	l.zl.Error().Err(err).Msg(msg)
}

func (l *Logger) Info(msg string) {
	l.zl.Info().Msg(msg)
}

func (l *Logger) Debug(msg string) {
	l.zl.Debug().Msg(msg)
}

func (l *Logger) Warn(msg string) {
	l.zl.Warn().Msg(msg)
}

func (l *Logger) With(key string, value any) *Logger {
	return &Logger{zl: l.zl.With().Any(key, value).Logger()}
}
