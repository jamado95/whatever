# Scripts

## binance_download_klines.go

Downloads historical kline (candlestick) data from Binance public archives.

### Usage

```bash
go run scripts/binance_download_klines.go -symbol BTCUSDT -interval 1h -start 2024-01-01 -end 2024-03-01 -out ./data
```

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `-symbol` | Yes | Trading pair symbol (e.g., BTCUSDT, ETHUSDT) |
| `-interval` | Yes | Kline interval (1m, 5m, 15m, 1h, 4h, 1d, etc.) |
| `-start` | Yes | Start date in YYYY-MM-DD format |
| `-end` | Yes | End date in YYYY-MM-DD format |
| `-out` | No | Output directory (default: ./data) |

### Output

Creates a CSV file named `{SYMBOL}-{INTERVAL}-{START_MONTH}.csv` in the output directory.

### Examples

```bash
# Download January 2024 hourly data for BTCUSDT
go run scripts/binance_download_klines.go -symbol BTCUSDT -interval 1h -start 2024-01-01 -end 2024-01-31 -out ./data

# Download Q1 2024 daily data for ETHUSDT
go run scripts/binance_download_klines.go -symbol ETHUSDT -interval 1d -start 2024-01-01 -end 2024-03-31 -out ./data
```

---

## binance_market_info.go

Fetches live market data from the Binance REST API.

### Usage

```bash
go run scripts/binance_market_info.go -cmd <command> [flags]
```

### Commands

| Command | Description |
|---------|-------------|
| `pairs` | List available trading pairs |
| `ticker` | Get 24h stats for a single symbol |
| `tickers` | Get 24h stats for all symbols |

### Flags

| Flag | Description |
|------|-------------|
| `-cmd` | Command to run (pairs, ticker, tickers) |
| `-quote` | Filter by quote asset (e.g., USDT, BTC) |
| `-symbol` | Symbol for ticker command |
| `-search` | Search term for pair names |
| `-json` | Output as JSON instead of table |

### Examples

```bash
# List all USDT trading pairs
go run scripts/binance_market_info.go -cmd pairs -quote USDT

# Search for BTC pairs
go run scripts/binance_market_info.go -cmd pairs -search BTC

# Get 24h ticker for BTCUSDT
go run scripts/binance_market_info.go -cmd ticker -symbol BTCUSDT

# Get all USDT tickers
go run scripts/binance_market_info.go -cmd tickers -quote USDT

# Output pairs as JSON
go run scripts/binance_market_info.go -cmd pairs -quote USDT -json
```
