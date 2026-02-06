.PHONY: build run test lint fmt tidy check

GO := go

build:
	$(GO) build -o whatever ./cmd

run:
	$(GO) run ./cmd/main.go

# Script paths
BINANCE_KLINES := cmd/scripts/binance_download_klines/binance_download_klines.go
BINANCE_MARKET := cmd/scripts/binance_market_info/binance_market_info.go

# ------------------------
# Binance: Download klines
# ------------------------
# Usage:
# make binance-klines SYMBOL=BTCUSDT INTERVAL=1h START=2024-01-01 END=2024-01-31 OUT=./data

binance-klines:
	$(GO) run $(BINANCE_KLINES) \
		-symbol $(SYMBOL) \
		-interval $(INTERVAL) \
		-start $(START) \
		-end $(END) \
		$(if $(OUT),-out $(OUT),)

# ------------------------
# Binance: Market info
# ------------------------
# Usage examples:
# make binance-pairs QUOTE=USDT
# make binance-pairs SEARCH=BTC
# make binance-ticker SYMBOL=BTCUSDT
# make binance-tickers QUOTE=USDT JSON=1

binance-pairs:
	$(GO) run $(BINANCE_MARKET) \
		-cmd pairs \
		$(if $(QUOTE),-quote $(QUOTE),) \
		$(if $(SEARCH),-search $(SEARCH),) \
		$(if $(JSON),-json,)

binance-ticker:
	$(GO) run $(BINANCE_MARKET) \
		-cmd ticker \
		-symbol $(SYMBOL) \
		$(if $(JSON),-json,)

binance-tickers:
	$(GO) run $(BINANCE_MARKET) \
		-cmd tickers \
		$(if $(QUOTE),-quote $(QUOTE),) \
		$(if $(JSON),-json,)


# ------------------------
# Make Utilities
# ------------------------

test:
	go test ./...

fmt:
	gofmt -w .

lint:
	golangci-lint run

lint-clean:
	golangci-lint cache clean && golangci-lint run

tidy:
	go mod tidy

check: fmt tidy test lint