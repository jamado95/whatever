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

# ------------------------
# Python visualization
# ------------------------
# Python virtual environment setup
VENV := .venv
PYTHON := $(VENV)/bin/python3
PIP := $(VENV)/bin/pip3

.PHONY: venv
venv:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip

.PHONY: plot-deps
plot-deps: venv
	$(PIP) install pandas plotly

.PHONY: plot
plot:
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE parameter required"; \
		echo "Usage: make plot FILE=outputs/market_data.jsonl"; \
		exit 1; \
	fi
	@if [ ! -f "$(FILE)" ]; then \
		echo "Error: File not found: $(FILE)"; \
		exit 1; \
	fi
	@cmd="$(PYTHON) scripts/plot_market_data.py $(FILE)"; \
	if [ -n "$(START)" ]; then cmd="$$cmd --start $(START)"; fi; \
	if [ -n "$(END)" ]; then cmd="$$cmd --end $(END)"; fi; \
	if [ -n "$(INDICATORS)" ]; then cmd="$$cmd --indicators $(INDICATORS)"; fi; \
	echo "$$cmd"; \
	$$cmd

.PHONY: quick-plot
quick-plot:
	@if [ ! -f "outputs/market_data.jsonl" ]; then \
		echo "Error: outputs/market_data.jsonl not found"; \
		exit 1; \
	fi
	$(PYTHON) scripts/plot_market_data.py outputs/market_data.jsonl

# Clean virtual environment
.PHONY: clean-venv
clean-venv:
	rm -rf $(VENV)