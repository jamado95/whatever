.PHONY: build run validate test lint fmt tidy check

GO := go

CONFIG ?= config.json

build:
	$(GO) build -o whatever ./cmd

run:
	$(GO) run ./cmd/main.go --config $(CONFIG)

# Validate a config without running the engine. No market data is fetched and
# no export files are written.
validate:
	$(GO) run ./cmd/main.go --config $(CONFIG) --validate-only

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
# Python tooling is managed by uv (https://docs.astral.sh/uv/).
# Dependencies are declared in pyproject.toml and pinned in uv.lock.
# `uv run` provisions the interpreter and syncs .venv on demand, so the
# plot targets below need no separate setup step.
UV := uv
PYTHON := $(UV) run

# Where the engine's exporter writes JSONL (must match "output_dir" in config.json)
EXPORT_DIR := data/exports

# Explicitly materialise .venv from uv.lock (optional; `uv run` does this itself)
.PHONY: py-sync
py-sync:
	$(UV) sync

# Re-resolve dependencies and update uv.lock after editing pyproject.toml
.PHONY: py-lock
py-lock:
	$(UV) lock

.PHONY: plot
plot:
	@if [ -z "$(FILE)" ]; then \
		echo "Error: FILE parameter required"; \
		echo "Usage: make plot FILE=$(EXPORT_DIR)/BTCUSDT_1h_data_logger_<start>_<end>.jsonl"; \
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

# Plot the most recently written export. Export filenames carry a timestamp
# range, so there is no fixed name to target.
.PHONY: quick-plot
quick-plot:
	@latest=$$(ls -t $(EXPORT_DIR)/*.jsonl 2>/dev/null | head -1); \
	if [ -z "$$latest" ]; then \
		echo "Error: no .jsonl exports found in $(EXPORT_DIR)"; \
		echo "Run 'make run' with an exporter configured to produce one"; \
		exit 1; \
	fi; \
	echo "$(PYTHON) scripts/plot_market_data.py $$latest"; \
	$(PYTHON) scripts/plot_market_data.py $$latest

# Clean virtual environment
.PHONY: clean-venv
clean-venv:
	rm -rf .venv