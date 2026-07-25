# Python Visualization for Go Trading System - Market Data Only

## Context
I'm building an algorithmic trading system in Go with an event-driven architecture. The system processes market data through a feature chain that calculates technical indicators. I need a simple Python visualization tool to plot candlestick data with technical indicators overlaid.

## Current Go Architecture
- **Feature Chain**: Processes `MarketData` sequentially through features that calculate indicators
- **Output**: Each candle produces `EnrichedMarketData` containing OHLCV + calculated indicators in a `Snapshot`
- **Data Flow**: CSV Provider → Feature Chain → (enriched data ready for visualization)

## Task: Build Simple Market Data Visualization

### 1. Go Export Feature (I'll implement this part)
I'll create a `PlotExporter` feature that writes a JSONL file during processing:
- **File**: `market_data.jsonl` in `outputs/` directory
- **Format**: One line per candle with OHLCV + all calculated indicators

**Example market_data.jsonl:**
```json
{"timestamp":1609459200000,"symbol":"BTCUSDT","open":29000.5,"high":29100.2,"low":28950.0,"close":29050.3,"volume":123.45,"sma_20":28900.5,"rsi_14":65.2}
{"timestamp":1609459260000,"symbol":"BTCUSDT","open":29050.3,"high":29200.0,"low":29040.0,"close":29180.5,"volume":234.56,"sma_20":28920.1,"rsi_14":68.5}
```

**Notes:**
- Timestamps are Unix milliseconds
- Indicator fields are dynamic (depends on which features are enabled)
- Common indicators: `sma_20`, `sma_50`, `rsi_14`, `ema_12`, `ema_26`, `macd`, etc.

### 2. Python Script Needed

**`plot_market_data.py` - Single visualization script**

**Requirements:**
- Accept JSONL file path as command-line argument
- Load market data into pandas DataFrame
- Create interactive multi-panel Plotly chart:
  - **Panel 1**: Candlestick chart (OHLCV)
    - Overlay any SMA/EMA indicators found as lines (different colors)
  - **Panel 2**: RSI indicator if present (0-100 range with 30/70 reference lines)
  - **Panel 3**: Volume bars
- Shared X-axis (time) across all panels
- Hover tooltips showing all values for each candle
- Save as `market_data_chart.html` in same directory as input file
- Print summary: date range, number of candles, which indicators were found

**Usage:**
```bash
python scripts/plot_market_data.py outputs/market_data.jsonl
```

**Expected output:**
```
Loaded 1000 candles for BTCUSDT
Date range: 2024-01-01 to 2024-01-10
Indicators found: sma_20, sma_50, rsi_14
Chart saved to: outputs/market_data_chart.html
```

**Auto-detect indicators:**
- Scan column names for common patterns
- Overlay indicators: anything with `sma`, `ema`, `ma`, `bb` (Bollinger Bands)
- Oscillators in separate panel: `rsi`, `macd`, `stoch`
- Volume always in bottom panel

**Chart styling:**
- Candlestick: green for up, red for down
- Indicators: distinct colors (use Plotly color sequence)
- Clean layout with proper labels and legends
- Responsive sizing

## Technical Requirements

**Python Libraries:**
- `pandas` - data loading and manipulation  
- `plotly` - interactive charts
- Standard library: `json`, `pathlib`, `argparse`, `sys`

**Design Principles:**
- **Single file**: Keep it simple, everything in one script
- **Robust**: Handle missing indicators gracefully (just plot what's available)
- **Clear error messages**: If file not found or malformed, print helpful message
- **No configuration needed**: Smart defaults, auto-detect what to plot

**File Structure:**
```
scripts/
└── plot_market_data.py    # Single script, ~150-200 lines
```

## Success Criteria
- I run my Go app, it produces `market_data.jsonl`
- I run `python scripts/plot_market_data.py outputs/market_data.jsonl`
- I get an interactive HTML chart showing price action + all indicators
- The chart opens in my browser automatically (or I can open the HTML file)
- Total workflow: 2 commands, under 5 seconds

## Example Enhanced Usage (Optional)
If you want to make it slightly more flexible:
```bash
# Basic usage
python scripts/plot_market_data.py outputs/market_data.jsonl

# Specify date range
python scripts/plot_market_data.py outputs/market_data.jsonl --start 2024-01-01 --end 2024-01-05

# Only plot specific indicators
python scripts/plot_market_data.py outputs/market_data.jsonl --indicators sma_20 rsi_14
```

But the basic version without arguments should work perfectly.

## Motivation
Python's Plotly creates beautiful interactive charts with minimal code. Rather than fighting with Go's limited plotting libraries, I export simple JSONL and visualize in Python. This gives me:
- Interactive charts (zoom, pan, hover) for analyzing indicator behavior
- Fast iteration - no recompilation to tweak charts
- Easy to add new indicator visualizations as I build more features
- Clean separation: Go does data processing (its strength), Python does visualization (its strength)

Keep the code simple and focused - just load JSONL, create a nice chart, done.