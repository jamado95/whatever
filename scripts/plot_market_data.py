#!/usr/bin/env python3
"""
Market Data Visualization Script

Loads JSONL market data and produces interactive Plotly charts with
candlesticks, volume, and auto-detected technical indicators.
"""

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Default number of candles to show initially when dataset is large
DEFAULT_VISIBLE_CANDLES = 200


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate interactive Plotly charts from JSONL market data"
    )
    parser.add_argument("input_file", help="Path to JSONL market data file")
    parser.add_argument(
        "--start",
        help="Start date filter (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
        default=None,
    )
    parser.add_argument(
        "--end",
        help="End date filter (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
        default=None,
    )
    parser.add_argument(
        "--indicators",
        help="Comma-separated list of indicator columns to include",
        default=None,
    )
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_VISIBLE_CANDLES,
        help=f"Number of candles to show initially (0 = show all, default: {DEFAULT_VISIBLE_CANDLES})",
    )
    parser.add_argument(
        "--rangeslider",
        action="store_true",
        help="Show range slider for navigation",
    )
    return parser.parse_args()


def load_data(path: str) -> pd.DataFrame:
    """Load JSONL file into a pandas DataFrame."""
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    df = pd.DataFrame(records)

    # Convert timestamp from Unix ms to datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def detect_indicators(df: pd.DataFrame, user_indicators: list = None) -> dict:
    """
    Categorize indicator columns into overlay and oscillator types.

    Returns dict with 'overlay' and 'oscillator' lists.
    """
    # Base OHLCV columns to exclude
    base_cols = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}

    # Patterns for overlay indicators (plotted on price chart)
    overlay_patterns = [r"^sma", r"^ema", r"^ma[_\d]", r"^bb", r"^vwap"]

    # Patterns for oscillator indicators (separate panel)
    oscillator_patterns = [r"^rsi", r"^macd", r"^stoch", r"^cci", r"^mfi", r"^adx"]

    # Patterns for candlestick pattern signals (plotted as arrows on price chart)
    candle_patterns = [r"_candle$"]

    overlays = []
    oscillators = []
    candles = []

    # Get candidate columns
    if user_indicators:
        candidates = [c for c in user_indicators if c in df.columns]
    else:
        candidates = [c for c in df.columns if c.lower() not in base_cols]

    for col in candidates:
        col_lower = col.lower()

        # Check if overlay
        is_overlay = any(re.match(p, col_lower) for p in overlay_patterns)
        if is_overlay:
            overlays.append(col)
            continue

        # Check if oscillator
        is_oscillator = any(re.match(p, col_lower) for p in oscillator_patterns)
        if is_oscillator:
            oscillators.append(col)
            continue

        # Check if candlestick pattern
        is_candle = any(re.search(p, col_lower) for p in candle_patterns)
        if is_candle:
            candles.append(col)

    return {"overlay": overlays, "oscillator": oscillators, "candles": candles}


def create_chart(
    df: pd.DataFrame,
    overlays: list,
    oscillators: list,
    candles: list = None,
    title: str = "Market Data",
    window: int = DEFAULT_VISIBLE_CANDLES,
    rangeslider: bool = False,
) -> go.Figure:
    """Build a multi-panel Plotly figure."""
    # Determine number of rows
    has_oscillators = len(oscillators) > 0
    has_volume = "volume" in df.columns

    num_rows = 1  # Always have candlestick
    if has_oscillators:
        num_rows += 1
    if has_volume:
        num_rows += 1

    # Row heights
    if num_rows == 1:
        row_heights = [1.0]
    elif num_rows == 2:
        row_heights = [0.7, 0.3]
    else:
        row_heights = [0.5, 0.25, 0.25]

    # Subplot titles
    subplot_titles = ["Price"]
    if has_oscillators:
        subplot_titles.append("Oscillators")
    if has_volume:
        subplot_titles.append("Volume")

    fig = make_subplots(
        rows=num_rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=subplot_titles,
    )

    # Row tracking
    current_row = 1

    # Panel 1: Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df["timestamp"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="OHLC",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
        ),
        row=current_row,
        col=1,
    )

    # Add overlay indicators to price chart
    colors = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
    ]
    for i, indicator in enumerate(overlays):
        color = colors[i % len(colors)]
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df[indicator],
                mode="lines",
                name=indicator,
                line=dict(color=color, width=1),
                hovertemplate=f"<b>{indicator}</b><br>%{{x}}<br>Value: %{{y:.4f}}<extra></extra>",
            ),
            row=current_row,
            col=1,
        )

    # Plot candlestick patterns as markers on price chart
    if candles:
        for candle_col in candles:
            # Up signals (value == 1): green triangle-up below candle low
            up_mask = df[candle_col] == 1
            fig.add_trace(
                go.Scatter(
                    x=df.loc[up_mask, "timestamp"],
                    y=df.loc[up_mask, "low"] * 0.998,
                    mode="markers",
                    marker=dict(symbol="triangle-up", size=12, color="lime"),
                    name=f"{candle_col} (bullish)",
                    hovertemplate=f"<b>{candle_col}</b><br>%{{x}}<br>Bullish<extra></extra>",
                ),
                row=1,
                col=1,
            )

            # Down signals (value == -1): red triangle-down above candle high
            down_mask = df[candle_col] == -1
            fig.add_trace(
                go.Scatter(
                    x=df.loc[down_mask, "timestamp"],
                    y=df.loc[down_mask, "high"] * 1.002,
                    mode="markers",
                    marker=dict(symbol="triangle-down", size=12, color="red"),
                    name=f"{candle_col} (bearish)",
                    hovertemplate=f"<b>{candle_col}</b><br>%{{x}}<br>Bearish<extra></extra>",
                ),
                row=1,
                col=1,
            )

    # Panel 2: Oscillators (if present)
    if has_oscillators:
        current_row += 1
        for i, indicator in enumerate(oscillators):
            color = colors[i % len(colors)]
            fig.add_trace(
                go.Scatter(
                    x=df["timestamp"],
                    y=df[indicator],
                    mode="lines",
                    name=indicator,
                    line=dict(color=color, width=1),
                    hovertemplate=f"<b>{indicator}</b><br>%{{x}}<br>Value: %{{y:.4f}}<extra></extra>",
                ),
                row=current_row,
                col=1,
            )

        # Add RSI reference lines if RSI indicator present
        rsi_cols = [c for c in oscillators if c.lower().startswith("rsi")]
        if rsi_cols:
            fig.add_hline(
                y=70, line_dash="dash", line_color="red", opacity=0.5, row=current_row, col=1
            )
            fig.add_hline(
                y=30, line_dash="dash", line_color="green", opacity=0.5, row=current_row, col=1
            )

        # Add reference lines at oscillator bounds
        fig.add_hline(
            y=150, line_dash="dot", line_color="gray", opacity=0.3, row=current_row, col=1
        )
        fig.add_hline(
            y=-150, line_dash="dot", line_color="gray", opacity=0.3, row=current_row, col=1
        )

    # Panel 3: Volume (if present)
    if has_volume:
        current_row += 1
        # Color volume bars based on price direction
        colors_vol = [
            "#26a69a" if close >= open_ else "#ef5350"
            for close, open_ in zip(df["close"], df["open"])
        ]
        fig.add_trace(
            go.Bar(
                x=df["timestamp"],
                y=df["volume"],
                name="Volume",
                marker_color=colors_vol,
                opacity=0.7,
            ),
            row=current_row,
            col=1,
        )

    # Update layout
    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=rangeslider,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template="plotly_dark",
        paper_bgcolor="#1e1e1e",
        plot_bgcolor="#1e1e1e",
        dragmode="pan",  # Default to pan mode for TradingView-like behavior
        autosize=True,
        margin=dict(l=50, r=50, t=80, b=50),
    )

    # Enable y-axis auto-scaling when zooming on x-axis
    fig.update_yaxes(autorange=True, fixedrange=False)

    # Apply initial zoom window for large datasets
    if window > 0 and len(df) > window:
        start_idx = len(df) - window
        x_start = df["timestamp"].iloc[start_idx]
        x_end = df["timestamp"].iloc[-1]
        fig.update_xaxes(range=[x_start, x_end])

    return fig


def main():
    args = parse_args()

    # Load data
    input_path = Path(args.input_file).resolve()
    if not input_path.exists():
        print(f"Error: File not found: {input_path}")
        return 1

    df = load_data(str(input_path))

    if df.empty:
        print("Error: No data loaded from file")
        return 1

    # Apply date filters
    if args.start:
        start_dt = pd.to_datetime(args.start)
        df = df[df["timestamp"] >= start_dt]

    if args.end:
        end_dt = pd.to_datetime(args.end)
        df = df[df["timestamp"] <= end_dt]

    if df.empty:
        print("Error: No data in specified date range")
        return 1

    # Parse user-specified indicators
    user_indicators = None
    if args.indicators:
        user_indicators = [i.strip() for i in args.indicators.split(",")]

    # Detect indicators
    indicators = detect_indicators(df, user_indicators)
    overlays = indicators["overlay"]
    oscillators = indicators["oscillator"]
    candles = indicators["candles"]

    # Get symbol for title
    symbol = df["symbol"].iloc[0] if "symbol" in df.columns else "Unknown"

    # Create chart
    fig = create_chart(
        df,
        overlays,
        oscillators,
        candles=candles,
        title=f"{symbol} Market Data",
        window=args.window,
        rangeslider=args.rangeslider,
    )

    # Save HTML in same directory as input file with full viewport styling
    output_path = input_path.with_suffix(".html")
    
    # Custom HTML with full viewport CSS
    html_string = fig.to_html(
        include_plotlyjs=True,
        full_html=False,
        config={"scrollZoom": True, "displayModeBar": True, "responsive": True},
    )
    
    full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{symbol} Market Data</title>
    <style>
        body {{
            margin: 0;
            padding: 0;
            overflow: hidden;
            background-color: #1e1e1e;
        }}
        .plotly-graph-div {{
            width: 100vw !important;
            height: 100vh !important;
        }}
    </style>
</head>
<body>
    {html_string}
    <script>
        // Ensure chart resizes with window
        window.addEventListener('resize', function() {{
            Plotly.Plots.resize(document.querySelector('.plotly-graph-div'));
        }});
    </script>
</body>
</html>"""
    
    with open(str(output_path), 'w') as f:
        f.write(full_html)

    # Print summary
    date_range_start = df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S")
    date_range_end = df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S")

    print(f"Chart saved to: {output_path}")
    print(f"Date range: {date_range_start} to {date_range_end}")
    print(f"Candle count: {len(df)}")
    print(f"Overlay indicators: {overlays if overlays else 'None'}")
    print(f"Oscillator indicators: {oscillators if oscillators else 'None'}")
    print(f"Candle patterns: {candles if candles else 'None'}")

    return 0


if __name__ == "__main__":
    exit(main())