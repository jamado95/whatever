#!/usr/bin/env python3
"""
CSV Data Quality Script

Checks a Binance kline CSV for completeness (missing column values per row)
and continuity (missing candles between the first and last open_time), and
prints a report to the terminal.
"""

import argparse
import csv
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# Binance kline CSV layout (12 columns, no header)
COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",
]

# Timestamps at or above this are microseconds (Binance spot archives from 2025-01)
MICROS_THRESHOLD = 10**14

UNIT_MS = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}

# Interval token as used by Binance, e.g. 5m, 1h, 1d, 1M (calendar month)
INTERVAL_TOKEN = re.compile(r"\d+[smhdwM]")
INTERVAL_IN_NAME = re.compile(r"-(\d+[smhdwM])-")

MAX_GAPS_SHOWN = 10


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Report completeness and continuity of a Binance kline CSV"
    )
    parser.add_argument("input_file", help="Path to kline CSV file")
    parser.add_argument(
        "--interval",
        help="Candle interval (e.g. 1m, 5m, 1h, 1d); defaults to the filename token, then inferred",
        default=None,
    )
    return parser.parse_args()


def interval_ms(token: str) -> int | None:
    """Convert an interval token to ms. Returns None for calendar months (1M) or zero."""
    m = re.fullmatch(r"(\d+)([smhdw])", token)
    if not m or int(m.group(1)) == 0:
        return None
    return int(m.group(1)) * UNIT_MS[m.group(2)]


def format_duration(ms: int) -> str:
    """Format a duration as e.g. '365d 23h', omitting zero parts."""
    secs = ms // 1000
    parts = []
    for unit, size in (("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)):
        n, secs = divmod(secs, size)
        if n:
            parts.append(f"{n}{unit}")
    return " ".join(parts) or "0s"


def format_ts(ms: int) -> str:
    """Format a ms epoch timestamp as UTC; falls back to the raw value if out of range."""
    try:
        return datetime.fromtimestamp(ms // 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (OverflowError, OSError, ValueError):
        return str(ms)


def parse_number(cell: str) -> float | None:
    """Parse a numeric cell; empty, non-numeric and non-finite values are None."""
    try:
        value = float(cell)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def normalise_ts(value: int) -> tuple[int, str]:
    """Return (ms timestamp, source unit), scaling microsecond values down to ms."""
    if value >= MICROS_THRESHOLD:
        return value // 1000, "µs"
    return value, "ms"


def detect_interval(path: Path, timestamps: list[int], override: str | None) -> tuple[str, int | None, str]:
    """
    Resolve the candle interval: the --interval override, then the filename
    token ({SYMBOL}-{INTERVAL}-{YYYY-MM}.csv), then the most common positive
    delta between sorted unique timestamps.

    Returns (label, step in ms or None when unsupported, source).
    """
    if override:
        return override, interval_ms(override), "--interval"

    m = INTERVAL_IN_NAME.search(path.name)
    if m:
        return m.group(1), interval_ms(m.group(1)), "filename"

    unique = sorted(set(timestamps))
    deltas = Counter(b - a for a, b in zip(unique, unique[1:]))
    if deltas:
        step = deltas.most_common(1)[0][0]
        return format_duration(step), step, "inferred"

    return "unknown", None, "none"


def scan_file(path: Path) -> dict:
    """
    Read the CSV in one pass, counting missing cells per column and collecting
    normalised open_times in file order. A cell is missing if absent (short
    row), empty, non-numeric or non-finite. Blank lines are ignored, and a
    leading row with no numeric cells is treated as a header and skipped.
    """
    missing = [0] * len(COLUMNS)
    rows = rows_missing = malformed = 0
    header = None
    timestamps = []
    units = set()

    with open(path, newline="") as f:
        for row in csv.reader(f):
            if not any(cell.strip() for cell in row):
                continue

            values = [parse_number(row[i]) if i < len(row) else None for i in range(len(COLUMNS))]
            if rows == 0 and header is None and all(v is None for v in values):
                header = row
                continue

            rows += 1
            if len(row) != len(COLUMNS):
                malformed += 1

            row_missing = False
            for i, value in enumerate(values):
                if value is None:
                    missing[i] += 1
                    row_missing = True
            rows_missing += row_missing

            if values[0] is not None:
                ts, unit = normalise_ts(int(values[0]))
                timestamps.append(ts)
                units.add(unit)

    return {
        "rows": rows,
        "header": header,
        "missing": missing,
        "rows_missing": rows_missing,
        "malformed": malformed,
        "timestamps": timestamps,
        "units": units,
    }


def check_continuity(timestamps: list[int], step: int) -> dict:
    """
    Measure continuity of open_times over [first, last] at a fixed step.

    The score is present / expected, where present counts unique timestamps on
    the step grid anchored at the first candle. Duplicates, out-of-order rows
    and misaligned timestamps are reported but do not affect the score.
    Gaps are (first missing, last missing, missing candles) tuples.
    """
    unique = sorted(set(timestamps))
    first, last = unique[0], unique[-1]
    aligned = [t for t in unique if (t - first) % step == 0]
    expected = (last - first) // step + 1

    # Sentinel one step past the last grid slot so trailing missing slots form a gap
    end_slot = first + (expected - 1) * step
    bounds = aligned + ([end_slot + step] if aligned[-1] != end_slot else [])

    gaps = [
        (a + step, b - step, (b - a) // step - 1)
        for a, b in zip(bounds, bounds[1:])
        if b - a > step
    ]

    return {
        "expected": expected,
        "present": len(aligned),
        "missing": expected - len(aligned),
        "gaps": gaps,
        "duplicates": len(timestamps) - len(unique),
        "out_of_order": sum(1 for a, b in zip(timestamps, timestamps[1:]) if b < a),
        "misaligned": len(unique) - len(aligned),
        "score": len(aligned) / expected * 100,
    }


def pct(n: int, total: int) -> str:
    """Format n / total as a percentage."""
    return f"{n / total * 100:.2f}%" if total else "N/A"


def candles(n: int) -> str:
    """Format a candle count, e.g. '1 candle', '3 candles'."""
    return f"{n} candle{'s' if n != 1 else ''}"


def ratio(n: int, total: int) -> str:
    """Format 'n / total   pct'."""
    return f"{f'{n} / {total}':<20}{pct(n, total)}"


def print_report(path: Path, scan: dict, interval: tuple[str, int | None, str], continuity: dict | None):
    """Print the completeness and continuity report."""
    label, step, source = interval
    timestamps = scan["timestamps"]
    rows = scan["rows"]
    first = min(timestamps) if timestamps else None
    last = max(timestamps) if timestamps else None

    units = sorted(scan["units"])
    unit_text = " + ".join(units) + (" (mixed)" if len(units) > 1 else "") if units else "N/A"

    print(f"CSV data quality: {path}")
    print(f"  {'Interval':<11}{label} ({source})")
    print(f"  {'Start':<11}{format_ts(first) if first is not None else 'N/A'}")
    print(f"  {'End':<11}{format_ts(last) if last is not None else 'N/A'}")
    print(f"  {'Span':<11}{format_duration(last - first) if first is not None else 'N/A'}")
    print(
        f"  {'Rows':<11}{rows}   header: {'yes' if scan['header'] else 'none'}"
        f"   timestamp unit: {unit_text}"
    )

    cells = rows * len(COLUMNS)
    print()
    print("Completeness")
    print(f"  {'Rows with missing data':<28}{ratio(scan['rows_missing'], rows)}")
    print(f"  {'Missing cells':<28}{ratio(sum(scan['missing']), cells)}")
    print(f"  {f'Malformed rows (≠{len(COLUMNS)} cols)':<28}{ratio(scan['malformed'], rows)}")
    print()
    print(f"  {'column':<18}{'missing':>10}{'%':>10}")
    for name, n in zip(COLUMNS, scan["missing"]):
        print(f"  {name:<18}{n:>10}{pct(n, rows):>10}")

    print()
    print("Continuity")
    if step is None:
        print(f"  skipped: unsupported interval '{label}' (fixed-length intervals only)")
        print(f"  {'Continuity score':<19}N/A")
        return
    if continuity is None:
        print("  skipped: fewer than 2 parseable open_time values")
        print(f"  {'Continuity score':<19}N/A")
        return

    c = continuity
    largest = max(c["gaps"], key=lambda g: g[2]) if c["gaps"] else None
    print(f"  {'Expected candles':<19}{c['expected']}")
    print(f"  {'Present':<19}{c['present']}")
    print(f"  {'Missing':<19}{c['missing']:<11}{pct(c['missing'], c['expected'])}")
    print(
        f"  {'Gaps':<19}{len(c['gaps']):<11}largest: "
        f"{candles(largest[2]) if largest else '—'}"
    )
    print(f"  {'Duplicates':<19}{c['duplicates']}")
    print(f"  {'Out of order':<19}{c['out_of_order']}")
    print(f"  {'Misaligned':<19}{c['misaligned']}")
    print(f"  {'Continuity score':<19}{c['score']:.2f}%")

    if c["gaps"]:
        top = sorted(c["gaps"], key=lambda g: (-g[2], g[0]))[:MAX_GAPS_SHOWN]
        print()
        print(f"  Largest gaps (top {len(top)} of {len(c['gaps'])})")
        for start, end, n in top:
            print(f"  {format_ts(start)} → {format_ts(end)}   {candles(n)}")


def main():
    args = parse_args()

    path = Path(args.input_file)
    if not path.is_file():
        print(f"Error: File not found: {path}")
        return 1

    if args.interval and not INTERVAL_TOKEN.fullmatch(args.interval):
        print(f"Error: Invalid interval '{args.interval}', want e.g. 1m, 5m, 1h, 1d")
        return 1

    try:
        scan = scan_file(path)
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        print(f"Error: Failed to read {path}: {e}")
        return 1

    if scan["rows"] == 0:
        print(f"Error: No data rows in {path}")
        return 1

    interval = detect_interval(path, scan["timestamps"], args.interval)
    step = interval[1]

    continuity = None
    if step is not None and len(scan["timestamps"]) >= 2:
        continuity = check_continuity(scan["timestamps"], step)

    print_report(path, scan, interval, continuity)
    return 0


if __name__ == "__main__":
    exit(main())
