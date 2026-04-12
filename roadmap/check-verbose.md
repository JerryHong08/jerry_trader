# check --verbose

**Status: COMPLETED (2026-04-12)**

## Problem

`check` command only shows READY/MISSING status without details:
- Which tickers are missing?
- What time ranges have gaps?
- How many rows per ticker?

## Current Output

```
Trades: raw=OK → parquet=OK
Quotes: raw=OK → parquet=OK
ClickHouse Snapshot: OK
  Rows: 641,839
  Tickers: 7226
```

## Solution Implemented

Added `--verbose` flag for detailed inspection:

```bash
poetry run python -m jerry_trader.services.backtest.data.cli check --date 2026-03-13 --verbose
```

Output includes:
- Top 10 tickers by row count (with window coverage %)
- Bottom 10 tickers (potential data gaps)
- Tickers with incomplete coverage (< 50% windows)

## Changes Made

1. Added `verbose_check()` function in checker.py
2. Added `--verbose` flag to check CLI subparser
3. Updated SKILL.md documentation
