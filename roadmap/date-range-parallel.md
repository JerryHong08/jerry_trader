# Date Range Parallel Execution

**Status: COMPLETED (2026-04-12)**

## Problem

Date range backtest runs serially. 5 dates = ~50 seconds total.

Each date is independent — natural parallelism opportunity.

## Current Implementation

```python
for date_str in dates:
    result = run_single_date(date_str, args, ch_client=ch_client)
```

Serial loop in `cli.py`.

## Solution Implemented

Added `--parallel` and `--workers` flags using ProcessPoolExecutor:

```bash
# Run dates in parallel (4 workers default)
poetry run python -m jerry_trader.services.backtest.cli \
    --date-range 2026-03-10 2026-03-15 --parallel

# Custom worker count
poetry run python -m jerry_trader.services.backtest.cli \
    --date-range 2026-03-01 2026-03-31 --parallel --workers 8
```

**Implementation details:**
- Worker function `_run_single_date_worker` creates its own CH client
- Args converted to dict (can't pickle argparse.Namespace)
- Results collected via `as_completed()` and sorted by date for summary

## Changes Made

1. Added `_run_single_date_worker()` function for parallel execution
2. Added `--parallel` and `--workers` CLI flags
3. Modified `main()` to use ProcessPoolExecutor when `--parallel` is set
4. Updated SKILL.md documentation
