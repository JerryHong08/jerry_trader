# Merge build-snapshot + process-snapshot

**Status: COMPLETED (2026-04-12)**

## Problem

Two separate commands confused users:
- `build-snapshot` — builds collector + calls process-from-collector internally
- `process-snapshot` — standalone command that only processes collector → snapshot

User question: "When do I use which?"

## Current Behavior

```
build-snapshot:
  1. Read trades/quotes parquet
  2. Aggregate into windows → write market_snapshot_collector
  3. Call process_from_collector() → write market_snapshot

process-snapshot:
  1. Read market_snapshot_collector
  2. Apply subscription logic
  3. Write market_snapshot
```

## Solution Implemented

Removed `process-snapshot` subcommand. `build-snapshot` handles everything with `--reprocess` flag:

```bash
# Build both tables (default)
poetry run python -m jerry_trader.services.backtest.data.cli build-snapshot --date 2026-03-13

# Force rebuild both tables
poetry run python -m jerry_trader.services.backtest.data.cli build-snapshot --date 2026-03-13 --force

# Re-process only (if collector exists, skip stage 1-2)
poetry run python -m jerry_trader.services.backtest.data.cli build-snapshot --date 2026-03-13 --reprocess
```

## Changes Made

1. Removed `cmd_process_snapshot` function and CLI subparser
2. Added `--reprocess` flag to `build-snapshot` that skips Stage 1-2 and only runs `process_from_collector()`
3. Updated `cmd_build_snapshot` to handle `--reprocess` case
4. Updated SKILL.md documentation
