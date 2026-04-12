# PreFilter use subscription logic

**Status: COMPLETED (2026-04-12)**

## Problem

PreFilter was using "事后重算" snapshot rank from market_snapshot:
- Query: `WHERE rank <= 20` on market_snapshot table
- Returned 101 candidates instead of 117 subscription set

Discrepancy cause:
- **Subscription rank**: computed on common stocks pool BEFORE subscription (rank=11 for AGCC)
- **Snapshot rank**: recomputed on subscribed tickers AFTER subscription (rank=21 for AGCC)

AGCC and 16 other tickers were subscribed via subscription logic (top 20 in common stocks pool) but had snapshot rank > 20.

## Solution Implemented

Rewrote PreFilter to simulate live processor subscription behavior:

1. Read from collector (all tickers, raw data)
2. Filter to common stocks
3. Per-window iteration with prev_df filling
4. Compute ordinal rank based on common stocks pool
5. Track subscription set (rank <= TOP_N with positive changePercent)

**Performance optimizations:**
- Partition by window upfront, release full DataFrame after
- Each window keeps only necessary columns
- Progress bar via tqdm

## Result

- Candidates: 117 (matches subscription set)
- AGCC correctly included (subscription_rank=11)
- Processing time: ~25 seconds for 3960 windows

## Files Modified

- `python/src/jerry_trader/services/backtest/pre_filter.py`
