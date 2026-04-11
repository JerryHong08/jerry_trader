# Task 3.24: Forward-fill null bars from first trade in BarBuilder

## Problem
BarBuilder only creates bars when trades arrive. For inactive stocks, this creates gaps:
- BIAF 10s bars: 8:29:30 → (gap) → 8:34:00 → (gap) → 9:17:50
- No bars between active periods, even though the stock has a known price
- Breaks technical indicators (EMA, SMA) that need continuous data
- Charts show gaps instead of flat candles with V=0

## Solution
Implement industry-standard forward-fill in Rust BarBuilder:
- After the FIRST trade of the day, forward-fill all subsequent empty bar periods
- Before the first trade: no fill (no reference price)
- Filled bars: OHLC = previous bar's close, volume=0, trade_count=0
- Implement in `advance()` — when closing a bar, emit continuation bars for gaps

## Implementation

### Location
`rust/src/bars.rs` — `BarBuilder::advance()` method

### Logic
When `advance()` closes a bar at `bar_end`, and the next trade hasn't arrived yet:
1. Record the closed bar's close price as the continuation reference
2. On the next `ingest_trade()`, if the trade's `bar_start` is more than one
   bar-duration past the last closed bar, emit continuation bars for each
   missed period between them
3. Each continuation bar: OHLC = prev close, volume=0, trade_count=0

### Alternative: fill in `advance()` eagerly
Instead of waiting for the next trade, `advance()` could immediately emit
continuation bars up to `now_ms`. But this creates many empty bars during
quiet periods. Better to fill lazily — only when a trade actually arrives.

### Chosen approach: lazy fill in `ingest_trade()`
When a trade arrives and its bar_start > last_closed_bar_end + duration:
- Emit continuation bars for [last_closed_bar_end, trade_bar_start) at each
  bar boundary
- Then process the trade normally

## Example
```
8:29:30  O=5.00 H=5.02 L=4.99 C=5.01 V=100  (real trade)
8:29:40  O=5.01 H=5.01 L=5.01 C=5.01 V=0    (forward-filled)
8:29:50  O=5.01 H=5.01 L=5.01 C=5.01 V=0    (forward-filled)
8:30:00  O=5.01 H=5.01 L=5.01 C=5.01 V=0    (forward-filled)
8:30:10  O=5.01 H=5.03 L=5.00 C=5.03 V=200  (real trade resumes)
```

## Related
- Affects all intraday timeframes (10s through 4h)
- Required for accurate EMA/SMA calculations
- Discovered during BIAF replay testing 2026-04-12
