# Live TRF Filtering Design

## Problem

In live trading, how should we handle FINRA TRF (exchange=4) delayed trades?

## Key Finding: Polygon WebSocket provides TRF timestamp

Polygon WebSocket trade message includes:
- `t` — SIP timestamp (Unix MS)
- `trft` — TRF timestamp (Unix MS)
- `x` — Exchange ID (4 = TRF)

This means we **CAN** filter delayed TRF trades in live mode with the same logic as backtest:

```python
if exchange == 4:  # TRF trade
    delay_ms = sip_timestamp - trf_timestamp
    if delay_ms > 1000:  # >1s delay
        return  # filter delayed TRF
```

**Result: Live and Backtest now have consistent TRF handling!**

## Complete TRF Filtering Points

### Can Filter (has trft/participant_timestamp)

| Component | Data Source | Fields | Implementation |
|-----------|-------------|--------|----------------|
| `UnifiedTickManager.normalize_data()` | Polygon WS | `t`, `trft`, `x` | Filter `x==4 && (t-trft)>1000ms` |
| `BarsBuilder.trades_backfill()` | Parquet | `sip_timestamp`, `participant_timestamp` | Same filter |
| `FactorEngine._process_bootstrap_trades()` | Parquet | Same | Same filter |
| `HistoricalLoader` | CH | Same | Same filter |
| `DataLoader` (backtest) | CH | Same | **Already implemented** |

### Cannot Filter (no trft/participant_timestamp)

| Component | Data Source | Solution |
|-----------|-------------|----------|
| `Collector.snapshot` | REST API | Use `weighted_mid` instead of `lastTrade.p` |

## Snapshot Solution

REST Snapshot doesn't provide TRF timestamp for `lastTrade`, but has quotes:

```json
"lastQuote": {
  "P": 20.6,    // ask price
  "S": 22,      // ask size
  "p": 20.5,    // bid price
  "s": 13       // bid size
},
"lastTrade": {
  "p": 20.506,  // may be delayed TRF
  "x": 4,       // TRF indicator
  "t": ...
}
```

**Solution**: Use `weighted_mid` (bid/ask weighted) for price, `lastTrade.s` for volume.

Already implemented in `processor.py`:
```python
def compute_weighted_mid_price(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        ((pl.col("bid") * pl.col("ask_size") + pl.col("ask") * pl.col("bid_size"))
         / (pl.col("bid_size") + pl.col("ask_size"))
        ).alias("weighted_mid")
    )
```

## Implementation Plan

```
┌─────────────────────────────────────────────────────────────┐
│                    TRF Filtering Architecture               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Live WebSocket ──► UnifiedTickManager.normalize_data()    │
│                           │                                 │
│                           ▼ Filter x==4 && (t-trft)>1000ms  │
│                           │                                 │
│           ┌───────────────┼───────────────┐                │
│           ▼               ▼               ▼                │
│     BarsBuilder      FactorEngine      ChartBFF            │
│       ._on_tick        ._on_tick        (all receive       │
│                          filtered trades)                  │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Replay/Bootstrap ──► trades_backfill / DataLoader         │
│                              │                              │
│                              ▼ Filter exch==4 && delay>1s   │
│                              │                              │
│                    Bootstrap bars/factors                   │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Snapshot (REST) ──► Collector.py                          │
│                              │                              │
│                              ▼ Cannot filter                │
│                              │                              │
│                    processor.py:                            │
│                    price = weighted_mid (bid/ask)           │
│                    volume = lastTrade.s                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Files to Modify

1. **`unified_tick_manager.py`**: Add TRF delay filter in `normalize_data()`
2. **`bars_builder_service.py`**: Add filter in `trades_backfill()` (Parquet path)
3. **`processor.py`**: Ensure `weighted_mid` is used for snapshot price (already done)

## References

- `roadmap/trf-trade-filtering.md` — TRF filtering design decision
- `notebooks/03-delayed-trades-investigation.ipynb` — Data analysis
- `memory/trf_delayed_trades.md` — TRF reference
