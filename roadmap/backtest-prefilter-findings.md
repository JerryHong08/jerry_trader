# Pre-filter Design & Test Findings

**Created:** 2026-04-06
**Related:** Task 9.2 (Candidate pre-filter)
**Test date:** 2026-03-13 (earliest available data on oldman machine)

## market_snapshot Table Schema

```sql
-- Key columns for pre-filter
symbol, date, mode, session_id,
event_time_ms (Int64), event_time (DateTime64),
price, changePercent, volume, prev_close, prev_volume, vwap,
bid, ask, bid_size, ask_size,
rank, competition_rank, change,
relativeVolumeDaily, relativeVolume5min
```

- **Partition:** `(date, mode)`
- **Order:** `(symbol, event_time_ms, session_id)`
- **Engine:** `ReplacingMergeTree(inserted_at)`

## Session ID & Mode

Session ID format: `{YYYYMMDD}_{mode}[_{suffix}]`

| Mode | Description | Example |
|------|-------------|---------|
| `live` | Real-time/live session | `20260313_live` |
| `replay` | Replay session | `20260313_replay` |
| `replay_{id}` | Replay with suffix | `20260313_replay_v2` |

**Important:** `mode` field is `live`/`replay`, NOT `premarket`/`regular`.
The session covers the full pre-market + regular session; there is no intra-day mode split.

`date` format in ClickHouse: `YYYY-MM-DD` (e.g. `'2026-03-13'`)

Source: `platform/config/session.py`

## Test Query & Results (2026-03-13)

```sql
SELECT symbol,
       argMin(event_time_ms, event_time_ms) as first_entry_ms,
       argMin(changePercent, event_time_ms) as gain_at_entry,
       any(prev_close) as prev_close,
       max(changePercent) as max_gain
FROM jerry_trader.market_snapshot FINAL
WHERE date = '2026-03-13'
  AND mode = 'live'
  AND rank <= 20
  AND changePercent >= 2.0
GROUP BY symbol
ORDER BY first_entry_ms;
```

**Result: 72 candidates** (214K rows scanned, 26ms)

### Two Candidate Categories

| Type | Count | first_entry_ms | Description |
|------|-------|----------------|-------------|
| **Open-in-top20** | ~20 | All `1773398366226` (same timestamp) | Already in top 20 at first snapshot |
| **New entry** | ~52 | Gradually increasing | Entered top 20 during session |

**Key insight:** Only "new entry" candidates represent the actual signal — stocks that *entered* top 20 during the session. "Open-in-top20" stocks were already there and don't represent a sudden momentum event.

### Notable Observations

- Top gainers at entry: ISPC (65%), CXAI (37%), ELPW (30%)
- Max gain runners: ISPC (112.5%), BIAF (84%), WTO (68.5%)
- Price range: $0.12 (PPCB) to $44.31 (VEON) — wide spread, price filter useful
- Some low-priced micro-caps (PPCB $0.12, EHGO $0.20) — likely candidates for exclusion

## Pre-filter Design Decisions

### 1. True Entry Detection (Confirmed: new entry only)

**Decision:** Only select stocks that *newly entered* top 20 during the session.
Exclude stocks already in top 20 at the first snapshot.

**Implementation approach:**
- Find the first snapshot timestamp per date
- For each symbol with `rank <= 20`, compare `first_entry_ms` against session start
- If `first_entry_ms == session_first_ms` → open-in-top20, exclude
- If `first_entry_ms > session_first_ms` → new entry, include

Alternative SQL approach (more precise):
```sql
-- Find symbols that appeared in top 20 for the first time AFTER the initial snapshot
WITH initial_top20 AS (
    SELECT DISTINCT symbol
    FROM jerry_trader.market_snapshot FINAL
    WHERE date = '2026-03-13' AND mode = 'live'
      AND event_time_ms = (
          SELECT min(event_time_ms)
          FROM jerry_trader.market_snapshot FINAL
          WHERE date = '2026-03-13' AND mode = 'live' AND rank <= 20
      )
      AND rank <= 20
)
SELECT symbol,
       min(event_time_ms) as first_entry_ms,
       argMin(changePercent, event_time_ms) as gain_at_entry,
       any(prev_close) as prev_close,
       max(changePercent) as max_gain
FROM jerry_trader.market_snapshot FINAL
WHERE date = '2026-03-13'
  AND mode = 'live'
  AND rank <= 20
  AND changePercent >= 2.0
  AND symbol NOT IN (SELECT symbol FROM initial_top20)
GROUP BY symbol
ORDER BY first_entry_ms;
```

### 2. Multi-machine Architecture

- Pre-filter only needs a ClickHouse client, no local collector/snapshot dependencies
- CH connection config passed via `BacktestConfig` or environment
- Backtest machine can point to oldman's ClickHouse or any machine with data
- `get_common_stocks(backtest_date)` already supports date-based filtering for delisted stocks

### 3. Future Filter Additions (config-driven)

These can be layered on top of the base "new entry" filter via `PreFilterConfig`:

- `min_price` / `max_price` — exclude micro-caps
- `min_volume` — minimum volume threshold
- `min_relative_volume` — relativeVolumeDaily threshold
- `min_gain_pct` — changePercent at entry threshold (tested: 2.0%)
- `exclude_etf` — use `get_common_stocks()` filter
- `min_float` — future: minimum share float
- `max_gain_at_entry` — exclude stocks that already ran too much before entry

### 4. PreFilterConfig Draft

`PreFilterConfig` stays in `services/backtest/` (service configuration):

```python
@dataclass
class PreFilterConfig:
    top_n: int = 20                    # Top N rank threshold
    min_gain_pct: float = 2.0          # Minimum changePercent at entry
    new_entry_only: bool = True        # Only stocks that newly entered top N
    min_price: float = 0.5             # Minimum price at entry
    max_price: float = 500.0           # Maximum price at entry
    min_volume: float = 0.0            # Minimum volume at entry
    min_relative_volume: float = 0.0   # Minimum relativeVolumeDaily
    exclude_etf: bool = True           # Filter via get_common_stocks()
```

### 5. Candidate Output

`Candidate` goes in `domain/backtest/` (pure value object, no I/O):

```python
@dataclass(frozen=True)
class Candidate:
    symbol: str
    first_entry_ms: int           # First time rank <= top_n
    gain_at_entry: float          # changePercent at first_entry
    price_at_entry: float         # price at first_entry
    prev_close: float             # previous close
    volume_at_entry: float        # volume at first_entry
    relative_volume: float        # relativeVolumeDaily at first_entry
    max_gain: float               # max changePercent across all snapshots (for reference)
```

## Performance

- 214K rows, 26ms on oldman — no concern for batch backtest
- For multi-date range: ~200K rows per date, 10 dates = 2M rows, still fast
- FINAL dedup is necessary due to ReplacingMergeTree engine
