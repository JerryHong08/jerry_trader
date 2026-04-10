# Snapshot Builder Enhanced Design

## Goal

Build pre-processed market snapshots that are **identical to live Processor output**,
enabling:
1. Fast replay without re-running Processor logic
2. Time-point seek (frontend progress bar)
3. Consistent data format between live and replay modes

## Current vs Enhanced

| Aspect | Current snapshot_builder | Enhanced snapshot_builder |
|--------|-------------------------|--------------------------|
| Output | Raw aggregated trades | Processor-ready snapshots |
| Filtering | None | Common stocks filter |
| Missing data | Not handled | Forward fill from previous window |
| Ranking | Per-window rank | Same as Processor |
| Subscription | N/A | Pre-compute top-N tracking |

## Architecture

```
Trades Parquet (150M+ rows)
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│              Enhanced Snapshot Builder                         │
│                                                                │
│  Stage 1: Aggregate trades → OHLCV per 5s window              │
│           (streaming, bounded memory)                          │
│                                                                │
│  Stage 2: Filter common stocks (one-time lookup)              │
│           - Load from polygon_tickers table or static file     │
│           - Filter out ETFs, ADRs, preferred stocks            │
│                                                                │
│  Stage 3: Forward fill missing tickers                         │
│           - Track "last known state" per ticker                │
│           - For each window, fill tickers not in current slice │
│                                                                │
│  Stage 4: Compute ranks and derived metrics                    │
│           - Rank by changePercent                              │
│           - Compute relativeVolumeDaily/5min                   │
│                                                                │
│  Stage 5: Write to ClickHouse                                  │
│           - Partition by (date, mode)                          │
│           - Order by (event_time_ms, symbol)                   │
└──────────────────────────────────────────────────────────────┘
       │
       ▼
ClickHouse market_snapshot_replay (NEW TABLE)
       │
       ▼
FastReplayer (seek + stream)
       │
       ▼
Frontend (progress bar + play/pause)
```

## New ClickHouse Table: market_snapshot_replay

Separate from live market_snapshot to avoid confusion:

```sql
CREATE TABLE market_snapshot_replay
(
    -- Identity
    symbol String,
    date String,              -- '2026-03-06'
    mode String,              -- 'replay'

    -- Time
    event_time_ms Int64,
    event_time DateTime64(3, 'UTC'),

    -- Price data (forward-filled)
    price Float64,
    changePercent Float64,
    volume Float64,
    prev_close Float64,
    prev_volume Float64,
    vwap Float64,

    -- Quote data (forward-filled)
    bid Float64,
    ask Float64,
    bid_size Float64,
    ask_size Float64,

    -- Computed (same as Processor)
    rank Int32,
    competition_rank Int32,
    change Float64,
    relativeVolumeDaily Float64,
    relativeVolume5min Float64,

    -- Forward fill tracking
    is_filled Boolean DEFAULT false,  -- true if forward-filled

    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY (date, mode)
ORDER BY (event_time_ms, symbol);
```

## Frontend Progress Bar API

```python
# FastReplayer methods
async def get_time_range(self) -> Tuple[int, int]:
    """Return (start_ms, end_ms) for progress bar."""

async def seek_to_time(self, target_ms: int) -> Dict:
    """Get snapshot at specific time point for immediate display."""

async def play_from(self, start_ms: int, speed: float = 1.0):
    """Start streaming from specific time point."""
```

## Frontend Component

```tsx
// TimeSlider.tsx
interface TimeSliderProps {
  startTime: number;  // epoch ms
  endTime: number;
  currentTime: number;
  onSeek: (time: number) => void;
  isPlaying: boolean;
  onPlayPause: () => void;
  speed: number;
  onSpeedChange: (speed: number) => void;
}

// Usage:
// [04:00] ━━━━━━━━━━●━━━━━━━━━━ [09:30]
//           ▶️  Speed: 10x
```

## Implementation Steps

1. **Create enhanced snapshot_builder_v2.py**
   - Reuse existing aggregation logic
   - Add common stocks filter (load from static file or DB)
   - Add forward fill logic with bounded memory

2. **Create new CH table market_snapshot_replay**
   - Separate from live market_snapshot
   - Optimized for time-range queries

3. **Update FastReplayer**
   - Add seek_to_time() method
   - Add get_time_range() for progress bar
   - Add WebSocket direct push option

4. **Add Frontend TimeSlider component**
   - Progress bar with play/pause
   - Speed selector
   - Time display

## Memory Considerations

Forward fill with bounded memory:
- Keep only "last known state" per ticker (~100 bytes × 8000 tickers = 800KB)
- Process windows sequentially
- No need to load entire dataset

## Common Stocks Filter

Options:
1. **Static file**: `data/common_stocks_{date}.parquet` (pre-generated)
2. **Database query**: `SELECT ticker FROM polygon_tickers WHERE type='CS'`
3. **Inline filter**: Hardcoded list of ETF prefixes (SPY, QQQ, etc.)

Recommend: Option 1 for simplicity, generated by separate script.
