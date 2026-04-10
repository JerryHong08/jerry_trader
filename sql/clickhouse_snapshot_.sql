-- =============================================================================
-- Table 1: market_snapshot_ranked
-- Purpose: Minimal format for Pre-filter true entry detection
-- =============================================================================

CREATE TABLE IF NOT EXISTS market_snapshot_ranked
(
    symbol String,
    event_time_ms Int64,
    changePercent Float64,
    rank Int32,

    -- Metadata
    date String,
    mode String,          -- 'live' or 'replay'
    session_id String,

    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY (date, mode)
ORDER BY (event_time_ms, symbol, session_id);


-- =============================================================================
-- Table 2: market_snapshot_collector
-- Purpose: Full format matching Polygon Collector API output
-- Used by: Replayer for visualization replay
-- =============================================================================

CREATE TABLE IF NOT EXISTS market_snapshot_collector
(
    -- Identity
    ticker String,
    timestamp Int64,        -- epoch milliseconds (named like Collector API)

    -- Price data
    price Float64,
    volume Float64,
    prev_close Float64,
    prev_volume Float64,
    vwap Float64,

    -- Quote data (from quotes parquet)
    bid Float64,
    ask Float64,
    bid_size Float64,
    ask_size Float64,

    -- Computed
    changePercent Float64,
    change Float64,
    rank Int32,

    -- Metadata
    date String,
    mode String,
    session_id String,

    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY (date, mode)
ORDER BY (timestamp, ticker, session_id);
