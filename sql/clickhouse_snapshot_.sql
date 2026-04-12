-- =============================================================================
-- market_snapshot_collector
-- Purpose: Raw snapshot data matching live MarketsnapshotCollector output
-- Used by: Replayer for visualization replay, process_from_collector for backtest
--
-- Schema aligned with live collector (collector.py Redis payload).
-- rank and change are NOT stored here — computed by processor (live) or
-- process_from_collector (backtest) and written to market_snapshot.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.market_snapshot_collector
(
    -- Identity
    ticker String,
    timestamp Int64,        -- epoch milliseconds (from Polygon `updated`)

    -- Price data (from live collector payload)
    price Float64,           -- lastTrade.p
    volume Float64,           -- min.av (cumulative)
    prev_close Float64,       -- prevDay.c
    prev_volume Float64,      -- prevDay.v
    vwap Float64,             -- min.vw

    -- Quote data (from live collector payload)
    bid Float64,              -- lastQuote.p
    ask Float64,              -- lastQuote.P
    bid_size Float64,         -- lastQuote.s
    ask_size Float64,         -- lastQuote.S

    -- Computed by live collector
    changePercent Float64,    -- todaysChangePerc

    -- Metadata
    date String,
    mode String,
    session_id String,

    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY (date, mode)
ORDER BY (timestamp, ticker, session_id);
