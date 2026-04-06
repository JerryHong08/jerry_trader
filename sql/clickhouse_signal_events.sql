-- ClickHouse schema for signal events from SignalEngine.
--
-- Table: signal_events
-- Engine: ReplacingMergeTree (dedup on replay)
-- Partition: by trade_date for fast date-range queries and easy TTL
-- Order: (rule_id, ticker, trigger_time) for per-rule per-ticker analysis
--
-- Returns (return_1m, return_5m, return_15m) are computed offline via
-- SQL JOIN with ohlcv bars and updated back into this table.
--
-- Run:
--   clickhouse-client --password <pw> < sql/clickhouse_signal_events.sql

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.signal_events
(
    id            UUID,
    rule_id       LowCardinality(String),    -- e.g. "premarket_momentum_v1"
    rule_version  UInt16,                    -- rule version at trigger time
    ticker        LowCardinality(String),    -- e.g. "AAPL"
    timeframe     LowCardinality(String),    -- e.g. "trade", "1m"
    trigger_time  Int64,                     -- epoch nanoseconds
    trigger_price Nullable(Float64),         -- price at trigger (from factors or None)
    factors       String,                    -- JSON {factor_name: value}
    session       String,                    -- session_id for this run

    -- Returns (filled offline via SQL JOIN with ohlcv)
    return_1m     Nullable(Float64),
    return_5m     Nullable(Float64),
    return_15m    Nullable(Float64),

    trade_date    Date DEFAULT toDate(fromUnixTimestamp64Nano(trigger_time)),
    inserted_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (rule_id, ticker, trigger_time)
TTL trade_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;
