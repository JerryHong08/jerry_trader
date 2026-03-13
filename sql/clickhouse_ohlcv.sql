-- ClickHouse schema for OHLCV bars built by BarsBuilderService.
--
-- Table: ohlcv_bars
-- Engine: ReplacingMergeTree (upserts on late arrivals / replays)
-- Partition: by trade_date for fast date-range queries and easy TTL
-- Order: (ticker, timeframe, bar_start) for efficient filtered lookups
--
-- Run:
--   clickhouse-client --password <pw> < sql/clickhouse_ohlcv.sql

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.ohlcv_bars
(
    ticker       LowCardinality(String),    -- e.g. "AAPL"
    timeframe    LowCardinality(String),    -- e.g. "1m", "5m", "1d"
    bar_start    Int64,                     -- epoch ms, bar open time
    bar_end      Int64,                     -- epoch ms, bar close time

    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    trade_count  UInt64,
    vwap         Float64,

    session      LowCardinality(String),    -- "premarket" | "regular" | "afterhours"
    trade_date   Date DEFAULT toDate(fromUnixTimestamp64Milli(bar_start)),  -- partition key
    inserted_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ticker, timeframe, bar_start)
TTL trade_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;
