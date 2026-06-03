-- ClickHouse schema for backtest bars (aggregated from trades during prepare).
--
-- Table: backtest_bars
-- Engine: MergeTree (immutable historical data)
-- Partition: by date for fast date-range queries
-- Order: (ticker, bar_start) for efficient filtered lookups
--
-- Purpose: Separated from ohlcv_bars (replay/live) for backtest visualization.
-- Built during prepare command, not by BarBuilder service.
--
-- Run:
--   clickhouse-client --password <pw> < sql/clickhouse_backtest_bars.sql

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.backtest_bars
(
    ticker       LowCardinality(String),    -- e.g. "AAPL"
    timeframe    LowCardinality(String),    -- e.g. "1m" (fixed for now)
    date         Date,                       -- partition key

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

    created_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (ticker, date, bar_start)
TTL date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;
