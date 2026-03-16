-- ClickHouse schema for computed factors from FactorEngine.
--
-- Table: factors
-- Engine: ReplacingMergeTree (upserts on replays / recomputation)
-- Partition: by trade_date for fast date-range queries and easy TTL
-- Order: (ticker, timestamp_ns, factor_name) for efficient filtered lookups
--
-- Run:
--   clickhouse-client --password <pw> < sql/clickhouse_factors.sql

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.factors
(
    ticker        LowCardinality(String),    -- e.g. "AAPL"
    timestamp_ns  Int64,                     -- epoch nanoseconds, factor computation time
    session       String,                    -- session_id for this run
    factor_name   LowCardinality(String),    -- e.g. "momentum", "volatility", "rsi"
    factor_value  Float64,                   -- computed factor value

    trade_date    Date DEFAULT toDate(fromUnixTimestamp64Nano(timestamp_ns)),  -- partition key
    inserted_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ticker, timestamp_ns, factor_name)
TTL trade_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Optional: Create materialized view for latest factors per ticker
-- Note: Uses AggregatingMergeTree for efficient incremental updates
CREATE MATERIALIZED VIEW IF NOT EXISTS jerry_trader.factors_latest
ENGINE = AggregatingMergeTree()
ORDER BY (ticker, factor_name)
AS SELECT
    ticker,
    factor_name,
    argMaxState(factor_value, timestamp_ns) as factor_value_state,
    maxState(timestamp_ns) as timestamp_ns_state,
    argMaxState(session, timestamp_ns) as session_state
FROM jerry_trader.factors
GROUP BY ticker, factor_name;

-- Query the materialized view with:
-- SELECT ticker, factor_name,
--        argMaxMerge(factor_value_state) as factor_value,
--        maxMerge(timestamp_ns_state) as timestamp_ns,
--        argMaxMerge(session_state) as session
-- FROM jerry_trader.factors_latest
-- GROUP BY ticker, factor_name;
