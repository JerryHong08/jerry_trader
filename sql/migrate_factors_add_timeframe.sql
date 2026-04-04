-- Migration: Add timeframe to ORDER BY for factors table
--
-- Problem: The original ORDER BY (ticker, timestamp_ns, factor_name) doesn't
-- include timeframe, causing factors from different timeframes (e.g., 10s vs 1m)
-- at the same timestamp to collide and overwrite each other.
--
-- Solution: Create new table with correct ORDER BY and migrate data.
--
-- Run:
--   clickhouse-client --password <pw> < sql/migrate_factors_add_timeframe.sql

-- Step 1: Create new table with correct ORDER BY
CREATE TABLE IF NOT EXISTS jerry_trader.factors_v2
(
    ticker        LowCardinality(String),
    timeframe     LowCardinality(String),
    timestamp_ns  Int64,
    session       String,
    factor_name   LowCardinality(String),
    factor_value  Float64,

    trade_date    Date DEFAULT toDate(fromUnixTimestamp64Nano(timestamp_ns)),
    inserted_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (ticker, timeframe, timestamp_ns, factor_name)
TTL trade_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Step 2: Migrate data from old table
INSERT INTO jerry_trader.factors_v2
SELECT ticker, timeframe, timestamp_ns, session, factor_name, factor_value, trade_date, inserted_at
FROM jerry_trader.factors;

-- Step 3: Rename tables (run manually after verifying data migration)
-- RENAME TABLE jerry_trader.factors TO jerry_trader.factors_old;
-- RENAME TABLE jerry_trader.factors_v2 TO jerry_trader.factors;

-- Step 4: Drop old table (run manually after confirming everything works)
-- DROP TABLE jerry_trader.factors_old;

-- Verification query:
-- SELECT ticker, timeframe, count()
-- FROM jerry_trader.factors_v2
-- GROUP BY ticker, timeframe
-- ORDER BY ticker, timeframe;
