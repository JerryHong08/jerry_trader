-- ClickHouse table for raw trades data (from Polygon).
--
-- Imported via: cli import-ticks --date YYYY-MM-DD --types trades_v1
-- Used by: DataLoader (preferred source for backtest trades)

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.trades
(
    ticker String,
    date String,

    sip_timestamp Int64,           -- nanoseconds (Polygon native)
    participant_timestamp Int64,
    price Float64,
    size Float64,
    exchange Int32,
    conditions String,             -- comma-separated condition codes
    correction Int32,
    tape Int32,
    trf_id Int64,
    trf_timestamp Int64,
    sequence_number Int64,

    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY date
ORDER BY (ticker, sip_timestamp);
