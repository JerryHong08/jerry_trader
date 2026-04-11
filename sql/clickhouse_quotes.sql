-- ClickHouse table for raw quotes data (from Polygon).
--
-- Imported via: cli import-ticks --date YYYY-MM-DD --types quotes_v1
-- Used by: DataLoader (preferred source for backtest quotes / spread analysis)

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.quotes
(
    ticker String,
    date String,

    sip_timestamp Int64,           -- nanoseconds (Polygon native)
    participant_timestamp Int64,
    bid_price Float64,
    bid_size UInt32,
    ask_price Float64,
    ask_size UInt32,
    bid_exchange Int32,
    ask_exchange Int32,
    conditions String,
    indicators String,
    tape Int32,
    trf_timestamp Int64,
    sequence_number Int64,

    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY date
ORDER BY (ticker, sip_timestamp);
