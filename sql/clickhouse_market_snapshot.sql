-- ClickHouse table for SnapshotProcessor gradual migration.
--
-- Strategy:
--   1) Keep InfluxDB writes (legacy)
--   2) Add parallel writes into this table (supplement)
--   3) Prefer this table for volume-history reload with Influx fallback

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.market_snapshot
(
    symbol String,
    date String,
    mode String,
    session_id String,

    event_time_ms Int64,
    event_time DateTime64(3, 'UTC'),

    price Float64,
    changePercent Float64,
    volume Float64,
    prev_close Float64,
    prev_volume Float64,
    vwap Float64,
    bid Float64,
    ask Float64,
    bid_size Float64,
    ask_size Float64,

    rank Int32,
    competition_rank Int32,
    change Float64,
    relativeVolumeDaily Float64,
    relativeVolume5min Float64,

    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY (date, mode)
ORDER BY (symbol, event_time_ms, session_id);
