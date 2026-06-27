-- NewsProcessor LLM classifications — one row per article classified.
--
-- Written by NewsProcessor in real-time (alongside Redis stream publish).
-- Replaces the ad-hoc plain-text / JSON log files on disk.
--
-- Run:
--   clickhouse-client --password "$CLICKHOUSE_PASSWORD" < sql/clickhouse_news_classifications.sql

CREATE TABLE IF NOT EXISTS jerry_trader.news_classifications
(
    -- Identity
    id              UUID,

    -- Ticker & time
    symbol          LowCardinality(String),
    session_id      String,
    current_time    DateTime64(3, 'UTC'),    -- when the LLM classified it
    published_time  Nullable(DateTime64(3, 'UTC')), -- when the article was published

    -- Classification result
    is_catalyst     UInt8,                   -- 1 = catalyst, 0 = not
    score           String,                  -- e.g. "7/10"
    score_num       UInt8,                   -- e.g. 7 (extracted from score)

    -- Article metadata
    title           String,
    url             Nullable(String),
    source_from     LowCardinality(String),  -- "benzinga", "momo", "fmp", etc.
    content_preview String,                  -- first 200 chars

    -- LLM reasoning
    model           LowCardinality(String),  -- "deepseek", "deepseek-chat", etc.
    explanation     Nullable(String),        -- JSON string of the explanation dict

    -- Forward returns (backfilled offline from market_snapshot)
    trigger_price   Nullable(Float64),
    return_5min     Nullable(Float64),
    return_15min    Nullable(Float64),
    return_30min    Nullable(Float64),
    max_return_15min Nullable(Float64),
    max_return_30min Nullable(Float64),

    -- Bookkeeping
    trade_date      Date DEFAULT toDate(current_time),
    inserted_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, current_time, title)
TTL trade_date + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;
