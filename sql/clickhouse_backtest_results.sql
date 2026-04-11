-- ClickHouse table for backtest results.
--
-- Written by: BacktestRunner → persist_results()
-- Read by: notebooks/02-backtest-results.ipynb, backtest output module

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.backtest_results
(
    run_id String,
    date Date,
    rule_id String,
    ticker String,
    trigger_time_ns Int64,
    entry_price Float64,
    trigger_price Float64,
    slippage_pct Float64,
    factors String,                -- JSON: factor values at trigger time

    return_30s Nullable(Float64),
    return_1m Nullable(Float64),
    return_2m Nullable(Float64),
    return_5m Nullable(Float64),
    return_10m Nullable(Float64),
    return_15m Nullable(Float64),
    return_30m Nullable(Float64),
    return_60m Nullable(Float64),

    mfe Nullable(Float64),         -- Maximum Favorable Excursion
    mae Nullable(Float64),         -- Maximum Adverse Excursion
    time_to_peak_ms Nullable(Int64),

    inserted_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, rule_id, ticker, trigger_time_ns);
