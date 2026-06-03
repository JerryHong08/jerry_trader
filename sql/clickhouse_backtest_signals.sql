-- ClickHouse table for backtest signals (visualization-focused).
--
-- Stores signal results without duplicating bar data.
-- Bar data comes from existing ohlcv_bars table.
--
-- Written by: BacktestRunner (backtest_app)
-- Read by: Backtest Backend API, Frontend visualization

CREATE DATABASE IF NOT EXISTS jerry_trader;

CREATE TABLE IF NOT EXISTS jerry_trader.backtest_signals
(
    experiment_id String,          -- UUID for this backtest run
    date Date,
    ticker String,
    event_name String,

    -- Signal info
    entry_time DateTime64(3),
    entry_price Float64,
    exit_time DateTime64(3),
    exit_price Float64,
    return_pct Float64,
    exit_reason String,            -- 'hold_duration', 'stop_loss', 'take_profit'

    -- Factors at entry (JSON for flexibility)
    factors String,                -- JSON: factor values at trigger time

    -- Price path analysis
    max_price Float64,
    min_price Float64,
    time_to_max_ms Int64,
    time_to_min_ms Int64,

    -- Metadata
    created_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (experiment_id, date, ticker, entry_time);
