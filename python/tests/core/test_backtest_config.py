"""Tests for services/backtest/config.py.

Covers:
  - PreFilterConfig: construction, defaults
  - DEFAULT_HORIZONS_MS and horizon_label()
  - BacktestConfig: construction, defaults, __post_init__ sync, session_range_ms(), data_range_ms()
  - TickerData: construction, defaults
"""

from __future__ import annotations

from jerry_trader.services.backtest.config import (
    DEFAULT_HORIZONS_MS,
    BacktestConfig,
    PreFilterConfig,
    TickerData,
    horizon_label,
)

# ══════════════════════════════════════════════════════════════════════
# PreFilterConfig
# ══════════════════════════════════════════════════════════════════════


class TestPreFilterConfig:
    def test_defaults(self):
        config = PreFilterConfig()
        assert config.top_n == 20
        assert config.min_gain_pct == 2.0
        assert config.new_entry_only is True
        assert config.min_price == 0.01
        assert config.max_price == 500.0
        assert config.min_volume == 0.0
        assert config.min_relative_volume == 0.0
        assert config.exclude_etf is True

    def test_custom(self):
        config = PreFilterConfig(
            top_n=10,
            min_gain_pct=5.0,
            new_entry_only=False,
            min_price=1.0,
        )
        assert config.top_n == 10
        assert config.min_gain_pct == 5.0
        assert config.new_entry_only is False
        assert config.min_price == 1.0


# ══════════════════════════════════════════════════════════════════════
# DEFAULT_HORIZONS_MS / horizon_label
# ══════════════════════════════════════════════════════════════════════


class TestHorizonLabel:
    def test_known_labels(self):
        assert horizon_label(30_000) == "30s"
        assert horizon_label(60_000) == "1m"
        assert horizon_label(300_000) == "5m"
        assert horizon_label(3_600_000) == "60m"

    def test_unknown_fallback(self):
        assert horizon_label(45_000) == "0m"  # 45000 // 60000 = 0
        assert horizon_label(90_000) == "1m"

    def test_all_default_horizons_have_valid_labels(self):
        for ms in DEFAULT_HORIZONS_MS:
            label = horizon_label(ms)
            assert len(label) > 0

    def test_default_horizons_non_empty(self):
        assert len(DEFAULT_HORIZONS_MS) > 0
        assert all(h > 0 for h in DEFAULT_HORIZONS_MS)


# ══════════════════════════════════════════════════════════════════════
# BacktestConfig
# ══════════════════════════════════════════════════════════════════════


class TestBacktestConfig:
    def test_minimal_construction(self):
        config = BacktestConfig(date="2026-03-06")
        assert config.date == "2026-03-06"
        assert config.gain_threshold == 2.0
        assert config.slippage_buffer == 0.001
        assert config.default_slippage == 0.002
        assert len(config.horizons_ms) == 8
        assert config.cooldown_ms == 60_000
        assert config.session_start == "040000"
        assert config.session_end == "093000"

    def test_default_pre_filter(self):
        config = BacktestConfig(date="2026-03-06")
        assert config.pre_filter is not None
        assert config.pre_filter.top_n == 20

    def test_output_flags(self):
        config = BacktestConfig(date="2026-03-06")
        assert config.output_clickhouse is True
        assert config.output_console is True
        assert config.candidates_only is False
        assert config.detailed is False

    def test_custom_date(self):
        config = BacktestConfig(date="2026-01-15")
        assert config.date == "2026-01-15"


class TestBacktestConfigPostInit:
    def test_gain_threshold_syncs_to_pre_filter(self):
        config = BacktestConfig(date="2026-03-06", gain_threshold=5.0)
        assert config.pre_filter.min_gain_pct == 5.0

    def test_gain_threshold_default_not_overwritten(self):
        config = BacktestConfig(
            date="2026-03-06",
            gain_threshold=2.0,
            pre_filter=PreFilterConfig(min_gain_pct=5.0),
        )
        # pre_filter already has non-default min_gain_pct → not overwritten
        assert config.pre_filter.min_gain_pct == 5.0


class TestBacktestConfigSessionRange:
    def test_session_range_returns_tuple(self):
        config = BacktestConfig(date="2026-03-06")
        start_ms, end_ms = config.session_range_ms()
        assert start_ms > 0
        assert end_ms > start_ms

    def test_data_range_extends_by_horizon(self):
        config = BacktestConfig(date="2026-03-06")
        start_ms, end_ms = config.data_range_ms()
        assert start_ms > 0
        assert end_ms > start_ms

    def test_data_range_with_metrics_end(self):
        config = BacktestConfig(
            date="2026-03-06",
            metrics_end="100000",
        )
        _, end_ms = config.data_range_ms()
        assert end_ms > 0


# ══════════════════════════════════════════════════════════════════════
# TickerData
# ══════════════════════════════════════════════════════════════════════


class TestTickerData:
    def test_minimal_construction(self):
        td = TickerData(symbol="AAPL")
        assert td.symbol == "AAPL"
        assert td.trades == []
        assert td.quotes == []
        assert td.bars_1m == []
        assert td.candidate is None

    def test_with_data(self):
        td = TickerData(
            symbol="AAPL",
            trades=[(1000, 150.0, 100)],
            quotes=[(1000, 149.0, 151.0, 500, 300)],
        )
        assert len(td.trades) == 1
        assert len(td.quotes) == 1
        assert td.trades[0] == (1000, 150.0, 100)
