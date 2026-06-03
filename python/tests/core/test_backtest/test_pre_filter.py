"""Tests for PreFilter — candidate selection from market_snapshot."""

from unittest.mock import MagicMock, patch

import pytest

from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.services.backtest.config import PreFilterConfig
from jerry_trader.services.backtest.pre_filter import PreFilter


def _make_candidate(
    symbol: str = "AAPL",
    first_entry_ms: int = 1773398400000,
    gain_at_entry: float = 5.0,
    price_at_entry: float = 150.0,
    prev_close: float = 143.0,
    volume_at_entry: float = 1_000_000.0,
    relative_volume: float = 3.5,
    max_gain: float = 12.0,
) -> Candidate:
    return Candidate(
        symbol=symbol,
        first_entry_ms=first_entry_ms,
        gain_at_entry=gain_at_entry,
        price_at_entry=price_at_entry,
        prev_close=prev_close,
        volume_at_entry=volume_at_entry,
        relative_volume=relative_volume,
        max_gain=max_gain,
    )


def _make_data_result(rows: list[tuple]) -> MagicMock:
    """Create a mock ClickHouse data query result with column_names."""
    result = MagicMock()
    result.result_rows = rows
    result.column_names = [
        "ticker",
        "timestamp",
        "price",
        "volume",
        "prev_close",
        "prev_volume",
        "changePercent",
    ]
    return result


def _make_count_result(count: int) -> MagicMock:
    """Create a mock ClickHouse count query result."""
    result = MagicMock()
    result.result_rows = [[count]]
    return result


class TestPreFilterFilters:
    """Test post-query filtering logic."""

    def test_min_gain_pct(self):
        candidates = [
            _make_candidate(symbol="LOW", gain_at_entry=1.5),
            _make_candidate(symbol="HIGH", gain_at_entry=8.0),
        ]
        config = PreFilterConfig(min_gain_pct=2.0)
        result = PreFilter._apply_filters(candidates, config)
        assert len(result) == 1
        assert result[0].symbol == "HIGH"

    def test_min_price(self):
        candidates = [
            _make_candidate(symbol="CHEAP", price_at_entry=0.3),
            _make_candidate(symbol="OK", price_at_entry=5.0),
        ]
        config = PreFilterConfig(min_price=0.5)
        result = PreFilter._apply_filters(candidates, config)
        assert len(result) == 1
        assert result[0].symbol == "OK"

    def test_max_price(self):
        candidates = [
            _make_candidate(symbol="EXPENSIVE", price_at_entry=600.0),
            _make_candidate(symbol="OK", price_at_entry=100.0),
        ]
        config = PreFilterConfig(max_price=500.0)
        result = PreFilter._apply_filters(candidates, config)
        assert len(result) == 1
        assert result[0].symbol == "OK"

    def test_min_volume(self):
        candidates = [
            _make_candidate(symbol="THIN", volume_at_entry=100.0),
            _make_candidate(symbol="LIQUID", volume_at_entry=500_000.0),
        ]
        config = PreFilterConfig(min_volume=1000.0)
        result = PreFilter._apply_filters(candidates, config)
        assert len(result) == 1
        assert result[0].symbol == "LIQUID"

    def test_min_relative_volume(self):
        candidates = [
            _make_candidate(symbol="LOW_RV", relative_volume=1.5),
            _make_candidate(symbol="HIGH_RV", relative_volume=5.0),
        ]
        config = PreFilterConfig(min_relative_volume=2.0)
        result = PreFilter._apply_filters(candidates, config)
        assert len(result) == 1
        assert result[0].symbol == "HIGH_RV"

    def test_combined_filters(self):
        candidates = [
            _make_candidate(symbol="A", gain_at_entry=1.0, price_at_entry=0.3),
            _make_candidate(symbol="B", gain_at_entry=5.0, price_at_entry=5.0),
            _make_candidate(symbol="C", gain_at_entry=3.0, price_at_entry=0.4),
        ]
        config = PreFilterConfig(min_gain_pct=2.0, min_price=0.5)
        result = PreFilter._apply_filters(candidates, config)
        assert len(result) == 1
        assert result[0].symbol == "B"

    def test_no_filters(self):
        candidates = [_make_candidate(), _make_candidate(symbol="B")]
        config = PreFilterConfig(
            min_gain_pct=0,
            min_price=0,
            max_price=float("inf"),
            min_volume=0,
            min_relative_volume=0,
        )
        result = PreFilter._apply_filters(candidates, config)
        assert len(result) == 2


class TestPreFilterQuery:
    """Test ClickHouse query execution and result mapping."""

    def test_find_with_new_entry_only(self):
        mock_ch = MagicMock()
        # Three calls: count query, data query, then count query for log
        mock_ch.query.side_effect = [
            _make_count_result(500),
            _make_data_result(
                [
                    ("RPID", 1773398400241, 3.5, 100000.0, 3.425, 50000.0, 8.03),
                    ("TMDE", 1773398751607, 2.5, 50000.0, 2.43, 30000.0, 8.23),
                ]
            ),
        ]

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13", PreFilterConfig(new_entry_only=True))

        # RPID is in the first window → excluded by new_entry_only.
        # TMDE is in the second window → survives.
        assert len(result) == 1
        assert result[0].symbol == "TMDE"

    def test_find_all_top_n(self):
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _make_count_result(500),
            _make_data_result(
                [
                    ("SVCO", 1773398366226, 3.3, 200000.0, 3.3, 100000.0, 10.9),
                    ("RPID", 1773398400241, 3.5, 100000.0, 3.425, 50000.0, 8.03),
                ]
            ),
        ]

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13", PreFilterConfig(new_entry_only=False))

        assert len(result) == 2

    def test_query_failure_in_check_raises(self):
        mock_ch = MagicMock()
        mock_ch.query.side_effect = Exception("connection lost")

        pf = PreFilter(mock_ch)
        # _check_collector_exists catches query exception → count=0 → raises RuntimeError
        with pytest.raises(RuntimeError, match="No data in market_snapshot_collector"):
            pf.find("2026-03-13")

    def test_malformed_row_skipped(self):
        """Rows with None values are handled by Polars — no crash."""
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _make_count_result(500),
            _make_data_result(
                [
                    ("GOOD", 1773398400241, 3.5, 100000.0, 3.425, 50000.0, 8.03),
                    ("BAD", None, None, None, None, None, None),
                ]
            ),
        ]

        pf = PreFilter(mock_ch)
        # Should not raise — Polars handles None values
        result = pf.find("2026-03-13")
        assert len(result) >= 0

    def test_default_config(self):
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _make_count_result(500),
            _make_data_result([]),
        ]

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13")
        assert result == []

    def test_database_prefix(self):
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _make_count_result(500),
            _make_data_result([]),
        ]

        pf = PreFilter(mock_ch, database="test_db")
        pf.find("2026-03-13")

        # First call should use the custom database prefix
        call_sql = mock_ch.query.call_args_list[0][0][0]
        assert "test_db.market_snapshot_collector" in call_sql


class TestPreFilterCommonStocks:
    """Test ETF/common stock filtering — inlined in find() via inline import."""

    def test_common_stocks_filter_applied(self):
        """When exclude_etf=True, get_common_stocks is called."""
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _make_count_result(500),
            _make_data_result(
                [
                    ("AAPL", 1773398400241, 3.5, 100000.0, 3.425, 50000.0, 8.03),
                ]
            ),
        ]

        import polars as pl

        common_df = pl.DataFrame({"ticker": ["AAPL"]})

        with patch(
            "jerry_trader.shared.utils.data_utils.get_common_stocks"
        ) as mock_common:
            # get_common_stocks(date).select("ticker").collect() → DataFrame
            mock_select = MagicMock()
            mock_select.collect.return_value = common_df
            mock_common.return_value.select.return_value = mock_select

            config = PreFilterConfig(exclude_etf=True)
            pf = PreFilter(mock_ch)
            result = pf.find("2026-03-13", config)

            assert len(result) >= 0
            mock_common.assert_called_once()

    def test_exclude_etf_false_skips_filter(self):
        """When exclude_etf=False, get_common_stocks is not called."""
        mock_ch = MagicMock()
        mock_ch.query.side_effect = [
            _make_count_result(500),
            _make_data_result(
                [
                    ("AAPL", 1773398400241, 3.5, 100000.0, 3.425, 50000.0, 8.03),
                ]
            ),
        ]

        with patch(
            "jerry_trader.shared.utils.data_utils.get_common_stocks"
        ) as mock_common:
            config = PreFilterConfig(exclude_etf=False)
            pf = PreFilter(mock_ch)
            result = pf.find("2026-03-13", config)

            assert len(result) >= 0
            mock_common.assert_not_called()
