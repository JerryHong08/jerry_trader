"""Tests for PreFilter — candidate selection from market_snapshot."""

from unittest.mock import MagicMock, patch

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


def _mock_query_result(rows: list[tuple]) -> MagicMock:
    """Create a mock ClickHouse query result."""
    result = MagicMock()
    result.result_rows = rows
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
        # Simulate CTE query result (new entries only)
        mock_ch.query.return_value = _mock_query_result(
            [
                ("RPID", 1773398400241, 8.03, 3.5, 3.425, 100000.0, 2.1, 8.03),
                ("TMDE", 1773398751607, 8.23, 2.5, 2.43, 50000.0, 1.8, 8.64),
            ]
        )

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13", PreFilterConfig(new_entry_only=True))

        assert len(result) == 2
        assert result[0].symbol == "RPID"
        assert result[1].symbol == "TMDE"
        assert result[0].gain_at_entry == 8.03
        assert result[1].prev_close == 2.43

        # Verify CTE query was used (check SQL contains 'initial_top20')
        call_sql = mock_ch.query.call_args[0][0]
        assert "initial_top20" in call_sql

    def test_find_all_top_n(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_query_result(
            [
                ("SVCO", 1773398366226, 10.9, 3.3, 3.3, 200000.0, 3.0, 31.8),
                ("RPID", 1773398400241, 8.03, 3.5, 3.425, 100000.0, 2.1, 8.03),
            ]
        )

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13", PreFilterConfig(new_entry_only=False))

        assert len(result) == 2
        # Should NOT use CTE
        call_sql = mock_ch.query.call_args[0][0]
        assert "initial_top20" not in call_sql

    def test_query_failure_returns_empty(self):
        mock_ch = MagicMock()
        mock_ch.query.side_effect = Exception("connection lost")

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13")
        assert result == []

    def test_malformed_row_skipped(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_query_result(
            [
                ("GOOD", 1773398400241, 8.03, 3.5, 3.425, 100000.0, 2.1, 8.03),
                ("BAD", None, None, None, None, None, None, None),  # will fail float()
            ]
        )

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13")
        assert len(result) == 1
        assert result[0].symbol == "GOOD"

    def test_default_config(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_query_result([])

        pf = PreFilter(mock_ch)
        result = pf.find("2026-03-13")  # No config passed, uses defaults
        assert result == []

        # Default is new_entry_only=True
        call_sql = mock_ch.query.call_args[0][0]
        assert "initial_top20" in call_sql

    def test_database_prefix(self):
        mock_ch = MagicMock()
        mock_ch.query.return_value = _mock_query_result([])

        pf = PreFilter(mock_ch, database="test_db")
        pf.find("2026-03-13")

        call_sql = mock_ch.query.call_args[0][0]
        assert "test_db.market_snapshot" in call_sql


class TestPreFilterCommonStocks:
    """Test ETF/common stock filtering."""

    def test_filter_common_stocks(self):
        candidates = [
            _make_candidate(symbol="AAPL"),
            _make_candidate(symbol="SPY"),  # ETF, should be excluded
        ]

        import polars as pl

        mock_fn = MagicMock(return_value=pl.LazyFrame({"ticker": ["AAPL"]}))

        with patch.dict(
            "sys.modules",
            {
                "jerry_trader.shared.utils.data_utils": MagicMock(
                    get_common_stocks=mock_fn
                )
            },
        ):
            result = PreFilter._filter_common_stocks(candidates, "2026-03-13")

        assert len(result) == 1
        assert result[0].symbol == "AAPL"

    def test_filter_failure_returns_all(self):
        candidates = [_make_candidate(symbol="AAPL")]

        mock_fn = MagicMock(side_effect=Exception("file not found"))

        with patch.dict(
            "sys.modules",
            {
                "jerry_trader.shared.utils.data_utils": MagicMock(
                    get_common_stocks=mock_fn
                )
            },
        ):
            result = PreFilter._filter_common_stocks(candidates, "2026-03-13")

        # Should return all candidates on failure
        assert len(result) == 1
