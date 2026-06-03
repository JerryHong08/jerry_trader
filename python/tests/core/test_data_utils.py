"""Tests for shared/utils/data_utils.py.

Covers:
  - generate_backtest_date: forward/reverse, week/month/day periods, limits
  - clear_common_stocks_cache: cache invalidation
  - get_common_stocks: caching behavior (mocked parquet I/O)
"""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import polars as pl

from jerry_trader.shared.utils.data_utils import (
    clear_common_stocks_cache,
    generate_backtest_date,
    get_common_stocks,
    get_common_stocks_full,
)

# ══════════════════════════════════════════════════════════════════════
# generate_backtest_date — forward (reverse=False)
# ══════════════════════════════════════════════════════════════════════


class TestGenerateBacktestDateForward:
    def test_weekly_from_past(self):
        """Weekly forward from a past date generates dates up to today."""
        # Start from 2 weeks ago
        two_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=2)
        start = two_weeks_ago.strftime("%Y-%m-%d")
        dates = generate_backtest_date(start, reverse=False, period="week")
        assert len(dates) >= 2
        assert dates[0] == start
        # Dates should be strictly increasing
        assert dates == sorted(dates)

    def test_daily_from_yesterday(self):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        start = yesterday.strftime("%Y-%m-%d")
        dates = generate_backtest_date(start, reverse=False, period="day")
        assert len(dates) >= 1
        assert dates[0] == start

    def test_monthly_from_january(self):
        # Start from January of current year
        jan1 = datetime.datetime(datetime.datetime.now().year, 1, 1)
        start = jan1.strftime("%Y-%m-%d")
        dates = generate_backtest_date(start, reverse=False, period="month")
        assert len(dates) >= 1
        assert dates == sorted(dates)

    def test_no_duplicates(self):
        start = "2025-01-01"
        dates = generate_backtest_date(start, reverse=False, period="week")
        assert len(dates) == len(set(dates))


# ══════════════════════════════════════════════════════════════════════
# generate_backtest_date — reverse (reverse=True)
# ══════════════════════════════════════════════════════════════════════


class TestGenerateBacktestDateReverse:
    def test_weekly_reverse_count(self):
        """Reverse weekly with count limit."""
        start = "2025-06-15"
        dates = generate_backtest_date(
            start, reverse=True, period="week", reverse_limit_count=4
        )
        assert len(dates) == 4
        # Should be decreasing
        assert dates == sorted(dates, reverse=True)

    def test_weekly_reverse_with_limit_date(self):
        """Reverse weekly with explicit limit date."""
        start = "2025-03-15"
        limit = "2025-02-15"
        dates = generate_backtest_date(
            start,
            reverse=True,
            period="week",
            reverse_limit=limit,
        )
        assert len(dates) > 0
        # All dates must be >= limit
        for d in dates:
            assert d >= limit

    def test_daily_reverse_count(self):
        start = "2025-06-15"
        dates = generate_backtest_date(
            start, reverse=True, period="day", reverse_limit_count=7
        )
        assert len(dates) == 7
        assert dates == sorted(dates, reverse=True)

    def test_monthly_reverse_count(self):
        start = "2025-06-15"
        dates = generate_backtest_date(
            start, reverse=True, period="month", reverse_limit_count=3
        )
        assert len(dates) == 3

    def test_reverse_limit_date_exact_boundary(self):
        """Start equals limit → just start date."""
        start = "2025-06-15"
        dates = generate_backtest_date(
            start,
            reverse=True,
            period="week",
            reverse_limit=start,
        )
        assert len(dates) == 1
        assert dates[0] == start

    def test_reverse_month_boundary(self):
        """Reverse monthly crosses year boundary (January → December)."""
        start = "2025-02-15"
        dates = generate_backtest_date(
            start, reverse=True, period="month", reverse_limit_count=3
        )
        assert len(dates) == 3
        assert dates[0] == "2025-02-15"
        assert dates[1] == "2025-01-15"
        assert dates[2] == "2024-12-15"


# ══════════════════════════════════════════════════════════════════════
# generate_backtest_date — edge cases
# ══════════════════════════════════════════════════════════════════════


class TestGenerateBacktestDateEdgeCases:
    def test_future_start_date_empty(self):
        """Forward from a future date returns empty list."""
        future = datetime.datetime.now() + datetime.timedelta(days=365)
        start = future.strftime("%Y-%m-%d")
        dates = generate_backtest_date(start, reverse=False, period="week")
        assert dates == []

    def test_count_zero_should_stop(self):
        """reverse_limit_count=0 should produce empty list."""
        start = "2025-06-15"
        dates = generate_backtest_date(
            start, reverse=True, period="week", reverse_limit_count=0
        )
        assert dates == []

    def test_default_period_is_week(self):
        """period defaults to 'week'."""
        start = "2025-06-15"
        dates_weekly = generate_backtest_date(
            start, reverse=True, reverse_limit_count=2, period="week"
        )
        assert len(dates_weekly) == 2

    def test_default_reverse_limit_count_is_52(self):
        """Default count limit is 52."""
        start = "2023-01-01"
        dates = generate_backtest_date(start, reverse=True, period="week")
        assert len(dates) == 52


# ══════════════════════════════════════════════════════════════════════
# clear_common_stocks_cache
# ══════════════════════════════════════════════════════════════════════


class TestClearCommonStocksCache:
    def test_clear_resets_cache(self):
        """After clearing, get_common_stocks should reload from disk."""
        # Clear first
        clear_common_stocks_cache()

        import jerry_trader.shared.utils.data_utils as du

        # Verify internal state is cleared
        assert du._common_stocks_cache is None
        assert du._common_stocks_cache_date is None

    def test_multiple_clears_are_idempotent(self):
        clear_common_stocks_cache()
        clear_common_stocks_cache()
        import jerry_trader.shared.utils.data_utils as du

        assert du._common_stocks_cache is None


# ══════════════════════════════════════════════════════════════════════
# get_common_stocks — caching behavior (mocked)
# ══════════════════════════════════════════════════════════════════════


class TestGetCommonStocksCaching:
    def setup_method(self):
        clear_common_stocks_cache()

    def teardown_method(self):
        clear_common_stocks_cache()

    @patch("jerry_trader.shared.utils.data_utils.pl.scan_parquet")
    def test_caches_result(self, mock_scan):
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_filtered = MagicMock(spec=pl.LazyFrame)
        mock_lf.filter.return_value = mock_filtered
        mock_filtered.select.return_value = mock_filtered
        mock_scan.return_value = mock_lf

        result1 = get_common_stocks()
        result2 = get_common_stocks()

        # Second call should use cache, not re-scan
        assert result1 is result2
        mock_scan.assert_called_once()

    @patch("jerry_trader.shared.utils.data_utils.pl.scan_parquet")
    def test_different_date_invalidates_cache(self, mock_scan):
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_filtered = MagicMock(spec=pl.LazyFrame)
        mock_lf.filter.return_value = mock_filtered
        mock_filtered.select.return_value = mock_filtered
        mock_scan.return_value = mock_lf

        get_common_stocks("2015-01-01")
        get_common_stocks("2020-01-01")

        # Different filter_date → reload
        assert mock_scan.call_count == 2


class TestGetCommonStocksFull:
    @patch("jerry_trader.shared.utils.data_utils.pl.read_parquet")
    def test_returns_dataframe(self, mock_read):
        mock_df = MagicMock(spec=pl.DataFrame)
        mock_filtered = MagicMock(spec=pl.DataFrame)
        mock_df.filter.return_value = mock_filtered
        mock_filtered.select.return_value = mock_filtered
        mock_read.return_value = mock_df

        result = get_common_stocks_full()
        assert result is mock_filtered
        mock_read.assert_called_once()
