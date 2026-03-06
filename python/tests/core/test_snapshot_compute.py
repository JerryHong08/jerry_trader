"""
Tests for jerry_trader.core.snapshot.compute

Covers:
    - VolumeTracker: sliding-window relativeVolume5min computation
    - compute_ranks: competition ranking by changePercent
    - compute_derived_metrics: change, relativeVolumeDaily, relativeVolume5min
    - compute_weighted_mid_price: bid/ask weighted mid
"""

from datetime import datetime, timedelta

import polars as pl
import pytest

from jerry_trader.core.snapshot.compute import (
    VolumeTracker,
    compute_derived_metrics,
    compute_ranks,
    compute_weighted_mid_price,
)

# =========================================================================
# VolumeTracker
# =========================================================================


class TestVolumeTracker:
    """Tests for VolumeTracker sliding-window volume tracker."""

    def test_initial_state_empty(self):
        vt = VolumeTracker()
        assert len(vt.history) == 0

    def test_single_data_point_returns_1(self):
        """With only one data point, there's no history → default 1.0."""
        vt = VolumeTracker()
        result = vt.compute_relative_volume_5min(
            "AAPL", datetime(2026, 1, 1, 9, 30), 1000.0
        )
        assert result == 1.0

    def test_two_data_points_same_volume(self):
        """Two identical volumes 30s apart → 1min volume = 0 → returns 1.0 fallback."""
        vt = VolumeTracker()
        t0 = datetime(2026, 1, 1, 9, 30, 0)
        vt.compute_relative_volume_5min("AAPL", t0, 1000.0)
        result = vt.compute_relative_volume_5min(
            "AAPL", t0 + timedelta(seconds=30), 1000.0
        )
        # volume didn't change → last_1min = 0, avg will be 0 → returns 1.0
        assert result == 1.0

    def test_increasing_volume_over_1min(self):
        """Volume increasing within 1min should produce a non-1.0 result."""
        vt = VolumeTracker()
        t0 = datetime(2026, 1, 1, 9, 30, 0)
        vt.compute_relative_volume_5min("AAPL", t0, 1000.0)
        # 2 minutes later, volume has grown
        result = vt.compute_relative_volume_5min(
            "AAPL", t0 + timedelta(minutes=2), 3000.0
        )
        assert isinstance(result, float)
        assert result > 0

    def test_history_window_pruning(self):
        """Data older than 6 minutes should be pruned."""
        vt = VolumeTracker()
        t0 = datetime(2026, 1, 1, 9, 30, 0)

        # Add entries spanning 7 minutes
        for i in range(8):
            vt.compute_relative_volume_5min(
                "AAPL", t0 + timedelta(minutes=i), 1000.0 * (i + 1)
            )

        # Only entries within last 6 minutes of the final timestamp should remain
        last_ts = t0 + timedelta(minutes=7)
        cutoff = last_ts - timedelta(minutes=6)
        for ts, _ in vt.history["AAPL"]:
            assert ts >= cutoff, f"Found stale entry at {ts}, cutoff was {cutoff}"

    def test_multiple_tickers_independent(self):
        """Each ticker has independent volume history."""
        vt = VolumeTracker()
        t0 = datetime(2026, 1, 1, 9, 30, 0)

        vt.compute_relative_volume_5min("AAPL", t0, 1000.0)
        vt.compute_relative_volume_5min("TSLA", t0, 5000.0)

        assert len(vt.history["AAPL"]) == 1
        assert len(vt.history["TSLA"]) == 1
        assert vt.history["AAPL"][0][1] == 1000.0
        assert vt.history["TSLA"][0][1] == 5000.0

    def test_reload_history(self):
        """reload_history() should bulk-load entries."""
        vt = VolumeTracker()
        t0 = datetime(2026, 1, 1, 9, 30, 0)
        entries = [
            (t0, 100.0),
            (t0 + timedelta(minutes=1), 200.0),
            (t0 + timedelta(minutes=2), 350.0),
        ]
        vt.reload_history("AAPL", entries)
        assert len(vt.history["AAPL"]) == 3

    def test_compute_batch(self):
        """compute_batch() should return one result per input row."""
        vt = VolumeTracker()
        t0 = datetime(2026, 1, 1, 9, 30, 0)

        tickers = ["AAPL", "TSLA", "NVDA"]
        timestamps = [t0, t0, t0]
        volumes = [1000.0, 2000.0, 3000.0]

        results = vt.compute_batch(tickers, timestamps, volumes)
        assert len(results) == 3
        assert all(isinstance(r, float) for r in results)

    def test_compute_batch_matches_individual(self):
        """Batch results should match calling compute_relative_volume_5min individually."""
        t0 = datetime(2026, 1, 1, 9, 30, 0)
        tickers = ["AAPL", "TSLA"]
        timestamps = [t0, t0]
        volumes = [1000.0, 2000.0]

        # Individual
        vt1 = VolumeTracker()
        individual = [
            vt1.compute_relative_volume_5min(t, ts, v)
            for t, ts, v in zip(tickers, timestamps, volumes)
        ]

        # Batch
        vt2 = VolumeTracker()
        batch = vt2.compute_batch(tickers, timestamps, volumes)

        assert individual == batch

    def test_steady_volume_stream(self):
        """Steady linear volume growth → stable relative volume."""
        vt = VolumeTracker()
        t0 = datetime(2026, 1, 1, 9, 30, 0)

        results = []
        for i in range(10):
            r = vt.compute_relative_volume_5min(
                "AAPL", t0 + timedelta(minutes=i), 1000.0 * (i + 1)
            )
            results.append(r)

        # After enough data, relative volume should stabilize around 1.0
        # (constant rate of volume increase → recent = avg)
        assert all(isinstance(r, float) for r in results)
        # The later values should be reasonable (not wildly off)
        assert 0.0 < results[-1] < 10.0


# =========================================================================
# compute_ranks
# =========================================================================


class TestComputeRanks:
    """Tests for compute_ranks."""

    def test_basic_ranking(self):
        df = pl.DataFrame(
            {
                "ticker": ["AAPL", "TSLA", "NVDA"],
                "changePercent": [5.0, 10.0, 2.0],
            }
        )
        result = compute_ranks(df)

        assert "rank" in result.columns
        # TSLA has highest changePercent → rank 1
        tsla_row = result.filter(pl.col("ticker") == "TSLA")
        assert tsla_row["rank"][0] == 1
        # AAPL is 2nd
        aapl_row = result.filter(pl.col("ticker") == "AAPL")
        assert aapl_row["rank"][0] == 2
        # NVDA is 3rd
        nvda_row = result.filter(pl.col("ticker") == "NVDA")
        assert nvda_row["rank"][0] == 3

    def test_sorted_descending(self):
        """Result should be sorted by changePercent descending."""
        df = pl.DataFrame(
            {
                "ticker": ["C", "A", "B"],
                "changePercent": [1.0, 3.0, 2.0],
            }
        )
        result = compute_ranks(df)
        values = result["changePercent"].to_list()
        assert values == sorted(values, reverse=True)

    def test_tied_ranks(self):
        """Tied changePercent values → same rank (method='min')."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL", "TSLA", "NVDA"],
                "changePercent": [5.0, 5.0, 2.0],
            }
        )
        result = compute_ranks(df)
        aapl_rank = result.filter(pl.col("ticker") == "AAPL")["rank"][0]
        tsla_rank = result.filter(pl.col("ticker") == "TSLA")["rank"][0]
        nvda_rank = result.filter(pl.col("ticker") == "NVDA")["rank"][0]
        assert aapl_rank == tsla_rank == 1
        assert nvda_rank == 3

    def test_single_row(self):
        df = pl.DataFrame({"ticker": ["AAPL"], "changePercent": [5.0]})
        result = compute_ranks(df)
        assert result["rank"][0] == 1

    def test_negative_changes(self):
        """Negative changePercent values should rank correctly."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL", "TSLA"],
                "changePercent": [-2.0, -5.0],
            }
        )
        result = compute_ranks(df)
        aapl_rank = result.filter(pl.col("ticker") == "AAPL")["rank"][0]
        tsla_rank = result.filter(pl.col("ticker") == "TSLA")["rank"][0]
        assert aapl_rank == 1  # -2 > -5
        assert tsla_rank == 2

    def test_rank_dtype_int32(self):
        df = pl.DataFrame({"ticker": ["A"], "changePercent": [1.0]})
        result = compute_ranks(df)
        assert result["rank"].dtype == pl.Int32


# =========================================================================
# compute_derived_metrics
# =========================================================================


class TestComputeDerivedMetrics:
    """Tests for compute_derived_metrics."""

    def _make_ranked_df(
        self,
        prices: list[float],
        prev_closes: list[float],
        volumes: list[float],
        prev_volumes: list[float] | None = None,
    ) -> pl.DataFrame:
        n = len(prices)
        data = {
            "ticker": [f"T{i}" for i in range(n)],
            "changePercent": [0.0] * n,
            "rank": list(range(1, n + 1)),
            "price": prices,
            "prev_close": prev_closes,
            "volume": volumes,
        }
        if prev_volumes is not None:
            data["prev_volume"] = prev_volumes
        return pl.DataFrame(data)

    def test_change_calculation(self):
        """change = price - prev_close."""
        df = self._make_ranked_df(prices=[105.0], prev_closes=[100.0], volumes=[1000.0])
        vt = VolumeTracker()
        result = compute_derived_metrics(df, datetime(2026, 1, 1, 9, 30), vt)
        assert result["change"][0] == pytest.approx(5.0)

    def test_relative_volume_daily(self):
        """relativeVolumeDaily = volume / prev_volume."""
        df = self._make_ranked_df(
            prices=[100.0],
            prev_closes=[100.0],
            volumes=[2000.0],
            prev_volumes=[1000.0],
        )
        vt = VolumeTracker()
        result = compute_derived_metrics(df, datetime(2026, 1, 1, 9, 30), vt)
        assert result["relativeVolumeDaily"][0] == pytest.approx(2.0)

    def test_relative_volume_daily_no_prev(self):
        """Without prev_volume column → relativeVolumeDaily = 0.0."""
        df = self._make_ranked_df(prices=[100.0], prev_closes=[100.0], volumes=[2000.0])
        vt = VolumeTracker()
        result = compute_derived_metrics(df, datetime(2026, 1, 1, 9, 30), vt)
        assert result["relativeVolumeDaily"][0] == 0.0

    def test_relative_volume_daily_zero_prev(self):
        """prev_volume = 0 → relativeVolumeDaily = 0.0 (avoid division by zero)."""
        df = self._make_ranked_df(
            prices=[100.0],
            prev_closes=[100.0],
            volumes=[2000.0],
            prev_volumes=[0.0],
        )
        vt = VolumeTracker()
        result = compute_derived_metrics(df, datetime(2026, 1, 1, 9, 30), vt)
        assert result["relativeVolumeDaily"][0] == 0.0

    def test_relative_volume_5min_column_added(self):
        """relativeVolume5min column should always be present in output."""
        df = self._make_ranked_df(prices=[100.0], prev_closes=[100.0], volumes=[1000.0])
        vt = VolumeTracker()
        result = compute_derived_metrics(df, datetime(2026, 1, 1, 9, 30), vt)
        assert "relativeVolume5min" in result.columns

    def test_multiple_rows(self):
        """Should compute metrics for all rows."""
        df = self._make_ranked_df(
            prices=[105.0, 95.0],
            prev_closes=[100.0, 100.0],
            volumes=[2000.0, 500.0],
            prev_volumes=[1000.0, 1000.0],
        )
        vt = VolumeTracker()
        result = compute_derived_metrics(df, datetime(2026, 1, 1, 9, 30), vt)
        assert result["change"].to_list() == [pytest.approx(5.0), pytest.approx(-5.0)]
        assert result["relativeVolumeDaily"][0] == pytest.approx(2.0)
        assert result["relativeVolumeDaily"][1] == pytest.approx(0.5)


# =========================================================================
# compute_weighted_mid_price
# =========================================================================


class TestComputeWeightedMidPrice:
    """Tests for compute_weighted_mid_price."""

    def test_basic_weighted_mid(self):
        """Standard case: weighted mid = (bid*ask_size + ask*bid_size) / (bid_size+ask_size)."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "price": [150.0],
                "bid": [149.0],
                "ask": [151.0],
                "bid_size": [100.0],
                "ask_size": [100.0],
            }
        )
        result = compute_weighted_mid_price(df)
        # Equal sizes → simple midpoint: (149*100 + 151*100) / 200 = 150.0
        assert result["price"][0] == pytest.approx(150.0)

    def test_asymmetric_sizes(self):
        """Heavier ask_size → price skews toward bid."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "price": [150.0],
                "bid": [149.0],
                "ask": [151.0],
                "bid_size": [100.0],
                "ask_size": [300.0],
            }
        )
        result = compute_weighted_mid_price(df)
        expected = (149.0 * 300 + 151.0 * 100) / (100 + 300)
        assert result["price"][0] == pytest.approx(expected)

    def test_zero_sizes_fallback(self):
        """Zero bid_size + ask_size → falls back to original price."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "price": [150.0],
                "bid": [149.0],
                "ask": [151.0],
                "bid_size": [0.0],
                "ask_size": [0.0],
            }
        )
        result = compute_weighted_mid_price(df)
        assert result["price"][0] == pytest.approx(150.0)

    def test_zero_bid_fallback(self):
        """bid = 0 → falls back to original price."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL"],
                "price": [150.0],
                "bid": [0.0],
                "ask": [151.0],
                "bid_size": [100.0],
                "ask_size": [100.0],
            }
        )
        result = compute_weighted_mid_price(df)
        assert result["price"][0] == pytest.approx(150.0)

    def test_missing_quote_columns(self):
        """Without bid/ask columns → returns DataFrame unchanged."""
        df = pl.DataFrame({"ticker": ["AAPL"], "price": [150.0]})
        result = compute_weighted_mid_price(df)
        assert result["price"][0] == pytest.approx(150.0)
        assert "bid" not in result.columns

    def test_multiple_rows_mixed(self):
        """Mix of valid and invalid quote data."""
        df = pl.DataFrame(
            {
                "ticker": ["AAPL", "TSLA"],
                "price": [150.0, 200.0],
                "bid": [149.0, 0.0],  # TSLA bid=0 → fallback
                "ask": [151.0, 201.0],
                "bid_size": [100.0, 100.0],
                "ask_size": [100.0, 100.0],
            }
        )
        result = compute_weighted_mid_price(df)
        assert result["price"][0] == pytest.approx(150.0)  # valid → weighted mid
        assert result["price"][1] == pytest.approx(200.0)  # fallback to original
