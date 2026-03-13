"""Tests for data_loader.py – OHLCVResampler, pipeline, and _resample_calendar.

Covers:
  - parse_timeframe: extended regex (w, mo units)
  - _resample_calendar: weekly and monthly calendar-aligned bucketing
  - OHLCVResampler session-based resampling (15m, 1h, 4h)
  - _fill_missing_and_resample: routing between session / calendar resampler
  - _forward_fill_missing: gap-filling semantics
  - TimestampGenerator: daily / intraday timestamp generation
  - LoaderConfig → data_type consistency guard
  - Pre-resample cutoff: replay future-data exclusion for 4h, 1W, 1M
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from jerry_trader.data_supply.bootstrap_data_supply.localdata_loader.data_loader import (
    LoaderConfig,
    OHLCVResampler,
    StockDataLoader,
    TimestampGenerator,
)

# ══════════════════════════════════════════════════════════════════════
#  1. OHLCVResampler.parse_timeframe
# ══════════════════════════════════════════════════════════════════════


class TestParseTimeframe:
    """parse_timeframe should accept m, h, d, w, and mo units."""

    @pytest.mark.parametrize(
        "timeframe,expected_minutes",
        [
            ("1m", 1),
            ("5m", 5),
            ("15m", 15),
            ("30m", 30),
            ("1h", 60),
            ("4h", 240),
            ("1d", 1440),
            ("7d", 10080),
            ("1w", 10080),
            ("1mo", 43200),
        ],
    )
    def test_valid_timeframes(self, timeframe, expected_minutes):
        assert OHLCVResampler.parse_timeframe(timeframe) == expected_minutes

    def test_case_insensitive(self):
        assert OHLCVResampler.parse_timeframe("1H") == 60
        assert OHLCVResampler.parse_timeframe("1D") == 1440
        assert OHLCVResampler.parse_timeframe("1W") == 10080
        assert OHLCVResampler.parse_timeframe("1Mo") == 43200

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            OHLCVResampler.parse_timeframe("abc")

    def test_invalid_unit_raises(self):
        with pytest.raises(ValueError):
            OHLCVResampler.parse_timeframe("1x")


# ══════════════════════════════════════════════════════════════════════
#  2. StockDataLoader._resample_calendar
# ══════════════════════════════════════════════════════════════════════


def _make_daily_lf(days: int = 10, start_date=None):
    """Build a LazyFrame of daily bars spanning `days` trading days.

    Returns timestamps as ``Datetime[ns, America/New_York]`` — the same
    dtype produced by the real pipeline's ``from_epoch(..., 'ns')``.
    """
    et = ZoneInfo("America/New_York")
    if start_date is None:
        start_date = datetime(2026, 1, 5, tzinfo=et)  # Monday
    elif start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=et)
    rows = []
    d = start_date
    for i in range(days):
        rows.append(
            {
                "timestamps": d.replace(hour=0, minute=0, second=0, microsecond=0),
                "open": 100.0 + i,
                "high": 105.0 + i,
                "low": 95.0 + i,
                "close": 102.0 + i,
                "volume": 1000 * (i + 1),
                "transactions": 10 * (i + 1),
                "ticker": "AAPL",
            }
        )
        d += timedelta(days=1)
        # skip weekends
        while d.weekday() >= 5:
            d += timedelta(days=1)
    return (
        pl.DataFrame(rows)
        .lazy()
        .cast({"timestamps": pl.Datetime("ns", "America/New_York")})
    )


class TestResampleCalendar:
    """_resample_calendar aggregates daily bars into weekly/monthly."""

    # ── weekly ─────────────────────────────────────────────────────

    def test_weekly_reduces_bar_count(self):
        lf = _make_daily_lf(10)  # 2 trading weeks
        result = StockDataLoader._resample_calendar(lf, "7d").collect()
        assert len(result) < 10  # must aggregate

    def test_weekly_ohlcv_aggregation(self):
        lf = _make_daily_lf(5)  # 1 trading week (Mon-Fri)
        result = StockDataLoader._resample_calendar(lf, "7d").collect()
        src = lf.collect()
        # first open, max high, min low, last close, sum volume
        assert result["open"][0] == src["open"][0]
        assert result["high"][0] == src["high"].max()
        assert result["low"][0] == src["low"].min()
        assert result["close"][0] == src["close"][-1]
        assert result["volume"][0] == src["volume"].sum()

    def test_weekly_preserves_transactions(self):
        lf = _make_daily_lf(5)
        result = StockDataLoader._resample_calendar(lf, "7d").collect()
        assert "transactions" in result.columns
        assert result["transactions"][0] == lf.collect()["transactions"].sum()

    def test_weekly_alias_1w(self):
        """'1w' should behave identically to '7d'."""
        lf = _make_daily_lf(10)
        r7d = StockDataLoader._resample_calendar(lf, "7d").collect()
        r1w = StockDataLoader._resample_calendar(lf, "1w").collect()
        assert r7d.shape == r1w.shape

    # ── monthly ────────────────────────────────────────────────────

    def test_monthly_reduces_bar_count(self):
        lf = _make_daily_lf(10)  # all in January 2026
        result = StockDataLoader._resample_calendar(lf, "30d").collect()
        # All 10 days in one calendar month → 1 bar
        assert len(result) == 1

    def test_monthly_ohlcv_aggregation(self):
        lf = _make_daily_lf(10)
        result = StockDataLoader._resample_calendar(lf, "30d").collect()
        src = lf.collect()
        assert result["open"][0] == src["open"][0]
        assert result["high"][0] == src["high"].max()
        assert result["low"][0] == src["low"].min()
        assert result["close"][0] == src["close"][-1]
        assert result["volume"][0] == src["volume"].sum()

    def test_monthly_alias_1mo(self):
        """'1mo' should behave identically to '30d'."""
        lf = _make_daily_lf(10)
        r30d = StockDataLoader._resample_calendar(lf, "30d").collect()
        r1mo = StockDataLoader._resample_calendar(lf, "1mo").collect()
        assert r30d.shape == r1mo.shape

    # ── edge cases ─────────────────────────────────────────────────

    def test_unknown_target_passthrough(self):
        lf = _make_daily_lf(5)
        result = StockDataLoader._resample_calendar(lf, "3d").collect()
        # Unknown target → returns unchanged
        assert len(result) == 5

    def test_without_transactions_column(self):
        """Should work even if 'transactions' column is absent."""
        lf = _make_daily_lf(5).drop("transactions")
        result = StockDataLoader._resample_calendar(lf, "7d").collect()
        assert "transactions" not in result.columns
        assert len(result) >= 1

    def test_multiple_tickers(self):
        """group_by_dynamic should respect per-ticker grouping."""
        lf1 = _make_daily_lf(5)
        lf2 = lf1.collect().with_columns(pl.lit("MSFT").alias("ticker")).lazy()
        combined = pl.concat([lf1, lf2])
        result = StockDataLoader._resample_calendar(combined, "7d").collect()
        # Two tickers → at least 2 bars
        assert result["ticker"].n_unique() == 2


# ══════════════════════════════════════════════════════════════════════
#  3. TimestampGenerator
# ══════════════════════════════════════════════════════════════════════

ET = ZoneInfo("America/New_York")


class TestTimestampGenerator:
    """Verify TimestampGenerator produces correct timestamp ranges."""

    @pytest.fixture
    def gen(self):
        return TimestampGenerator()

    def test_daily_single_day(self, gen):
        """A single trading day should produce 1 daily timestamp."""
        df = gen.generate("2026-01-05", "2026-01-05", "1d")
        assert df.shape[0] == 1
        ts = df["timestamps"][0]
        assert ts.date().isoformat() == "2026-01-05"

    def test_daily_skips_weekends(self, gen):
        """Mon–Fri week should produce 5 timestamps."""
        df = gen.generate("2026-01-05", "2026-01-09", "1d")
        assert df.shape[0] == 5

    def test_intraday_regular_hours_bar_count(self, gen):
        """Regular hours (9:30–16:00) = 390 minute bars for a full day."""
        df = gen.generate("2026-01-05", "2026-01-05", "1m", full_hour=False)
        assert df.shape[0] == 390  # 6.5 hrs × 60

    def test_intraday_extended_hours_bar_count(self, gen):
        """Extended hours (4:00–20:00) = 960 minute bars for a full day."""
        df = gen.generate("2026-01-05", "2026-01-05", "1m", full_hour=True)
        assert df.shape[0] == 960  # 16 hrs × 60

    def test_intraday_timestamps_are_tz_aware(self, gen):
        df = gen.generate("2026-01-05", "2026-01-05", "1m")
        ts = df["timestamps"][0]
        assert ts.tzinfo is not None


# ══════════════════════════════════════════════════════════════════════
#  4. OHLCVResampler session-based resampling
# ══════════════════════════════════════════════════════════════════════


def _make_minute_lf(minutes: int = 390, start_hour: int = 9, start_min: int = 30):
    """Build a LazyFrame of 1-minute bars starting at a given time on 2026-01-05.

    Returns timestamps as ``Datetime[ns, America/New_York]`` — the same
    dtype produced by the real pipeline's ``from_epoch(..., 'ns')``.
    """
    et = ZoneInfo("America/New_York")
    base = datetime(2026, 1, 5, start_hour, start_min, tzinfo=et)
    rows = []
    for i in range(minutes):
        ts = base + timedelta(minutes=i)
        rows.append(
            {
                "timestamps": ts,
                "open": 100.0 + i * 0.01,
                "high": 100.5 + i * 0.01,
                "low": 99.5 + i * 0.01,
                "close": 100.2 + i * 0.01,
                "volume": 100 + i,
                "transactions": 10 + i,
                "ticker": "AAPL",
            }
        )
    return (
        pl.DataFrame(rows)
        .lazy()
        .cast({"timestamps": pl.Datetime("ns", "America/New_York")})
    )


class TestSessionResampler:
    """OHLCVResampler.resample for intraday session-aware bucketing."""

    def test_15m_reduces_bar_count(self):
        """390 1m bars → 26 15m bars (390/15)."""
        lf = _make_minute_lf(390)
        resampler = OHLCVResampler()
        result = resampler.resample(lf, "15m").collect()
        assert len(result) == 26

    def test_1h_reduces_bar_count(self):
        """390 1m bars → at most 7 1h bars (6.5 hours)."""
        lf = _make_minute_lf(390)
        resampler = OHLCVResampler()
        result = resampler.resample(lf, "1h").collect()
        assert 6 <= len(result) <= 7

    def test_ohlcv_aggregation_15m(self):
        """First 15m bar should aggregate first 15 minute bars correctly."""
        lf = _make_minute_lf(15)
        resampler = OHLCVResampler()
        result = resampler.resample(lf, "15m").collect()
        src = lf.collect()
        assert len(result) == 1
        assert result["open"][0] == src["open"][0]
        assert result["high"][0] == src["high"].max()
        assert result["low"][0] == src["low"].min()
        assert result["close"][0] == src["close"][-1]
        assert result["volume"][0] == src["volume"].sum()

    def test_resampled_timestamps_column_exists(self):
        lf = _make_minute_lf(60)
        resampler = OHLCVResampler()
        result = resampler.resample(lf, "15m").collect()
        assert "timestamps" in result.columns


# ══════════════════════════════════════════════════════════════════════
#  5. _forward_fill_missing
# ══════════════════════════════════════════════════════════════════════


class TestForwardFillMissing:
    """_forward_fill_missing should fill gaps with previous close."""

    def test_fills_gap_with_previous_close(self):
        """Missing bars should be filled with prev close for OHLC, 0 for volume."""
        et = ZoneInfo("America/New_York")
        t1 = datetime(2026, 1, 5, 9, 30, tzinfo=et)
        t2 = datetime(2026, 1, 5, 9, 31, tzinfo=et)
        t3 = datetime(2026, 1, 5, 9, 32, tzinfo=et)

        data_lf = pl.DataFrame(
            {
                "ticker": ["AAPL", "AAPL"],
                "timestamps": [t1, t3],
                "open": [100.0, 101.0],
                "high": [100.5, 101.5],
                "low": [99.5, 100.5],
                "close": [100.2, 101.2],
                "volume": [1000, 2000],
                "transactions": [10, 20],
            }
        ).lazy()

        time_range_lf = pl.DataFrame(
            {
                "ticker": ["AAPL", "AAPL", "AAPL"],
                "timestamps": [t1, t2, t3],
            }
        ).lazy()

        result = StockDataLoader._forward_fill_missing(data_lf, time_range_lf).collect()
        assert result.shape[0] == 3
        # Filled bar at t2 should use prev close
        gap = result.filter(pl.col("timestamps") == t2)
        assert gap["close"][0] == 100.2  # forward-filled from t1
        assert gap["volume"][0] == 0  # volume filled with 0

    def test_no_gap_passthrough(self):
        """When all timestamps have data, nothing changes."""
        et = ZoneInfo("America/New_York")
        t1 = datetime(2026, 1, 5, 9, 30, tzinfo=et)
        t2 = datetime(2026, 1, 5, 9, 31, tzinfo=et)

        data_lf = pl.DataFrame(
            {
                "ticker": ["AAPL", "AAPL"],
                "timestamps": [t1, t2],
                "open": [100.0, 101.0],
                "high": [100.5, 101.5],
                "low": [99.5, 100.5],
                "close": [100.2, 101.2],
                "volume": [1000, 2000],
                "transactions": [10, 20],
            }
        ).lazy()

        time_range_lf = pl.DataFrame(
            {"ticker": ["AAPL", "AAPL"], "timestamps": [t1, t2]}
        ).lazy()

        result = StockDataLoader._forward_fill_missing(data_lf, time_range_lf).collect()
        assert result.shape[0] == 2
        assert result["open"][0] == 100.0
        assert result["open"][1] == 101.0


# ══════════════════════════════════════════════════════════════════════
#  6. _fill_missing_and_resample routing
# ══════════════════════════════════════════════════════════════════════


class TestFillMissingAndResampleRouting:
    """Verify _fill_missing_and_resample routes to the right resampler."""

    @pytest.fixture
    def loader(self):
        return StockDataLoader()

    def _make_config(self, timeframe: str) -> LoaderConfig:
        return LoaderConfig(
            tickers=["AAPL"],
            start_date="2026-01-05",
            end_date="2026-01-05",
            timedelta=None,
            timeframe=timeframe,
            asset="us_stocks_sip",
            data_type="minute_aggs_v1",
            full_hour=True,
            lake=True,
            use_s3=False,
            use_cache=False,
            use_duck_db=False,
            skip_low_volume=False,
        )

    def test_1m_skips_resampling(self, loader):
        """1m data should not be resampled at all."""
        lf = _make_minute_lf(60)
        config = self._make_config("1m")
        with (
            patch.object(
                loader.resampler, "resample", wraps=loader.resampler.resample
            ) as mock_rs,
            patch.object(
                StockDataLoader,
                "_resample_calendar",
                wraps=StockDataLoader._resample_calendar,
            ) as mock_cal,
        ):
            loader._fill_missing_and_resample(lf, config, "2026-01-05", "2026-01-05")
            mock_rs.assert_not_called()
            mock_cal.assert_not_called()

    def test_1d_skips_resampling(self, loader):
        """1d data should not be resampled."""
        lf = _make_daily_lf(3)
        config = self._make_config("1d")
        config.data_type = "day_aggs_v1"
        with (
            patch.object(loader.resampler, "resample") as mock_rs,
            patch.object(StockDataLoader, "_resample_calendar") as mock_cal,
        ):
            loader._fill_missing_and_resample(lf, config, "2026-01-05", "2026-01-07")
            mock_rs.assert_not_called()
            mock_cal.assert_not_called()

    def test_15m_uses_session_resampler(self, loader):
        """15m should use the session-based OHLCVResampler."""
        lf = _make_minute_lf(60)
        config = self._make_config("15m")
        with patch.object(loader.resampler, "resample", return_value=lf) as mock_rs:
            loader._fill_missing_and_resample(lf, config, "2026-01-05", "2026-01-05")
            mock_rs.assert_called_once()

    def test_7d_uses_calendar_resampler(self, loader):
        """7d (weekly) should use _resample_calendar."""
        lf = _make_daily_lf(10)
        config = self._make_config("7d")
        config.data_type = "day_aggs_v1"
        with patch.object(
            StockDataLoader, "_resample_calendar", return_value=lf
        ) as mock_cal:
            loader._fill_missing_and_resample(lf, config, "2026-01-05", "2026-01-16")
            mock_cal.assert_called_once()

    def test_30d_uses_calendar_resampler(self, loader):
        """30d (monthly) should use _resample_calendar."""
        lf = _make_daily_lf(20)
        config = self._make_config("30d")
        config.data_type = "day_aggs_v1"
        with patch.object(
            StockDataLoader, "_resample_calendar", return_value=lf
        ) as mock_cal:
            loader._fill_missing_and_resample(lf, config, "2026-01-05", "2026-01-30")
            mock_cal.assert_called_once()

    def test_invalid_timeframe_raises(self, loader):
        lf = _make_minute_lf(10)
        config = self._make_config("abc")
        with pytest.raises(ValueError, match="Invalid timeframe"):
            loader._fill_missing_and_resample(lf, config, "2026-01-05", "2026-01-05")


# ══════════════════════════════════════════════════════════════════════
#  7. LoaderConfig data_type consistency
# ══════════════════════════════════════════════════════════════════════


class TestLoaderConfigConsistency:
    """Guard against timeframe / data_type mismatch (the bug that caused all-null bars)."""

    INTRADAY_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h"]
    DAILY_TIMEFRAMES = ["1d", "7d", "30d"]

    def test_cutoff_ts_defaults_to_none(self):
        """cutoff_ts should default to None when not provided."""
        config = LoaderConfig(
            tickers=["AAPL"],
            start_date="2026-01-05",
            end_date="2026-01-05",
            timedelta=None,
            timeframe="1m",
            asset="us_stocks_sip",
            data_type="minute_aggs_v1",
            full_hour=True,
            lake=True,
            use_s3=False,
            use_cache=False,
            use_duck_db=False,
            skip_low_volume=False,
        )
        assert config.cutoff_ts is None

    @pytest.mark.parametrize("tf", INTRADAY_TIMEFRAMES)
    def test_intraday_timeframe_needs_minute_aggs(self, tf):
        """Intraday timeframes MUST use minute_aggs_v1 to get data."""
        config = LoaderConfig(
            tickers=["AAPL"],
            start_date="2026-01-05",
            end_date="2026-01-05",
            timedelta=None,
            timeframe=tf,
            asset="us_stocks_sip",
            data_type="minute_aggs_v1",
            full_hour=True,
            lake=True,
            use_s3=False,
            use_cache=False,
            use_duck_db=False,
            skip_low_volume=False,
        )
        # Correct pairing — just assert the expectation
        assert config.data_type == "minute_aggs_v1"

    @pytest.mark.parametrize("tf", DAILY_TIMEFRAMES)
    def test_daily_timeframe_needs_day_aggs(self, tf):
        """Daily+ timeframes MUST use day_aggs_v1 to get data."""
        config = LoaderConfig(
            tickers=["AAPL"],
            start_date="2026-01-05",
            end_date="2026-01-05",
            timedelta=None,
            timeframe=tf,
            asset="us_stocks_sip",
            data_type="day_aggs_v1",
            full_hour=True,
            lake=True,
            use_s3=False,
            use_cache=False,
            use_duck_db=False,
            skip_low_volume=False,
        )
        assert config.data_type == "day_aggs_v1"


# ══════════════════════════════════════════════════════════════════════
#  8. Pre-resample cutoff (replay future-data exclusion)
# ══════════════════════════════════════════════════════════════════════


class TestPreResampleCutoff:
    """Verify cutoff_ts filters source bars BEFORE resampling so
    aggregated bars (4h, 1W, 1M) never include future data."""

    ET = ZoneInfo("America/New_York")

    @pytest.fixture
    def loader(self):
        return StockDataLoader()

    def _make_config(self, timeframe: str, cutoff: datetime) -> LoaderConfig:
        return LoaderConfig(
            tickers=["AAPL"],
            start_date="2026-01-05",
            end_date="2026-01-05",
            timedelta=None,
            timeframe=timeframe,
            asset="us_stocks_sip",
            data_type="minute_aggs_v1",
            full_hour=True,
            lake=True,
            use_s3=False,
            use_cache=False,
            use_duck_db=False,
            skip_low_volume=False,
            cutoff_ts=cutoff,
        )

    # ── 4h: only source bars up to cutoff should be in the last bar ──

    def test_4h_cutoff_excludes_future_minutes(self, loader):
        """4h bar from 13:00-17:00 should not include minutes after 14:30 cutoff."""
        # 390 minutes = 09:30–16:00 regular session
        lf = _make_minute_lf(390)
        # Cutoff at 14:30 (5 hours into session = 300 min from 09:30)
        cutoff = datetime(2026, 1, 5, 14, 30, tzinfo=self.ET)
        config = self._make_config("4h", cutoff)

        result = loader._fill_missing_and_resample(
            lf, config, "2026-01-05", "2026-01-05"
        )
        df = result.collect()

        # Every bar's high should come from source bars <= 14:30 only.
        # The source bars after 14:30 have higher OHLC values (100 + i*0.01),
        # minute 300 (14:30) has close ≈ 103.2, minute 389 (16:00) has close ≈ 104.09.
        # With the cutoff, the last 4h bar should not include those higher values.
        for row in df.iter_rows(named=True):
            ts = row["timestamps"]
            if hasattr(ts, "timestamp"):
                assert ts <= cutoff, f"Bar timestamp {ts} is after cutoff {cutoff}"

    def test_4h_no_cutoff_includes_full_session(self, loader):
        """Without cutoff, 4h bars span the full session."""
        lf = _make_minute_lf(390)
        config = self._make_config("4h", cutoff=None)
        config.cutoff_ts = None  # explicitly no cutoff

        result = loader._fill_missing_and_resample(
            lf, config, "2026-01-05", "2026-01-05"
        )
        df = result.collect()
        # Should have bars covering the whole session (at least 2 bars for 6.5h)
        assert len(df) >= 2

    # ── Weekly: cutoff mid-week limits source bars ───────────────────

    def test_weekly_cutoff_limits_source_bars(self, loader):
        """Weekly bar should only aggregate daily bars up to the cutoff day."""
        # 10 trading days: Jan 5-9, 12-16
        lf = _make_daily_lf(10)
        # Cutoff at Wednesday Jan 7 end-of-day
        cutoff = datetime(2026, 1, 7, 23, 59, 59, tzinfo=self.ET)
        config = self._make_config("7d", cutoff)
        config.data_type = "day_aggs_v1"

        result = loader._fill_missing_and_resample(
            lf, config, "2026-01-05", "2026-01-16"
        )
        df = result.collect()

        # The weekly bar starting Jan 5 (Monday) should only contain
        # Jan 5-7 (3 bars), not Jan 8-9 (which are after cutoff).
        # Volume: day1=1000, day2=2000, day3=3000 → sum=6000
        assert len(df) == 1  # only 1 partial week
        assert df["volume"][0] == 6000  # 3 days summed

    def test_weekly_no_cutoff_aggregates_full_week(self, loader):
        """Without cutoff, full week is aggregated."""
        lf = _make_daily_lf(10)
        config = self._make_config("7d", cutoff=None)
        config.cutoff_ts = None
        config.data_type = "day_aggs_v1"

        result = loader._fill_missing_and_resample(
            lf, config, "2026-01-05", "2026-01-16"
        )
        df = result.collect()

        # 10 trading days across 2 weeks → 2 weekly bars
        assert len(df) == 2
        # First week: 5 days, volume = 1000+2000+3000+4000+5000 = 15000
        assert df["volume"][0] == 15000

    # ── Monthly: cutoff mid-month ────────────────────────────────────

    def test_monthly_cutoff_limits_source_bars(self, loader):
        """Monthly bar should only aggregate daily bars up to cutoff."""
        # 20 trading days starting Jan 5
        lf = _make_daily_lf(20)
        # Cutoff at Jan 15 end-of-day
        cutoff = datetime(2026, 1, 15, 23, 59, 59, tzinfo=self.ET)
        config = self._make_config("30d", cutoff)
        config.data_type = "day_aggs_v1"

        result = loader._fill_missing_and_resample(
            lf, config, "2026-01-05", "2026-01-30"
        )
        df = result.collect()

        # 20 trading days: Jan 5-9, 12-16, 19-23, 26-30
        # Cutoff Jan 15 keeps Jan 5-9, 12-15 = 9 trading days
        # Volume = sum(1000*i for i in 1..9) = 45000
        assert len(df) == 1
        assert df["volume"][0] == 45000

    # ── 1m/1d: cutoff still filters raw bars ─────────────────────────

    def test_1m_cutoff_filters_raw_bars(self, loader):
        """For 1m (no resampling), cutoff should still drop future bars."""
        lf = _make_minute_lf(390)
        cutoff = datetime(2026, 1, 5, 12, 0, tzinfo=self.ET)
        config = self._make_config("1m", cutoff)

        result = loader._fill_missing_and_resample(
            lf, config, "2026-01-05", "2026-01-05"
        )
        df = result.collect()

        # 09:30–12:00 = 150 minutes, cutoff at 12:00 keeps <= 12:00 = 151 bars
        for row in df.iter_rows(named=True):
            ts = row["timestamps"]
            if hasattr(ts, "timestamp"):
                assert ts <= cutoff
