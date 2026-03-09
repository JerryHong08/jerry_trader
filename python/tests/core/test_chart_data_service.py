"""Tests for ChartDataService — OHLCV bar provider for frontend ChartModule.

Tests cover:
  - get_bars routing: replay mode → local, live mode → Polygon (no local fallback)
  - Clock integration: uses global clock (not datetime.now)
  - _to_local_timeframe mapping (all timeframes, including 1W→7d, 1M→30d)
  - _format_dataframe: Polygon → chart response dict conversion
  - _format_local_dataframe: local data → chart response dict conversion
  - _fill_intraday_gaps: forward-fill logic
  - get_timeframes: metadata for frontend dropdown
  - _log_result: logging helper (no crash)
  - _fetch_from_local: LoaderConfig assembly (data_type for daily vs minute)
  - Error paths: empty ticker, bad timeframe, empty results
"""

from datetime import datetime, timedelta
from typing import Dict, Optional
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from jerry_trader import clock
from jerry_trader.DataManager.chart_data_service import (
    CACHE_TTL,
    DEFAULT_LOOKBACK,
    REPLAY_LOOKBACK,
    TIMEFRAME_MAP,
    ChartDataService,
    _is_intraday,
)

# ── Fixtures ─────────────────────────────────────────────────────────

# Jan 15 2026, 09:30 ET → epoch ns
JAN15_0930_NS = 1_768_487_400_000_000_000


@pytest.fixture(autouse=True)
def reset_clock():
    """Ensure live mode after each test."""
    yield
    clock.set_live_mode()


@pytest.fixture
def service():
    """ChartDataService with a mocked CustomBarsFetcher (no real API/Redis)."""
    with patch(
        "jerry_trader.DataManager.chart_data_service.CustomBarsFetcher"
    ) as MockFetcher:
        mock_fetcher = MockFetcher.return_value
        mock_fetcher.bars_fetch.return_value = None  # default: no data
        svc = ChartDataService(session_id="test-session")
        yield svc


def _make_polygon_df(n: int = 3, start_ts_ms: int = 1_700_000_000_000) -> pl.DataFrame:
    """Build a minimal Polygon-style DataFrame with n bars."""
    rows = []
    for i in range(n):
        rows.append(
            {
                "timestamp": start_ts_ms + i * 60_000,
                "open": 100.0 + i,
                "high": 101.0 + i,
                "low": 99.0 + i,
                "close": 100.5 + i,
                "volume": 1000 * (i + 1),
                "vwap": 100.25 + i,
                "transactions": 50 + i,
            }
        )
    return pl.DataFrame(rows)


def _make_local_df(n: int = 3, start_dt: Optional[datetime] = None) -> pl.DataFrame:
    """Build a minimal local-loader-style DataFrame with n bars."""
    if start_dt is None:
        start_dt = datetime(2026, 1, 15, 9, 30)
    rows = []
    for i in range(n):
        rows.append(
            {
                "timestamps": start_dt + timedelta(minutes=i),
                "open": 100.0 + i,
                "high": 101.0 + i,
                "low": 99.0 + i,
                "close": 100.5 + i,
                "volume": 1000 * (i + 1),
                "ticker": "AAPL",
            }
        )
    return pl.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════
#  1. Timeframe mapping
# ══════════════════════════════════════════════════════════════════════


class TestToLocalTimeframe:
    """_to_local_timeframe maps frontend strings → local loader strings."""

    @pytest.mark.parametrize(
        "frontend, expected",
        [
            ("1m", "1m"),
            ("5m", "5m"),
            ("15m", "15m"),
            ("30m", "30m"),
            ("1h", "1h"),
            ("4h", "4h"),
            ("1D", "1d"),
            ("1W", "7d"),
            ("1M", "30d"),
        ],
    )
    def test_all_timeframes_have_mapping(self, frontend, expected):
        assert ChartDataService._to_local_timeframe(frontend) == expected

    def test_unknown_timeframe_returns_none(self):
        assert ChartDataService._to_local_timeframe("10s") is None
        assert ChartDataService._to_local_timeframe("2h") is None

    def test_every_frontend_timeframe_is_mapped(self):
        """All keys in TIMEFRAME_MAP should have a local mapping (no None)."""
        for tf in TIMEFRAME_MAP:
            result = ChartDataService._to_local_timeframe(tf)
            assert result is not None, f"Timeframe {tf} has no local mapping"


# ══════════════════════════════════════════════════════════════════════
#  2. Intraday helper
# ══════════════════════════════════════════════════════════════════════


class TestIsIntraday:

    def test_minute_is_intraday(self):
        assert _is_intraday("minute") is True

    def test_hour_is_intraday(self):
        assert _is_intraday("hour") is True

    def test_day_is_not_intraday(self):
        assert _is_intraday("day") is False

    def test_week_is_not_intraday(self):
        assert _is_intraday("week") is False


# ══════════════════════════════════════════════════════════════════════
#  3. get_bars routing: replay vs live
# ══════════════════════════════════════════════════════════════════════


class TestGetBarsRouting:
    """get_bars routes to local in replay mode, Polygon in live mode."""

    def test_replay_mode_calls_local_not_polygon(self, service):
        """In replay mode, _fetch_from_local is called, never _fetch_from_polygon."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        local_result = {
            "ticker": "AAPL",
            "timeframe": "1D",
            "bars": [
                {"time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
            ],
            "barCount": 1,
            "barDurationSec": 86400,
            "source": "local",
            "from": "2025-01-01",
            "to": "2026-01-15",
            "lastBarTime": 1,
        }

        with (
            patch.object(
                service, "_fetch_from_local", return_value=local_result
            ) as mock_local,
            patch.object(service, "_fetch_from_polygon") as mock_polygon,
        ):
            result = service.get_bars("AAPL", "1D")
            mock_local.assert_called_once()
            mock_polygon.assert_not_called()
            assert result["source"] == "local"

    def test_live_mode_calls_polygon_not_local(self, service):
        """In live mode, _fetch_from_polygon is called, never _fetch_from_local."""
        polygon_result = {
            "ticker": "AAPL",
            "timeframe": "1D",
            "bars": [
                {"time": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
            ],
            "barCount": 1,
            "barDurationSec": 86400,
            "source": "polygon",
            "from": "2025-01-01",
            "to": "2026-01-15",
            "lastBarTime": 1,
        }

        with (
            patch.object(
                service, "_fetch_from_polygon", return_value=polygon_result
            ) as mock_polygon,
            patch.object(service, "_fetch_from_local") as mock_local,
        ):
            result = service.get_bars("AAPL", "1D")
            mock_polygon.assert_called_once()
            mock_local.assert_not_called()
            assert result["source"] == "polygon"

    def test_live_mode_no_fallback_to_local(self, service):
        """Live mode returns None when Polygon fails — no local fallback."""
        with (
            patch.object(
                service, "_fetch_from_polygon", return_value=None
            ) as mock_polygon,
            patch.object(service, "_fetch_from_local") as mock_local,
        ):
            result = service.get_bars("AAPL", "1D")
            mock_polygon.assert_called_once()
            mock_local.assert_not_called()
            assert result is None

    def test_replay_mode_returns_none_on_local_miss(self, service):
        """Replay mode returns None when local data is not available."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        with patch.object(service, "_fetch_from_local", return_value=None):
            result = service.get_bars("AAPL", "1D")
            assert result is None


# ══════════════════════════════════════════════════════════════════════
#  4. Clock integration — uses global clock, not datetime.now
# ══════════════════════════════════════════════════════════════════════


class TestClockIntegration:
    """get_bars uses clock.now_datetime() for the default to_date."""

    def test_replay_mode_uses_clock_time_for_to_date(self, service):
        """In replay mode the to_date default comes from the replay clock,
        NOT from datetime.now()."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        with patch.object(
            service, "_fetch_from_local", return_value=None
        ) as mock_local:
            service.get_bars("AAPL", "1m")

            # _fetch_from_local receives to_date derived from clock.now_datetime()
            call_args = mock_local.call_args
            to_date_arg = call_args[0][
                3
            ]  # positional: ticker, timeframe, from_date, to_date, ...
            # ReplayClock was initialized to Jan 15 2026 09:30 ET
            assert "2026-01-15" == to_date_arg

    def test_replay_mode_uses_larger_lookback(self, service):
        """In replay mode, the lookback uses REPLAY_LOOKBACK (larger) instead
        of DEFAULT_LOOKBACK, giving the chart more historical context."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        with patch.object(
            service, "_fetch_from_local", return_value=None
        ) as mock_local:
            service.get_bars("AAPL", "1m")

            call_args = mock_local.call_args
            from_date_arg = call_args[0][2]
            to_date_arg = call_args[0][3]

            from_dt = datetime.strptime(from_date_arg, "%Y-%m-%d")
            to_dt = datetime.strptime(to_date_arg, "%Y-%m-%d")
            actual_days = (to_dt - from_dt).days

            # Should use REPLAY_LOOKBACK["1m"] = 5, not DEFAULT_LOOKBACK["1m"] = 1
            assert actual_days == REPLAY_LOOKBACK["1m"]
            assert actual_days > DEFAULT_LOOKBACK["1m"]

    def test_live_mode_uses_clock_time(self, service):
        """In live mode the clock still provides current time (falling through
        to time.time), not a hardcoded datetime.now()."""
        # Just verify it doesn't crash; in live mode clock.now_datetime() ≈ datetime.now()
        with patch.object(service, "_fetch_from_polygon", return_value=None):
            result = service.get_bars("AAPL", "1D")
            assert result is None  # No polygon data, just verify no crash


# ══════════════════════════════════════════════════════════════════════
#  5. Error handling in get_bars
# ══════════════════════════════════════════════════════════════════════


class TestGetBarsValidation:

    def test_empty_ticker(self, service):
        assert service.get_bars("") is None
        assert service.get_bars("  ") is None

    def test_unknown_timeframe(self, service):
        assert service.get_bars("AAPL", "10s") is None
        assert service.get_bars("AAPL", "2h") is None

    def test_explicit_date_range(self, service):
        """Explicit from/to dates are passed through correctly."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        with patch.object(
            service, "_fetch_from_local", return_value=None
        ) as mock_local:
            service.get_bars("AAPL", "1D", from_date="2025-06-01", to_date="2025-12-31")
            call_args = mock_local.call_args[0]
            assert call_args[2] == "2025-06-01"  # from_date
            assert call_args[3] == "2025-12-31"  # to_date


# ══════════════════════════════════════════════════════════════════════
#  6. _format_dataframe (Polygon response)
# ══════════════════════════════════════════════════════════════════════


class TestFormatDataframe:
    """Convert Polygon-style DataFrame to chart response dict."""

    def test_basic_conversion(self):
        df = _make_polygon_df(3)
        result = ChartDataService._format_dataframe(
            df, "AAPL", "1D", 86400, "2025-01-01", "2025-01-03", source="polygon"
        )

        assert result["ticker"] == "AAPL"
        assert result["timeframe"] == "1D"
        assert result["barCount"] == 3
        assert result["source"] == "polygon"
        assert result["from"] == "2025-01-01"
        assert result["to"] == "2025-01-03"

        # Check bar structure
        bar = result["bars"][0]
        assert "time" in bar
        assert "open" in bar
        assert "high" in bar
        assert "low" in bar
        assert "close" in bar
        assert "volume" in bar

    def test_timestamp_ms_to_seconds(self):
        """Polygon timestamps (ms) are converted to epoch seconds."""
        df = _make_polygon_df(1, start_ts_ms=1_700_000_000_000)
        result = ChartDataService._format_dataframe(
            df, "X", "1D", 86400, "2023-11-14", "2023-11-14"
        )
        assert result["bars"][0]["time"] == 1_700_000_000

    def test_empty_dataframe(self):
        """Empty DataFrame still produces valid response (0 bars)."""
        df = _make_polygon_df(0)
        result = ChartDataService._format_dataframe(
            df, "X", "1D", 86400, "2025-01-01", "2025-01-01"
        )
        assert result["barCount"] == 0
        assert result["bars"] == []
        assert result["lastBarTime"] is None

    def test_intraday_triggers_gap_fill(self):
        """Intraday timeframes (minute/hour) trigger _fill_intraday_gaps."""
        # Create 2 bars with a 3-minute gap → should fill 2 synthetic bars
        df = pl.DataFrame(
            [
                {
                    "timestamp": 1_700_000_000_000,
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 1000,
                },
                {
                    "timestamp": 1_700_000_180_000,
                    "open": 102,
                    "high": 103,
                    "low": 101,
                    "close": 102,
                    "volume": 2000,
                },
            ]
        )
        result = ChartDataService._format_dataframe(
            df, "X", "1m", 60, "2023-11-14", "2023-11-14"
        )
        # 2 real bars + 2 synthetic = 4
        assert result["barCount"] == 4

    def test_daily_does_not_gap_fill(self):
        """Daily timeframe does NOT trigger gap filling."""
        df = _make_polygon_df(2, start_ts_ms=1_700_000_000_000)
        # Overwrite to have a large gap
        df = pl.DataFrame(
            [
                {
                    "timestamp": 1_700_000_000_000,
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 1000,
                },
                {
                    "timestamp": 1_700_200_000_000,
                    "open": 102,
                    "high": 103,
                    "low": 101,
                    "close": 102,
                    "volume": 2000,
                },
            ]
        )
        result = ChartDataService._format_dataframe(
            df, "X", "1D", 86400, "2023-11-14", "2023-11-16"
        )
        assert result["barCount"] == 2  # No gap filling


# ══════════════════════════════════════════════════════════════════════
#  7. _format_local_dataframe
# ══════════════════════════════════════════════════════════════════════


class TestFormatLocalDataframe:
    """Convert local-loader-style DataFrame to chart response dict."""

    def test_basic_conversion(self):
        df = _make_local_df(3)
        result = ChartDataService._format_local_dataframe(
            df, "AAPL", "1m", 60, "2026-01-15", "2026-01-15"
        )

        assert result["ticker"] == "AAPL"
        assert result["timeframe"] == "1m"
        assert result["barCount"] == 3
        assert result["source"] == "local"

        bar = result["bars"][0]
        assert isinstance(bar["time"], int)
        assert bar["time"] > 0

    def test_filters_by_ticker(self):
        """If DataFrame has a 'ticker' column, filters to requested ticker."""
        df1 = _make_local_df(2)
        df2 = df1.with_columns(pl.lit("GOOG").alias("ticker"))
        combined = pl.concat([df1, df2])

        result = ChartDataService._format_local_dataframe(
            combined, "AAPL", "1m", 60, "2026-01-15", "2026-01-15"
        )
        assert result["barCount"] == 2  # Only AAPL, not GOOG

    def test_sorted_by_timestamps(self):
        """Output bars are sorted by timestamps ascending."""
        dt1 = datetime(2026, 1, 15, 10, 0)
        dt2 = datetime(2026, 1, 15, 9, 30)
        df = pl.DataFrame(
            [
                {
                    "timestamps": dt1,
                    "open": 102,
                    "high": 103,
                    "low": 101,
                    "close": 102,
                    "volume": 2000,
                    "ticker": "A",
                },
                {
                    "timestamps": dt2,
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 1000,
                    "ticker": "A",
                },
            ]
        )
        result = ChartDataService._format_local_dataframe(
            df, "A", "1m", 60, "2026-01-15", "2026-01-15"
        )
        assert result["bars"][0]["time"] < result["bars"][1]["time"]

    def test_null_ohlcv_rows_are_dropped(self):
        """Rows with None OHLCV values (from forward-fill gaps) are skipped."""
        df = pl.DataFrame(
            [
                {
                    "timestamps": datetime(2026, 1, 15, 9, 30),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": 0,
                    "ticker": "X",
                },
                {
                    "timestamps": datetime(2026, 1, 15, 9, 31),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                    "ticker": "X",
                },
                {
                    "timestamps": datetime(2026, 1, 15, 9, 32),
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": 0,
                    "ticker": "X",
                },
            ]
        )
        result = ChartDataService._format_local_dataframe(
            df, "X", "1m", 60, "2026-01-15", "2026-01-15"
        )
        # Only the non-null row survives
        assert result["barCount"] == 1
        assert result["bars"][0]["open"] == 100.0

    def test_null_volume_treated_as_zero(self):
        """A null volume value should become 0, not crash."""
        df = pl.DataFrame(
            [
                {
                    "timestamps": datetime(2026, 1, 15, 9, 30),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": None,
                    "ticker": "X",
                },
            ]
        )
        result = ChartDataService._format_local_dataframe(
            df, "X", "1m", 60, "2026-01-15", "2026-01-15"
        )
        assert result["bars"][0]["volume"] == 0

    def test_cutoff_prunes_future_bars(self):
        """Bars after cutoff_epoch_s are excluded (replay-time pruning)."""
        dt_930 = datetime(2026, 1, 15, 9, 30)
        dt_931 = datetime(2026, 1, 15, 9, 31)
        dt_932 = datetime(2026, 1, 15, 9, 32)
        df = pl.DataFrame(
            [
                {
                    "timestamps": dt_930,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1000,
                    "ticker": "A",
                },
                {
                    "timestamps": dt_931,
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 2000,
                    "ticker": "A",
                },
                {
                    "timestamps": dt_932,
                    "open": 102.0,
                    "high": 103.0,
                    "low": 101.0,
                    "close": 102.5,
                    "volume": 3000,
                    "ticker": "A",
                },
            ]
        )
        # Cutoff at 09:31 → should keep 09:30 and 09:31, drop 09:32
        cutoff = int(dt_931.timestamp())
        result = ChartDataService._format_local_dataframe(
            df, "A", "1m", 60, "2026-01-15", "2026-01-15", cutoff_epoch_s=cutoff
        )
        assert result["barCount"] == 2
        assert result["lastBarTime"] == cutoff

    def test_no_cutoff_keeps_all_bars(self):
        """Without cutoff, all bars are kept (live mode / explicit range)."""
        df = _make_local_df(5)
        result = ChartDataService._format_local_dataframe(
            df, "AAPL", "1m", 60, "2026-01-15", "2026-01-15", cutoff_epoch_s=None
        )
        assert result["barCount"] == 5

    def test_empty_df(self):
        df = pl.DataFrame(
            schema={
                "timestamps": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "ticker": pl.Utf8,
            }
        )
        result = ChartDataService._format_local_dataframe(
            df, "AAPL", "1m", 60, "2026-01-15", "2026-01-15"
        )
        assert result["barCount"] == 0
        assert result["lastBarTime"] is None


# ══════════════════════════════════════════════════════════════════════
#  8. _fill_intraday_gaps
# ══════════════════════════════════════════════════════════════════════


class TestFillIntradayGaps:

    def test_no_gaps(self):
        """Consecutive bars with exact spacing → no synthetic bars."""
        bars = [
            {"time": 1000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            {"time": 1060, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
            {"time": 1120, "open": 3, "high": 3, "low": 3, "close": 3, "volume": 3},
        ]
        filled = ChartDataService._fill_intraday_gaps(bars, 60)
        assert len(filled) == 3

    def test_one_gap(self):
        """One missing bar between two real bars → one synthetic bar."""
        bars = [
            {"time": 1000, "open": 1, "high": 1, "low": 1, "close": 10.0, "volume": 1},
            {"time": 1120, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
        ]
        filled = ChartDataService._fill_intraday_gaps(bars, 60)
        assert len(filled) == 3

        # Synthetic bar carries previous close
        synthetic = filled[1]
        assert synthetic["time"] == 1060
        assert synthetic["open"] == 10.0
        assert synthetic["high"] == 10.0
        assert synthetic["low"] == 10.0
        assert synthetic["close"] == 10.0
        assert synthetic["volume"] == 0

    def test_large_gap_capped_at_60(self):
        """Gaps larger than 60 * bar_duration are capped at 60 synthetic bars."""
        bars = [
            {"time": 0, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            {"time": 6000, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2},
        ]
        filled = ChartDataService._fill_intraday_gaps(bars, 60)
        # 6000/60 - 1 = 99 missing, capped to 60 synthetic + 2 real = 62
        assert len(filled) == 62

    def test_single_bar(self):
        bars = [{"time": 1000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
        filled = ChartDataService._fill_intraday_gaps(bars, 60)
        assert len(filled) == 1

    def test_empty_bars(self):
        filled = ChartDataService._fill_intraday_gaps([], 60)
        assert filled == []


# ══════════════════════════════════════════════════════════════════════
#  9. get_timeframes
# ══════════════════════════════════════════════════════════════════════


class TestGetTimeframes:

    def test_returns_all_timeframes(self, service):
        tfs = service.get_timeframes()
        values = {tf["value"] for tf in tfs}
        assert values == set(TIMEFRAME_MAP.keys())

    def test_each_has_required_keys(self, service):
        for tf in service.get_timeframes():
            assert "value" in tf
            assert "label" in tf
            assert "barDurationSec" in tf
            assert "defaultLookbackDays" in tf


# ══════════════════════════════════════════════════════════════════════
#  10. _fetch_from_local — LoaderConfig assembly
# ══════════════════════════════════════════════════════════════════════


class TestFetchFromLocalConfig:
    """Verify LoaderConfig is assembled correctly for different timeframes."""

    def test_minute_timeframe_uses_minute_aggs(self, service):
        """Minute-level timeframes use data_type='minute_aggs_v1'."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None  # no data
        service._local_loader = mock_loader

        # We need a real LoaderConfig — mock with a capture
        configs_captured = []
        original_LoaderConfig = None

        # Import and capture
        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        service._fetch_from_local("AAPL", "5m", "2026-01-01", "2026-01-15", 300)

        # Check the config passed to load()
        call_args = mock_loader.load.call_args
        config = call_args[0][0]
        assert config.timeframe == "5m"
        assert config.data_type == "minute_aggs_v1"

    def test_daily_timeframe_uses_day_aggs(self, service):
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None
        service._local_loader = mock_loader

        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        service._fetch_from_local("AAPL", "1D", "2025-01-01", "2025-12-31", 86400)
        config = mock_loader.load.call_args[0][0]
        assert config.timeframe == "1d"
        assert config.data_type == "day_aggs_v1"

    def test_weekly_timeframe_uses_day_aggs(self, service):
        """1W (→ 7d) uses day_aggs_v1; resampling handled by data_loader."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None
        service._local_loader = mock_loader

        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        service._fetch_from_local("AAPL", "1W", "2024-01-01", "2025-12-31", 604800)
        config = mock_loader.load.call_args[0][0]
        assert config.timeframe == "7d"
        assert config.data_type == "day_aggs_v1"

    def test_monthly_timeframe_uses_day_aggs(self, service):
        """1M (→ 30d) uses day_aggs_v1; resampling handled by data_loader."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None
        service._local_loader = mock_loader

        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        service._fetch_from_local("AAPL", "1M", "2020-01-01", "2025-12-31", 2592000)
        config = mock_loader.load.call_args[0][0]
        assert config.timeframe == "30d"
        assert config.data_type == "day_aggs_v1"

    def test_hourly_timeframe_uses_minute_aggs(self, service):
        """1h and 4h use minute_aggs_v1."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None
        service._local_loader = mock_loader

        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        for tf, expected_local in [("1h", "1h"), ("4h", "4h")]:
            service._fetch_from_local("AAPL", tf, "2025-12-01", "2026-01-15", 3600)
            config = mock_loader.load.call_args[0][0]
            assert config.timeframe == expected_local
            assert config.data_type == "minute_aggs_v1"


class TestFetchFromLocalCutoffPropagation:
    """Verify cutoff_epoch_s is converted to cutoff_ts on LoaderConfig."""

    def test_cutoff_propagated_to_config(self, service):
        """When cutoff_epoch_s is set, LoaderConfig.cutoff_ts should be a datetime."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None
        service._local_loader = mock_loader

        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        cutoff_s = int(clock.now_ms() / 1000)
        service._fetch_from_local(
            "AAPL",
            "4h",
            "2025-12-01",
            "2026-01-15",
            14400,
            cutoff_epoch_s=cutoff_s,
        )
        config = mock_loader.load.call_args[0][0]
        assert config.cutoff_ts is not None
        assert abs(int(config.cutoff_ts.timestamp()) - cutoff_s) <= 1

    def test_cutoff_disables_cache(self, service):
        """When cutoff is set, use_cache must be False."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None
        service._local_loader = mock_loader

        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        cutoff_s = int(clock.now_ms() / 1000)
        service._fetch_from_local(
            "AAPL",
            "1W",
            "2024-01-01",
            "2025-12-31",
            604800,
            cutoff_epoch_s=cutoff_s,
        )
        config = mock_loader.load.call_args[0][0]
        assert config.use_cache is False

    def test_no_cutoff_keeps_cache_enabled(self, service):
        """Without cutoff, use_cache should remain True."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        mock_loader = MagicMock()
        mock_loader.load.return_value = None
        service._local_loader = mock_loader

        from jerry_trader.DataSupply.bootstrapdataSupply.localdata_loader.data_loader import (
            LoaderConfig,
        )

        service._LoaderConfig = LoaderConfig

        service._fetch_from_local(
            "AAPL",
            "1D",
            "2025-01-01",
            "2025-12-31",
            86400,
        )
        config = mock_loader.load.call_args[0][0]
        assert config.cutoff_ts is None
        assert config.use_cache is True


class TestReplayCutoff:
    """Daily+ bars use midnight cutoff; intraday uses exact replay time."""

    def test_daily_cutoff_excludes_current_day(self, service):
        """For 1D timeframe, the cutoff is midnight of the replay day,
        so the incomplete current day bar is excluded."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        with patch.object(
            service, "_fetch_from_local", return_value=None
        ) as mock_local:
            service.get_bars("AAPL", "1D")
            call_kwargs = mock_local.call_args
            cutoff = call_kwargs[1].get("cutoff_epoch_s") or call_kwargs[0][5]
            # cutoff should be < the replay clock time (before 09:30 means
            # it's midnight-1 of Jan 15, i.e. end of Jan 14)
            replay_now_s = int(clock.now_ms() / 1000)
            assert cutoff < replay_now_s

    def test_intraday_cutoff_uses_exact_replay_time(self, service):
        """For 1m timeframe, the cutoff is the exact replay timestamp."""
        clock.init_replay(JAN15_0930_NS, speed=1.0)

        with patch.object(
            service, "_fetch_from_local", return_value=None
        ) as mock_local:
            service.get_bars("AAPL", "1m")
            call_kwargs = mock_local.call_args
            cutoff = call_kwargs[1].get("cutoff_epoch_s") or call_kwargs[0][5]
            replay_now_s = int(clock.now_ms() / 1000)
            # Intraday cutoff ≈ replay now (within 1 second)
            assert abs(cutoff - replay_now_s) <= 1


# ══════════════════════════════════════════════════════════════════════
#  12. _log_result smoke test
# ══════════════════════════════════════════════════════════════════════


class TestLogResult:

    def test_does_not_crash_with_bars(self):
        result = {
            "bars": [
                {
                    "time": 1_700_000_000,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                },
                {
                    "time": 1_700_000_060,
                    "open": 2,
                    "high": 2,
                    "low": 2,
                    "close": 2,
                    "volume": 2,
                },
            ]
        }
        ChartDataService._log_result("AAPL", "1m", "TEST", result)

    def test_does_not_crash_empty_bars(self):
        result = {"bars": []}
        ChartDataService._log_result("AAPL", "1m", "TEST", result)

    def test_does_not_crash_missing_bars(self):
        result = {}
        ChartDataService._log_result("AAPL", "1m", "TEST", result)


# ══════════════════════════════════════════════════════════════════════
#  13. Constants / config sanity checks
# ══════════════════════════════════════════════════════════════════════


class TestConstants:

    def test_timeframe_map_and_lookback_aligned(self):
        """Every TIMEFRAME_MAP key has a DEFAULT_LOOKBACK entry."""
        for tf in TIMEFRAME_MAP:
            assert tf in DEFAULT_LOOKBACK, f"{tf} missing from DEFAULT_LOOKBACK"

    def test_replay_lookback_has_all_timeframes(self):
        """Every TIMEFRAME_MAP key has a REPLAY_LOOKBACK entry."""
        for tf in TIMEFRAME_MAP:
            assert tf in REPLAY_LOOKBACK, f"{tf} missing from REPLAY_LOOKBACK"

    def test_replay_lookback_gte_default(self):
        """Replay lookback should be >= default lookback for every timeframe."""
        for tf in TIMEFRAME_MAP:
            assert REPLAY_LOOKBACK[tf] >= DEFAULT_LOOKBACK[tf], (
                f"REPLAY_LOOKBACK[{tf}]={REPLAY_LOOKBACK[tf]} < "
                f"DEFAULT_LOOKBACK[{tf}]={DEFAULT_LOOKBACK[tf]}"
            )

    def test_cache_ttl_has_all_timeframes(self):
        for tf in TIMEFRAME_MAP:
            assert tf in CACHE_TTL, f"{tf} missing from CACHE_TTL"

    def test_timeframe_map_values_are_reasonable(self):
        for tf, (mult, span, dur) in TIMEFRAME_MAP.items():
            assert mult > 0
            assert span in ("minute", "hour", "day", "week", "month")
            assert dur > 0
