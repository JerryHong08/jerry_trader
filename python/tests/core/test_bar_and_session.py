"""Tests for domain/market/bar.py and domain/session.py.

Covers:
  - Bar construction and __post_init__ validation (OHLC, non-negative, timeframe)
  - Bar properties (range, body, is_bullish, is_bearish, is_doji, period)
  - Bar.merge() with VWAP calculation and precondition checks
  - Bar.from_rust_dict / to_clickhouse_dict / to_event_dict serialization
  - BarPeriod (contains, is_complete, from_timestamp, duration, end_ms)
  - SessionPhase enum and get_session_phase / is_effective_window
"""

from __future__ import annotations

import pytest

from jerry_trader.domain.market.bar import TIMEFRAME_LABELS, Bar, BarPeriod, Timeframe
from jerry_trader.domain.session import (
    SessionPhase,
    get_session_phase,
    get_session_phase_from_epoch_ms,
    get_session_phase_from_ns,
    is_effective_window,
)

# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════


def _bar(**overrides) -> Bar:
    """Create a valid default bar, override specific fields."""
    defaults = dict(
        symbol="TEST",
        timeframe="1m",
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        volume=5000.0,
        trade_count=50,
        vwap=100.5,
        bar_start=1700000000000,
        bar_end=1700000060000,
        session="premarket",
    )
    defaults.update(overrides)
    return Bar(**defaults)


# ══════════════════════════════════════════════════════════════════════
# Bar __post_init__ validation
# ══════════════════════════════════════════════════════════════════════


class TestBarValidation:
    def test_valid_bar_constructs(self):
        bar = _bar()
        assert bar.symbol == "TEST"
        assert bar.close == 101.0

    def test_high_lt_low_raises(self):
        with pytest.raises(ValueError, match="High .* < Low"):
            _bar(high=98.0, low=100.0)

    def test_high_lt_max_open_close_raises(self):
        """high must be >= max(open, close)."""
        with pytest.raises(ValueError, match="High .* < max"):
            _bar(open=100.0, close=105.0, high=103.0, low=99.0)

    def test_low_gt_min_open_close_raises(self):
        """low must be <= min(open, close)."""
        with pytest.raises(ValueError, match="Low .* > min"):
            _bar(open=100.0, close=95.0, high=102.0, low=98.0)

    def test_high_equal_to_max_open_close_ok(self):
        """high == max(open, close) is valid."""
        bar = _bar(open=100.0, close=105.0, high=105.0, low=99.0)
        assert bar.high == 105.0

    def test_low_equal_to_min_open_close_ok(self):
        """low == min(open, close) is valid."""
        bar = _bar(open=100.0, close=95.0, high=102.0, low=95.0)
        assert bar.low == 95.0

    def test_negative_volume_raises(self):
        with pytest.raises(ValueError, match="Volume must be non-negative"):
            _bar(volume=-1.0)

    def test_negative_trade_count_raises(self):
        with pytest.raises(ValueError, match="Trade count must be non-negative"):
            _bar(trade_count=-1)

    def test_invalid_timeframe_raises(self):
        with pytest.raises(ValueError, match="Invalid timeframe"):
            _bar(timeframe="2m")

    def test_all_valid_timeframes_construct(self):
        """Every Timeframe literal should construct a valid bar."""
        for tf in TIMEFRAME_LABELS:
            bar = _bar(timeframe=tf)
            assert bar.timeframe == tf

    def test_zero_volume_valid(self):
        """Zero volume is allowed (no trades in period)."""
        bar = _bar(volume=0.0, trade_count=0, vwap=0.0)
        assert bar.volume == 0.0

    def test_doji_bar_valid(self):
        """Flat bar with open==close==high==low is valid."""
        bar = _bar(open=100.0, high=100.0, low=100.0, close=100.0)
        assert bar.range == 0.0


# ══════════════════════════════════════════════════════════════════════
# Bar properties
# ══════════════════════════════════════════════════════════════════════


class TestBarProperties:
    def test_range(self):
        bar = _bar(high=105.0, low=100.0)
        assert bar.range == 5.0

    def test_body_bullish(self):
        bar = _bar(open=100.0, close=103.0, high=103.0)
        assert bar.body == 3.0

    def test_body_bearish(self):
        bar = _bar(open=103.0, close=100.0, high=103.0)
        assert bar.body == 3.0

    def test_body_doji(self):
        bar = _bar(open=100.0, close=100.0)
        assert bar.body == 0.0

    def test_is_bullish(self):
        assert _bar(open=100.0, close=101.0).is_bullish is True
        assert _bar(open=100.0, close=99.0).is_bullish is False

    def test_is_bearish(self):
        assert _bar(open=100.0, close=99.0).is_bearish is True
        assert _bar(open=100.0, close=101.0).is_bearish is False

    def test_is_doji_zero_range(self):
        """Zero-range bar is always doji."""
        bar = _bar(open=100.0, high=100.0, low=100.0, close=100.0)
        assert bar.is_doji is True

    def test_is_doji_tight_body(self):
        """Body < 10% of range by default."""
        bar = _bar(open=100.0, close=100.1, high=102.0, low=99.0)
        assert bar.is_doji is True  # body=0.1, range=3.0, ratio=0.033

    def test_is_doji_wide_body(self):
        """Body > 10% of range."""
        bar = _bar(open=100.0, close=101.0, high=102.0, low=99.0)
        assert bar.is_doji is False  # body=1.0, range=3.0, ratio=0.33

    def test_is_doji_body_well_over_threshold(self):
        """Body at 20% of range exceeds default 10% threshold → not doji."""
        bar = _bar(open=100.0, close=101.0, high=105.0, low=100.0)
        # body=1.0, range=5.0, ratio=0.2 > 0.1
        assert bar.is_doji is False

    def test_period_returns_barperiod(self):
        bar = _bar(bar_start=1700000000000, timeframe="1m")
        period = bar.period
        assert isinstance(period, BarPeriod)
        assert period.start_ms == 1700000000000
        assert period.timeframe == "1m"


# ══════════════════════════════════════════════════════════════════════
# Bar.merge()
# ══════════════════════════════════════════════════════════════════════


class TestBarMerge:
    def test_merge_combines_ohlc(self):
        a = _bar(
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=1000.0,
            vwap=100.5,
            trade_count=10,
            bar_start=1000,
            bar_end=1060,
        )
        b = _bar(
            open=100.0,
            high=103.0,
            low=98.0,
            close=102.0,
            volume=500.0,
            vwap=101.5,
            trade_count=5,
            bar_start=1000,
            bar_end=1060,
        )

        merged = a.merge(b)
        assert merged.open == 100.0  # Earlier bar's open
        assert merged.close == 102.0  # Later bar's close
        assert merged.high == 103.0  # max(102, 103)
        assert merged.low == 98.0  # min(99, 98)
        assert merged.volume == 1500.0
        assert merged.trade_count == 15

    def test_merge_vwap_weighted(self):
        """VWAP is weighted by volume."""
        a = _bar(volume=1000.0, vwap=100.0, bar_start=1000, bar_end=1060)
        b = _bar(volume=500.0, vwap=103.0, bar_start=1000, bar_end=1060)

        merged = a.merge(b)
        # (100 * 1000 + 103 * 500) / 1500 = 101.0
        assert merged.vwap == pytest.approx(101.0)

    def test_merge_zero_total_volume(self):
        """When both bars have zero volume, VWAP is 0."""
        a = _bar(volume=0.0, vwap=0.0, trade_count=0, bar_start=1000, bar_end=1060)
        b = _bar(volume=0.0, vwap=0.0, trade_count=0, bar_start=1000, bar_end=1060)

        merged = a.merge(b)
        assert merged.volume == 0.0
        assert merged.vwap == 0.0

    def test_merge_different_timeframe_raises(self):
        a = _bar(timeframe="1m", bar_start=1000, bar_end=1060)
        b = _bar(timeframe="5m", bar_start=1000, bar_end=1060)

        with pytest.raises(ValueError, match="different timeframes"):
            a.merge(b)

    def test_merge_different_bar_start_raises(self):
        a = _bar(bar_start=1000, bar_end=1060)
        b = _bar(bar_start=2000, bar_end=2060)

        with pytest.raises(ValueError, match="different periods"):
            a.merge(b)

    def test_merge_preserves_symbol_and_session(self):
        a = _bar(symbol="AAPL", session="regular", bar_start=1000, bar_end=1060)
        b = _bar(symbol="AAPL", session="regular", bar_start=1000, bar_end=1060)

        merged = a.merge(b)
        assert merged.symbol == "AAPL"
        assert merged.session == "regular"


# ══════════════════════════════════════════════════════════════════════
# Bar.from_rust_dict / to_clickhouse_dict / to_event_dict
# ══════════════════════════════════════════════════════════════════════


class TestBarSerialization:
    def test_from_rust_dict_maps_fields(self):
        d = {
            "ticker": "AAPL",
            "timeframe": "1m",
            "open": 150.0,
            "high": 151.0,
            "low": 149.5,
            "close": 150.8,
            "volume": 10000.0,
            "trade_count": 200,
            "vwap": 150.3,
            "bar_start": 1700000000000,
            "bar_end": 1700000060000,
            "session": "regular",
        }
        bar = Bar.from_rust_dict(d)
        assert bar.symbol == "AAPL"
        assert bar.timeframe == "1m"
        assert bar.close == 150.8
        assert bar.trade_count == 200

    def test_to_clickhouse_dict_uses_ticker_key(self):
        bar = _bar(symbol="MSFT")
        d = bar.to_clickhouse_dict()
        assert d["ticker"] == "MSFT"
        assert d["timeframe"] == "1m"
        assert "symbol" not in d
        assert d["bar_start"] == bar.bar_start
        assert d["bar_end"] == bar.bar_end

    def test_to_event_dict_uses_ticker_key(self):
        bar = _bar(symbol="MSFT")
        d = bar.to_event_dict()
        assert d["ticker"] == "MSFT"
        assert "bar_end" not in d  # Event dict excludes bar_end

    def test_roundtrip_rust_dict(self):
        """from_rust_dict → to_clickhouse_dict preserves key data."""
        original = _bar(
            symbol="GOOG",
            timeframe="5m",
            open=140.0,
            high=142.0,
            low=139.0,
            close=141.0,
            volume=8000.0,
            trade_count=80,
            vwap=140.5,
            bar_start=1700000000000,
            bar_end=1700000300000,
            session="regular",
        )
        rust_dict = {
            "ticker": original.symbol,
            "timeframe": original.timeframe,
            "open": original.open,
            "high": original.high,
            "low": original.low,
            "close": original.close,
            "volume": original.volume,
            "trade_count": original.trade_count,
            "vwap": original.vwap,
            "bar_start": original.bar_start,
            "bar_end": original.bar_end,
            "session": original.session,
        }
        bar = Bar.from_rust_dict(rust_dict)
        assert bar.symbol == original.symbol
        assert bar.close == original.close
        assert bar.vwap == original.vwap


# ══════════════════════════════════════════════════════════════════════
# BarPeriod
# ══════════════════════════════════════════════════════════════════════


class TestBarPeriod:
    def test_duration_1m(self):
        bp = BarPeriod(timeframe="1m", start_ms=0)
        assert bp.duration_ms == 60_000

    def test_duration_10s(self):
        bp = BarPeriod(timeframe="10s", start_ms=0)
        assert bp.duration_ms == 10_000

    def test_duration_1d(self):
        bp = BarPeriod(timeframe="1d", start_ms=0)
        assert bp.duration_ms == 86_400_000

    def test_end_ms(self):
        bp = BarPeriod(timeframe="1m", start_ms=1000)
        assert bp.end_ms == 61_000

    def test_contains_inside(self):
        bp = BarPeriod(timeframe="1m", start_ms=60_000)
        assert bp.contains(60_000) is True  # Start boundary
        assert bp.contains(90_000) is True  # Middle
        assert bp.contains(119_999) is True  # Just before end

    def test_contains_outside(self):
        bp = BarPeriod(timeframe="1m", start_ms=60_000)
        assert bp.contains(59_999) is False  # Before start
        assert bp.contains(120_000) is False  # Exactly end (exclusive)

    def test_is_complete(self):
        bp = BarPeriod(timeframe="1m", start_ms=60_000)
        assert bp.is_complete(120_000) is True  # At end
        assert bp.is_complete(120_001) is True  # After end
        assert bp.is_complete(119_999) is False  # Before end

    def test_from_timestamp_1m(self):
        bp = BarPeriod.from_timestamp(600_123, "1m")
        assert bp.start_ms == 600_000
        assert bp.timeframe == "1m"

    def test_from_timestamp_5m(self):
        bp = BarPeriod.from_timestamp(1_800_500, "5m")
        assert bp.start_ms == 1_800_000

    def test_from_timestamp_exact_boundary(self):
        """Timestamp exactly at boundary → that bar's start."""
        bp = BarPeriod.from_timestamp(120_000, "1m")
        assert bp.start_ms == 120_000

    def test_str_repr(self):
        bp = BarPeriod(timeframe="1m", start_ms=600_000)
        assert "1m" in str(bp)
        assert "600000" in str(bp)

    def test_all_timeframes_have_duration(self):
        """All Timeframe literals must have a duration entry."""
        for tf in TIMEFRAME_LABELS:
            bp = BarPeriod(timeframe=tf, start_ms=0)
            assert bp.duration_ms > 0


# ══════════════════════════════════════════════════════════════════════
# Bar immutability (frozen=True)
# ══════════════════════════════════════════════════════════════════════


class TestBarImmutability:
    def test_cannot_set_attribute(self):
        bar = _bar()
        with pytest.raises(Exception):
            bar.close = 999.0  # type: ignore[misc]


# ══════════════════════════════════════════════════════════════════════
# SessionPhase enum
# ══════════════════════════════════════════════════════════════════════


class TestSessionPhaseEnum:
    def test_values(self):
        assert SessionPhase.EARLY.value == "early"
        assert SessionPhase.MID.value == "mid"
        assert SessionPhase.LATE.value == "late"

    def test_from_string(self):
        assert SessionPhase("early") == SessionPhase.EARLY
        assert SessionPhase("mid") == SessionPhase.MID


# ══════════════════════════════════════════════════════════════════════
# get_session_phase (datetime ET)
# ══════════════════════════════════════════════════════════════════════


class TestGetSessionPhase:
    def test_early_before_7am(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 4, 0, 0, tzinfo=et)  # 4:00 AM
        assert get_session_phase(t) == SessionPhase.EARLY

    def test_early_at_655am(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 6, 55, 0, tzinfo=et)
        assert get_session_phase(t) == SessionPhase.EARLY

    def test_mid_at_7am(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 7, 0, 0, tzinfo=et)
        assert get_session_phase(t) == SessionPhase.MID

    def test_mid_at_830am(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 8, 30, 0, tzinfo=et)
        assert get_session_phase(t) == SessionPhase.MID

    def test_mid_at_855am(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 8, 55, 0, tzinfo=et)
        assert get_session_phase(t) == SessionPhase.MID

    def test_late_at_9am(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 9, 0, 0, tzinfo=et)
        assert get_session_phase(t) == SessionPhase.LATE

    def test_late_at_920am(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 9, 20, 0, tzinfo=et)
        assert get_session_phase(t) == SessionPhase.LATE


# ══════════════════════════════════════════════════════════════════════
# get_session_phase_from_ns
# ══════════════════════════════════════════════════════════════════════


class TestGetSessionPhaseFromNs:
    def test_noon_utc_is_morning_et(self):
        """12:00 UTC = 7:00 AM ET → MID."""
        # 2026-03-06 12:00 UTC = 2026-03-06 07:00 ET (EST, UTC-5)
        import calendar

        ns = calendar.timegm((2026, 3, 6, 12, 0, 0)) * 1_000_000_000
        assert get_session_phase_from_ns(ns) == SessionPhase.MID

    def test_4am_utc_is_late_night_et(self):
        """4:00 UTC = 11:00 PM ET (previous day) → LATE? No, hour 23 >= 9 → LATE.
        Actually 4:00 UTC on March 6 = 11:00 PM ET on March 5 → hour=23 → LATE."""
        import calendar

        ns = calendar.timegm((2026, 3, 6, 4, 0, 0)) * 1_000_000_000
        result = get_session_phase_from_ns(ns)
        # 4:00 UTC = 23:00 ET (previous day) → hour 23 >= 9 → LATE
        assert result in (SessionPhase.LATE, SessionPhase.EARLY)
        # Actually: 2026-03-06 04:00 UTC = 2026-03-05 23:00 ET.
        # datetime.fromtimestamp gives 2026-03-06 04:00 UTC.
        # astimezone gives 2026-03-05 23:00 ET → hour=23 ≥ 9 → LATE
        assert result == SessionPhase.LATE


# ══════════════════════════════════════════════════════════════════════
# get_session_phase_from_epoch_ms
# ══════════════════════════════════════════════════════════════════════


class TestGetSessionPhaseFromEpochMs:
    def test_delegates_to_ns(self):
        """epoch_ms * 1_000_000 → same phase as ns version."""
        import calendar

        epoch_ms = calendar.timegm((2026, 3, 6, 12, 0, 0)) * 1000
        assert get_session_phase_from_epoch_ms(epoch_ms) == SessionPhase.MID

    def test_early_morning_utc_is_early_et(self):
        """9:00 UTC = 4:00 AM ET → EARLY."""
        import calendar

        epoch_ms = calendar.timegm((2026, 3, 6, 9, 0, 0)) * 1000
        assert get_session_phase_from_epoch_ms(epoch_ms) == SessionPhase.EARLY


# ══════════════════════════════════════════════════════════════════════
# is_effective_window
# ══════════════════════════════════════════════════════════════════════


class TestIsEffectiveWindow:
    def test_mid_is_effective(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 7, 30, 0, tzinfo=et)
        assert is_effective_window(t) is True

    def test_early_not_effective(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 5, 0, 0, tzinfo=et)
        assert is_effective_window(t) is False

    def test_late_not_effective(self):
        from datetime import datetime

        import pytz

        et = pytz.timezone("America/New_York")
        t = datetime(2026, 3, 6, 9, 15, 0, tzinfo=et)
        assert is_effective_window(t) is False
