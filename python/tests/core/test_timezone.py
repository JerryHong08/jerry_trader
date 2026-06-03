"""Tests for shared/time/timezone.py.

Covers:
  - ms_to_et / ms_to_et_short / ms_to_hhmmss formatting
  - ms_to_readable with UTC and named timezones
  - utc_ms_to_et_ms / et_ms_to_utc_ms conversion and roundtrip
  - convert_bar_et_to_utc in-place mutation
  - is_closed_session_utc boundary checks
  - parse_to_et_datetime with int/float/str/datetime/None
  - hhmm_to_epoch_ms with HH:MM / HHMMSS
  - DST boundaries (EST vs EDT offset)
"""

from __future__ import annotations

import calendar
import datetime

from jerry_trader.shared.time.timezone import (
    convert_bar_et_to_utc,
    et_ms_to_utc_ms,
    hhmm_to_epoch_ms,
    is_closed_session_utc,
    ms_to_et,
    ms_to_et_short,
    ms_to_hhmmss,
    ms_to_readable,
    parse_to_et_datetime,
    utc_ms_to_et_ms,
)

# ══════════════════════════════════════════════════════════════════════
# Helpers — fixed timestamps
# ══════════════════════════════════════════════════════════════════════

# 2026-03-06 12:00:00 UTC = 07:00:00 ET (EST, UTC-5)
_TS_MS_12UTC = calendar.timegm((2026, 3, 6, 12, 0, 0)) * 1000

# 2026-03-06 14:00:00 UTC = 09:00:00 ET (EST, UTC-5)
_TS_MS_14UTC = calendar.timegm((2026, 3, 6, 14, 0, 0)) * 1000

# 2026-03-06 09:30:00 UTC = 04:30:00 ET (EST)
_TS_MS_0930UTC = calendar.timegm((2026, 3, 6, 9, 30, 0)) * 1000

# 2026-03-06 00:00:00 UTC = 19:00:00 ET (previous day, EST)
_TS_MS_00UTC = calendar.timegm((2026, 3, 6, 0, 0, 0)) * 1000

# 2026-06-06 12:00:00 UTC = 08:00:00 ET (EDT, UTC-4)
_TS_MS_SUMMER = calendar.timegm((2026, 6, 6, 12, 0, 0)) * 1000


# ══════════════════════════════════════════════════════════════════════
# ms_to_et / ms_to_et_short / ms_to_hhmmss
# ══════════════════════════════════════════════════════════════════════


class TestMsToEt:
    def test_format_includes_date_and_et(self):
        result = ms_to_et(_TS_MS_12UTC)
        assert result.startswith("2026-03-06")
        assert result.endswith("ET")
        assert "07:00:00" in result

    def test_format_includes_milliseconds(self):
        ts = _TS_MS_12UTC + 123  # 12:00:00.123 UTC
        result = ms_to_et(ts)
        assert "07:00:00.123" in result


class TestMsToEtShort:
    def test_short_format_no_date(self):
        result = ms_to_et_short(_TS_MS_14UTC)
        # 14:00 UTC = 09:00 ET (EST)
        assert result == "09:00:00.000"

    def test_short_format_with_ms(self):
        ts = _TS_MS_14UTC + 456
        result = ms_to_et_short(ts)
        assert result == "09:00:00.456"


class TestMsToHhmmss:
    def test_hhmmss_format(self):
        result = ms_to_hhmmss(_TS_MS_12UTC)
        assert result == "07:00:00"

    def test_hhmmss_summer(self):
        # 12:00 UTC = 08:00 ET in summer (EDT)
        result = ms_to_hhmmss(_TS_MS_SUMMER)
        assert result == "08:00:00"


# ══════════════════════════════════════════════════════════════════════
# ms_to_readable
# ══════════════════════════════════════════════════════════════════════


class TestMsToReadable:
    def test_default_utc(self):
        result = ms_to_readable(_TS_MS_12UTC)
        assert "2026-03-06" in result
        assert "UTC" in result

    def test_et_timezone(self):
        result = ms_to_readable(_TS_MS_12UTC, tz="America/New_York")
        assert "07:00:00" in result  # 12 UTC = 07 ET
        assert "America/New_York" in result

    def test_custom_tz_label(self):
        result = ms_to_readable(_TS_MS_12UTC, tz="Asia/Tokyo")
        assert "Asia/Tokyo" in result


# ══════════════════════════════════════════════════════════════════════
# utc_ms_to_et_ms / et_ms_to_utc_ms
# ══════════════════════════════════════════════════════════════════════


class TestUtcEtConversion:
    def test_utc_to_et_est(self):
        """EST: UTC-5 → et_ms = utc_ms - 5*3600*1000."""
        et_ms = utc_ms_to_et_ms(_TS_MS_12UTC)
        assert et_ms == _TS_MS_12UTC - 5 * 3600_000

    def test_utc_to_et_edt(self):
        """EDT: UTC-4 → et_ms = utc_ms - 4*3600*1000."""
        et_ms = utc_ms_to_et_ms(_TS_MS_SUMMER)
        assert et_ms == _TS_MS_SUMMER - 4 * 3600_000

    def test_et_to_utc_est(self):
        """Reverse of utc_ms_to_et_ms."""
        et_ms = utc_ms_to_et_ms(_TS_MS_12UTC)
        utc_ms = et_ms_to_utc_ms(et_ms)
        assert utc_ms == _TS_MS_12UTC

    def test_et_to_utc_edt(self):
        et_ms = utc_ms_to_et_ms(_TS_MS_SUMMER)
        utc_ms = et_ms_to_utc_ms(et_ms)
        assert utc_ms == _TS_MS_SUMMER

    def test_roundtrip_utc_first(self):
        """utc → et → utc preserves original."""
        for ts_ms in (_TS_MS_12UTC, _TS_MS_14UTC, _TS_MS_00UTC, _TS_MS_SUMMER):
            et_ms = utc_ms_to_et_ms(ts_ms)
            back = et_ms_to_utc_ms(et_ms)
            assert back == ts_ms

    def test_roundtrip_et_first(self):
        """et → utc → et preserves original."""
        for ts_ms in (_TS_MS_12UTC, _TS_MS_14UTC, _TS_MS_SUMMER):
            et_input = utc_ms_to_et_ms(ts_ms)
            utc_ms = et_ms_to_utc_ms(et_input)
            et_back = utc_ms_to_et_ms(utc_ms)
            assert et_back == et_input


# ══════════════════════════════════════════════════════════════════════
# convert_bar_et_to_utc
# ══════════════════════════════════════════════════════════════════════


class TestConvertBarEtToUtc:
    def test_converts_both_fields(self):
        bar = {"bar_start": _TS_MS_12UTC, "bar_end": _TS_MS_14UTC}
        result = convert_bar_et_to_utc(bar)
        # After conversion, the values should be shifted back by offset
        expected_start = et_ms_to_utc_ms(_TS_MS_12UTC)
        expected_end = et_ms_to_utc_ms(_TS_MS_14UTC)
        assert bar["bar_start"] == expected_start
        assert bar["bar_end"] == expected_end
        assert result is bar  # Returns same dict

    def test_roundtrip_with_utc_to_et(self):
        """utc_ms_to_et_ms → convert_bar_et_to_utc should restore originals."""
        orig_start = calendar.timegm((2026, 3, 6, 12, 0, 0)) * 1000
        orig_end = calendar.timegm((2026, 3, 6, 12, 1, 0)) * 1000
        bar = {
            "bar_start": utc_ms_to_et_ms(orig_start),
            "bar_end": utc_ms_to_et_ms(orig_end),
        }
        convert_bar_et_to_utc(bar)
        assert bar["bar_start"] == orig_start
        assert bar["bar_end"] == orig_end


# ══════════════════════════════════════════════════════════════════════
# is_closed_session_utc
# ══════════════════════════════════════════════════════════════════════


class TestIsClosedSessionUtc:
    def test_noon_utc_is_open(self):
        """12:00 UTC = 07:00 ET → open session."""
        assert is_closed_session_utc(_TS_MS_12UTC) is False

    def test_2000_utc_is_closed(self):
        """20:00 UTC = 15:00 ET → open (15 < 20)."""
        ts = calendar.timegm((2026, 3, 6, 20, 0, 0)) * 1000
        assert is_closed_session_utc(ts) is False

    def test_0200_utc_is_closed(self):
        """02:00 UTC = 21:00 ET (previous day) → closed (21 >= 20)."""
        ts = calendar.timegm((2026, 3, 6, 2, 0, 0)) * 1000
        assert is_closed_session_utc(ts) is True

    def test_0900_utc_is_early_open(self):
        """09:00 UTC = 04:00 ET → open (at boundary, hour=4 >= 4)."""
        ts = calendar.timegm((2026, 3, 6, 9, 0, 0)) * 1000
        assert is_closed_session_utc(ts) is False

    def test_0500_utc_is_closed(self):
        """05:00 UTC = 00:00 ET → closed (hour=0 < 4)."""
        ts = calendar.timegm((2026, 3, 6, 5, 0, 0)) * 1000
        assert is_closed_session_utc(ts) is True

    def test_end_of_session_1930_utc(self):
        """19:30 UTC = 14:30 ET → open (14.5 < 20)."""
        ts = calendar.timegm((2026, 3, 6, 19, 30, 0)) * 1000
        assert is_closed_session_utc(ts) is False


# ══════════════════════════════════════════════════════════════════════
# parse_to_et_datetime
# ══════════════════════════════════════════════════════════════════════


class TestParseToEtDatetime:
    def test_int_epoch_seconds(self):
        sec = calendar.timegm((2026, 3, 6, 12, 0, 0))
        dt = parse_to_et_datetime(sec)
        assert dt.tzinfo is not None
        assert dt.hour == 7  # 12 UTC = 07 ET
        assert dt.minute == 0

    def test_int_epoch_ms(self):
        ms = calendar.timegm((2026, 3, 6, 12, 0, 0)) * 1000
        dt = parse_to_et_datetime(ms)
        assert dt.hour == 7
        assert dt.minute == 0

    def test_float_epoch_seconds(self):
        sec = float(calendar.timegm((2026, 3, 6, 12, 0, 0)))
        dt = parse_to_et_datetime(sec)
        assert dt.hour == 7

    def test_iso_string(self):
        dt = parse_to_et_datetime("2026-03-06T12:00:00Z")
        assert dt.hour == 7  # 12 UTC = 07 ET
        assert dt.tzinfo is not None

    def test_iso_string_with_offset(self):
        dt = parse_to_et_datetime("2026-03-06T07:00:00-05:00")
        assert dt.hour == 7

    def test_naive_datetime(self):
        naive = datetime.datetime(2026, 3, 6, 10, 0, 0)
        dt = parse_to_et_datetime(naive)
        assert dt.hour == 10
        assert dt.tzinfo is not None

    def test_aware_datetime(self):
        aware = datetime.datetime(2026, 3, 6, 12, 0, 0, tzinfo=datetime.timezone.utc)
        dt = parse_to_et_datetime(aware)
        assert dt.hour == 7  # Converted from UTC to ET

    def test_none_returns_now(self):
        dt = parse_to_et_datetime(None)
        assert dt.tzinfo is not None

    def test_numeric_string(self):
        sec = str(calendar.timegm((2026, 3, 6, 12, 0, 0)))
        dt = parse_to_et_datetime(sec)
        assert dt.hour == 7

    def test_invalid_type_returns_now(self):
        """Unknown types fall through to the default return."""
        dt = parse_to_et_datetime([])
        assert dt.tzinfo is not None


# ══════════════════════════════════════════════════════════════════════
# hhmm_to_epoch_ms
# ══════════════════════════════════════════════════════════════════════


class TestHhmmToEpochMs:
    def test_hhmm_colon_format(self):
        result = hhmm_to_epoch_ms("20260306", "09:30")
        # 09:30 ET → should be a valid epoch ms
        assert result > 0
        # Verify roundtrip
        assert ms_to_hhmmss(result) == "09:30:00"

    def test_hhmm_no_colon(self):
        result = hhmm_to_epoch_ms("20260306", "0930")
        assert ms_to_hhmmss(result) == "09:30:00"

    def test_hhmmss_format(self):
        result = hhmm_to_epoch_ms("20260306", "093000")
        assert ms_to_hhmmss(result) == "09:30:00"

    def test_timezone_aware(self):
        """hhmm_to_epoch_ms returns UTC ms, not ET ms."""
        result = hhmm_to_epoch_ms("20260306", "12:00")
        # 12:00 ET (EST) = 17:00 UTC
        utc_dt = datetime.datetime.fromtimestamp(
            result / 1000, tz=datetime.timezone.utc
        )
        assert utc_dt.hour == 17
        assert utc_dt.minute == 0

    def test_midnight(self):
        result = hhmm_to_epoch_ms("20260306", "00:00")
        assert ms_to_hhmmss(result) == "00:00:00"
