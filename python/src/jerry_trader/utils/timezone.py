"""
Timezone conversion utilities for US/Eastern ↔ UTC.

The Rust BarBuilder operates on "ET milliseconds" — a shifted epoch where
ms-of-day arithmetic yields the correct US/Eastern wall-clock time.  These
helpers convert between real UTC epoch-ms and that shifted representation,
handling DST transitions via zoneinfo.

All functions are pure (no side effects, no state) and safe to call from
any thread.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


# ── Logging helpers ──────────────────────────────────────────────────


def ms_to_et(ts_ms: int) -> str:
    """Convert UTC epoch-ms to ``'YYYY-MM-DD HH:MM:SS.mmm ET'`` for logging."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=_ET)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " ET"


def ms_to_et_short(ts_ms: int) -> str:
    """Convert UTC epoch-ms to ``'HH:MM:SS.mmm'`` (ET) for compact logging."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=_ET)
    return dt.strftime("%H:%M:%S.%f")[:-3]


def ms_to_readable(ts_ms: int, tz: str = "UTC") -> str:
    """Convert epoch-ms to a human-readable timestamp string.

    Args:
        ts_ms: Timestamp in milliseconds since Unix epoch.
        tz: Timezone name (default ``"UTC"``, or e.g. ``"America/New_York"``).

    Returns:
        ``'YYYY-MM-DD HH:MM:SS.mmm TZ'``
    """
    if tz == "UTC":
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
    else:
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=ZoneInfo(tz))
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + f" {tz}"


# ── Epoch-ms conversion ─────────────────────────────────────────────


def utc_ms_to_et_ms(utc_ms: int) -> int:
    """Convert UTC epoch-ms to ET epoch-ms (shifted representation).

    The Rust BarBuilder expects timestamps where ``ms % 86_400_000`` yields
    the US/Eastern time-of-day.  This function applies the ET offset
    (−5 h EST / −4 h EDT) so modular arithmetic works.

    Example (EST, UTC−5)::

        UTC 14:00  →  ET 09:00
        1710511200000  →  1710511200000 + (−5 × 3600 × 1000)  =  1710493200000
    """
    utc_dt = datetime.fromtimestamp(utc_ms / 1000, tz=timezone.utc)
    et_dt = utc_dt.astimezone(_ET)
    offset_seconds = et_dt.utcoffset().total_seconds()  # type: ignore[union-attr]
    return utc_ms + int(offset_seconds * 1000)


def et_ms_to_utc_ms(et_ms: int) -> int:
    """Convert ET epoch-ms back to UTC epoch-ms.

    Reverses :func:`utc_ms_to_et_ms`.  Interprets *et_ms* as a pseudo-UTC
    value with the wall-clock digits of the ET moment, then re-localises
    it to recover the true UTC instant.
    """
    pseudo_utc_dt = datetime.fromtimestamp(et_ms / 1000, tz=timezone.utc)
    et_naive = pseudo_utc_dt.replace(tzinfo=None)
    try:
        et_dt = et_naive.replace(tzinfo=_ET)
    except Exception:
        et_dt = pseudo_utc_dt.astimezone(_ET)
    utc_dt = et_dt.astimezone(timezone.utc)
    return int(utc_dt.timestamp() * 1000)


# ── Bar dict helpers ─────────────────────────────────────────────────


def convert_bar_et_to_utc(bar: dict) -> dict:
    """Convert ``bar_start`` / ``bar_end`` from ET-ms to UTC-ms **in-place**.

    Returns the same dict for chaining convenience.
    """
    bar["bar_start"] = et_ms_to_utc_ms(bar["bar_start"])
    bar["bar_end"] = et_ms_to_utc_ms(bar["bar_end"])
    return bar


def is_closed_session_utc(bar_start_ms: int) -> bool:
    """Return True if *bar_start_ms* (UTC) falls in the ET closed session (20:00–04:00).

    Useful for filtering out bars that shouldn't exist (e.g. Polygon backfill
    artefacts outside trading hours).
    """
    et_dt = datetime.fromtimestamp(bar_start_ms / 1000, tz=timezone.utc).astimezone(_ET)
    et_hour = et_dt.hour + et_dt.minute / 60.0
    return et_hour >= 20.0 or et_hour < 4.0
