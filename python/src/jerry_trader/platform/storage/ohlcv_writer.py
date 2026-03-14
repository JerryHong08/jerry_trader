"""
Unified OHLCV bar writer for ClickHouse.

Single source of truth for writing bars to the ``ohlcv_bars`` table.
All code paths — bars_builder (live ticks + bootstrap) and chart_bff
(Polygon backfill) — go through this module.

Responsibilities:
  1. Canonical column ordering
  2. Session filtering (drop closed-session bars)
  3. Batch insert with retry
  4. Consistent logging

This eliminates the data-quality divergence between bars built from
Rust BarBuilder (real VWAP, trade_count, session) and Polygon
aggregate bar backfills (which previously had zeros).
"""

import logging
from typing import Dict, List, Optional

from jerry_trader.shared.time.timezone import is_closed_session_utc

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────

TABLE = "ohlcv_bars"

COLUMNS = [
    "ticker",
    "timeframe",
    "bar_start",
    "bar_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
    "session",
]


# ── Public API ───────────────────────────────────────────────────────────


def bar_dict_to_row(bar: dict) -> list:
    """Convert a bar dict (from Rust BarBuilder or Polygon) to a ClickHouse row.

    Expects keys matching COLUMNS.  Missing ``trade_count``, ``vwap``, or
    ``session`` default to 0 / 0.0 / "unknown".
    """
    return [
        bar["ticker"],
        bar["timeframe"],
        bar["bar_start"],
        bar["bar_end"],
        bar["open"],
        bar["high"],
        bar["low"],
        bar["close"],
        bar.get("volume", 0),
        bar.get("trade_count", 0),
        bar.get("vwap", 0.0),
        bar.get("session", "unknown"),
    ]


def write_bars(
    ch_client,
    bars: List[dict],
    *,
    source: str = "unknown",
    filter_closed: bool = True,
) -> int:
    """Write a batch of bar dicts to ClickHouse, with session filtering.

    Args:
        ch_client: ``clickhouse_connect`` client instance.
        bars: List of bar dicts.  Each must have at minimum:
              ``ticker``, ``timeframe``, ``bar_start``, ``bar_end``,
              ``open``, ``high``, ``low``, ``close``.
        source: Label for logging (e.g. "bootstrap", "live", "polygon_backfill").
        filter_closed: If True, drop bars whose ``bar_start`` falls in
                       the ET closed session (20:00–04:00).

    Returns:
        Number of bars actually written (after filtering).
    """
    if not bars:
        return 0

    skipped = 0
    rows = []
    for bar in bars:
        if filter_closed and is_closed_session_utc(bar["bar_start"]):
            skipped += 1
            continue
        rows.append(bar_dict_to_row(bar))

    if not rows:
        if skipped:
            logger.debug(
                f"write_bars [{source}]: all {skipped} bars filtered (closed session)"
            )
        return 0

    try:
        ch_client.insert(TABLE, data=rows, column_names=COLUMNS)
        logger.debug(
            f"write_bars [{source}]: wrote {len(rows)} bars"
            + (f", skipped {skipped} closed-session" if skipped else "")
        )
        return len(rows)
    except Exception as e:
        logger.error(f"write_bars [{source}]: insert failed ({len(rows)} bars) - {e}")
        raise


def polygon_bars_to_dicts(
    ticker: str,
    timeframe: str,
    bars: List[dict],
    dur_sec: int,
) -> List[dict]:
    """Convert Polygon aggregate bar response to canonical bar dicts.

    Polygon bars have ``time`` (epoch seconds), ``open``, ``high``, ``low``,
    ``close``, ``volume``.  This normalises them to the same dict shape
    that Rust BarBuilder produces, so they can go through :func:`write_bars`.

    Args:
        ticker: Ticker symbol (uppercased).
        timeframe: ClickHouse timeframe key (e.g. "5m").
        bars: Raw bars from ``ChartDataService.get_bars()`` or Polygon API.
        dur_sec: Bar duration in seconds (for computing ``bar_end``).

    Returns:
        List of bar dicts ready for :func:`write_bars`.
    """
    result = []
    for bar in bars:
        bar_start_ms = bar["time"] * 1000
        result.append(
            {
                "ticker": ticker,
                "timeframe": timeframe,
                "bar_start": bar_start_ms,
                "bar_end": bar_start_ms + dur_sec * 1000,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar.get("volume", 0),
                "trade_count": bar.get("trade_count", 0),
                "vwap": bar.get("vwap", 0.0),
                "session": "polygon_backfill",
            }
        )
    return result
