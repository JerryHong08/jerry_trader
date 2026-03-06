"""
Pure computation functions for snapshot processing.

This module contains stateless compute functions and the VolumeTracker
stateful class. These are the **Rust migration targets** — every function
here has a 1:1 counterpart planned in ``rust/src/snapshot/``.

The orchestration layer (processor.py) calls these via the _bridge module,
which dispatches to the Rust implementation when available.

Functions:
    compute_ranks          — competition ranking by changePercent
    compute_derived_metrics — change, relativeVolumeDaily, relativeVolume5min
    compute_weighted_mid_price — bid/ask weighted mid-price

Classes:
    VolumeTracker          — sliding-window volume history for relativeVolume5min
"""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import polars as pl

# =========================================================================
# VolumeTracker — sliding-window volume history (Rust migration target)
# =========================================================================


class VolumeTracker:
    """
    Maintains per-ticker volume history for relativeVolume5min computation.

    Rust equivalent: ``jerry_trader._rust.VolumeTracker`` (pyclass).

    The tracker keeps a rolling 6-minute window of (timestamp, volume) pairs
    per ticker and computes:
        relativeVolume5min = last_1min_volume / last_5min_avg_volume
    """

    def __init__(self) -> None:
        # {ticker: [(timestamp, volume), ...]}
        self._history: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)

    @property
    def history(self) -> Dict[str, List[Tuple[datetime, float]]]:
        """Read access to volume history (for InfluxDB reload in processor)."""
        return self._history

    def reload_history(
        self, ticker: str, entries: List[Tuple[datetime, float]]
    ) -> None:
        """Bulk-load historical entries (used for recovery from InfluxDB)."""
        self._history[ticker].extend(entries)

    def compute_relative_volume_5min(
        self, ticker: str, timestamp: datetime, volume: float
    ) -> float:
        """Compute relativeVolume5min = last_1min_volume / last_5min_avg_volume."""
        self._history[ticker].append((timestamp, volume))

        # Keep only last 6 minutes of data
        cutoff_time = timestamp - timedelta(minutes=6)
        self._history[ticker] = [
            (ts, vol) for ts, vol in self._history[ticker] if ts >= cutoff_time
        ]

        history = self._history[ticker]
        if len(history) < 2:
            return 1.0

        # Calculate last 1 minute volume
        one_min_ago = timestamp - timedelta(minutes=1)
        volume_1min_ago = None
        for ts, vol in reversed(history):
            if ts <= one_min_ago:
                volume_1min_ago = vol
                break

        if volume_1min_ago is None:
            volume_1min_ago = history[0][1]

        last_1min_volume = max(0.0, volume - volume_1min_ago)

        # Calculate last 5 minute average
        five_min_ago = timestamp - timedelta(minutes=5)
        volume_5min_ago = None
        for ts, vol in history:
            if ts <= five_min_ago:
                volume_5min_ago = vol
            else:
                break

        if volume_5min_ago is None:
            volume_5min_ago = history[0][1]
            earliest_ts = history[0][0]
            time_span_minutes = max(
                1.0, (timestamp - earliest_ts).total_seconds() / 60.0
            )
        else:
            time_span_minutes = 5.0

        last_5min_total_volume = max(0.0, volume - volume_5min_ago)
        last_5min_avg_volume = last_5min_total_volume / time_span_minutes

        if last_5min_avg_volume > 0:
            return last_1min_volume / last_5min_avg_volume

        return 1.0

    def compute_batch(
        self,
        tickers: List[str],
        timestamps: List[datetime],
        volumes: List[float],
    ) -> List[float]:
        """Batch compute relativeVolume5min for a list of tickers.

        This is the preferred API — a single call replaces the Python
        ``for row in df.iter_rows()`` loop. The Rust implementation
        processes the batch as a contiguous array.
        """
        return [
            self.compute_relative_volume_5min(ticker, ts, vol)
            for ticker, ts, vol in zip(tickers, timestamps, volumes)
        ]


# =========================================================================
# Stateless compute functions (Rust migration targets)
# =========================================================================


def compute_ranks(df: pl.DataFrame) -> pl.DataFrame:
    """Add competition ranking based on changePercent (descending).

    Args:
        df: DataFrame with ``changePercent`` column, already sorted descending.

    Returns:
        DataFrame with added ``rank`` column (Int32, method='min').
    """
    return df.sort("changePercent", descending=True).with_columns(
        pl.col("changePercent")
        .rank(method="min", descending=True)
        .cast(pl.Int32)
        .alias("rank")
    )


def compute_derived_metrics(
    ranked_df: pl.DataFrame,
    timestamp: datetime,
    volume_tracker: VolumeTracker,
) -> pl.DataFrame:
    """Compute change, relativeVolumeDaily, and relativeVolume5min.

    Args:
        ranked_df: DataFrame with rank column and price/prev_close/volume columns.
        timestamp: Current snapshot timestamp.
        volume_tracker: VolumeTracker instance holding per-ticker history.

    Returns:
        Enriched DataFrame with change, relativeVolumeDaily, relativeVolume5min.
    """
    # Compute change
    df = ranked_df.with_columns(
        (pl.col("price") - pl.col("prev_close")).alias("change")
    )

    # Compute relativeVolumeDaily
    if "prev_volume" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("prev_volume") > 0)
            .then(pl.col("volume") / pl.col("prev_volume"))
            .otherwise(0.0)
            .alias("relativeVolumeDaily")
        )
    else:
        df = df.with_columns(pl.lit(0.0).alias("relativeVolumeDaily"))

    # Compute relativeVolume5min (batch via VolumeTracker)
    tickers = df["ticker"].to_list()
    volumes = df["volume"].to_list()
    timestamps_list = [timestamp] * len(tickers)

    relative_5min_values = volume_tracker.compute_batch(
        tickers, timestamps_list, volumes
    )

    df = df.with_columns(pl.Series("relativeVolume5min", relative_5min_values))

    return df


def compute_weighted_mid_price(df: pl.DataFrame) -> pl.DataFrame:
    """Compute weighted mid-price from bid/ask quote data.

    price = (bid * ask_size + ask * bid_size) / (bid_size + ask_size)
    Falls back to lastTrade price when quote data is missing or invalid.

    Args:
        df: DataFrame with bid, ask, bid_size, ask_size, price columns.

    Returns:
        DataFrame with updated ``price`` column.
    """
    quote_cols = {"bid", "ask", "bid_size", "ask_size"}
    if not quote_cols.issubset(df.columns):
        return df

    return df.with_columns(
        pl.when(
            (pl.col("bid_size") + pl.col("ask_size") > 0)
            & (pl.col("bid") > 0)
            & (pl.col("ask") > 0)
        )
        .then(
            (pl.col("bid") * pl.col("ask_size") + pl.col("ask") * pl.col("bid_size"))
            / (pl.col("bid_size") + pl.col("ask_size"))
        )
        .otherwise(pl.col("price"))
        .alias("price")
    )
