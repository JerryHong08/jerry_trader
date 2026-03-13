"""
Pure computation functions for the factor engine.

This module contains stateless math helpers that are **Rust migration targets**.
Every function here has a planned counterpart in ``rust/src/factors/``.

Functions:
    z_score        — rolling z-score over a deque baseline
    price_accel    — direction-aware price acceleration (return-rate diff)
    compute_trade_rate — trades/sec in a time window
"""

import math
from collections import deque
from typing import List, Optional, Tuple


def z_score(value: float, history: deque) -> Optional[float]:
    """Return z-score of ``value`` relative to ``history``, or None if < 2 samples.

    z = (value - mean) / std

    Args:
        value: Current observation.
        history: Rolling baseline (deque of floats).

    Returns:
        z-score float, or None if insufficient data.
    """
    n = len(history)
    if n < 2:
        return None
    mean = sum(history) / n
    var = sum((x - mean) ** 2 for x in history) / n
    std = math.sqrt(var)
    if std < 1e-9:
        return 0.0
    return (value - mean) / std


def price_accel(
    recent: List[Tuple[int, float]],
    older: List[Tuple[int, float]],
) -> float:
    """Direction-aware price acceleration.

    Computes the return-rate (% per second) in each half of the window,
    then returns the difference:  accel = return_rate_recent - return_rate_older.

    Positive → price accelerating upward.
    Negative → price accelerating downward.
    Units: fractional return per second (e.g. 0.0002 = 0.02 %/s).

    Args:
        recent: List of (timestamp_ms, price) tuples for the recent half-window.
        older:  List of (timestamp_ms, price) tuples for the older half-window.

    Returns:
        Acceleration value (float).
    """

    def _return_rate(trades: List[Tuple[int, float]]) -> float:
        if len(trades) < 2:
            return 0.0
        first_price = trades[0][1]
        last_price = trades[-1][1]
        dt_sec = (trades[-1][0] - trades[0][0]) / 1000.0
        if dt_sec <= 0 or first_price <= 0:
            return 0.0
        return ((last_price / first_price) - 1.0) / dt_sec

    return _return_rate(recent) - _return_rate(older)


def compute_trade_rate(trade_count: int, window_ms: int) -> float:
    """Compute trades per second for a given window.

    Args:
        trade_count: Number of trades in the window.
        window_ms: Window size in milliseconds.

    Returns:
        Trades per second.
    """
    window_sec = window_ms / 1000.0
    if window_sec <= 0:
        return 0.0
    return trade_count / window_sec
