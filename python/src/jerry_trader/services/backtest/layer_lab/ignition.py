"""Ignition detection — finds the transition point where a ticker moves
from sparse/sleeping trading to active trading.

These module-level helpers search the forward trade stream for the first
moment where a ticker transitions to active trading, using multiple
detection methods (volume expansion, range breakout, trade rate surge,
sustained activity).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ══════════════════════════════════════════════════════════════════════════
# Ignition Detection — find the "transition point" where a ticker moves
# from sparse/sleeping trading to active trading
# ══════════════════════════════════════════════════════════════════════════


def _detect_ignitions(
    trades: pd.DataFrame,
    entry_ms: int,
    session_end_ms: int,
    quotes: pd.DataFrame | None = None,
    spread_bps: float = 100.0,
    vol_exp_ratio: float = 3.0,
    surge_sigma: float = 3.0,
    sustained_min_trades: int = 3,
) -> dict:
    """Detect ignition points using multiple methods.

    Each method searches the forward trade stream for the first moment
    where the ticker transitions from sparse to active trading.

    Returns dict keyed by method name, each value is {"ms": int, "price": float}
    or None if no ignition detected.

    Methods:
      vol_exp — rolling 60s volume / trailing 5-min avg > ratio
      range_breakout — price breaks above max of last 10 min
      trade_rate_surge — 60s trade count > rolling mean + Nσ
      sustained_activity — first 3 consecutive 60s windows each with ≥N trades
      spread_collapse — bid-ask spread tightens below threshold (needs quotes)

    Args:
        spread_bps: Max spread in bps for spread_collapse detection (default 100).
        vol_exp_ratio: Volume expansion ratio threshold (default 3.0).
        surge_sigma: Sigma multiplier for trade_rate_surge (default 3.0).
        sustained_min_trades: Min trades per 60s window for sustained_activity (default 3).
    """
    fwd = trades[(trades["ts_ms"] > entry_ms) & (trades["ts_ms"] <= session_end_ms)]
    if fwd.empty:
        return {m: None for m in _IGNITION_METHODS}

    ts = fwd["ts_ms"].values
    prices = fwd["price"].values
    sizes = fwd["size"].values

    spread_result = None
    if quotes is not None and not quotes.empty:
        spread_result = _ig_spread_collapse(
            quotes, entry_ms, session_end_ms, threshold_bps=spread_bps
        )

    return {
        "vol_exp": _ig_volume_expansion(
            ts, prices, sizes, entry_ms, ratio=vol_exp_ratio
        ),
        "range_breakout": _ig_range_breakout(ts, prices, entry_ms),
        "trade_rate_surge": _ig_trade_rate_surge(
            ts, prices, entry_ms, session_end_ms, sigma=surge_sigma
        ),
        "sustained_activity": _ig_sustained_activity(
            ts, prices, entry_ms, session_end_ms, min_trades=sustained_min_trades
        ),
        "spread_collapse": spread_result,
    }


_IGNITION_METHODS = [
    "vol_exp",
    "range_breakout",
    "trade_rate_surge",
    "sustained_activity",
    "spread_collapse",
]


def _ig_volume_expansion(
    ts: np.ndarray,
    prices: np.ndarray,
    sizes: np.ndarray,
    entry_ms: int,
    ratio: float = 3.0,
) -> dict | None:
    """A. Volume Expansion: rolling 60s volume / trailing 5-min avg > ratio."""
    min_data_ms = entry_ms + 360_000  # need 6 min of trades

    for i in range(len(ts)):
        t = ts[i]
        if t < min_data_ms:
            continue

        idx_60s = np.searchsorted(ts, t - 60_000, side="left")
        vol_60s = float(sizes[idx_60s : i + 1].sum())
        if vol_60s == 0:
            continue

        idx_trail_end = idx_60s
        idx_trail_start = np.searchsorted(ts, t - 360_000, side="left")
        if idx_trail_end <= idx_trail_start:
            continue

        vol_trail = float(sizes[idx_trail_start:idx_trail_end].sum())
        if vol_trail < 100:
            continue  # not enough history to calibrate

        avg_vol_per_60s = vol_trail / 5.0
        if avg_vol_per_60s > 0 and vol_60s / avg_vol_per_60s > ratio:
            return {"ms": int(t), "price": float(prices[i])}

    return None


def _ig_range_breakout(
    ts: np.ndarray, prices: np.ndarray, entry_ms: int
) -> dict | None:
    """C. Range Breakout: price > max of last 10 minutes."""
    min_data_ms = entry_ms + 600_000  # need 10 min of history

    for i in range(len(ts)):
        t = ts[i]
        if t < min_data_ms:
            continue

        idx_10min = np.searchsorted(ts, t - 600_000, side="left")
        if i <= idx_10min:
            continue  # no prior trade in 10-min window

        prior_max = float(prices[idx_10min:i].max())
        if prices[i] > prior_max:
            return {"ms": int(t), "price": float(prices[i])}

    return None


def _ig_trade_rate_surge(
    ts: np.ndarray,
    prices: np.ndarray,
    entry_ms: int,
    session_end_ms: int,
    sigma: float = 3.0,
) -> dict | None:
    """D. Self-Calibrating: 60s trade count > rolling mean + Nσ."""
    bucket_edges = np.arange(entry_ms, session_end_ms, 60_000)
    if len(bucket_edges) < 21:
        return None

    bucket_counts = np.zeros(len(bucket_edges), dtype=int)
    bucket_first_price = np.full(len(bucket_edges), np.nan)

    for i, edge in enumerate(bucket_edges):
        mask = (ts >= edge) & (ts < edge + 60_000)
        bucket_counts[i] = int(mask.sum())
        if mask.any():
            bucket_first_price[i] = float(prices[mask][0])

    for i in range(20, len(bucket_counts)):
        prior = bucket_counts[:i]
        mean = float(prior.mean())
        std = float(prior.std())
        if std <= 0 or bucket_counts[i] <= mean + sigma * std:
            continue

        ms = int(bucket_edges[i])
        price = (
            float(bucket_first_price[i])
            if not np.isnan(bucket_first_price[i])
            else float(prices[ts >= bucket_edges[i]][0])
        )
        return {"ms": ms, "price": price}

    return None


def _ig_sustained_activity(
    ts: np.ndarray,
    prices: np.ndarray,
    entry_ms: int,
    session_end_ms: int,
    min_trades: int = 3,
) -> dict | None:
    """E. Sustained Activity: first 3 consecutive 60s windows each with ≥min_trades trades."""
    bucket_edges = np.arange(entry_ms, session_end_ms, 60_000)
    if len(bucket_edges) < 3:
        return None

    for i in range(len(bucket_edges) - 2):
        c1 = int(((ts >= bucket_edges[i]) & (ts < bucket_edges[i] + 60_000)).sum())
        if c1 < min_trades:
            continue
        c2 = int(
            ((ts >= bucket_edges[i + 1]) & (ts < bucket_edges[i + 1] + 60_000)).sum()
        )
        if c2 < min_trades:
            continue
        c3 = int(
            ((ts >= bucket_edges[i + 2]) & (ts < bucket_edges[i + 2] + 60_000)).sum()
        )
        if c3 < min_trades:
            continue

        ms = int(bucket_edges[i])
        mask = (ts >= ms) & (ts < ms + 60_000)
        if mask.any():
            return {"ms": ms, "price": float(prices[mask][0])}

    return None


def _ig_spread_collapse(
    quotes: pd.DataFrame,
    entry_ms: int,
    session_end_ms: int,
    threshold_bps: float = 100.0,
) -> dict | None:
    """Spread collapse: first moment bid-ask spread tightens below threshold.

    Spread in bps = (ask - bid) / mid * 10000. Uses quotes data to find
    the transition from wide premarket spreads to tight regular-hours spreads,
    which signals that liquidity providers have arrived.

    Args:
        quotes: DataFrame with ts_ms, bid, ask columns
        entry_ms: Candidate entry timestamp
        session_end_ms: Session end
        threshold_bps: Spread must drop below this to count as ignition (default 100)

    Returns {"ms": int, "price": float} for the first quote where spread < threshold,
    or None if spread never tightens.
    """
    fwd = quotes[(quotes["ts_ms"] > entry_ms) & (quotes["ts_ms"] <= session_end_ms)]
    if fwd.empty:
        return None

    for row in fwd.itertuples():
        bid = float(row.bid)
        ask = float(row.ask)
        if bid <= 0 or ask <= 0 or ask <= bid:
            continue

        mid = (bid + ask) / 2.0
        spread_bps = (ask - bid) / mid * 10000.0
        if spread_bps < threshold_bps:
            return {"ms": int(row.ts_ms), "price": float(mid)}

    return None
