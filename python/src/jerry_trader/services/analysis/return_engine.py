"""Return enrichment — enrich Observations with forward returns from ClickHouse."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from jerry_trader.domain.analysis import Horizon, Observation, Outcome


class ReturnEngine:
    """Enrich :class:`Observation`\\s with forward returns from ClickHouse snapshots.

    Loads ``market_snapshot`` data per symbol once (batched and cached), then
    for each observation finds the trigger price and forward prices at each
    horizon to compute *returns* and *max_returns*.
    """

    def __init__(self, ch_client: Any):
        self._ch = ch_client

    def enrich(
        self,
        observations: list[Observation],
        horizons: list[Horizon],
    ) -> list[Outcome]:
        """Return one :class:`Outcome` per observation."""
        # 1. Group unique (symbol, date) pairs
        needed: set[tuple[str, str]] = set()
        for obs in observations:
            needed.add((obs.symbol, obs.date_str))

        # 2. Load snapshot prices per symbol+date (cached)
        cache: dict[tuple[str, str], list[tuple[datetime, float]]] = {}
        for symbol, date_str in sorted(needed):
            cache[(symbol, date_str)] = self._load_prices(symbol, date_str)

        # 3. Enrich each observation
        outcomes: list[Outcome] = []
        for obs in observations:
            prices = cache.get((obs.symbol, obs.date_str), [])
            outcome = self._compute_outcome(obs, prices, horizons)
            outcomes.append(outcome)

        return outcomes

    # ── Internals ────────────────────────────────────────────────────────

    def _load_prices(
        self, symbol: str, date_str: str
    ) -> list[tuple[datetime, float]]:
        """Load all (event_time, price) for a symbol on a date."""
        query = (
            f"SELECT event_time, price FROM jerry_trader.market_snapshot "
            f"WHERE date = '{date_str}' AND symbol = '{symbol}' "
            f"ORDER BY event_time"
        )
        try:
            result = self._ch.query(query)
            rows = result.result_rows
        except Exception:
            return []
        return [
            (row[0].replace(tzinfo=timezone.utc), float(row[1]))
            for row in rows
            if row[1] is not None
        ]

    @staticmethod
    def _compute_outcome(
        obs: Observation,
        prices: list[tuple[datetime, float]],
        horizons: list[Horizon],
    ) -> Outcome:
        """Compute returns for one observation against pre-loaded prices."""
        trigger_price = obs.trigger_price
        if trigger_price is None:
            trigger_price = _find_price_at(prices, obs.trigger_time, window_sec=120)

        returns: dict[Horizon, float | None] = {}
        max_returns: dict[Horizon, float | None] = {}

        for h in horizons:
            target = obs.trigger_time + timedelta(minutes=_horizon_minutes(h))
            fwd_price = _find_price_at(prices, target, window_sec=120)
            max_price = _max_price_in_window(prices, obs.trigger_time, target)

            if trigger_price and fwd_price and trigger_price > 0:
                returns[h] = round((fwd_price - trigger_price) / trigger_price, 6)
            else:
                returns[h] = None

            if trigger_price and max_price and trigger_price > 0:
                max_returns[h] = round((max_price - trigger_price) / trigger_price, 6)
            else:
                max_returns[h] = None

        # Store trigger_price back on the observation for downstream use
        obs_with_price = Observation(
            symbol=obs.symbol,
            trigger_time=obs.trigger_time,
            source=obs.source,
            trigger_price=trigger_price,
            metadata=obs.metadata,
        )

        return Outcome(observation=obs_with_price, returns=returns, max_returns=max_returns)


# ── Price lookup helpers ────────────────────────────────────────────────────


def _horizon_minutes(h: Horizon) -> int:
    return int(h.value.rstrip("m"))


def _find_price_at(
    prices: list[tuple[datetime, float]],
    target: datetime,
    window_sec: int = 60,
) -> float | None:
    """Find the price closest to *target* within ±*window_sec*."""
    target_ts = target.timestamp()
    best_price = None
    best_dist = float("inf")

    for ts, price in prices:
        dist = abs(ts.timestamp() - target_ts)
        if dist < best_dist and dist <= window_sec:
            best_dist = dist
            best_price = price

    return best_price


def _max_price_in_window(
    prices: list[tuple[datetime, float]],
    start: datetime,
    end: datetime,
) -> float | None:
    """Get the maximum price observed in [start, end]."""
    start_ts = start.timestamp()
    end_ts = end.timestamp()
    best = None
    for ts, price in prices:
        t = ts.timestamp()
        if start_ts <= t <= end_ts:
            if best is None or price > best:
                best = price
    return best
