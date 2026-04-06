"""Offline return backfill for signal_events.

Computes return_1m/5m/15m by joining trigger events with ohlcv 1m bars.
Designed to run as a batch job after signal events have been recorded.

Usage:
    poetry run python -m jerry_trader.services.signal.return_fill
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("return_fill", log_to_file=True)

# Return horizons: label → offset in milliseconds
RETURN_HORIZONS = {
    "return_1m": 60_000,
    "return_5m": 300_000,
    "return_15m": 900_000,
}


class ReturnFiller:
    """Compute and backfill returns for signal events.

    For each signal event with trigger_price but NULL returns:
    1. Look up the 1m bar close at trigger_time + N minutes
    2. Compute return = (exit_close - trigger_price) / trigger_price
    3. UPDATE the signal_events row
    """

    SIGNAL_TABLE = "signal_events"
    OHLCV_TABLE = "ohlcv_bars"

    def __init__(self, ch_client: Any = None, clickhouse_config: Optional[dict] = None):
        self._ch = ch_client
        self._ch_config = clickhouse_config

    def _get_client(self):
        """Get ClickHouse client — use shared client or create thread-local one."""
        if self._ch:
            return self._ch

        if not self._ch_config:
            return None

        import threading

        _tl = getattr(ReturnFiller, "_thread_local", None)
        if _tl is None:
            _tl = threading.local()
            ReturnFiller._thread_local = _tl

        if not hasattr(_tl, "ch_client") or _tl.ch_client is None:
            from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

            _tl.ch_client = get_clickhouse_client(self._ch_config)
        return _tl.ch_client

    def run(self, limit: int = 1000) -> int:
        """Fill returns for signal events that have NULL return columns.

        Args:
            limit: Max events to process per run.

        Returns:
            Number of events updated.
        """
        ch = self._get_client()
        if not ch:
            logger.error("ReturnFiller: no ClickHouse client")
            return 0

        # Fetch events needing return computation
        events = self._fetch_pending_events(limit)
        if not events:
            logger.info("ReturnFiller: no pending events")
            return 0

        logger.info(f"ReturnFiller: processing {len(events)} events")

        # Group by ticker for batch bar lookup
        by_ticker: dict[str, list[dict]] = {}
        for ev in events:
            by_ticker.setdefault(ev["ticker"], []).append(ev)

        updated = 0
        for ticker, ticker_events in by_ticker.items():
            updated += self._process_ticker(ticker, ticker_events)

        logger.info(f"ReturnFiller: updated {updated}/{len(events)} events")
        return updated

    def _fetch_pending_events(self, limit: int) -> list[dict]:
        """Fetch signal events where returns are NULL."""
        query = f"""
            SELECT id, ticker, trigger_time, trigger_price
            FROM {self.SIGNAL_TABLE} FINAL
            WHERE return_1m IS NULL
              AND trigger_price IS NOT NULL
            ORDER BY trigger_time ASC
            LIMIT %(limit)u
        """
        try:
            result = self._get_client().query(query, parameters={"limit": limit})
            return [
                {
                    "id": row[0],
                    "ticker": row[1],
                    "trigger_time_ns": row[2],
                    "trigger_price": float(row[3]),
                }
                for row in result.result_rows
            ]
        except Exception as e:
            logger.error(f"ReturnFiller: failed to fetch events - {e}")
            return []

    def _process_ticker(self, ticker: str, events: list[dict]) -> int:
        """Process all events for a single ticker."""
        if not events:
            return 0

        # Find the time range needed
        min_ts_ns = min(ev["trigger_time_ns"] for ev in events)
        max_ts_ns = max(ev["trigger_time_ns"] for ev in events)

        # Convert to ms, extend by 15m + buffer
        start_ms = min_ts_ns // 1_000_000
        end_ms = max_ts_ns // 1_000_000 + 900_000 + 120_000  # 15m + 2m buffer

        # Fetch all 1m bars in range
        bars = self._fetch_bars(ticker, start_ms, end_ms)
        if not bars:
            logger.warning(f"ReturnFiller: no 1m bars for {ticker} in range")
            return 0

        # Build a sorted list of (bar_start_ms, close)
        bar_closes = sorted(bars)  # list of (bar_start_ms, close)

        updated = 0
        for ev in events:
            entry_price = ev["trigger_price"]
            trigger_ms = ev["trigger_time_ns"] // 1_000_000
            returns = {}

            for col, offset_ms in RETURN_HORIZONS.items():
                target_ms = trigger_ms + offset_ms
                close = self._find_close_at(bar_closes, target_ms)
                if close is not None:
                    returns[col] = (close - entry_price) / entry_price

            if returns:
                self._update_returns(ev["id"], returns)
                updated += 1

        return updated

    def _fetch_bars(
        self, ticker: str, start_ms: int, end_ms: int
    ) -> list[tuple[int, float]]:
        """Fetch 1m bar (bar_start, close) for a ticker and time range."""
        query = f"""
            SELECT bar_start, close
            FROM {self.OHLCV_TABLE} FINAL
            WHERE ticker = %(ticker)s
              AND timeframe = '1m'
              AND bar_start >= %(start_ms)s
              AND bar_start <= %(end_ms)s
            ORDER BY bar_start ASC
        """
        try:
            result = self._get_client().query(
                query,
                parameters={
                    "ticker": ticker,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                },
            )
            return [(row[0], float(row[1])) for row in result.result_rows]
        except Exception as e:
            logger.error(f"ReturnFiller: failed to fetch bars for {ticker} - {e}")
            return []

    @staticmethod
    def _find_close_at(
        bar_closes: list[tuple[int, float]], target_ms: int
    ) -> Optional[float]:
        """Find the close price of the bar that contains target_ms.

        Uses binary search to find the bar whose start <= target_ms < start + 60_000.
        Falls back to the nearest bar within 2 minutes of target.
        """
        import bisect

        if not bar_closes:
            return None

        # Binary search: find insertion point for target_ms
        starts = [b[0] for b in bar_closes]
        idx = bisect.bisect_right(starts, target_ms)

        # The bar containing target_ms is at idx-1 (if bar_start <= target_ms)
        if idx > 0:
            bar_start, close = bar_closes[idx - 1]
            if bar_start <= target_ms < bar_start + 60_000:
                return close

        # Fallback: check the nearest bar after target (within 2 minutes)
        if idx < len(bar_closes):
            bar_start, close = bar_closes[idx]
            if bar_start - target_ms <= 120_000:
                return close

        return None

    def _update_returns(self, event_id: str, returns: dict[str, float]) -> None:
        """Update return columns for a single event."""
        set_parts = [f"{col} = %(val)s" for col in returns.keys()]
        set_clause = ", ".join(set_parts)

        query = f"""
            ALTER TABLE {self.SIGNAL_TABLE}
            UPDATE {set_clause}
            WHERE id = %(id)s
        """
        params = {**returns, "id": event_id}
        try:
            self._get_client().command(query, parameters=params)
        except Exception as e:
            logger.error(f"ReturnFiller: failed to update {event_id} - {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    client = get_clickhouse_client()
    if not client:
        print("Failed to connect to ClickHouse")
        exit(1)

    filler = ReturnFiller(client)
    count = filler.run()
    print(f"Updated {count} signal events")
    client.close()
