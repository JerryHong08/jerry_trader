"""CHReplayer — replays market snapshot data from ClickHouse to Redis INPUT Stream.

Unlike the legacy MarketSnapshotReplayer (which reads parquet files),
this reads from the `market_snapshot_collector` CH table with standardized
column names and gates timing on the local ReplayClock.

Supports:
- Local ReplayClock gating (polls clock.now_ms() in asyncio loop)
- RemoteClockFollower for cross-machine deployment
- start_from to skip already-bootstrapped windows
- Rollback and clear utilities (inherited from legacy replayer)
"""

import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import polars as pl
import redis

from jerry_trader.platform.config.session import make_session_id, session_to_influx_tags
from jerry_trader.shared.ids.redis_keys import (
    clear_session_keys,
    market_snapshot_stream,
    rollback_session_streams,
)
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.remote_clock import RemoteClockFollower
from jerry_trader.shared.time.timezone import ms_to_hhmmss

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)

ET = ZoneInfo("America/New_York")

# Columns to select from CH (already standardized in market_snapshot_collector)
_REPLAY_COLUMNS = [
    "ticker",
    "timestamp",
    "price",
    "changePercent",
    "volume",
    "prev_close",
    "prev_volume",
    "vwap",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
]


class CHReplayer:
    """Replays market snapshot data from ClickHouse with clock-based timing.

    Reads windows from `market_snapshot_collector`, waits for the ReplayClock
    to reach each window's timestamp, then pushes to Redis INPUT Stream.
    """

    def __init__(
        self,
        replay_date: str,
        ch_client: Any,
        redis_config: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
        speed: float = 1.0,
        start_from_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        remote_clock: Optional[RemoteClockFollower] = None,
        rollback_to: Optional[str] = None,
        clear: bool = False,
        bootstrap_complete_event: Optional[threading.Event] = None,
    ):
        self.replay_date = replay_date
        self.ch_client = ch_client
        self.speed = speed
        self.start_from_ms = start_from_ms
        self.end_ms = end_ms
        self.rollback_to = rollback_to
        self.clear = clear
        self.remote_clock = remote_clock
        self.bootstrap_complete_event = bootstrap_complete_event

        self.session_id = session_id or make_session_id(replay_date=replay_date)
        self.date_tag, self.mode_tag = session_to_influx_tags(self.session_id)

        # Redis
        redis_cfg = redis_config or {}
        self.r = redis.Redis(
            host=redis_cfg.get("host", "127.0.0.1"),
            port=redis_cfg.get("port", 6379),
            db=redis_cfg.get("db", 0),
            decode_responses=True,
        )
        self.INPUT_STREAM = market_snapshot_stream(self.session_id)

        self._running = False
        self._windows_replayed = 0
        self._warned_remote_unsynced = False

        logger.info(
            f"CHReplayer initialized: date={replay_date}, session={self.session_id}, "
            f"speed={speed}x, start_from_ms={start_from_ms}, end_ms={end_ms}"
        )

    async def start(self):
        """Start the replayer loop."""
        self._running = True
        logger.info("CHReplayer starting...")

        # Wait for remote clock sync if configured
        if self.remote_clock is not None:
            await self._await_remote_clock_sync(timeout_s=5.0)

        # Handle rollback / clear
        if self.rollback_to:
            self._rollback_to_timestamp(self.rollback_to)
        if self.clear:
            clear_session_keys(self.r, self.session_id)
            self._running = False
            return

        await self._run_replay()

        logger.info(f"CHReplayer stopped. Replayed {self._windows_replayed} windows.")

    def stop(self):
        self._running = False

    async def _await_remote_clock_sync(self, timeout_s: float = 5.0) -> bool:
        if self.remote_clock is None:
            return False
        deadline = time.time() + timeout_s
        while self._running and time.time() < deadline:
            if self.remote_clock.has_sync:
                logger.info(
                    "Remote clock synced: speed=%sx paused=%s",
                    self.remote_clock.speed,
                    self.remote_clock.is_paused,
                )
                return True
            await asyncio.sleep(0.05)
        logger.warning(
            "Remote clock not synced within %.1fs, using local fallback", timeout_s
        )
        return False

    async def _run_replay(self):
        """Main replay loop: wait for clock, query CH, gate on clock, push to Redis."""
        # Wait for clock to unpause before querying (bootstrap may still be running)
        await self._wait_for_clock(0, 0, None)

        # Wait for bootstrap completion if an event is provided
        # This ensures start_from_ms is set by bootstrap before we query windows
        if self.bootstrap_complete_event is not None:
            logger.info("Waiting for bootstrap completion before querying windows...")
            while self._running and not self.bootstrap_complete_event.is_set():
                await asyncio.sleep(0.1)
            if self.bootstrap_complete_event.is_set():
                logger.info("Bootstrap complete, proceeding with window query")
            else:
                logger.warning("Replayer stopped before bootstrap completed")
                return

        # Now query windows — start_from_ms may have been updated by bootstrap
        windows = self._query_windows()
        if not windows:
            logger.warning("No windows found for %s/%s", self.date_tag, self.mode_tag)
            return

        logger.info(
            f"CHReplayer: {len(windows)} windows, "
            f"range: {ms_to_hhmmss(windows[0])} - {ms_to_hhmmss(windows[-1])}"
        )

        prev_ts_ms: Optional[int] = None

        for i, window_ts_ms in enumerate(windows):
            if not self._running:
                logger.info("Replay stopped by user")
                break

            # Gate on clock time
            await self._wait_for_clock(window_ts_ms, i, prev_ts_ms)

            # Query this window's data from CH
            df = self._query_window(window_ts_ms)
            if df.is_empty():
                prev_ts_ms = window_ts_ms
                continue

            # Push to Redis INPUT Stream
            payload = df.write_json()
            self.r.xadd(self.INPUT_STREAM, {"data": payload}, maxlen=10000)
            if self.r.ttl(self.INPUT_STREAM) < 0:
                self.r.expire(self.INPUT_STREAM, 19 * 3600)

            self._windows_replayed += 1
            if self._windows_replayed % 500 == 0:
                logger.info(
                    f"CHReplayer: {self._windows_replayed}/{len(windows)} windows replayed"
                )

            prev_ts_ms = window_ts_ms

    async def _wait_for_clock(
        self, target_ts_ms: int, idx: int, prev_ts_ms: Optional[int]
    ):
        """Wait until clock reaches target timestamp.

        When target_ts_ms=0, only waits for clock to unpause (used before
        querying windows so start_from_ms is updated by bootstrap first).
        """
        if self.remote_clock is not None and self.remote_clock.has_sync:
            # Remote clock path — wait for unpause first
            while self._running and self.remote_clock.is_paused:
                await asyncio.sleep(0.05)
            if target_ts_ms > 0:
                target_ts_ns = target_ts_ms * 1_000_000
                while self._running and self.remote_clock.now_ns() < target_ts_ns:
                    await asyncio.sleep(0.02)
        else:
            # Local clock path — poll clock.now_ms()
            from jerry_trader import clock

            if clock.is_replay():
                # Wait for clock to be unpaused first
                _clk = clock.get_clock()
                while self._running and _clk and _clk.is_paused:
                    await asyncio.sleep(0.05)
                # Gate on virtual time
                if target_ts_ms > 0:
                    while self._running and clock.now_ms() < target_ts_ms:
                        await asyncio.sleep(0.02)
            elif idx > 0 and prev_ts_ms is not None:
                # No clock (live mode or clock not available) — sleep fallback
                time_diff = (target_ts_ms - prev_ts_ms) / 1000.0
                adjusted = time_diff / self.speed
                if adjusted > 0:
                    await asyncio.sleep(adjusted)

    def _query_windows(self) -> List[int]:
        """Query distinct window timestamps from CH."""
        query = """
            SELECT DISTINCT timestamp
            FROM market_snapshot_collector FINAL
            WHERE date = {date:String} AND mode = {mode:String}
        """
        params: Dict[str, Any] = {
            "date": self.date_tag,
            "mode": self.mode_tag,
        }

        if self.start_from_ms is not None:
            query += " AND timestamp > {start_ms:Int64}"
            params["start_ms"] = self.start_from_ms
        if self.end_ms is not None:
            query += " AND timestamp <= {end_ms:Int64}"
            params["end_ms"] = self.end_ms

        query += " ORDER BY timestamp ASC"

        result = self.ch_client.query(query, parameters=params)
        if not result.result_rows:
            return []
        return [row[0] for row in result.result_rows]

    def _query_window(self, timestamp_ms: int) -> pl.DataFrame:
        """Query a single window's data from CH."""
        cols = ", ".join(_REPLAY_COLUMNS)
        query = f"""
            SELECT {cols}
            FROM market_snapshot_collector FINAL
            WHERE date = {{date:String}} AND mode = {{mode:String}}
              AND timestamp = {{timestamp:Int64}}
        """

        result = self.ch_client.query(
            query,
            parameters={
                "date": self.date_tag,
                "mode": self.mode_tag,
                "timestamp": timestamp_ms,
            },
        )

        if not result.result_rows:
            return pl.DataFrame()

        return pl.DataFrame(
            {
                col: [row[i] for row in result.result_rows]
                for i, col in enumerate(_REPLAY_COLUMNS)
            }
        )

    def _rollback_to_timestamp(self, rollback_time: str) -> None:
        """Rollback Redis data to a specific timestamp (HHMMSS)."""
        from jerry_trader.shared.ids.redis_keys import (
            movers_state_stream,
            movers_subscribed_set,
            state_cursor,
        )

        rollback_str = str(rollback_time).zfill(6)
        rollback_dt = datetime(
            int(self.replay_date[:4]),
            int(self.replay_date[4:6]),
            int(self.replay_date[6:8]),
            int(rollback_str[:2]),
            int(rollback_str[2:4]),
            int(rollback_str[4:6]),
            tzinfo=ET,
        )
        rollback_ts_ms = int(rollback_dt.timestamp() * 1000)

        logger.info(f"Rolling back to {rollback_dt}")
        rollback_session_streams(self.r, self.session_id, rollback_ts_ms)

        subscribed_key = movers_subscribed_set(self.session_id)
        if self.r.exists(subscribed_key):
            removed = self.r.zremrangebyscore(
                subscribed_key, rollback_dt.timestamp() + 0.001, "+inf"
            )
            logger.info(f"Removed {removed} tickers from subscription set")

        cursor_key = state_cursor(self.session_id)
        if self.r.exists(cursor_key):
            for symbol, val in self.r.hgetall(cursor_key).items():
                try:
                    if datetime.fromisoformat(val) > rollback_dt:
                        self.r.hset(cursor_key, symbol, rollback_dt.isoformat())
                except (ValueError, TypeError):
                    pass
