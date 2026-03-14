"""
State Engine – Factor-based signal detector.

Polls Redis HSET snapshots written by FactorEngine (``factor:{session_id}:{symbol}``)
and fires signal events on state transitions:

    QUIET  →  ACTIVE   when  warm=True  AND  trade_rate_z > THRESHOLD_Z  AND  accel > 0
    ACTIVE →  QUIET    when  trade_rate_z <= THRESHOLD_Z  OR  accel <= 0  OR  warm=False

Outputs:
    - InfluxDB ``signal_events`` measurement (historical record)
    - Redis Stream ``signal_events:{session_id}`` (real-time push to BFF / replay)
"""

import logging
import os
import time
from enum import Enum
from threading import Event, Thread
from typing import Any, Dict, Optional

import influxdb_client
import redis
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, WriteType

from jerry_trader.shared.ids.redis_keys import factor_hset_prefix, signal_events_stream
from jerry_trader.shared.utils.logger import setup_logger
from jerry_trader.shared.utils.session import (
    make_session_id,
    parse_session_id,
    session_to_influx_tags,
)

logger = setup_logger("state_engine", log_to_file=True, level=logging.DEBUG)

# ── Tuning constants ──────────────────────────────────────────────────
POLL_INTERVAL_SEC = 1.0  # how often to scan factor HSETs
THRESHOLD_Z = 5.0  # trade_rate_z threshold for ACTIVE
COOLDOWN_SEC = 10.0  # min time in ACTIVE before allowing QUIET transition


# ── State enum ────────────────────────────────────────────────────────
class TickerState(str, Enum):
    QUIET = "QUIET"
    ACTIVE = "ACTIVE"


class _TickerTracker:
    """In-memory state for one tracked ticker."""

    __slots__ = ("symbol", "state", "last_transition_ts", "last_factors")

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.state = TickerState.QUIET
        self.last_transition_ts: float = 0.0  # epoch seconds
        self.last_factors: Dict[str, str] = {}


class StateEngine:
    """
    Poll-based signal detector.

    Constructor signature is compatible with backend_starter.py:
        StateEngine(session_id=..., redis_config=..., influxdb_config=...)
    """

    def __init__(
        self,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
    ):
        # ── Session ──────────────────────────────────────────────────
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # ── InfluxDB ─────────────────────────────────────────────────
        self.influx_org = "jerryhong"
        influx_cfg = influxdb_config or {}
        self.influx_bucket = influx_cfg.get("bucket", "")

        influx_token_env = influx_cfg.get("influx_token_env")
        token = os.environ.get(influx_token_env) if influx_token_env else None

        influx_url_env = influx_cfg.get("influx_url_env")
        url = os.getenv(influx_url_env) if influx_url_env else "http://localhost:8086"

        self._influx_client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=self.influx_org
        )
        self.write_api = self._influx_client.write_api(
            write_options=WriteOptions(write_type=WriteType.synchronous)
        )

        # ── Redis ────────────────────────────────────────────────────
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Output stream key
        self.signal_stream = signal_events_stream(self.session_id)

        # HSET scan prefix
        self._factor_prefix = factor_hset_prefix(self.session_id)

        # ── In-memory state ──────────────────────────────────────────
        self._trackers: Dict[str, _TickerTracker] = {}

        # ── Lifecycle ────────────────────────────────────────────────
        self._stop_event = Event()

        logger.info(
            f"__init__ - StateEngine initialized: session_id={self.session_id}, "
            f"mode={self.run_mode}, threshold_z={THRESHOLD_Z}"
        )

    # ================================================================
    # Public API
    # ================================================================
    def start(self):
        """Start the poll loop in a background thread."""
        t = Thread(target=self._poll_loop, daemon=True, name="state-engine-poll")
        t.start()
        logger.info("start - StateEngine poll thread started")
        return t

    def close(self):
        """Graceful shutdown."""
        self._stop_event.set()
        if self._influx_client:
            self._influx_client.close()
        logger.info("close - StateEngine closed")

    # ================================================================
    # Poll loop
    # ================================================================
    def _poll_loop(self):
        """Every POLL_INTERVAL_SEC, scan factor HSETs and evaluate state."""
        logger.info("_poll_loop - Starting…")
        while not self._stop_event.is_set():
            try:
                self._scan_and_evaluate()
            except Exception as exc:
                logger.error(f"_poll_loop - {exc}", exc_info=True)
            self._stop_event.wait(POLL_INTERVAL_SEC)
        logger.info("_poll_loop - Stopped")

    def _scan_and_evaluate(self):
        """Discover all factor HSETs and evaluate each."""
        pattern = f"{self._factor_prefix}*"
        cursor = 0
        discovered: set = set()

        while True:
            cursor, keys = self.redis_client.scan(cursor, match=pattern, count=200)
            for key in keys:
                # key = "factor:{session_id}:{symbol}"
                symbol = key[len(self._factor_prefix) :]
                discovered.add(symbol)
                self._evaluate_ticker(symbol, key)
            if cursor == 0:
                break

        # Clean up trackers for symbols no longer present
        stale = set(self._trackers) - discovered
        for sym in stale:
            del self._trackers[sym]

    def _evaluate_ticker(self, symbol: str, hset_key: str):
        """Read factor HSET and evaluate state transition."""
        factors = self.redis_client.hgetall(hset_key)
        if not factors:
            return

        # Parse fields
        warm = factors.get("warm") == "1"
        trade_rate_z_str = factors.get("trade_rate_z")
        accel_str = factors.get("accel")
        ts_ms_str = factors.get("ts_ms")

        trade_rate_z = float(trade_rate_z_str) if trade_rate_z_str else None
        accel = float(accel_str) if accel_str else None
        from jerry_trader import clock

        ts_ms = int(ts_ms_str) if ts_ms_str else clock.now_ms()

        # Get or create tracker
        if symbol not in self._trackers:
            self._trackers[symbol] = _TickerTracker(symbol)
        tracker = self._trackers[symbol]
        tracker.last_factors = factors

        # Evaluate condition: warm AND z > threshold AND accel > 0
        active_condition = (
            warm
            and trade_rate_z is not None
            and accel is not None
            and trade_rate_z > THRESHOLD_Z
            and accel > 0
        )

        now = clock.now_s()
        old_state = tracker.state

        if old_state == TickerState.QUIET and active_condition:
            # QUIET → ACTIVE
            tracker.state = TickerState.ACTIVE
            tracker.last_transition_ts = now
            self._emit_signal(symbol, TickerState.ACTIVE, ts_ms, factors)
            logger.info(
                f"_evaluate_ticker - {symbol}: QUIET → ACTIVE "
                f"(z={trade_rate_z:.2f}, accel={accel:.2f})"
            )

        elif old_state == TickerState.ACTIVE and not active_condition:
            # ACTIVE → QUIET (with cooldown)
            elapsed = now - tracker.last_transition_ts
            if elapsed >= COOLDOWN_SEC:
                tracker.state = TickerState.QUIET
                tracker.last_transition_ts = now
                self._emit_signal(symbol, TickerState.QUIET, ts_ms, factors)
                logger.info(
                    f"_evaluate_ticker - {symbol}: ACTIVE → QUIET "
                    f"(z={trade_rate_z}, accel={accel}, elapsed={elapsed:.1f}s)"
                )

    # ================================================================
    # Output: InfluxDB + Redis Stream
    # ================================================================
    def _emit_signal(
        self,
        symbol: str,
        new_state: TickerState,
        ts_ms: int,
        factors: Dict[str, str],
    ) -> None:
        """Write a state-change event to InfluxDB and Redis Stream."""

        # ── InfluxDB point ───────────────────────────────────────────
        date_tag, mode_tag = session_to_influx_tags(self.session_id)
        point = (
            Point("signal_events")
            .tag("symbol", symbol)
            .tag("date", date_tag)
            .tag("mode", mode_tag)
            .field("state", new_state.value)
            .field("trade_rate", float(factors.get("trade_rate", 0)))
            .field("accel", float(factors.get("accel", 0)))
            .field("trade_count", int(factors.get("trade_count", 0)))
        )
        tr_z = factors.get("trade_rate_z")
        ac_z = factors.get("accel_z")
        if tr_z is not None:
            point = point.field("trade_rate_z", float(tr_z))
        if ac_z is not None:
            point = point.field("accel_z", float(ac_z))
        point = point.time(ts_ms, WritePrecision.MS)

        try:
            self.write_api.write(
                bucket=self.influx_bucket, org=self.influx_org, record=point
            )
        except Exception as exc:
            logger.error(f"_emit_signal - InfluxDB write failed: {exc}")

        # ── Redis Stream (for BFF / replay) ──────────────────────────
        stream_payload = {
            "symbol": symbol,
            "state": new_state.value,
            "ts_ms": str(ts_ms),
            "trade_rate": factors.get("trade_rate", "0"),
            "accel": factors.get("accel", "0"),
        }
        if tr_z is not None:
            stream_payload["trade_rate_z"] = tr_z
        if ac_z is not None:
            stream_payload["accel_z"] = ac_z

        try:
            self.redis_client.xadd(self.signal_stream, stream_payload, maxlen=10_000)
        except Exception as exc:
            logger.error(f"_emit_signal - Redis XADD failed: {exc}")
