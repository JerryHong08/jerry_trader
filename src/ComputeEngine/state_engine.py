"""
State Engine for Market Mover Web Analyzer
Handles state computation and state change notification.

Listens to processed snapshot stream and computes state changes for each ticker.
Writes state changes to InfluxDB and Redis Stream for BFF notification.

Architecture:
- Redis Stream Input: market_snapshot_processed:{date} (from SnapshotProcessor)
- Redis Stream Output: movers_state:{date} (for BFF notification)
- Redis HSET: state_cursor:{date} (cursor tracking for recovery)
- InfluxDB movers_state: stores all state change events
"""

import json
import logging
import os
import socket
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import influxdb_client
import redis
from influxdb_client.client.write_api import SYNCHRONOUS

from utils.logger import setup_logger
from utils.session import db_date_to_date, make_session_id, parse_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class StateEngine:
    """
    State engine for tracking ticker state changes.

    Responsibilities:
    - Listen to processed snapshot stream
    - Compute state for each subscribed ticker
    - Detect state changes
    - Write to:
        - InfluxDB movers_state (historical state records)
        - Redis HSET state_cursor:{date} (cursor tracking)
        - Redis Stream movers_state:{date} (BFF notification)
    """

    def __init__(
        self,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
    ):

        # Unified session id — single source of truth for mode & date
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # ---------- InfluxDB Configuration ----------
        self.org = "jerryhong"
        influx_cfg = influxdb_config or {}
        self.bucket = influx_cfg.get("bucket", "jerryib_trade")

        # Token from env var
        influx_token_env = influx_cfg.get("influx_token_env")
        token = os.environ.get(influx_token_env) if influx_token_env else None

        # URL from env var
        influx_url_env = influx_cfg.get("influx_url_env")
        url = os.getenv(influx_url_env) if influx_url_env else "http://localhost:8086"

        self._influx_client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=self.org
        )
        self._write_api = self._influx_client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._influx_client.query_api()

        # ---------- Redis Configuration ----------
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Input stream (from SnapshotProcessor)
        self.INPUT_STREAM_NAME = f"market_snapshot_processed:{self.session_id}"

        # Output stream (for BFF notification)
        self.OUTPUT_STREAM_NAME = f"movers_state:{self.session_id}"

        # HSET for cursor tracking
        self.CURSOR_HSET_NAME = f"state_cursor:{self.session_id}"

        # Consumer group config
        self.CONSUMER_GROUP = "state_consumers"
        self.CONSUMER_NAME = f"state_{socket.gethostname()}_{os.getpid()}"

        # Create consumer group if not exists
        try:
            self.r.xgroup_create(
                self.INPUT_STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        # In-memory state cache for state change detection
        # Structure: {ticker: {state, rank, prev_rank, rank_velocity, changePercent, timestamp}}
        self._ticker_states: Dict[str, Dict] = {}

        # Reload states from InfluxDB for recovery
        self._reload_states_from_influx()

        logger.info(
            f"__init__ - StateEngine initialized: mode={self.run_mode}, "
            f"session_id={self.session_id}, INPUT={self.INPUT_STREAM_NAME}, "
            f"OUTPUT={self.OUTPUT_STREAM_NAME}"
        )

    # =========================================================================
    # PUBLIC API - Start Listener
    # =========================================================================

    def start(self):
        """Start the state engine listener in a background thread."""
        listener_thread = Thread(target=self._stream_listener, daemon=True)
        listener_thread.start()
        logger.info("start - StateEngine listener thread started")
        return listener_thread

    def _stream_listener(self):
        """Main listener loop - receives from processed snapshot stream, computes states."""
        logger.info(
            f"_stream_listener - Starting Redis Stream consumer {self.CONSUMER_NAME}..."
        )

        while True:
            try:
                self.r.ping()

                messages = self.r.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.CONSUMER_NAME,
                    {self.INPUT_STREAM_NAME: ">"},
                    count=1,
                    block=2000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            self._process_snapshot_message(message_id, message_data)
                else:
                    logger.debug("_stream_listener - No new messages, waiting...")

            except KeyboardInterrupt:
                logger.info("_stream_listener - Stopping listener...")
                break
            except Exception as e:
                logger.error(f"_stream_listener - Error: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(5)

        logger.info("_stream_listener - Listener stopped")

    def _process_snapshot_message(self, message_id, message_data):
        """Process a single snapshot message and compute state changes."""
        try:
            # Parse message data
            timestamp_iso = message_data.get("timestamp")
            data_json = message_data.get("data")

            if not data_json:
                logger.warning("_process_snapshot_message - No data in message")
                self.r.xack(self.INPUT_STREAM_NAME, self.CONSUMER_GROUP, message_id)
                return

            timestamp = (
                datetime.fromisoformat(timestamp_iso)
                if timestamp_iso
                else datetime.now(ZoneInfo("America/New_York"))
            )

            try:
                tickers_data = json.loads(data_json)
            except json.JSONDecodeError as e:
                logger.error(f"_process_snapshot_message - JSON decode error: {e}")
                self.r.xack(self.INPUT_STREAM_NAME, self.CONSUMER_GROUP, message_id)
                return

            # Process each ticker for state changes
            state_changes = []
            for ticker_data in tickers_data:
                symbol = ticker_data.get("symbol")
                if not symbol:
                    continue

                # Compute current state
                current_state = self._compute_ticker_state(
                    symbol, ticker_data, timestamp
                )

                # Check if state changed
                if self._has_state_changed(symbol, current_state):
                    prev_state = self._ticker_states.get(symbol, {}).get(
                        "state", "unknown"
                    )
                    new_state = current_state.get("state")

                    # Write to InfluxDB
                    self._write_state_to_influx(symbol, current_state, timestamp)

                    # Write to output stream for BFF notification
                    self._write_state_to_stream(
                        symbol, prev_state, current_state, timestamp
                    )

                    # Update in-memory cache
                    self._ticker_states[symbol] = current_state

                    state_changes.append(
                        {
                            "symbol": symbol,
                            "from": prev_state,
                            "to": new_state,
                            "timestamp": timestamp.isoformat(),
                        }
                    )

                # Always update cursor with current state
                self._update_state_cursor(symbol, current_state, timestamp)

            # Acknowledge the message
            self.r.xack(self.INPUT_STREAM_NAME, self.CONSUMER_GROUP, message_id)

            if state_changes:
                logger.info(
                    f"_process_snapshot_message - {len(state_changes)} state changes: "
                    f"{[sc['symbol'] + ':' + sc['to'] for sc in state_changes]}"
                )

        except Exception as e:
            logger.error(f"_process_snapshot_message - Error: {e}")
            import traceback

            traceback.print_exc()

    # =========================================================================
    # STATE COMPUTATION
    # =========================================================================

    def _compute_ticker_state(
        self, symbol: str, ticker_data: Dict, timestamp: datetime
    ) -> Dict:
        """
        Compute the current state for a ticker.

        State categories (matching frontend directly):
        - "Best": new entrant or rank improved by >= 5
        - "Good": rank improved by >= 2
        - "OnWatch": rank relatively unchanged (stable)
        - "NotGood": rank dropped by >= 2
        - "Bad": rank dropped by >= 5

        TODO: Add more sophisticated state logic in the future
        """
        current_rank = ticker_data.get("rank", 999)
        changePercent = ticker_data.get("changePercent", 0.0)

        # Get previous state for velocity calculation
        prev_state = self._ticker_states.get(symbol, {})
        prev_rank = prev_state.get("rank", current_rank)

        # Calculate rank velocity (positive = improving, negative = falling)
        rank_velocity = prev_rank - current_rank

        # Determine state and reason (using frontend state values directly)
        if symbol not in self._ticker_states:
            state = "Best"
            state_reason = "New to top gainers"
        elif rank_velocity >= 5:
            state = "Best"
            state_reason = f"Rank +{rank_velocity} ({changePercent:+.1f}%)"
        elif rank_velocity >= 2:
            state = "Good"
            state_reason = f"Rank +{rank_velocity}"
        elif rank_velocity <= -5:
            state = "Bad"
            state_reason = f"Rank {rank_velocity}"
        elif rank_velocity <= -2:
            state = "NotGood"
            state_reason = f"Rank {rank_velocity}"
        else:
            state = "OnWatch"
            state_reason = f"Rank #{current_rank}"

        if current_rank > 30:
            state = "Bad"
            state_reason = "Dropped out of top 30"

        return {
            "state": state,
            "stateReason": state_reason,
            "rank": current_rank,
            "prev_rank": prev_rank,
            "rank_velocity": rank_velocity,
            "changePercent": changePercent,
            "timestamp": timestamp,
        }

    def _has_state_changed(self, symbol: str, current_state: Dict) -> bool:
        """
        Detect if the state has meaningfully changed.
        Checks: state category change or significant rank change.
        """
        if symbol not in self._ticker_states:
            return True  # New ticker

        prev_state = self._ticker_states[symbol]

        # State category changed
        if prev_state.get("state") != current_state.get("state"):
            return True

        # Significant rank change (even within same category)
        rank_diff = abs(prev_state.get("rank", 0) - current_state.get("rank", 0))
        if rank_diff >= 3:
            return True

        return False

    # =========================================================================
    # OUTPUT: INFLUXDB
    # =========================================================================

    def _write_state_to_influx(
        self, symbol: str, state: Dict, timestamp: datetime
    ) -> None:
        """
        Write state change event to InfluxDB.

        Measurement: movers_state
        Tags: symbol, run_mode, db_id
        Fields: state, stateReason, rank, changePercent, rank_velocity
        Time: state_change timestamp
        """
        point = (
            influxdb_client.Point("movers_state")
            .tag("symbol", symbol)
            .tag("session_id", self.session_id)
            .field("state", state.get("state", "unknown"))
            .field("stateReason", state.get("stateReason", ""))
            .field("rank", int(state.get("rank", 0)))
            .field("changePercent", float(state.get("changePercent", 0.0)))
            .field("rank_velocity", int(state.get("rank_velocity", 0)))
            .time(timestamp)
        )

        self._write_api.write(bucket=self.bucket, org=self.org, record=point)
        logger.debug(
            f"_write_state_to_influx - Wrote state {state.get('state')} for {symbol}"
        )

    # =========================================================================
    # OUTPUT: REDIS STREAM (BFF Notification)
    # =========================================================================

    def _write_state_to_stream(
        self,
        symbol: str,
        from_state: str,
        current_state: Dict,
        timestamp: datetime,
    ) -> None:
        """
        Write state change to Redis Stream for BFF notification.

        Message format:
        {
            "symbol": "ROKU",
            "from": "confirming",
            "to": "running-up",
            "reason": "changePercent",
            "ts": 1704898330
        }
        """
        to_state = current_state.get("state", "unknown")
        state_reason = current_state.get("stateReason", "")

        # Determine reason for state change
        reason = self._determine_state_change_reason(current_state)

        stream_message = {
            "symbol": symbol,
            "from": from_state,
            "to": to_state,
            "stateReason": state_reason,
            "reason": reason,
            "ts": str(int(timestamp.timestamp())),
        }

        self.r.xadd(self.OUTPUT_STREAM_NAME, stream_message, maxlen=1000)

        # Set 19-hour TTL on stream
        if self.r.ttl(self.OUTPUT_STREAM_NAME) < 0:
            self.r.expire(self.OUTPUT_STREAM_NAME, 19 * 3600)

        logger.debug(
            f"_write_state_to_stream - {symbol}: {from_state} -> {to_state} ({reason})"
        )

    def _determine_state_change_reason(self, current_state: Dict) -> str:
        """
        Determine the reason for state change.

        TODO: Expand with more sophisticated reason detection
        """
        rank_velocity = current_state.get("rank_velocity", 0)

        if abs(rank_velocity) >= 5:
            return "rankVelocity"
        elif abs(rank_velocity) >= 2:
            return "rankChange"
        else:
            return "stateTransition"

    # =========================================================================
    # OUTPUT: REDIS HSET (Cursor)
    # =========================================================================

    def _update_state_cursor(
        self, symbol: str, current_state: Dict, timestamp: datetime
    ) -> None:
        """
        Update the state cursor in Redis HSET.
        Used for recovery/replay to know where to resume state computation.
        Only stores the timestamp cursor value.
        """
        cursor_value = timestamp.isoformat()
        self.r.hset(self.CURSOR_HSET_NAME, symbol, cursor_value)

    # =========================================================================
    # PUBLIC API - State Access
    # =========================================================================

    def get_state_cursor(self, ticker: str) -> Optional[str]:
        """Get the last state update time for a ticker."""
        return self.r.hget(self.CURSOR_HSET_NAME, ticker)

    def get_all_state_cursors(self) -> Dict[str, str]:
        """Get all state cursors for recovery/replay."""
        return self.r.hgetall(self.CURSOR_HSET_NAME)

    def get_ticker_state(self, ticker: str) -> Optional[Dict]:
        """Get current in-memory state for a ticker."""
        return self._ticker_states.get(ticker)

    def get_all_ticker_states(self) -> Dict[str, Dict]:
        """Get all in-memory ticker states."""
        return self._ticker_states.copy()

    # =========================================================================
    # RECOVERY SUPPORT
    # =========================================================================

    def _reload_states_from_influx(self) -> None:
        """
        Reload ticker states from InfluxDB movers_state on initialization.
        Queries the latest state for each ticker with cursor in HSET.
        """
        # Get all cursors to know which tickers to reload
        cursors = self.get_all_state_cursors()
        if not cursors:
            logger.info("_reload_states_from_influx - No cursors found, starting fresh")
            return

        range_start, range_end = self._get_reload_time_range()

        logger.info(
            f"_reload_states_from_influx - Reloading states for {len(cursors)} tickers "
            f"(range: {range_start} to {range_end})..."
        )

        for ticker in cursors.keys():
            query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {range_start}, stop: {range_end})
                |> filter(fn: (r) => r["_measurement"] == "movers_state")
                |> filter(fn: (r) => r["symbol"] == "{ticker}")
                |> filter(fn: (r) => r["session_id"] == "{self.session_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"], desc: true)
                |> limit(n: 1)
            """
            try:
                tables = self._query_api.query(query, org=self.org)
                for table in tables:
                    for record in table.records:
                        self._ticker_states[ticker] = {
                            "state": record.values.get("state", "unknown"),
                            "rank": int(record.values.get("rank", 999)),
                            "changePercent": float(
                                record.values.get("changePercent", 0.0)
                            ),
                            "rank_velocity": int(record.values.get("rank_velocity", 0)),
                            "timestamp": record.get_time(),
                        }
                        break
            except Exception as e:
                logger.error(f"_reload_states_from_influx - Error for {ticker}: {e}")

        logger.info(
            f"_reload_states_from_influx - Reloaded {len(self._ticker_states)} states"
        )

    def _get_reload_time_range(self) -> Tuple[str, str]:
        """Get time range for InfluxDB reload queries."""
        if self.run_mode == "replay":
            cursors = self.get_all_state_cursors()

            if cursors:
                min_ts = None
                for ticker, cursor_ts in cursors.items():
                    try:
                        ts = datetime.fromisoformat(cursor_ts)
                        if min_ts is None or ts < min_ts:
                            min_ts = ts
                    except (ValueError, TypeError):
                        continue

                if min_ts:
                    range_start = (
                        f"{db_date_to_date(self.db_date).isoformat()}T00:00:00Z"
                    )
                    range_end = min_ts.isoformat()
                    return range_start, range_end

            range_start = f"{db_date_to_date(self.db_date).isoformat()}T00:00:00Z"
            range_end = "now()"
            return range_start, range_end
        else:
            return "-1d", "now()"

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def close(self):
        """Clean up resources."""
        if self._influx_client:
            self._influx_client.close()
        logger.info("close - StateEngine closed")
