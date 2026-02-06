import asyncio
import logging
import os
import socket
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock, Thread
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import influxdb_client
import redis
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, WriteType

from DataSupply.tickDataSupply.unified_tick_manager import UnifiedTickManager
from utils.logger import setup_logger
from utils.session import make_session_id, parse_session_id

logger = setup_logger("factor_engine", log_to_file=True, level=logging.DEBUG)

load_dotenv()

FACTOR_HISTORY_LIMIT = 2_048


# ============================================================================
# Ticker Context
# ============================================================================
@dataclass
class TickerContext:
    """Ticker Context"""

    symbol: str

    quote_window: list = field(default_factory=list)
    trade_window: list = field(default_factory=list)

    # relative timestamps for last flush
    last_quote_flush_ts: float = 0.0
    last_trade_flush_ts: float = 0.0
    last_factor_flush_ts: float = 0.0

    trade_ts_window: list = field(default_factory=list)
    aggressor_window: list = field(default_factory=list)

    belief_state: dict = field(default_factory=dict)

    last_tick_ts: float = 0.0
    last_calc_ts: float = 0.0
    lock: Lock = field(default_factory=Lock)

    def add_quote(self, quote):
        normalized = dict(quote)
        normalized_timestamp = self._coerce_ts(normalized.get("timestamp"))
        normalized["timestamp"] = normalized_timestamp

        with self.lock:
            self.quote_window.append(normalized)
            self.last_tick_ts = normalized_timestamp

    def add_trade(self, trade):
        normalized = dict(trade)
        normalized_timestamp = self._coerce_ts(normalized.get("timestamp"))
        normalized["timestamp"] = normalized_timestamp

        with self.lock:
            self.trade_window.append(normalized)
            self.trade_ts_window.append(normalized_timestamp)

            aggressor = 0
            if self.quote_window:
                last_q = self.quote_window[-1]
                bid = float(last_q.get("bid", 0) or 0)
                ask = float(last_q.get("ask", 0) or 0)
                price = float(normalized.get("price", 0) or 0)
                if ask and price >= ask:
                    aggressor = 1  # Buy
                elif bid and price <= bid:
                    aggressor = -1  # Sell
            self.aggressor_window.append(aggressor)

            if len(self.trade_ts_window) > FACTOR_HISTORY_LIMIT:
                self.trade_ts_window = self.trade_ts_window[-FACTOR_HISTORY_LIMIT:]
                self.aggressor_window = self.aggressor_window[-FACTOR_HISTORY_LIMIT:]

            self.last_tick_ts = normalized_timestamp

    @staticmethod
    def _coerce_ts(timestamp) -> int:
        if timestamp is None:
            return 0
        if isinstance(timestamp, (int, float)):
            return int(timestamp)
        try:
            return int(float(timestamp))
        except (TypeError, ValueError):
            return 0


class FactorManager:
    """
    Factor Manager - Multi-source data support (Polygon, ThetaData, Replayer)

    Improvements from original:
    - Support for multiple data providers (polygon, theta, replayer)
    - Unified data format handling
    - Cleaner architecture with adapter pattern
    - Better error handling and logging
    """

    DEFAULT_INFLUX_ORG = "jerryhong"
    DEFAULT_INFLUX_BUCKET = "jerryib_trade"

    def __init__(
        self,
        manager_type=None,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize Factor Manager

        Args:
            manager_type: Data manager type ('polygon', 'theta', 'replayer')
                         If None, reads from env var DATA_MANAGER (default: polygon)
            session_id: Unified session identifier (e.g. '20260115_replay_v1')
            redis_config: Redis connection config dict with host, port, db keys
            influxdb_config: InfluxDB connection config dict with influx_url_env key
        """
        # Unified session id — single source of truth for mode & date
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # Parse influxdb config
        influx_cfg = influxdb_config or {}
        influx_url_env = influx_cfg.get("influx_url_env")
        self.influx_url = (
            os.getenv(influx_url_env) if influx_url_env else "http://localhost:8086"
        )
        self.influx_org = influx_cfg.get("org", self.DEFAULT_INFLUX_ORG)
        self.influx_bucket = influx_cfg.get("bucket", self.DEFAULT_INFLUX_BUCKET)
        influx_token_env = influx_cfg.get("influx_token_env")
        self.influx_token = os.getenv(influx_token_env) if influx_token_env else None

        write_client = influxdb_client.InfluxDBClient(
            url=self.influx_url, token=self.influx_token, org=self.influx_org
        )

        self.factor_set = os.getenv("FACTOR_SET", "belief_micro_v1")

        self.quote_window_sec = float(os.getenv("FACTOR_QUOTE_WINDOW_SEC", "0.2"))
        self.trade_window_sec = float(os.getenv("FACTOR_TRADE_WINDOW_SEC", "0.2"))
        self.factor_window_sec = float(os.getenv("FACTOR_FLUSH_WINDOW_SEC", "1.0"))

        # ============================================================================
        # InfluxDB Write Configuration - Real-time + Performance Optimized
        # ============================================================================

        # Callbacks for monitoring write status
        def _influx_success(conf, data):
            logger.debug(
                f"✅ InfluxDB write success: {len(data.split(chr(10)))} points"
            )

        def _influx_error(conf, data, exception):
            logger.error(f"❌ InfluxDB write error: {exception}")

        def _influx_retry(conf, data, exception):
            logger.warning(f"⚠️ InfluxDB write retry: {exception}")

        # Batch write configuration optimized for real-time monitoring
        # flush_interval=1000ms (1 second) provides good balance between:
        #   - Real-time data visibility (max 1s delay)
        #   - Write performance (batching reduces overhead)
        self.write_api = write_client.write_api(
            write_options=WriteOptions(
                batch_size=500,  # Write every 500 points
                flush_interval=1_000,  # Flush every 1 second (real-time monitoring)
                jitter_interval=200,  # Add 200ms jitter to avoid thundering herd
                retry_interval=5_000,  # Retry interval on failure
                max_retries=3,  # Max retry attempts
                max_retry_delay=30_000,  # Max retry delay
                exponential_base=2,  # Exponential backoff base
                write_type=WriteType.asynchronous,  # Async mode for better performance
            ),
            success_callback=_influx_success,
            error_callback=_influx_error,
            retry_callback=_influx_retry,
        )
        # ------- Redis Configuration -------

        # Parse redis config
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        self.STREAM_NAME = f"factor_tasks:{self.session_id}"
        self.CONSUMER_GROUP = "factor_tasks_consumer"
        self.CONSUMER_NAME = f"consumer_{socket.gethostname()}_{os.getpid()}"

        try:
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        # ------- Data Manager Configuration -------
        # Use UnifiedTickManager for all data sources
        self.ws_manager = UnifiedTickManager(provider=manager_type)

        logger.info(
            f"🚀 FactorManager using {self.ws_manager.provider.upper()} data manager"
        )

        # ------- Runtime State -------
        self.active_tickers = set()
        self.contexts = {}
        self.stream_consumers: Dict[str, Future] = {}
        self._threads: Dict[str, Thread] = {}
        self._running = True
        self._shutdown = False

        # thread pool for CPU-bound factor compute
        self.pool = ThreadPoolExecutor(max_workers=8)

        # start an event loop asyncio, for websocket listening
        self.ws_loop = asyncio.new_event_loop()
        self.ws_thread = Thread(target=self._start_ws_loop, daemon=True)
        self.ws_thread.start()

    def _start_ws_loop(self):
        """start WebSocket event loop"""
        asyncio.set_event_loop(self.ws_loop)
        self.ws_loop.create_task(self.ws_manager.stream_forever())
        try:
            self.ws_loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop=self.ws_loop)
            for task in pending:
                task.cancel()
            if pending:
                self.ws_loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self.ws_loop.run_until_complete(self.ws_loop.shutdown_asyncgens())
            self.ws_loop.close()

    def _tasks_listener(self):
        """Redis Stream Listener"""
        logger.info(f"_tasks_listener -  Start consumer: {self.CONSUMER_NAME}")

        while self._running:
            try:
                messages = self.redis_client.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.CONSUMER_NAME,
                    {self.STREAM_NAME: ">"},
                    count=10,
                    block=2000,
                )

                if not messages:
                    continue

                for stream_name, msg_list in messages:
                    for msg_id, msg_data in msg_list:
                        action = msg_data.get(b"action", b"").decode()
                        symbol = msg_data.get(b"ticker", b"").decode().upper()
                        logger.info(
                            f"_tasks_listener - action: {action} symbol: {symbol}"
                        )

                        if action == "add":
                            self.add_ticker(symbol)
                            ctx = TickerContext(symbol)
                            self.contexts[symbol] = ctx

                        elif action == "remove":
                            self.remove_ticker(symbol)

                        self.redis_client.xack(
                            self.STREAM_NAME, self.CONSUMER_GROUP, msg_id
                        )

            except Exception as e:
                if not self._running:
                    break
                logger.error(f"_tasks_listener - Redis: {e}")
                time.sleep(5)

        logger.info("_tasks_listener - Stopped consumer thread")

    def add_ticker(self, symbol):
        """Add ticker with multi-source support using UnifiedTickManager"""
        if symbol in self.active_tickers:
            return

        logger.info(
            f"add_ticker - Adding {symbol} to {self.ws_manager.provider} manager"
        )

        self.active_tickers.add(symbol)
        events = ["Q", "T"]  # Subscribe to both quotes and trades

        # Subscribe using unified interface (no if-else needed!)
        asyncio.run_coroutine_threadsafe(
            self.ws_manager.subscribe(
                websocket_client="factor_manager",
                symbols=[symbol],
                events=events,
            ),
            self.ws_loop,
        )

        # Create consumer tasks for all stream keys
        stream_keys = self.ws_manager.generate_stream_keys(
            symbols=[symbol], events=events
        )
        for stream_key in stream_keys:
            future = asyncio.run_coroutine_threadsafe(
                self._consume_stream_key(stream_key), self.ws_loop
            )
            old_future = self.stream_consumers.pop(stream_key, None)
            if old_future:
                old_future.cancel()
            self.stream_consumers[stream_key] = future
            logger.debug(f"add_ticker - Created consumer for {stream_key}")

    def remove_ticker(self, symbol):
        """Remove ticker with multi-source support using UnifiedTickManager"""
        if symbol not in self.active_tickers:
            return

        logger.info(
            f"remove_ticker - Removing {symbol} from {self.ws_manager.provider} manager"
        )

        self.active_tickers.remove(symbol)
        self.contexts.pop(symbol, None)
        events = ["Q", "T"]

        # Unsubscribe using unified interface (no if-else needed!)
        asyncio.run_coroutine_threadsafe(
            self.ws_manager.unsubscribe(
                websocket_client="factor_manager", symbol=symbol, events=events
            ),
            self.ws_loop,
        )

        stream_keys = self.ws_manager.generate_stream_keys(
            symbols=[symbol], events=events
        )
        for stream_key in stream_keys:
            future = self.stream_consumers.pop(stream_key, None)
            if future:
                future.cancel()

    async def _consume_stream_key(self, stream_key):
        """Consume WebSocket queue with data normalization using UnifiedTickManager"""
        logger.debug(f"_consume_stream_key - Starting consumer for {stream_key}")
        q = self.ws_manager.queues.get(stream_key)
        if q is None:
            logger.warning(f"_consume_stream_key - No queue found for {stream_key}")
            return

        while True:
            try:
                data = await q.get()

                # Normalize data using unified manager (no if-else needed!)
                normalized_data = self.ws_manager.normalize_data(data)

                # logger.debug(f"_consume_stream_key - Received data for {stream_key}: {normalized_data}")
                self.on_tick(stream_key, normalized_data)

            except asyncio.CancelledError:
                logger.info(
                    f"_consume_stream_key - Cancelled consumer for {stream_key}"
                )
                break
            except Exception as e:
                logger.error(
                    f"_consume_stream_key - Error processing {stream_key}: {e}"
                )

    def on_tick(self, stream_key, data):
        """
        Tick data handler - unified format processing

        Data format (normalized):
        - Quote: { event_type: "Q", symbol, bid, ask, bid_size, ask_size, timestamp }
        - Trade: { event_type: "T", symbol, price, size, timestamp }
        """
        symbol = data.get("symbol")
        if not symbol:
            logger.warning(f"on_tick - Missing symbol in data: {data}")
            return

        ctx = self.contexts.get(symbol)
        if not ctx:
            logger.warning(f"on_tick - No context for symbol: {symbol}")
            return

        event_type = data.get("event_type", "").upper()

        if event_type == "Q":
            ctx.add_quote(data)
        elif event_type == "T":
            ctx.add_trade(data)
        else:
            logger.warning(f"on_tick - Unknown event type: {event_type}")
            return

        # Thread pool for factor computation
        self.pool.submit(self.compute_factors, ctx)

    def compute_belief_dynamics(self, ctx: TickerContext) -> dict:
        """
        Compute belief dynamics factors using time-based window (not tick count), Core factor computation part. will continue
        to improve later.

        Args:
            ctx (TickerContext): Context containing trade timestamp window and aggressor window
        Returns:
            dict: Computed belief dynamics factors
        """
        if not ctx.trade_ts_window:
            return None

        lookback_ms = 20000  # 20 seconds
        now_ts = ctx.trade_ts_window[-1]
        cutoff_ts = now_ts - lookback_ms

        # Filter trades within time window
        indices = [i for i, ts in enumerate(ctx.trade_ts_window) if ts >= cutoff_ts]
        if len(indices) < 5:  # Need at least 5 trades to compute
            return None

        ts = [ctx.trade_ts_window[i] for i in indices]
        aggressors = [ctx.aggressor_window[i] for i in indices]

        intervals = [ts[i] - ts[i - 1] for i in range(1, len(ts))]
        if not intervals:
            return None

        mean_interval = sum(intervals) / len(intervals)

        # 1. Trade Rate
        trade_rate = (
            1000.0 / mean_interval if mean_interval > 0 else 0
        )  # trade per second

        # 2. Acceleration: compare first half vs second half of window
        mid = len(intervals) // 2
        first_half = intervals[:mid]
        second_half = intervals[mid:]

        accel = 0.0
        if first_half and second_half:
            accel = (sum(first_half) / len(first_half)) - (
                sum(second_half) / len(second_half)
            )

        # 3. Aggressiveness
        aggressiveness = sum(aggressors) / len(aggressors)

        # 4. buy&sell ratio
        buy_ratio = sum(1 for a in aggressors if a > 0) / len(aggressors)
        sell_ratio = sum(1 for a in aggressors if a < 0) / len(aggressors)

        # logger.debug(
        #     f"compute_belief_dynamics - {ctx.symbol} "
        #     f"trade_rate: {trade_rate:.4f}, "
        #     f"accel: {accel:.4f}, "
        #     f"aggressiveness: {aggressiveness:.4f}")
        return {
            "trade_rate": round(trade_rate, 4),
            "accel": round(accel, 4),
            "aggressiveness": round(aggressiveness, 4),
            "buy_ratio": round(buy_ratio, 4),  # pure buy pressure
            "sell_ratio": round(sell_ratio, 4),  # pure sell pressure
            "trade_count": len(ts),  # actual trade count in window
        }

    def compute_liquidity_depth(self, ctx: TickerContext) -> dict:
        """
        Measure market depth - critical for exit timing
        Small float stocks can reverse violently
        """
        if not ctx.quote_window:
            return None

        lookback_ms = 10_000  # 10 seconds
        now_ts = ctx.last_tick_ts
        cutoff_ts = now_ts - lookback_ms

        recent_quotes = [
            q for q in ctx.quote_window if q.get("timestamp", 0) >= cutoff_ts
        ]
        if len(recent_quotes) < 5:
            return None

        # Bid-Ask imbalance
        bid_sizes = [float(q.get("bid_size", 0)) for q in recent_quotes]
        ask_sizes = [float(q.get("ask_size", 0)) for q in recent_quotes]

        avg_bid_size = sum(bid_sizes) / len(bid_sizes)
        avg_ask_size = sum(ask_sizes) / len(ask_sizes)

        # Positive = more bids (buy pressure), Negative = more asks (sell pressure)
        size_imbalance = (
            (avg_bid_size - avg_ask_size) / (avg_bid_size + avg_ask_size)
            if (avg_bid_size + avg_ask_size) > 0
            else 0
        )

        # Spread widening = liquidity drying up (危险信号)
        spreads = [
            float(q.get("ask", 0)) - float(q.get("bid", 0)) for q in recent_quotes
        ]
        spread_trend = (spreads[-1] - spreads[0]) / spreads[0] if spreads[0] > 0 else 0

        return {
            "bid_ask_imbalance": round(size_imbalance, 4),
            "spread_widening": round(spread_trend, 4),
            "avg_depth": round((avg_bid_size + avg_ask_size) / 2, 2),
        }

    def compute_factors(self, ctx: TickerContext):
        "Compute factors"

        # 200ms for less computing, but not now
        # if now - ctx.last_calc_ts < 0.2:
        #     return

        with ctx.lock:
            belief = self.compute_belief_dynamics(ctx)
            if not belief:
                return

            ctx.belief_state = belief
            factor_ts = ctx.last_tick_ts

        # now = time.time()
        # ctx.last_calc_ts = now

        # Write belief dynamics (factors) with rate limiting
        # if self._should_write_factor(ctx):
        #     self._write_factors_to_influx(ctx.symbol, belief)
        # ctx.last_factor_write_ts = ctx.last_calc_ts

        # with ctx.lock:
        #     if not ctx.quote_window:
        #         return

        #     last = ctx.quote_window[-1]
        #     spread = last["ask"] - last["bid"]

        # TODO: add Redis push later
        # self.redis_client.hset(
        #     f"factor:{ctx.symbol}",
        #     mapping={"mid_price": mid, "ts": now}
        # )

        if factor_ts:
            human_time = datetime.fromtimestamp(factor_ts / 1000).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]
        else:
            human_time = "1970-01-01 00:00:00.000"

        # logger.debug(
        #     f"compute_factors - {ctx.symbol} "
        #     f"factor:belief_state  {ctx.belief_state},"
        #     f"ts: {human_time},"
        # )

    # new influx write methods with window flush
    def flush_quote_window(self, ctx: TickerContext, now: float):
        with ctx.lock:
            if not ctx.quote_window:
                return False
            window = list(ctx.quote_window)
            ctx.quote_window.clear()
            ctx.last_quote_flush_ts = now

        point = self._build_quote_point(ctx.symbol, window)
        self._emit_point(point)
        return point is not None

    def flush_trade_window(self, ctx: TickerContext, now: float):
        with ctx.lock:
            if not ctx.trade_window:
                return False
            window = list(ctx.trade_window)
            ctx.trade_window.clear()
            ctx.last_trade_flush_ts = now

        point = self._build_trade_point(ctx.symbol, window)
        self._emit_point(point)
        return point is not None

    def compute_and_flush_factors(self, ctx: TickerContext, now: float):
        with ctx.lock:
            if not ctx.belief_state:
                return False
            factors = dict(ctx.belief_state)
            # logger.debug(f"compute_and_flush_factors - Flushing factors for {ctx.symbol}: {factors}")
            factor_ts = ctx.last_tick_ts or factors.get("last_tick_ts")
            ctx.last_factor_flush_ts = now

        point = self._build_factor_point(ctx.symbol, factors, factor_ts)
        self._emit_point(point)
        return point is not None

    def _window_flush_loop(self):
        while self._running:
            now = time.time()

            for ctx in list(self.contexts.values()):
                if now - ctx.last_quote_flush_ts >= self.quote_window_sec:
                    self.flush_quote_window(ctx, now)

                if now - ctx.last_trade_flush_ts >= self.trade_window_sec:
                    self.flush_trade_window(ctx, now)

                if now - ctx.last_factor_flush_ts >= self.factor_window_sec:
                    self.compute_and_flush_factors(ctx, now)

            time.sleep(0.01)

        logger.info("_window_flush_loop - Stopped window scheduler")

    def _build_quote_point(self, symbol: str, window: List[dict]) -> Optional[Point]:
        if not window:
            return None

        last = window[-1]
        ts = TickerContext._coerce_ts(last.get("timestamp"))
        if not ts:
            return None

        bids = [float(entry.get("bid", 0) or 0.0) for entry in window]
        asks = [float(entry.get("ask", 0) or 0.0) for entry in window]
        bid_sizes = [float(entry.get("bid_size", 0) or 0.0) for entry in window]
        ask_sizes = [float(entry.get("ask_size", 0) or 0.0) for entry in window]
        mids = [(b + a) / 2 for b, a in zip(bids, asks)]
        spreads = [max(a - b, 0.0) for b, a in zip(bids, asks)]

        mid_price = (bids[-1] + asks[-1]) / 2 if bids and asks else 0.0

        # logger.debug(f"_build_quote_point - Writing quote for {symbol}: {window}")

        return (
            Point("market_quote")
            .tag("symbol", symbol)
            .tag("session_id", self.session_id)
            .field("last_bid", bids[-1] if bids else 0.0)
            .field("last_ask", asks[-1] if asks else 0.0)
            .field("last_bid_size", bid_sizes[-1] if bid_sizes else 0.0)
            .field("last_ask_size", ask_sizes[-1] if ask_sizes else 0.0)
            .field("mid_price", mid_price)
            .field("avg_mid", self._safe_avg(mids))
            .field("spread", spreads[-1] if spreads else 0.0)
            .field("avg_spread", self._safe_avg(spreads))
            .field("quote_count", len(window))
            .time(ts, WritePrecision.MS)
        )

    def _build_trade_point(self, symbol: str, window: List[dict]) -> Optional[Point]:
        if not window:
            return None

        last = window[-1]
        ts = TickerContext._coerce_ts(last.get("timestamp"))
        if not ts:
            return None

        prices = [float(entry.get("price", 0) or 0.0) for entry in window]
        sizes = [float(entry.get("size", 0) or 0.0) for entry in window]
        notional = sum(p * s for p, s in zip(prices, sizes))
        volume = sum(sizes)
        vwap = notional / volume if volume else (prices[-1] if prices else 0.0)

        return (
            Point("market_trade")
            .tag("symbol", symbol)
            .tag("session_id", self.session_id)
            .field("last_price", prices[-1] if prices else 0.0)
            .field("last_size", sizes[-1] if sizes else 0.0)
            .field("vwap", vwap)
            .field("volume", volume)
            .field("trade_count", len(window))
            .time(ts, WritePrecision.MS)
        )

    def _build_factor_point(
        self, symbol: str, factors: dict, factor_ts
    ) -> Optional[Point]:
        ts = TickerContext._coerce_ts(factor_ts)
        if not ts:
            return None

        return (
            Point("belief_dynamics")
            .tag("symbol", symbol)
            .tag("session_id", self.session_id)
            .tag("factor_set", self.factor_set)
            .field("trade_rate", float(factors.get("trade_rate", 0.0)))
            .field("accel", float(factors.get("accel", 0.0)))
            .field("aggressiveness", float(factors.get("aggressiveness", 0.0)))
            .time(ts, WritePrecision.MS)
        )

    def _emit_point(self, point: Optional[Point]):
        if point is None:
            return
        try:
            self.write_api.write(
                bucket=self.influx_bucket, org=self.influx_org, record=point
            )
        except Exception as exc:
            logger.error(f"_emit_point - Failed to write to InfluxDB: {exc}")

    @staticmethod
    def _safe_avg(values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    def stop(self):
        if self._shutdown:
            return

        logger.info("stop - Shutting down FactorManager")
        self._shutdown = True
        self._running = False

        for future in list(self.stream_consumers.values()):
            future.cancel()
        self.stream_consumers.clear()

        if self.ws_loop.is_running():
            self.ws_loop.call_soon_threadsafe(self.ws_loop.stop)

        self.pool.shutdown(wait=False)

        for name, thread in self._threads.items():
            thread.join(timeout=2)
            logger.debug(f"stop - Joined thread {name}")

        if hasattr(self, "ws_thread"):
            self.ws_thread.join(timeout=2)

        try:
            self.redis_client.close()
        except Exception:
            pass

        logger.info("stop - FactorManager stopped")

    def start(self):
        """Start manager"""
        tasks_thread = Thread(target=self._tasks_listener, daemon=True)
        tasks_thread.start()
        self._threads["tasks_listener"] = tasks_thread

        window_thread = Thread(target=self._window_flush_loop, daemon=True)
        window_thread.start()
        self._threads["window_flush"] = window_thread

        logger.info("start - FactorManager started.")


if __name__ == "__main__":
    """
    Run Factor Manager with support for multiple data sources

    Usage:
        python -m src.FactorEngine.factor_engine
        python -m src.FactorEngine.factor_engine --replay-date 20240115
        python -m src.FactorEngine.factor_engine --manager-type theta
        python -m src.FactorEngine.factor_engine --replay-date 20240115 --manager-type replayer

    Environment Variables:
        DATA_MANAGER: Data source type (polygon/theta/replayer), default: polygon
        POLYGON_API_KEY: Polygon.io API key (for polygon manager)
        REPLAY_URL: Replay server URL (for replayer manager), default: ws://127.0.0.1:8765
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Factor Manager - Multi-source market data processor"
    )

    parser.add_argument(
        "--replay-date",
        help="Replay date in YYYYMMDD format (e.g., 20240115)",
        type=str,
    )

    parser.add_argument(
        "--manager-type",
        choices=["polygon", "theta", "replayer"],
        help="Data manager type (overrides DATA_MANAGER env var)",
        type=str,
    )

    parser.add_argument(
        "--suffix-id",
        help="Optional suffix_id tag for database identification (defaults to replay date)",
        type=str,
    )

    args = parser.parse_args()

    # Validate replay date format
    if args.replay_date:
        try:
            datetime.strptime(args.replay_date, "%Y%m%d")
        except ValueError:
            parser.error("replay-date must be in format YYYYMMDD (e.g., 20240115)")

    # Build session_id from CLI args
    session_id = make_session_id(
        replay_date=args.replay_date,
        suffix_id=args.suffix_id,
    )

    # Create and start manager
    fm = FactorManager(
        session_id=session_id,
        manager_type=args.manager_type,
    )
    fm.start()

    # Keep running
    logger.info("Factor Manager is running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down Factor Manager...")
    finally:
        fm.stop()
