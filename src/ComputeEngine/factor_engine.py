import asyncio
import logging
import math
import os
import socket
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import influxdb_client
import redis
import requests
from dotenv import load_dotenv
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, WriteType

from DataSupply.tickDataSupply.unified_tick_manager import UnifiedTickManager
from utils.logger import setup_logger
from utils.redis_keys import factor_tasks_stream
from utils.session import make_session_id, parse_session_id

logger = setup_logger("factor_engine", log_to_file=True, level=logging.DEBUG)

load_dotenv()

# ── Tuning constants ─────────────────────────────────────────────────────
TRADE_TS_MAXLEN = 50_000  # max raw trade timestamps to keep (~10 min of busy stock)
RATE_WINDOW_MS = 20_000  # 20 s window for trade_rate
BASELINE_LEN = 120  # rolling baseline length (120 × 1 s = 2 min)
COMPUTE_INTERVAL_SEC = 1.0  # factor compute every 1 s
MIN_TRADES_FOR_RATE = 5  # need at least 5 trades to compute rate

# ── REST bootstrap ───────────────────────────────────────────────────────
POLYGON_REST_BASE = "https://api.polygon.io"
BOOTSTRAP_BATCH_SIZE = 50_000  # max per page
BOOTSTRAP_MARKET_OPEN_ET = 4  # 4:00 AM ET (pre-market open)


# ============================================================================
# Ticker Context  (minimal — only what factors need)
# ============================================================================
@dataclass
class TickerContext:
    """Per-ticker state consumed by the factor compute loop."""

    symbol: str

    # Raw trades as (ts_ms, price) tuples, sorted ascending by ts.
    # Populated by real-time ticks *and* historical bootstrap.
    trades: deque = field(default_factory=lambda: deque(maxlen=TRADE_TS_MAXLEN))

    # Rolling factor baselines for z-score (one value per compute tick)
    trade_rate_history: deque = field(
        default_factory=lambda: deque(maxlen=BASELINE_LEN)
    )
    accel_history: deque = field(default_factory=lambda: deque(maxlen=BASELINE_LEN))

    # Is the baseline warm enough for z-score output?
    # Set to True after historical bootstrap fills enough history,
    # or after BASELINE_LEN compute ticks have passed.
    warm: bool = False

    # Last compute-step timestamp covered by bootstrap.
    # Real-time emit starts only for ts_ms > bootstrap_end_ms.
    # 0 = no bootstrap ran (emit freely).
    bootstrap_end_ms: int = 0

    lock: Lock = field(default_factory=Lock)

    # ── helpers ──────────────────────────────────────────────────────────
    def add_trade(self, ts_ms: int, price: float) -> None:
        """Append a single trade (timestamp in ms, price)."""
        with self.lock:
            self.trades.append((ts_ms, price))

    def bootstrap_trades(self, trade_list: List[Tuple[int, float]]) -> None:
        """Bulk-load historical trades as (ts_ms, price) tuples.
        Called once after REST fetch completes.  Deduplicates against
        any real-time ticks that arrived while the fetch was in flight.
        NOTE: Does NOT set warm — that is done by _build_baseline."""
        with self.lock:
            existing = {t[0]: t for t in self.trades}  # keyed by ts_ms
            for ts_ms, price in trade_list:
                if ts_ms not in existing:
                    existing[ts_ms] = (ts_ms, price)
            merged = sorted(existing.values(), key=lambda t: t[0])
            self.trades.clear()
            self.trades.extend(merged[-TRADE_TS_MAXLEN:])
            logger.info(
                f"bootstrap_trades - {self.symbol}: loaded {len(trade_list)} "
                f"historical trades, merged to {len(self.trades)}"
            )

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
    Factor Engine — computes trade_rate & accel with rolling z-scores.

    Architecture:
    - Real-time trade timestamps arrive via fan-out queues (UnifiedTickManager)
    - Time-triggered compute loop runs every COMPUTE_INTERVAL_SEC (1 s)
    - For each active ticker:
        1. trade_rate  = trades in last RATE_WINDOW_MS / window_seconds
        2. accel       = rate(recent half) − rate(older half)
        3. z-score     = (value − mean(baseline)) / std(baseline)
    - Outputs to InfluxDB (trade_activity measurement) and Redis HSET
    """

    DEFAULT_INFLUX_ORG = "jerryhong"
    DEFAULT_INFLUX_BUCKET = "jerryib_trade"

    def __init__(
        self,
        manager_type=None,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
        ws_manager: Optional[UnifiedTickManager] = None,
        ws_loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        # Unified session id
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # ── InfluxDB ─────────────────────────────────────────────────────
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
        self.write_api = write_client.write_api(
            write_options=WriteOptions(
                batch_size=500,
                flush_interval=1_000,
                jitter_interval=200,
                retry_interval=5_000,
                max_retries=3,
                max_retry_delay=30_000,
                exponential_base=2,
                write_type=WriteType.asynchronous,
            ),
        )

        # ── Redis ────────────────────────────────────────────────────────
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        self.STREAM_NAME = factor_tasks_stream(self.session_id)
        self.CONSUMER_GROUP = "factor_tasks_consumer"
        self.CONSUMER_NAME = f"consumer_{socket.gethostname()}_{os.getpid()}"

        try:
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        # ── Data Manager ─────────────────────────────────────────────────
        if ws_manager:
            self.ws_manager = ws_manager
            self._owns_manager = False
        else:
            self.ws_manager = UnifiedTickManager(provider=manager_type)
            self._owns_manager = True

        logger.info(
            f"🚀 FactorManager using {self.ws_manager.provider.upper()} data manager "
            f"(shared={not self._owns_manager})"
        )

        # ── Runtime State ────────────────────────────────────────────────
        self.active_tickers: set = set()
        self.contexts: Dict[str, TickerContext] = {}
        self.stream_consumers: Dict[str, Future] = {}
        self._threads: Dict[str, Thread] = {}
        self._running = True
        self._shutdown = False

        # Event loop for async operations
        if self._owns_manager:
            self.ws_loop = asyncio.new_event_loop()
            self.ws_thread = Thread(target=self._start_ws_loop, daemon=True)
            self.ws_thread.start()
        else:
            self.ws_loop = ws_loop
            if self.ws_loop is None:
                raise ValueError("ws_loop is required when using a shared ws_manager")

    # ====================================================================
    # Event loop / WS plumbing  (unchanged)
    # ====================================================================
    def _start_ws_loop(self):
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

    # ====================================================================
    # Redis Stream Listener  (unchanged)
    # ====================================================================
    def _tasks_listener(self):
        logger.info(f"_tasks_listener - Start consumer: {self.CONSUMER_NAME}")
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
                        action = msg_data.get("action", "")
                        symbol = msg_data.get("ticker", "").upper()
                        logger.info(
                            f"_tasks_listener - action: {action} symbol: {symbol}"
                        )
                        if action == "add":
                            self.add_ticker(symbol)
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

    # ====================================================================
    # Ticker add / remove  (only T events needed for factor compute)
    # ====================================================================
    def add_ticker(self, symbol):
        if symbol in self.active_tickers:
            return

        logger.info(f"add_ticker - Adding {symbol}")
        self.active_tickers.add(symbol)
        self.contexts[symbol] = TickerContext(symbol=symbol)

        # Only subscribe to Trades — quotes go to frontend via fan-out
        events = ["T"]

        asyncio.run_coroutine_threadsafe(
            self.ws_manager.subscribe(
                websocket_client="factor_manager",
                symbols=[symbol],
                events=events,
            ),
            self.ws_loop,
        )

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

        # Kick off background REST bootstrap (historical trades → warm baseline)
        Thread(
            target=self._bootstrap_ticker,
            args=(symbol,),
            daemon=True,
            name=f"bootstrap-{symbol}",
        ).start()

    # ====================================================================
    # Historical trade bootstrap (Polygon REST API)
    # ====================================================================
    def _bootstrap_ticker(self, symbol: str) -> None:
        """Fetch historical trades from Polygon REST API back to market open
        (4:00 AM ET) and warm the baseline.  Runs in a background thread.

        Flow:
        1. REST fetch  (IO-bound, takes seconds)
        2. Merge into trade_ts deque
        3. _build_baseline  (fast, in-memory) → fills deques, sets warm +
           bootstrap_end_ms, returns list of factor dicts
        4. Real-time compute loop can now emit z-scores immediately
        5. Background thread writes historical points to InfluxDB (non-blocking)
        """
        ctx = self.contexts.get(symbol)
        if ctx is None:
            return

        api_key = os.getenv("POLYGON_API_KEY")
        if not api_key:
            logger.warning(f"_bootstrap_ticker - No POLYGON_API_KEY, skipping {symbol}")
            return

        try:
            trades = self._fetch_polygon_trades(symbol, api_key)
            if not trades:
                logger.info(f"_bootstrap_ticker - {symbol}: no historical trades found")
                return

            ctx.bootstrap_trades(trades)

            # Phase 1 (fast): fill baseline deques + set warm
            points = self._build_baseline(ctx)
            logger.info(
                f"_bootstrap_ticker - {symbol}: baseline ready, "
                f"{len(ctx.trade_rate_history)} samples, "
                f"bootstrap_end_ms={ctx.bootstrap_end_ms}, "
                f"{len(points)} points to emit"
            )

            # Phase 2 (async): write historical points to InfluxDB
            if points:
                Thread(
                    target=self._emit_bootstrap_points,
                    args=(symbol, points),
                    daemon=True,
                    name=f"bootstrap-emit-{symbol}",
                ).start()
        except Exception as e:
            logger.error(f"_bootstrap_ticker - {symbol}: {e}")

    def _fetch_polygon_trades(
        self, symbol: str, api_key: str
    ) -> List[Tuple[int, float]]:
        """Paginate Polygon /v3/trades/{symbol} from now back to 4 AM ET.

        Returns list of (ts_ms, price) tuples, sorted ascending by ts.
        """
        # Compute cutoff: today 4:00 AM ET
        now_et = datetime.now(ZoneInfo("America/New_York"))
        cutoff_et = now_et.replace(
            hour=BOOTSTRAP_MARKET_OPEN_ET, minute=0, second=0, microsecond=0
        )
        cutoff_ns = int(cutoff_et.timestamp() * 1e9)  # API uses nanoseconds

        all_trades: List[Tuple[int, float]] = []
        url = (
            f"{POLYGON_REST_BASE}/v3/trades/{symbol}"
            f"?order=desc&limit={BOOTSTRAP_BATCH_SIZE}&sort=timestamp"
            f"&timestamp.gte={cutoff_ns}"
            f"&apiKey={api_key}"
        )

        page = 0
        while url:
            page += 1
            try:
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"_fetch_polygon_trades - {symbol} page {page}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for trade in results:
                sip_ts_ns = trade.get("sip_timestamp", 0)
                price = trade.get("price", 0.0)
                ts_ms = sip_ts_ns // 1_000_000  # ns → ms
                if price > 0:
                    all_trades.append((ts_ms, price))

            # Check if we've reached data before cutoff
            oldest_ns = results[-1].get("sip_timestamp", 0)
            if oldest_ns and oldest_ns < cutoff_ns:
                break

            # Pagination: follow next_url if present
            next_url = data.get("next_url")
            if next_url:
                # next_url already has query params; just append apiKey
                url = f"{next_url}&apiKey={api_key}"
            else:
                break

            # Ticker may have been removed while we're fetching
            if symbol not in self.active_tickers:
                logger.info(f"_fetch_polygon_trades - {symbol} removed, aborting")
                return []

        logger.info(
            f"_fetch_polygon_trades - {symbol}: fetched {len(all_trades)} trades "
            f"in {page} pages"
        )
        # Return sorted ascending by timestamp
        all_trades.sort(key=lambda t: t[0])
        return all_trades

    def _build_baseline(self, ctx: TickerContext) -> List[dict]:
        """Fast, in-memory only.  Walk historical trades to fill
        trade_rate_history / accel_history, set warm + bootstrap_end_ms.

        Returns a list of factor dicts for async InfluxDB emit."""
        with ctx.lock:
            snap = list(ctx.trades)
        if len(snap) < MIN_TRADES_FOR_RATE:
            return []

        start_ms = snap[0][0] + RATE_WINDOW_MS  # need a full window
        end_ms = snap[-1][0]
        step_ms = int(COMPUTE_INTERVAL_SEC * 1000)

        points: List[dict] = []
        t = start_ms
        while t <= end_ms:
            cutoff = t - RATE_WINDOW_MS
            half_cutoff = t - RATE_WINDOW_MS // 2

            window = [(ts, p) for ts, p in snap if cutoff <= ts <= t]
            recent = [(ts, p) for ts, p in window if ts >= half_cutoff]
            older = [(ts, p) for ts, p in window if ts < half_cutoff]

            if len(window) >= MIN_TRADES_FOR_RATE:
                window_sec = RATE_WINDOW_MS / 1000.0
                trade_rate = len(window) / window_sec
                accel = self._price_accel(recent, older)

                with ctx.lock:
                    ctx.trade_rate_history.append(trade_rate)
                    ctx.accel_history.append(accel)

                trade_rate_z = self._z_score(trade_rate, ctx.trade_rate_history)
                accel_z = self._z_score(accel, ctx.accel_history)

                points.append(
                    {
                        "ts_ms": t,
                        "trade_rate": trade_rate,
                        "accel": accel,
                        "trade_rate_z": trade_rate_z,
                        "accel_z": accel_z,
                        "trade_count": len(window),
                    }
                )

            t += step_ms

        # ── Set warm + handoff boundary under lock ───────────────────────
        with ctx.lock:
            if len(ctx.trade_rate_history) >= MIN_TRADES_FOR_RATE:
                ctx.warm = True
            ctx.bootstrap_end_ms = end_ms

        return points

    def _emit_bootstrap_points(self, symbol: str, points: List[dict]) -> None:
        """Write historical factor points to InfluxDB in a background thread.
        Does NOT touch Redis HSET (that is real-time only)."""
        batch: List[Point] = []
        for p in points:
            pt = (
                Point("trade_activity")
                .tag("symbol", symbol)
                .tag("session_id", self.session_id)
                .tag("source", "bootstrap")
                .field("trade_rate", round(p["trade_rate"], 4))
                .field("accel", round(p["accel"], 4))
                .field("trade_count", p["trade_count"])
                .field("warm", True)
            )
            if p["trade_rate_z"] is not None:
                pt = pt.field("trade_rate_z", round(p["trade_rate_z"], 4))
            if p["accel_z"] is not None:
                pt = pt.field("accel_z", round(p["accel_z"], 4))
            pt = pt.time(p["ts_ms"], WritePrecision.MS)
            batch.append(pt)

        try:
            self.write_api.write(
                bucket=self.influx_bucket, org=self.influx_org, record=batch
            )
            logger.info(
                f"_emit_bootstrap_points - {symbol}: queued {len(batch)} points"
            )
        except Exception as exc:
            logger.error(f"_emit_bootstrap_points - {symbol}: {exc}")

    def remove_ticker(self, symbol):
        if symbol not in self.active_tickers:
            return
        logger.info(f"remove_ticker - Removing {symbol}")
        self.active_tickers.discard(symbol)
        self.contexts.pop(symbol, None)
        events = ["T"]

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

        # Clean up Redis HSET for this ticker
        hset_key = f"factor:{self.session_id}:{symbol}"
        try:
            self.redis_client.delete(hset_key)
        except Exception:
            pass

    # ====================================================================
    # Queue consumer — just appends trade timestamps
    # ====================================================================
    async def _consume_stream_key(self, stream_key):
        logger.debug(f"_consume_stream_key - Starting consumer for {stream_key}")
        q = self.ws_manager.get_client_queue("factor_manager", stream_key)
        if q is None:
            logger.warning(f"_consume_stream_key - No queue found for {stream_key}")
            return
        while True:
            try:
                data = await q.get()
                normalized = self.ws_manager.normalize_data(data)
                self._on_trade(normalized)
            except asyncio.CancelledError:
                logger.info(f"_consume_stream_key - Cancelled for {stream_key}")
                break
            except Exception as e:
                logger.error(f"_consume_stream_key - Error {stream_key}: {e}")

    def _on_trade(self, data: dict) -> None:
        """Append trade (timestamp, price) to the ticker context."""
        symbol = data.get("symbol")
        if not symbol:
            return
        ctx = self.contexts.get(symbol)
        if not ctx:
            return
        ts = TickerContext._coerce_ts(data.get("timestamp"))
        price = data.get("price")
        if ts and price is not None:
            ctx.add_trade(ts, float(price))

    # ====================================================================
    # Time-triggered compute loop  (replaces tick-triggered compute)
    # ====================================================================
    def _compute_loop(self):
        """
        Runs every COMPUTE_INTERVAL_SEC.  For each active ticker:
        1. trade_rate  = count(trades in last RATE_WINDOW_MS) / window_sec
        2. accel       = price return_rate(recent) − return_rate(older)
        3. z-score     = (value − μ) / σ  over rolling baseline
        4. Emit to InfluxDB + Redis HSET
        """
        logger.info("_compute_loop - Started")
        while self._running:
            for ctx in list(self.contexts.values()):
                try:
                    self._compute_ticker(ctx)
                except Exception as e:
                    logger.error(f"_compute_loop - {ctx.symbol}: {e}")
            time.sleep(COMPUTE_INTERVAL_SEC)
        logger.info("_compute_loop - Stopped")

    def _compute_ticker(self, ctx: TickerContext) -> None:
        """Compute trade_rate, price accel, z-scores for one ticker."""
        with ctx.lock:
            if len(ctx.trades) < MIN_TRADES_FOR_RATE:
                return
            # snapshot the deque under the lock
            snap = list(ctx.trades)

        now_ms = snap[-1][0]  # latest trade timestamp as reference
        cutoff = now_ms - RATE_WINDOW_MS
        half_cutoff = now_ms - RATE_WINDOW_MS // 2

        # Trades in the full window and each half
        window = [(ts, p) for ts, p in snap if ts >= cutoff]
        recent = [(ts, p) for ts, p in window if ts >= half_cutoff]
        older = [(ts, p) for ts, p in window if ts < half_cutoff]

        if len(window) < MIN_TRADES_FOR_RATE:
            return

        window_sec = RATE_WINDOW_MS / 1000.0

        # ── trade_rate (trades / sec) ────────────────────────────────────
        trade_rate = len(window) / window_sec

        # ── price accel (direction-aware) ────────────────────────────────
        accel = self._price_accel(recent, older)

        # ── append to baseline ───────────────────────────────────────────
        with ctx.lock:
            ctx.trade_rate_history.append(trade_rate)
            ctx.accel_history.append(accel)

            # Mark warm if enough baseline
            if not ctx.warm and len(ctx.trade_rate_history) >= BASELINE_LEN:
                ctx.warm = True
                logger.info(f"_compute_ticker - {ctx.symbol} baseline warm")

        # ── z-scores (only if warm) ──────────────────────────────────────
        trade_rate_z = self._z_score(trade_rate, ctx.trade_rate_history)
        accel_z = self._z_score(accel, ctx.accel_history)

        # ── skip emit for timestamps already covered by bootstrap ────────
        if ctx.bootstrap_end_ms and now_ms <= ctx.bootstrap_end_ms:
            return

        # ── emit ─────────────────────────────────────────────────────────
        self._emit_factors(
            symbol=ctx.symbol,
            ts_ms=now_ms,
            trade_rate=trade_rate,
            accel=accel,
            trade_rate_z=trade_rate_z,
            accel_z=accel_z,
            warm=ctx.warm,
            trade_count=len(window),
        )

    # ====================================================================
    # z-score helper
    # ====================================================================
    @staticmethod
    def _z_score(value: float, history: deque) -> Optional[float]:
        """Return z-score, or None if not enough data."""
        n = len(history)
        if n < 2:
            return None
        mean = sum(history) / n
        var = sum((x - mean) ** 2 for x in history) / n
        std = math.sqrt(var)
        if std < 1e-9:
            return 0.0
        return (value - mean) / std

    # ====================================================================
    # Price acceleration helper
    # ====================================================================
    @staticmethod
    def _price_accel(
        recent: List[Tuple[int, float]],
        older: List[Tuple[int, float]],
    ) -> float:
        """Direction-aware price acceleration.

        Computes the return-rate (% per second) in each half of the window,
        then returns the difference:  accel = return_rate_recent − return_rate_older.

        Positive → price accelerating upward.
        Negative → price accelerating downward.
        Units: fractional return per second (e.g. 0.0002 = 0.02 %/s).
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

    # ====================================================================
    # Output: InfluxDB + Redis HSET
    # ====================================================================
    def _emit_factors(
        self,
        symbol: str,
        ts_ms: int,
        trade_rate: float,
        accel: float,
        trade_rate_z: Optional[float],
        accel_z: Optional[float],
        warm: bool,
        trade_count: int,
        source: str = "realtime",
    ) -> None:
        """Write factor snapshot to InfluxDB and Redis."""

        # ── InfluxDB point ───────────────────────────────────────────────
        point = (
            Point("trade_activity")
            .tag("symbol", symbol)
            .tag("session_id", self.session_id)
            .tag("source", source)
            .field("trade_rate", round(trade_rate, 4))
            .field("accel", round(accel, 4))
            .field("trade_count", trade_count)
            .field("warm", warm)
        )
        if trade_rate_z is not None:
            point = point.field("trade_rate_z", round(trade_rate_z, 4))
        if accel_z is not None:
            point = point.field("accel_z", round(accel_z, 4))
        point = point.time(ts_ms, WritePrecision.MS)

        try:
            self.write_api.write(
                bucket=self.influx_bucket, org=self.influx_org, record=point
            )
        except Exception as exc:
            logger.error(f"_emit_factors - InfluxDB write failed: {exc}")

        # ── Redis HSET (latest snapshot for state_engine / debug) ────────
        hset_key = f"factor:{self.session_id}:{symbol}"
        mapping = {
            "trade_rate": str(round(trade_rate, 4)),
            "accel": str(round(accel, 4)),
            "trade_count": str(trade_count),
            "warm": "1" if warm else "0",
            "ts_ms": str(ts_ms),
        }
        if trade_rate_z is not None:
            mapping["trade_rate_z"] = str(round(trade_rate_z, 4))
        if accel_z is not None:
            mapping["accel_z"] = str(round(accel_z, 4))
        try:
            self.redis_client.hset(hset_key, mapping=mapping)
        except Exception as exc:
            logger.error(f"_emit_factors - Redis HSET failed: {exc}")

    # ====================================================================
    # Lifecycle
    # ====================================================================
    def stop(self):
        if self._shutdown:
            return
        logger.info("stop - Shutting down FactorManager")
        self._shutdown = True
        self._running = False

        for future in list(self.stream_consumers.values()):
            future.cancel()
        self.stream_consumers.clear()

        if self._owns_manager and self.ws_loop and self.ws_loop.is_running():
            self.ws_loop.call_soon_threadsafe(self.ws_loop.stop)

        for name, thread in self._threads.items():
            thread.join(timeout=2)
            logger.debug(f"stop - Joined thread {name}")

        if self._owns_manager and hasattr(self, "ws_thread"):
            self.ws_thread.join(timeout=2)

        try:
            self.redis_client.close()
        except Exception:
            pass
        logger.info("stop - FactorManager stopped")

    def start(self):
        """Start the factor engine threads."""
        tasks_thread = Thread(target=self._tasks_listener, daemon=True)
        tasks_thread.start()
        self._threads["tasks_listener"] = tasks_thread

        compute_thread = Thread(target=self._compute_loop, daemon=True)
        compute_thread.start()
        self._threads["compute_loop"] = compute_thread

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
