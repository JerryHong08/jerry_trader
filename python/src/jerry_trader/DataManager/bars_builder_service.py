"""
BarsBuilderService — orchestrates tick-to-bar aggregation.

Wires together:
  1. UnifiedTickManager  (existing, shared with TickDataServer)
  2. Rust BarBuilder      (jerry_trader._rust.BarBuilder)
  3. ClickHouse           (clickhouse-connect for bar persistence)
  4. Redis pub/sub        (broadcast completed bars to WebSocket layer)

Architecture
────────────
  Polygon / ThetaData / Replayer ticks
            │
            ▼
  UnifiedTickManager (shared asyncio queue fan-out)
            │
            ▼
  BarBuilder (Rust #[pyclass])  ←  this service feeds it
            │
    ┌───────┼────────────┐
    ▼       ▼            ▼
 ClickHouse  Redis PUB   Future: FactorEngine
 (persist)  (WS push)

Co-location with TickDataServer
────────────────────────────────
When both BarsBuilder and TickDataServer run on the same machine,
they share a single UnifiedTickManager instance (same as FactorEngine).
backend_starter passes ws_manager + ws_loop in that case.
"""

import asyncio
import logging
import os
import time
from concurrent.futures import Future
from threading import Thread
from typing import Any, Dict, List, Optional

import clickhouse_connect
import redis

from jerry_trader._rust import BarBuilder
from jerry_trader.clock import clock
from jerry_trader.DataSupply.tickDataSupply.unified_tick_manager import (
    UnifiedTickManager,
)
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.redis_keys import factor_tasks_stream
from jerry_trader.utils.session import make_session_id, parse_session_id

logger = setup_logger("bars_builder", log_to_file=True, level=logging.DEBUG)

# ── Constants ────────────────────────────────────────────────────────────
CLIENT_ID = "bars_builder"  # queue fan-out identity in UnifiedTickManager
BATCH_SIZE = 500  # ClickHouse insert batch size
FLUSH_INTERVAL_SEC = 2.0  # max seconds before flushing pending bars


class BarsBuilderService:
    """
    Tick-to-bar aggregation service.

    Subscribes to trade ticks via UnifiedTickManager, feeds them to
    the Rust BarBuilder, persists completed bars to ClickHouse,
    and publishes them on Redis for real-time WebSocket delivery.
    """

    def __init__(
        self,
        *,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
        timeframes: Optional[List[str]] = None,
        ws_manager: Optional[UnifiedTickManager] = None,
        ws_loop: Optional[asyncio.AbstractEventLoop] = None,
        manager_type: Optional[str] = None,
    ):
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # ── Rust BarBuilder ──────────────────────────────────────────
        self.timeframes = timeframes or [
            "10s",
            "1m",
            "5m",
            "15m",
            "30m",
            "1h",
            "4h",
            "1d",
            "1w",
        ]
        self.bar_builder = BarBuilder(self.timeframes)
        logger.info(f"BarBuilder initialized: {self.bar_builder}")

        # ── ClickHouse ───────────────────────────────────────────────
        ch_cfg = clickhouse_config or {}
        ch_host = ch_cfg.get("host", "localhost")
        ch_port = ch_cfg.get("port", 8123)
        ch_user = ch_cfg.get("user", "default")
        ch_db = ch_cfg.get("database", "jerry_trader")
        password_env = ch_cfg.get("password_env", "CLICKHOUSE_PASSWORD")
        ch_password = os.getenv(password_env, "")

        self.ch_client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_password,
            database=ch_db,
        )
        logger.info(
            f"ClickHouse connected: {ch_host}:{ch_port}/{ch_db} "
            f"(user={ch_user}, password={'***' if ch_password else 'EMPTY'})"
        )

        # Pending completed bars waiting to be flushed to ClickHouse
        self._pending_bars: List[Dict] = []

        # ── Redis ────────────────────────────────────────────────────
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )
        self.STREAM_NAME = factor_tasks_stream(self.session_id)
        self.CONSUMER_GROUP = "bars_builder_consumer"
        self.CONSUMER_NAME = f"bars_builder_{os.getpid()}"

        try:
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        # ── UnifiedTickManager ───────────────────────────────────────
        if ws_manager:
            self.ws_manager = ws_manager
            self._owns_manager = False
        else:
            self.ws_manager = UnifiedTickManager(provider=manager_type)
            self._owns_manager = True

        if self._owns_manager:
            self.ws_loop = asyncio.new_event_loop()
            self._ws_thread = Thread(
                target=self._run_ws_loop, daemon=True, name="BarsBuilder-WS"
            )
            self._ws_thread.start()
        else:
            self.ws_loop = ws_loop
            if self.ws_loop is None:
                raise ValueError("ws_loop required when using shared ws_manager")

        logger.info(
            f"UnifiedTickManager: provider={self.ws_manager.provider}, "
            f"shared={not self._owns_manager}"
        )

        # ── Runtime state ────────────────────────────────────────────
        self.active_tickers: set = set()
        self.stream_consumers: Dict[str, Future] = {}
        self._threads: Dict[str, Thread] = {}
        self._running = True
        self._shutdown = False
        self._stats = {"ticks_ingested": 0, "bars_completed": 0, "bars_written": 0}

    # ════════════════════════════════════════════════════════════════════
    # Lifecycle
    # ════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        """Start the bars builder threads."""
        tasks_thread = Thread(
            target=self._tasks_listener, daemon=True, name="BarsBuilder-Tasks"
        )
        tasks_thread.start()
        self._threads["tasks_listener"] = tasks_thread

        flush_thread = Thread(
            target=self._flush_loop, daemon=True, name="BarsBuilder-Flush"
        )
        flush_thread.start()
        self._threads["flush_loop"] = flush_thread

        logger.info("BarsBuilderService started")

    def stop(self) -> None:
        """Graceful shutdown: flush remaining bars, close connections."""
        if self._shutdown:
            return
        logger.info("Shutting down BarsBuilderService...")
        self._shutdown = True
        self._running = False

        # Cancel stream consumers
        for future in list(self.stream_consumers.values()):
            future.cancel()
        self.stream_consumers.clear()

        # Flush remaining bars from BarBuilder
        remaining = self.bar_builder.flush()
        if remaining:
            self._pending_bars.extend(remaining)
            self._stats["bars_completed"] += len(remaining)
        self._flush_to_clickhouse()

        # Stop ws loop if we own it
        if self._owns_manager and self.ws_loop and self.ws_loop.is_running():
            self.ws_loop.call_soon_threadsafe(self.ws_loop.stop)

        # Join threads
        for name, thread in self._threads.items():
            thread.join(timeout=2)
            logger.debug(f"Joined thread {name}")

        if self._owns_manager and hasattr(self, "_ws_thread"):
            self._ws_thread.join(timeout=2)

        try:
            self.redis_client.close()
        except Exception:
            pass

        try:
            self.ch_client.close()
        except Exception:
            pass

        logger.info(f"BarsBuilderService stopped. " f"Stats: {self._stats}")

    # ════════════════════════════════════════════════════════════════════
    # WS event loop (standalone mode)
    # ════════════════════════════════════════════════════════════════════

    def _run_ws_loop(self) -> None:
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

    # ════════════════════════════════════════════════════════════════════
    # Redis stream: listen for ticker add/remove
    # ════════════════════════════════════════════════════════════════════

    def _tasks_listener(self) -> None:
        """Listen for add/remove ticker commands on the same Redis stream
        that TickDataServer publishes to."""
        logger.info(f"Tasks listener started on {self.STREAM_NAME}")
        while self._running:
            try:
                results = self.redis_client.xreadgroup(
                    groupname=self.CONSUMER_GROUP,
                    consumername=self.CONSUMER_NAME,
                    streams={self.STREAM_NAME: ">"},
                    count=10,
                    block=1000,
                )
                if not results:
                    continue

                for stream_name, messages in results:
                    for msg_id, msg_data in messages:
                        action = msg_data.get("action", "")
                        symbol = msg_data.get("ticker", "") or msg_data.get(
                            "symbol", ""
                        )
                        if not symbol:
                            continue

                        if action == "add":
                            self._add_ticker(symbol)
                        elif action == "remove":
                            self._remove_ticker(symbol)

                        self.redis_client.xack(
                            self.STREAM_NAME, self.CONSUMER_GROUP, msg_id
                        )
            except redis.exceptions.ConnectionError:
                logger.warning("Redis connection error, retrying in 2s...")
                time.sleep(2)
            except Exception as e:
                if self._running:
                    logger.error(f"Tasks listener error: {e}")
                    time.sleep(1)

    # ════════════════════════════════════════════════════════════════════
    # Ticker management
    # ════════════════════════════════════════════════════════════════════

    def _add_ticker(self, symbol: str) -> None:
        if symbol in self.active_tickers:
            return
        logger.info(f"Adding ticker: {symbol}")
        self.active_tickers.add(symbol)

        # Subscribe to trade events via shared UnifiedTickManager
        # NOTE: subscribe() is an async coroutine — must schedule on ws_loop
        asyncio.run_coroutine_threadsafe(
            self.ws_manager.subscribe(
                websocket_client=CLIENT_ID,
                symbols=[symbol],
                events=["T"],
            ),
            self.ws_loop,
        )

        # Create async consumer for each trade stream key
        stream_key = f"T.{symbol}"
        future = asyncio.run_coroutine_threadsafe(
            self._consume_stream_key(stream_key, symbol), self.ws_loop
        )
        self.stream_consumers[stream_key] = future
        logger.debug(f"Created consumer for {stream_key}")

    def _remove_ticker(self, symbol: str) -> None:
        if symbol not in self.active_tickers:
            return
        logger.info(f"Removing ticker: {symbol}")

        # Cancel stream consumer
        stream_key = f"T.{symbol}"
        future = self.stream_consumers.pop(stream_key, None)
        if future:
            future.cancel()

        # Flush bars for this ticker to ClickHouse
        completed = self.bar_builder.remove_ticker(symbol)
        if completed:
            self._pending_bars.extend(completed)
            self._stats["bars_completed"] += len(completed)

        self.active_tickers.discard(symbol)

    # ════════════════════════════════════════════════════════════════════
    # Tick consumption (async, runs on ws_loop)
    # ════════════════════════════════════════════════════════════════════

    async def _consume_stream_key(self, stream_key: str, symbol: str) -> None:
        """Read from the per-client asyncio.Queue and feed to BarBuilder."""
        logger.debug(f"Starting consumer for {stream_key}")

        # Retry: subscribe() is async and may not have completed yet
        q = None
        for attempt in range(10):
            q = self.ws_manager.get_client_queue(CLIENT_ID, stream_key)
            if q is not None:
                break
            logger.debug(
                f"Queue not ready for {stream_key}, retrying ({attempt + 1}/10)..."
            )
            await asyncio.sleep(0.3)

        if q is None:
            logger.warning(f"No queue found for {stream_key} after retries")
            return

        while True:
            try:
                data = await q.get()
                normalized = self.ws_manager.normalize_data(data)
                self._on_trade(normalized)
            except asyncio.CancelledError:
                logger.info(f"Consumer cancelled for {stream_key}")
                break
            except Exception as e:
                logger.error(f"Consumer error {stream_key}: {e}")

    def _on_trade(self, data: dict) -> None:
        """Feed a single trade into the Rust BarBuilder."""
        symbol = data.get("symbol")
        if not symbol:
            return

        ts = data.get("timestamp")
        price = data.get("price")
        size = data.get("size", 0)

        if ts is None or price is None:
            return

        # Coerce timestamp to int ms
        ts_ms = int(ts) if isinstance(ts, (int, float)) else 0
        if ts_ms <= 0:
            return

        # Feed to Rust BarBuilder
        completed = self.bar_builder.ingest_trade(
            symbol, float(price), float(size), ts_ms
        )
        self._stats["ticks_ingested"] += 1

        if completed:
            self._stats["bars_completed"] += len(completed)
            self._pending_bars.extend(completed)

            # Publish completed bars to Redis for real-time WebSocket delivery
            for bar in completed:
                self._publish_bar(bar)

    # ════════════════════════════════════════════════════════════════════
    # Redis pub/sub: broadcast completed bars
    # ════════════════════════════════════════════════════════════════════

    def _publish_bar(self, bar: dict) -> None:
        """Publish a completed bar to a Redis channel for WebSocket fan-out."""
        import json

        channel = f"bars:{bar['ticker']}:{bar['timeframe']}"
        try:
            self.redis_client.publish(channel, json.dumps(bar))
        except Exception as e:
            logger.error(f"Failed to publish bar on {channel}: {e}")

    # ════════════════════════════════════════════════════════════════════
    # ClickHouse flush loop
    # ════════════════════════════════════════════════════════════════════

    def _flush_loop(self) -> None:
        """Periodically check for expired bars and flush to ClickHouse."""
        logger.info("Flush loop started")
        while self._running:
            time.sleep(FLUSH_INTERVAL_SEC)
            # Wall-time bar completion: close any bars whose boundary
            # has passed, even if no new trade arrived.
            now_ms = clock.now_ms()
            expired = self.bar_builder.check_expired(now_ms)
            if expired:
                self._stats["bars_completed"] += len(expired)
                self._pending_bars.extend(expired)
                for bar in expired:
                    self._publish_bar(bar)
                logger.debug(
                    f"check_expired closed {len(expired)} bars " f"(now_ms={now_ms})"
                )
            self._flush_to_clickhouse()
        logger.info("Flush loop stopped")

    def _flush_to_clickhouse(self) -> None:
        """Insert pending bars into ClickHouse in a batch."""
        if not self._pending_bars:
            return

        # Drain pending bars atomically
        bars = self._pending_bars
        self._pending_bars = []

        columns = [
            "ticker",
            "timeframe",
            "bar_start",
            "bar_end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
            "session",
        ]

        rows = [
            [
                bar["ticker"],
                bar["timeframe"],
                bar["bar_start"],
                bar["bar_end"],
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                bar["volume"],
                bar["trade_count"],
                bar["vwap"],
                bar["session"],
            ]
            for bar in bars
        ]

        try:
            self.ch_client.insert(
                table="ohlcv_bars",
                data=rows,
                column_names=columns,
            )
            self._stats["bars_written"] += len(rows)
            logger.debug(
                f"Flushed {len(rows)} bars to ClickHouse "
                f"(total written: {self._stats['bars_written']})"
            )
        except Exception as e:
            logger.error(f"ClickHouse insert failed ({len(rows)} bars): {e}")
            # Re-queue failed bars for retry
            self._pending_bars = bars + self._pending_bars

    # ════════════════════════════════════════════════════════════════════
    # Query API (for REST endpoints / frontend)
    # ════════════════════════════════════════════════════════════════════

    def query_bars(
        self,
        ticker: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 5000,
    ) -> List[Dict]:
        """Query historical bars from ClickHouse.

        Returns list of bar dicts sorted by bar_start ascending.
        """
        query = """
            SELECT
                ticker, timeframe, bar_start, bar_end,
                open, high, low, close,
                volume, trade_count, vwap, session
            FROM ohlcv_bars FINAL
            WHERE ticker = {ticker:String}
              AND timeframe = {timeframe:String}
              AND bar_start >= {start_ms:Int64}
              AND bar_start < {end_ms:Int64}
            ORDER BY bar_start ASC
            LIMIT {limit:UInt32}
        """
        result = self.ch_client.query(
            query,
            parameters={
                "ticker": ticker,
                "timeframe": timeframe,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "limit": limit,
            },
        )
        columns = [
            "ticker",
            "timeframe",
            "bar_start",
            "bar_end",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_count",
            "vwap",
            "session",
        ]
        return [dict(zip(columns, row)) for row in result.result_rows]

    def get_partial_bar(self, ticker: str, timeframe: str) -> Optional[Dict]:
        """Get the current in-progress bar from the Rust BarBuilder."""
        return self.bar_builder.get_current_bar(ticker, timeframe)
