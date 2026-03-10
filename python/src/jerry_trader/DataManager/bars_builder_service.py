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

from jerry_trader import clock as clock_mod
from jerry_trader._rust import BarBuilder, load_trades_from_parquet
from jerry_trader.config import lake_data_dir
from jerry_trader.DataSupply.bootstrapdataSupply.polygon_fetcher import (
    fetch_polygon_trades,
)
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

        # ── 10s bootstrap / WS merge state ───────────────────────────
        # first_ws_tick_ts:   epoch ms of the very first WS tick per ticker
        # meeting_bar_start:  bar_start of the 10s bar that straddles the
        #                     REST→WS boundary (REST has prefix, WS has suffix)
        # ws_meeting_bars:    completed WS meeting bar, held for later merge
        self._first_ws_tick_ts: Dict[str, int] = {}
        self._meeting_bar_start: Dict[str, int] = {}
        self._ws_meeting_bars: Dict[str, dict] = {}
        self._bootstrap_done: set = set()  # tickers that finished bootstrap

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

        # Check if 10s bars need bootstrap (single-date only)
        # Live mode: fetch from Polygon REST API
        # Replay mode: TODO - fetch from local parquet files
        if "10s" in self.timeframes:
            if not self._has_10s_bars_today(symbol):
                Thread(
                    target=self._bootstrap_10s_bars,
                    args=(symbol,),
                    daemon=True,
                    name=f"bootstrap-10s-{symbol}",
                ).start()

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
    # 10s Bootstrap (Polygon REST → BarBuilder → ClickHouse)
    # ════════════════════════════════════════════════════════════════════

    def _has_10s_bars_today(self, symbol: str) -> bool:
        """Check if any 10s bars exist in ClickHouse for this ticker on this replay date.

        Returns True if bars exist (skip bootstrap), False if empty (need bootstrap).
        Only checks current replay date — no cross-date bootstrap for 10s.
        """
        from datetime import datetime, timedelta

        # Get date boundaries for current replay date
        start_dt = datetime.strptime(self.db_date, "%Y%m%d")
        end_dt = start_dt + timedelta(days=1)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        try:
            query = """
                SELECT COUNT(*) as cnt
                FROM ohlcv_bars
                WHERE ticker = {ticker:String}
                  AND timeframe = '10s'
                  AND bar_start >= {start_ms:Int64}
                  AND bar_start < {end_ms:Int64}
            """
            result = self.ch_client.query(
                query,
                parameters={
                    "ticker": symbol,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                },
            )
            count = result.result_rows[0][0] if result.result_rows else 0
            logger.info(
                f"_has_10s_bars_today - {symbol}: {count} bars on {self.db_date}"
            )
            return count > 0
        except Exception as e:
            logger.error(f"_has_10s_bars_today - {symbol}: {e}")
            return False  # On error, assume no bars → trigger bootstrap

    def _load_trades_from_parquet(self, symbol: str) -> List[tuple]:
        """Load trades from local parquet via Rust.

        Delegates to jerry_trader._rust.load_trades_from_parquet which
        handles partitioned/monolithic fallback, ticker filtering, ns→ms
        conversion, and sorting — all in Rust/Polars.

        Uses the replay clock's current time as the upper bound so only
        trades up to "now" are loaded (predicate pushdown at scan time).

        Returns List[(ts_ms, price, size)] sorted ascending by timestamp.
        """
        end_ts_ms = clock_mod.now_ms()
        trades = load_trades_from_parquet(
            lake_data_dir, symbol, self.db_date, end_ts_ms
        )
        logger.info(
            f"_load_trades_from_parquet - {symbol}: "
            f"loaded {len(trades)} trades from parquet (via Rust), "
            f"end_ts={self._ms_to_et(end_ts_ms)}"
        )
        return trades

    def _bootstrap_10s_bars(self, symbol: str) -> None:
        """Fetch historical trades and merge with WS at the meeting bar.

        Uses a dedicated BarBuilder(["10s"]) to avoid thread-race with the
        real-time WebSocket path.

        Only feeds trades where ts < first_ws_tick_ts to avoid
        double-counting.  The "meeting bar" (the 10s window straddling
        the boundary) is merged from both sources:
          historical prefix  [bar_start … first_ws_ts)
          WS         suffix  [first_ws_ts … bar_end)
        producing a single complete bar written to ClickHouse.

        Live mode:   Polygon REST API (fetch_polygon_trades)
        Replay mode: local parquet files (partitioned or monolithic)
        """
        logger.info(f"_bootstrap_10s_bars - {symbol}: starting ({self.run_mode} mode)")

        try:
            # Dispatch trade loading based on mode
            if self.run_mode == "live":
                raw_trades = fetch_polygon_trades(symbol)
            else:
                raw_trades = self._load_trades_from_parquet(symbol)

            if not raw_trades:
                logger.info(
                    f"_bootstrap_10s_bars - {symbol}: no historical trades found"
                )
                return

            # Normalise to List[(ts_ms, price, size)]
            trades: List[tuple] = raw_trades

            first_trade = trades[0]
            last_trade = trades[-1]
            logger.info(
                f"_bootstrap_10s_bars - {symbol}: fetched {len(trades)} trades, "
                f"range [{self._ms_to_et(first_trade[0])} → {self._ms_to_et(last_trade[0])}]"
            )

            # Filter: only keep REST trades BEFORE first WS tick
            # to avoid double-counting in the overlap zone.
            first_ws_ts = self._first_ws_tick_ts.get(symbol)
            if first_ws_ts:
                original_len = len(trades)
                trades = [(t, p, s) for t, p, s in trades if t < first_ws_ts]
                dropped = original_len - len(trades)
                if dropped:
                    logger.info(
                        f"_bootstrap_10s_bars - {symbol}: filtered {dropped} trades "
                        f"after first_ws_ts={self._ms_to_et(first_ws_ts)}"
                    )

            if not trades:
                logger.info(
                    f"_bootstrap_10s_bars - {symbol}: no trades before WS start"
                )
                return

            # Build 10s bars with separate BarBuilder using bulk batch API
            # (single FFI call instead of 441K individual calls — ~10-20x faster)
            bootstrap_builder = BarBuilder(["10s"])
            bootstrap_bars = bootstrap_builder.ingest_trades_batch(symbol, trades)

            # Flush the last open bar (partial meeting bar from REST prefix)
            remaining = bootstrap_builder.flush()
            if remaining:
                bootstrap_bars.extend(remaining)

            # Separate meeting bar from clean bars
            meeting_start = self._meeting_bar_start.get(symbol)
            clean_bars: List[Dict] = []
            rest_meeting_bar: Optional[Dict] = None

            for bar in bootstrap_bars:
                if meeting_start is not None and bar["bar_start"] == meeting_start:
                    rest_meeting_bar = bar
                else:
                    clean_bars.append(bar)

            # Write clean bars (no overlap with WS)
            self._pending_bars.extend(clean_bars)

            # Merge meeting bar: REST prefix + WS suffix
            if rest_meeting_bar:
                ws_bar = self._ws_meeting_bars.get(symbol)
                if ws_bar:
                    merged = self._merge_10s_bars(rest_meeting_bar, ws_bar)
                    self._pending_bars.append(merged)
                    logger.info(
                        f"_bootstrap_10s_bars - {symbol}: merged meeting bar "
                        f"bar_start={self._ms_to_et(merged['bar_start'])}, "
                        f"REST trades={rest_meeting_bar['trade_count']}, "
                        f"WS trades={ws_bar['trade_count']}, "
                        f"merged trades={merged['trade_count']}"
                    )
                else:
                    # WS meeting bar not available yet — write REST partial
                    # (ClickHouse ReplacingMergeTree will keep whichever has
                    #  later inserted_at; if WS bar was already flushed,
                    #  REST partial overwrites it — acceptable since REST
                    #  has the prefix WS was missing)
                    self._pending_bars.append(rest_meeting_bar)
                    logger.warning(
                        f"_bootstrap_10s_bars - {symbol}: WS meeting bar not captured, "
                        f"writing REST partial only"
                    )

            total = len(clean_bars) + (1 if rest_meeting_bar else 0)
            logger.info(
                f"_bootstrap_10s_bars - {symbol}: completed, "
                f"generated {total} 10s bars (clean={len(clean_bars)}, "
                f"meeting={'merged' if rest_meeting_bar and self._ws_meeting_bars.get(symbol) else 'partial' if rest_meeting_bar else 'none'}), "
                f"last_trade={self._ms_to_et(trades[-1][0])}"
            )

            # Clean up merge state — no longer needed
            self._meeting_bar_start.pop(symbol, None)
            self._ws_meeting_bars.pop(symbol, None)
            self._bootstrap_done.add(symbol)

        except Exception as e:
            logger.error(f"_bootstrap_10s_bars - {symbol}: {e}", exc_info=True)

    def _capture_meeting_bar(self, bar: dict) -> None:
        """Hold a copy of the WS meeting bar for later merge with bootstrap.

        Called for every completed bar (from ingest_trade or check_expired).
        Only captures the first 10s bar matching the meeting bar start.
        """
        ticker = bar.get("ticker", "")
        if (
            bar.get("timeframe") == "10s"
            and ticker in self._meeting_bar_start
            and bar["bar_start"] == self._meeting_bar_start[ticker]
            and ticker not in self._ws_meeting_bars
        ):
            self._ws_meeting_bars[ticker] = dict(bar)  # defensive copy
            logger.info(
                f"MEETING_BAR_WS - {ticker}: captured WS meeting bar "
                f"bar_start={self._ms_to_et(bar['bar_start'])}, "
                f"trades={bar['trade_count']}"
            )

    @staticmethod
    def _merge_10s_bars(rest_bar: dict, ws_bar: dict) -> dict:
        """Merge REST-prefix and WS-suffix partial bars into one complete bar.

        REST bar has trades [bar_start … first_ws_ts).
        WS  bar has trades [first_ws_ts … bar_end).
        No double-counting because trades were filtered at first_ws_ts.

        Merge rules:
          open  = REST (has earlier trades)
          close = WS   (has later trades)
          high  = max(REST, WS)
          low   = min(REST, WS)
          volume      = REST + WS
          trade_count = REST + WS
          vwap  = volume-weighted average
        """
        total_vol = rest_bar["volume"] + ws_bar["volume"]
        vwap = (
            (
                (
                    rest_bar["vwap"] * rest_bar["volume"]
                    + ws_bar["vwap"] * ws_bar["volume"]
                )
                / total_vol
            )
            if total_vol > 0
            else 0.0
        )

        return {
            "ticker": rest_bar["ticker"],
            "timeframe": "10s",
            "bar_start": rest_bar["bar_start"],
            "bar_end": rest_bar["bar_end"],
            "open": rest_bar["open"],
            "close": ws_bar["close"],
            "high": max(rest_bar["high"], ws_bar["high"]),
            "low": min(rest_bar["low"], ws_bar["low"]),
            "volume": total_vol,
            "trade_count": rest_bar["trade_count"] + ws_bar["trade_count"],
            "vwap": vwap,
            "session": rest_bar["session"],
        }

    @staticmethod
    def _ms_to_et(ts_ms: int) -> str:
        """Convert epoch ms to ET time string for logging."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        dt = datetime.fromtimestamp(ts_ms / 1000, tz=ZoneInfo("America/New_York"))
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " ET"

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

        # Log first WebSocket tick — record timestamp for bootstrap merge
        if symbol not in self._first_ws_tick_ts:
            self._first_ws_tick_ts[symbol] = ts_ms
            logger.info(
                f"FIRST_WS_TICK - {symbol}: ts={self._ms_to_et(ts_ms)}, "
                f"price={price}, size={size}"
            )

        # Feed to Rust BarBuilder (uses UTC epoch ms — same as clock_mod.now_ms())
        completed = self.bar_builder.ingest_trade(
            symbol, float(price), float(size), ts_ms
        )
        self._stats["ticks_ingested"] += 1

        # Identify meeting bar start from the first WS tick for this ticker
        # Skip if bootstrap already completed (avoid spurious re-detection)
        if (
            symbol not in self._meeting_bar_start
            and symbol not in self._bootstrap_done
            and "10s" in self.timeframes
        ):
            partial = self.bar_builder.get_current_bar(symbol, "10s")
            if partial:
                self._meeting_bar_start[symbol] = partial["bar_start"]
                logger.info(
                    f"MEETING_BAR - {symbol}: bar_start={self._ms_to_et(partial['bar_start'])}"
                )

        if completed:
            self._stats["bars_completed"] += len(completed)
            for bar in completed:
                self._pending_bars.append(bar)
                self._publish_bar(bar)

                # Hold a copy of the WS meeting bar for merge with bootstrap
                self._capture_meeting_bar(bar)

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
        """Periodically check for expired bars and flush to ClickHouse.

        Polls every 50 ms real-time.  When virtual time crosses a 500 ms
        boundary we run ``check_expired``; any completed bars are flushed
        to ClickHouse immediately so writes land right at :00, :10, …
        """
        POLL_SEC = 0.05  # 50 ms real-time poll

        logger.info("Flush loop started")
        # Align to the current virtual-time 500 ms slot.
        last_slot = clock_mod.now_ms() // 500

        while self._running:
            time.sleep(POLL_SEC)

            now_ms = clock_mod.now_ms()
            cur_slot = now_ms // 500

            if cur_slot <= last_slot:
                continue  # haven't crossed a 500 ms boundary yet
            last_slot = cur_slot

            # Wall-time bar completion: close any bars whose boundary
            # has passed, even if no new trade arrived.
            expired = self.bar_builder.check_expired(now_ms)
            if expired:
                self._stats["bars_completed"] += len(expired)
                for bar in expired:
                    self._pending_bars.append(bar)
                    self._publish_bar(bar)
                    self._capture_meeting_bar(bar)
                logger.debug(
                    f"check_expired closed {len(expired)} bars " f"(now_ms={now_ms})"
                )

            # Flush to ClickHouse immediately when bars are pending.
            if self._pending_bars:
                self._flush_to_clickhouse()

        # Final flush on shutdown
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

    def get_pending_bars(self, ticker: str, timeframe: str) -> List[Dict]:
        """Return completed bars not yet flushed to ClickHouse.

        These bars exist in ``_pending_bars`` for up to ~50 ms before
        the flush loop writes them to ClickHouse.  Including them in
        REST responses prevents a gap between ClickHouse and the
        partial (in-progress) bar.
        """
        return [
            bar
            for bar in self._pending_bars
            if bar.get("ticker") == ticker and bar.get("timeframe") == timeframe
        ]
