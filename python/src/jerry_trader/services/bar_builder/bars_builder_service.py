"""
BarsBuilderService — orchestrates tick-to-bar aggregation.

Wires together:
  1. UnifiedTickManager  (existing, shared with ChartBFF
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

Co-location with ChartBFF
────────────────────────────────
When both BarsBuilder and ChartBFF run on the same machine,
they share a single UnifiedTickManager instance (same as FactorEngine).
backend_starter passes ws_manager + ws_loop in that case.
"""

import asyncio
import logging
import os
import time
from concurrent.futures import Future
from threading import Event, Lock, Thread
from typing import Any, Dict, List, Optional

import clickhouse_connect
import redis

from jerry_trader import clock as clock_mod
from jerry_trader._rust import BarBuilder, load_trades_from_parquet
from jerry_trader.domain.market import Bar
from jerry_trader.platform.config.config import lake_data_dir
from jerry_trader.platform.config.session import make_session_id, parse_session_id
from jerry_trader.platform.messaging.event_bus import Event as BusEvent
from jerry_trader.platform.messaging.event_bus import EventBus
from jerry_trader.platform.storage.ohlcv_writer import write_bars
from jerry_trader.services.bar_builder.bar_query_service import ClickHouseClient
from jerry_trader.services.market_data.bootstrap.polygon_fetcher import (
    fetch_polygon_trades,
)
from jerry_trader.services.market_data.feeds.unified_tick_manager import (
    UnifiedTickManager,
)
from jerry_trader.shared.ids.redis_keys import factor_tasks_stream
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import (
    convert_bar_et_to_utc,
    et_ms_to_utc_ms,
    ms_to_et,
    utc_ms_to_et_ms,
)

logger = setup_logger("bars_builder", log_to_file=True, level=logging.INFO)

# ── Constants ────────────────────────────────────────────────────────────
CLIENT_ID = "bars_builder"  # queue fan-out identity in UnifiedTickManager
BATCH_SIZE = 500  # ClickHouse insert batch size

# Intraday timeframes eligible for trade bootstrap.
# Excludes 1d/1w which span session boundaries — those are
# aggregated purely from WS ticks (no historical back-fill).
BOOTSTRAP_TIMEFRAMES = {"10s", "1m", "5m", "15m", "30m", "1h", "4h"}


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
        late_arrival_ms: int = 100,
        idle_close_ms: int = 2000,
        bootstrap_late_arrival_ms: int = 0,
        bootstrap_idle_close_ms: int = 1,
        ws_manager: Optional[UnifiedTickManager] = None,
        ws_loop: Optional[asyncio.AbstractEventLoop] = None,
        manager_type: Optional[str] = None,
        event_bus: Optional[EventBus] = None,
        coordinator=None,
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
        self.bar_builder.configure_watermark(
            late_arrival_ms=late_arrival_ms,
            idle_close_ms=idle_close_ms,
        )
        self._bootstrap_late_arrival_ms = bootstrap_late_arrival_ms
        self._bootstrap_idle_close_ms = bootstrap_idle_close_ms
        logger.info(f"BarBuilder initialized: {self.bar_builder}")
        logger.info(
            "BarBuilder watermark config: runtime late_arrival_ms=%s, "
            "runtime idle_close_ms=%s, bootstrap late_arrival_ms=%s, "
            "bootstrap idle_close_ms=%s",
            late_arrival_ms,
            idle_close_ms,
            bootstrap_late_arrival_ms,
            bootstrap_idle_close_ms,
        )

        # ── ClickHouse ───────────────────────────────────────────────
        ch_cfg = clickhouse_config or {}
        ch_host = ch_cfg.get("host", "localhost")
        ch_port = ch_cfg.get("port", 8123)
        ch_user = ch_cfg.get("user", "default")
        ch_db = ch_cfg.get("database", "jerry_trader")
        password_env = ch_cfg.get("password_env", "CLICKHOUSE_PASSWORD")
        ch_password = os.getenv(password_env, "")

        # Store config for per-thread client creation (thread-safe writes)
        self._ch_config = clickhouse_config or {}
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

        # ClickHouse utility for shared query methods
        self._ch_util = ClickHouseClient(
            session_id=self.session_id,
            redis_config=redis_config,
            clickhouse_config=clickhouse_config,
        )

        # Pending completed bars waiting to be flushed to ClickHouse
        self._pending_bars: List[Dict] = []
        self._pending_lock = Lock()

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
            # Create consumer group starting from latest message ($)
            # This ensures we only process new messages, not old history
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="$", mkstream=True
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
        # Track active timeframes per ticker to prevent unnecessary trades_backfill
        # when switching timeframes for the same ticker.
        # Key: ticker, Value: set of active timeframes
        self._active_timeframes: Dict[str, set] = {}
        self.stream_consumers: Dict[str, Future] = {}
        self._threads: Dict[str, Thread] = {}
        self._running = True
        self._shutdown = False
        self._stats = {"ticks_ingested": 0, "bars_completed": 0, "bars_written": 0}

        # ── Bootstrap / WS merge state ────────────────────────────
        # Computed once: intersection of configured TFs with bootstrappable TFs
        self._bootstrap_tfs_set = set(self.timeframes) & BOOTSTRAP_TIMEFRAMES

        # first_ws_tick_ts:  epoch ms of the first WS tick per ticker (Python-side tracking)
        # bootstrap_done:    ticker → set of TFs that completed bootstrap
        # bootstrap_events:  ticker → threading.Event, set when trades_backfill
        #                    completes so tickdata_server can wait before
        #                    serving REST bar responses with stale gap data.
        #
        # Meeting bar merge state is now in the Rust BarBuilder:
        #   bar_builder.set_first_ws_tick_ts()  — first WS tick
        #   bar_builder.detect_meeting_bars()    — identify straddling bars
        #   bar_builder.set_rest_partial()       — store REST half
        #   bar_builder.drain_completed()        — returns merged bars
        self._first_ws_tick_ts: Dict[str, int] = {}
        self._bootstrap_done: Dict[str, set] = {}
        self._bootstrap_events: Dict[str, Event] = {}

        # ── EventBus (inter-service messaging) ──────────────────────
        self._event_bus = event_bus

        # ── BootstrapCoordinator integration ─────────────────────────
        self._coordinator = coordinator

    # ════════════════════════════════════════════════════════════════════
    # Lifecycle
    # ════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        """Start the bars builder threads."""
        # Register with coordinator if available (new design: direct calls instead of Redis Stream)
        if self._coordinator:
            self._coordinator.register_service("bars_builder", self)

        flush_thread = Thread(
            target=self._flush_loop, daemon=True, name="BarsBuilder-Flush"
        )
        flush_thread.start()
        self._threads["flush_loop"] = flush_thread

        logger.info("BarsBuilderService started")

    def is_ready(self, symbol: str) -> bool:
        """Check if ticker has active bars being built.

        Part of BootstrapableService protocol.
        """
        return (
            symbol in self._active_timeframes
            and len(self._active_timeframes[symbol]) > 0
        )

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
            # Convert ET → UTC before storing
            remaining = [self._convert_bar_et_to_utc(bar) for bar in remaining]
            self._enqueue_bars(remaining, source="stop")
            self._stats["bars_completed"] += len(remaining)
        self._flush_to_clickhouse(caller="stop")

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

    def set_coordinator(self, coordinator) -> None:
        """Wire BootstrapCoordinator for unified bootstrap orchestration.

        Args:
            coordinator: BootstrapCoordinator instance
        """
        self._coordinator = coordinator
        logger.info(
            f"BarsBuilderService: coordinator {'set' if coordinator else 'cleared'}"
        )

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

    # ════════════════════════════════════════════════════════════════════
    # Ticker management (BootstrapableService implementation)
    # ════════════════════════════════════════════════════════════════════

    def add_ticker(self, symbol: str, timeframes: list[str] | None = None) -> bool:
        """Add a ticker for processing. Public API for Coordinator.

        Part of BootstrapableService protocol. Replaces Redis Stream _tasks_listener.

        Args:
            symbol: Ticker symbol
            timeframes: List of timeframes being subscribed

        Returns:
            True if successfully started (or immediately complete)
        """
        timeframes = timeframes or []
        current_active = self._active_timeframes.get(symbol, set())

        # Check if this is a new ticker (no active timeframes)
        is_new_ticker = len(current_active) == 0

        if is_new_ticker:
            logger.info(f"add_ticker - {symbol}: subscribing to tick stream")
            # Initialize the timeframe set
            self._active_timeframes[symbol] = set()

            # Subscribe to trade events via shared UnifiedTickManager (non-blocking)
            # The subscription completes asynchronously; _consume_stream_key will
            # retry until the queue is available
            logger.debug(
                f"add_ticker - {symbol}: starting ws_manager.subscribe() async"
            )
            try:
                asyncio.run_coroutine_threadsafe(
                    self.ws_manager.subscribe(
                        websocket_client=CLIENT_ID,
                        symbols=[symbol],
                        events=["T"],
                    ),
                    self.ws_loop,
                )
                logger.debug(
                    f"add_ticker - {symbol}: ws_manager.subscribe() dispatched"
                )
            except Exception as e:
                logger.error(
                    f"add_ticker - {symbol}: ws_manager.subscribe() dispatch failed - {e}"
                )
                # Continue anyway - the subscription may still complete

            # Create async consumer for trade stream
            # This will retry until the queue is available
            stream_key = f"T.{symbol}"
            future = asyncio.run_coroutine_threadsafe(
                self._consume_stream_key(stream_key, symbol), self.ws_loop
            )
            self.stream_consumers[stream_key] = future
            logger.debug(f"add_ticker - {symbol}: created consumer for {stream_key}")

            # Bootstrap intraday bars from historical trades
            bootstrap_tfs = list(self._bootstrap_tfs_set)
            if bootstrap_tfs:
                evt = self._bootstrap_events.get(symbol)
                if evt is None or evt.is_set():
                    evt = Event()
                self._bootstrap_events[symbol] = evt

                # Check if bars already exist in ClickHouse
                # per_tf_starts is used for dedup: skip bars already written
                per_tf_starts: Dict[str, int] = {}
                for tf in bootstrap_tfs:
                    last_start = self._get_last_bar_start_today(symbol, tf)
                    if last_start is not None:
                        per_tf_starts[tf] = last_start

                if per_tf_starts:
                    logger.info(
                        f"add_ticker - {symbol}: re-subscribe with existing bars, "
                        f"will gap-fill and dedup: {per_tf_starts}"
                    )
                else:
                    logger.info(
                        f"add_ticker - {symbol}: first subscribe, "
                        f"bootstrapping all TFs from session start"
                    )

                # Always run trades_backfill:
                # - First subscribe: from_ms=None, per_tf_starts={}
                # - Re-subscribe: from_ms=None, per_tf_starts={tf: last_bar_start}
                # The backfill will fetch all trades and dedup based on per_tf_starts
                Thread(
                    target=self.trades_backfill,
                    args=(symbol, bootstrap_tfs, None, per_tf_starts),
                    daemon=True,
                    name=f"bootstrap-{symbol}",
                ).start()

        # Track all timeframes for this ticker
        new_timeframes_added = False
        for tf in timeframes:
            if tf not in self._active_timeframes[symbol]:
                self._active_timeframes[symbol].add(tf)
                new_timeframes_added = True

        if timeframes:
            logger.debug(
                f"add_ticker - {symbol}: tracking timeframes {timeframes}, "
                f"total active: {self._active_timeframes[symbol]}"
            )

        # If new timeframes were added to an existing ticker, check if they need bootstrap
        if not is_new_ticker and new_timeframes_added:
            # Find bootstrap timeframes that need processing
            tfs_to_bootstrap = []
            per_tf_starts_new: Dict[str, int] = {}
            for tf in timeframes:
                if (
                    tf in self._bootstrap_tfs_set
                    and tf not in self._bootstrap_done.get(symbol, set())
                ):
                    last_start = self._get_last_bar_start_today(symbol, tf)
                    if last_start is not None:
                        per_tf_starts_new[tf] = last_start
                    tfs_to_bootstrap.append(tf)
                    logger.info(
                        f"add_ticker - {symbol}/{tf}: new timeframe, "
                        f"last_bar_start={last_start}"
                    )

            if tfs_to_bootstrap:
                # Start bootstrap for the new timeframes
                evt = self._bootstrap_events.get(symbol)
                if evt is None or evt.is_set():
                    evt = Event()
                    self._bootstrap_events[symbol] = evt

                Thread(
                    target=self.trades_backfill,
                    args=(symbol, tfs_to_bootstrap, None, per_tf_starts_new),
                    daemon=True,
                    name=f"bootstrap-{symbol}-new-tfs",
                ).start()

        return True

    def remove_ticker(self, symbol: str, timeframes: list[str] | None = None) -> bool:
        """Remove timeframes for a ticker. Public API for Coordinator.

        Part of BootstrapableService protocol.

        Only fully unsubscribes when ALL timeframes are removed.
        This prevents unnecessary trades_backfill when switching timeframes.

        Args:
            symbol: Ticker symbol
            timeframes: List of timeframes being unsubscribed (empty = remove all)

        Returns:
            True if successfully removed
        """

        timeframes = timeframes or []
        active = self._active_timeframes.get(symbol, set())

        if not active:
            # No active timeframes for this ticker
            return True

        # Remove the specified timeframes
        if timeframes:
            for tf in timeframes:
                active.discard(tf)
            logger.debug(
                f"remove_ticker - {symbol}: removed timeframes {timeframes}, "
                f"remaining: {active}"
            )

            # If there are still active timeframes, don't fully unsubscribe
            if active:
                logger.debug(
                    f"remove_ticker - {symbol}: still has active timeframes, "
                    "keeping tick stream"
                )
                return True
        else:
            # No timeframes specified = remove all
            active.clear()

        # All timeframes removed — fully unsubscribe
        logger.info(
            f"remove_ticker - {symbol}: no more timeframes, unsubscribing from tick stream"
        )

        # Unsubscribe from WebSocket stream via UnifiedTickManager
        logger.debug(f"remove_ticker - {symbol}: calling ws_manager.unsubscribe()")
        try:
            unsub_future = asyncio.run_coroutine_threadsafe(
                self.ws_manager.unsubscribe(
                    websocket_client=CLIENT_ID,
                    symbol=symbol,  # Note: singular 'symbol', not 'symbols'
                    events=["T"],
                ),
                self.ws_loop,
            )
            # Wait for unsubscribe to complete (with timeout)
            unsub_future.result(timeout=2.0)
            logger.debug(
                f"remove_ticker - {symbol}: ws_manager.unsubscribe() completed"
            )
        except Exception as e:
            logger.error(
                f"remove_ticker - {symbol}: ws_manager.unsubscribe() failed - {e}"
            )

        # Cancel stream consumer
        stream_key = f"T.{symbol}"
        future = self.stream_consumers.pop(stream_key, None)
        if future:
            future.cancel()

        # Flush bars for this ticker to ClickHouse
        completed = self.bar_builder.remove_ticker(symbol)
        if completed:
            # Convert ET → UTC before storing
            completed = [self._convert_bar_et_to_utc(bar) for bar in completed]
            self._enqueue_bars(completed, source="remove_ticker")
            self._stats["bars_completed"] += len(completed)

        # Flush any pending WS meeting bars and REST partials from Rust.
        # If trades_backfill stored REST partials but the WS meeting bar never
        # completed (e.g., quick unsubscribe), drain them as-is.
        rest_partials = self.bar_builder.get_rest_partials(symbol)
        if rest_partials:
            for tf, bar_dict in rest_partials.items():
                bar_dict["bar_start"] = self._et_ms_to_utc_ms(bar_dict["bar_start"])
                bar_dict["bar_end"] = self._et_ms_to_utc_ms(bar_dict["bar_end"])
                self._enqueue_bars([bar_dict], source="remove_ticker/rest_partial")
                logger.info(
                    f"remove_ticker - {symbol}/{tf}: flushing unmerged "
                    f"REST meeting bar partial"
                )

        # Clear meeting bar state in Rust BarBuilder
        self.bar_builder.clear_meeting_bar_state(symbol)

        # Clear bootstrap state so re-subscribe triggers fresh bootstrap
        self._first_ws_tick_ts.pop(symbol, None)
        self._bootstrap_done.pop(symbol, None)
        # Remove bootstrap event (trades_backfill may still be running —
        # setting it ensures any wait_for_bootstrap() call returns immediately)
        evt = self._bootstrap_events.pop(symbol, None)
        if evt:
            evt.set()

        # Remove from active timeframes
        self._active_timeframes.pop(symbol, None)

        return True

    # ════════════════════════════════════════════════════════════════════
    # trades_backfill: Polygon/Parquet Trades → BarBuilder → ClickHouse
    # ═════════════════════════════════════════════════════════════════════

    def _get_last_bar_start_today(self, symbol: str, tf: str) -> Optional[int]:
        """Return the last bar_start (ms) for this ticker+timeframe today.

        Returns None if no bars exist (first subscription → bootstrap from
        4 AM ET session start).  On re-subscribe, returns the START of the
        last bar so the bootstrap re-fetches trades from that point — this
        ensures the partial bar written during unsubscribe is rebuilt with
        complete trade data rather than overwritten with a gap.

        Delegates to ClickHouseClient.get_last_bar_start (shared utility).
        """
        result = self._ch_util.get_last_bar_start(symbol, tf, self.db_date)
        if result:
            logger.info(
                f"_get_last_bar_start_today - {symbol}/{tf}: "
                f"last_start={self._ms_to_et(result)}"
            )
        else:
            logger.info(
                f"_get_last_bar_start_today - {symbol}/{tf}: "
                f"no bars on {self.db_date}"
            )
        return result

    def _load_trades_from_parquet(
        self, symbol: str, start_ts_ms: int = 0
    ) -> List[tuple]:
        """Load trades from local parquet via Rust.

        Delegates to jerry_trader._rust.load_trades_from_parquet which
        handles partitioned/monolithic fallback, ticker filtering, ns→ms
        conversion, and sorting — all in Rust/Polars.

        Both start_ts_ms and end_ts_ms are applied as predicate-pushdown
        filters at scan time so only the needed trade window is loaded.

        Returns List[(ts_ms, price, size)] sorted ascending by timestamp.
        """
        end_ts_ms = clock_mod.now_ms()
        trades = load_trades_from_parquet(
            lake_data_dir, symbol, self.db_date, end_ts_ms, start_ts_ms
        )
        logger.info(
            f"_load_trades_from_parquet - {symbol}: "
            f"loaded {len(trades)} trades from parquet (via Rust), "
            f"range=[{self._ms_to_et(start_ts_ms) if start_ts_ms else 'start'} → "
            f"{self._ms_to_et(end_ts_ms)}]"
        )
        return trades

    def trades_backfill(
        self,
        symbol: str,
        bootstrap_tfs: List[str],
        from_ms: Optional[int],
        per_tf_starts: Optional[Dict[str, int]] = None,
    ) -> None:
        """Fetch historical trades and build bars for all bootstrap timeframes.

        This handles today's intraday bars [10s-4h] by building them from
        raw trade data. Historical (pre-today) bars are handled by
        custom_bar_backfill which fetches pre-aggregated bars.

        Uses a dedicated BarBuilder(bootstrap_tfs) to avoid thread-race with
        the real-time WebSocket path.

        Trade window: [from_ms, first_ws_ts)
          - from_ms comes from _get_last_bar_start_today() — None means first
            subscription (defaults to 4 AM ET session start).
          - first_ws_ts is the timestamp of the first WS tick (upper bound).

        For each timeframe, the "meeting bar" (the bar straddling the
        REST→WS boundary) is merged from both sources:
          historical prefix  [bar_start … first_ws_ts)
          WS         suffix  [first_ws_ts … bar_end)

        Args:
            per_tf_starts: On re-subscribe, maps TF → last bar_start in
                ClickHouse. Bars with bar_start < per_tf_starts[tf] are
                skipped (already exist correctly from prior subscription).
                Empty/None on first subscribe.

        Live mode:   Polygon REST API (fetch_polygon_trades)
        Replay mode: local parquet files (partitioned or monolithic)
        """
        logger.info(
            f"trades_backfill - {symbol}: starting ({self.run_mode} mode), "
            f"tfs={bootstrap_tfs}, "
            f"from_ms={self._ms_to_et(from_ms) if from_ms else 'session_start'}"
        )

        try:
            # ── Fetch trades ──────────────────────────────────────────
            if self.run_mode == "live":
                raw_trades = fetch_polygon_trades(symbol, from_ms=from_ms)
            else:
                raw_trades = self._load_trades_from_parquet(
                    symbol, start_ts_ms=from_ms or 0
                )

            if not raw_trades:
                logger.info(f"trades_backfill - {symbol}: no historical trades found")
                return

            trades: List[tuple] = raw_trades
            first_trade = trades[0]
            last_trade = trades[-1]
            logger.info(
                f"trades_backfill - {symbol}: fetched {len(trades)} trades, "
                f"range [{self._ms_to_et(first_trade[0])} → "
                f"{self._ms_to_et(last_trade[0])}]"
            )

            # ── Filter: only trades BEFORE first WS tick ─────────────
            # Filtering is needed for BAR building (avoids duplicate bars
            # between REST backfill and live WS stream).  We keep the
            # unfiltered list for tick-indicator warmup (trade_rate etc.).
            tick_warmup_trades = trades  # unfiltered
            first_ws_ts = self._first_ws_tick_ts.get(symbol)
            if first_ws_ts:
                original_len = len(trades)
                trades = [(t, p, s) for t, p, s in trades if t < first_ws_ts]
                dropped = original_len - len(trades)
                if dropped:
                    logger.info(
                        f"trades_backfill - {symbol}: filtered {dropped} trades "
                        f"after first_ws_ts={self._ms_to_et(first_ws_ts)}"
                    )

            if not trades:
                logger.info(f"trades_backfill - {symbol}: no trades before WS start")
                return

            # ── Convert UTC → ET for Rust BarBuilder ─────────────────
            # Rust expects timestamps in US/Eastern time (handles session boundaries correctly)
            trades_et = [(self._utc_ms_to_et_ms(t), p, s) for t, p, s in trades]

            # ── Build bars for all bootstrap TFs at once ──────────────
            bootstrap_builder = BarBuilder(bootstrap_tfs)
            bootstrap_builder.configure_watermark(
                late_arrival_ms=self._bootstrap_late_arrival_ms,
                idle_close_ms=self._bootstrap_idle_close_ms,
            )
            bootstrap_bars = bootstrap_builder.ingest_trades_batch(symbol, trades_et)

            # FIX: Close all time-expired bars before flushing.
            # Without this, if there's a 6-minute gap with no trades, the 1min
            # bars during that gap won't be generated. advance() forces
            # the BarBuilder to close bars at their time boundaries.
            last_trade_et_ms = trades_et[-1][0]
            expired = bootstrap_builder.advance(last_trade_et_ms)
            if expired:
                bootstrap_bars.extend(expired)
                logger.debug(
                    f"trades_backfill - {symbol}: advance closed "
                    f"{len(expired)} time-based bars (last_trade={self._ms_to_et(last_trade_et_ms)})"
                )

            # Flush last open bar per TF (partial meeting bar from REST prefix)
            remaining = bootstrap_builder.flush()
            if remaining:
                bootstrap_bars.extend(remaining)

            # Convert ET → UTC before creating domain Bar objects
            bootstrap_bars = [
                self._convert_bar_et_to_utc(bar) for bar in bootstrap_bars
            ]

            # Convert Rust dicts to domain Bar objects (timestamps now in UTC)
            bars = [Bar.from_rust_dict(bar) for bar in bootstrap_bars]

            # ── Separate meeting bars from clean bars per TF ──────────
            # Get meeting bar starts from Rust BarBuilder (detected on first WS tick)
            rust_meeting = self.bar_builder.detect_meeting_bars(symbol)
            meeting_starts = rust_meeting if rust_meeting else {}
            clean_bars: List[Bar] = []
            rest_meeting_bars: Dict[str, Bar] = {}  # tf → Bar

            for bar in bars:
                expected_start = meeting_starts.get(bar.timeframe)
                if expected_start is not None and bar.bar_start == expected_start:
                    rest_meeting_bars[bar.timeframe] = bar
                else:
                    clean_bars.append(bar)

            # ── Re-subscribe dedup: skip bars already in ClickHouse ─
            if per_tf_starts:
                original_count = len(clean_bars)
                clean_bars = [
                    bar
                    for bar in clean_bars
                    if bar.bar_start >= per_tf_starts.get(bar.timeframe, 0)
                ]
                skipped = original_count - len(clean_bars)
                if skipped:
                    logger.info(
                        f"trades_backfill - {symbol}: skipped {skipped} "
                        f"already-existing bars (re-subscribe dedup)"
                    )

            # Write clean bars (no overlap with WS)
            clean_dicts = [b.to_clickhouse_dict() for b in clean_bars]
            self._enqueue_bars(clean_dicts, source="trades_backfill/clean")

            # ── Store REST meeting bar partials in Rust ──────────────
            # Rust handles the merge: if WS bar is already pending, merges
            # immediately and adds to completed_queue. Otherwise stores for
            # later merge when WS bar completes.
            if rest_meeting_bars:
                for tf, rest_bar in rest_meeting_bars.items():
                    # Convert to dict, then convert timestamps back to ET for Rust
                    rest_et = rest_bar.to_clickhouse_dict()
                    rest_et["bar_start"] = self._utc_ms_to_et_ms(rest_et["bar_start"])
                    rest_et["bar_end"] = self._utc_ms_to_et_ms(rest_et["bar_end"])
                    self.bar_builder.set_rest_partial(symbol, tf, rest_et)
                    logger.info(
                        f"trades_backfill - {symbol}/{tf}: stored REST meeting bar "
                        f"partial in Rust for merge "
                        f"(bar_start={self._ms_to_et(rest_bar.bar_start)}, "
                        f"trades={rest_bar.trade_count})"
                    )

                # Drain any bars that were merged by set_rest_partial
                merged_drain = self.bar_builder.drain_completed()
                if merged_drain:
                    merged_bars = [
                        Bar.from_rust_dict(self._convert_bar_et_to_utc(d))
                        for d in merged_drain
                    ]
                    merged_dicts = [b.to_clickhouse_dict() for b in merged_bars]
                    self._enqueue_bars(
                        merged_dicts, source="trades_backfill/meeting_merge"
                    )
                    for mb in merged_bars:
                        logger.info(
                            f"MEETING_BAR_MERGED - {symbol}/{mb.timeframe}: "
                            f"bar_start={self._ms_to_et(mb.bar_start)}, "
                            f"trades={mb.trade_count}, vol={mb.volume}"
                        )

            total_bars = len(clean_bars) + len(rest_meeting_bars)
            logger.info(
                f"trades_backfill - {symbol}: completed, "
                f"generated {total_bars} bars across {len(bootstrap_tfs)} TFs "
                f"(clean={len(clean_bars)}, "
                f"meeting_deferred={len(rest_meeting_bars)}), "
                f"last_trade={self._ms_to_et(last_trade_et_ms)}"
            )

            # Accumulate done timeframes (support adding new TFs to existing ticker)
            if symbol not in self._bootstrap_done:
                self._bootstrap_done[symbol] = set()
            self._bootstrap_done[symbol].update(bootstrap_tfs)

            # Flush pending bars to ClickHouse NOW, before signaling the
            # bootstrap Event.  Without this, the REST handler unblocks
            # (via evt.set()) and queries ClickHouse before the 50 ms
            # _flush_loop has a chance to drain _pending_bars, resulting
            # in stale / missing data served to the frontend.
            self._flush_to_clickhouse(caller="trades_backfill")

            # Notify coordinator that bars are ready for each timeframe
            if self._coordinator:
                for tf in bootstrap_tfs:
                    self._coordinator.on_bars_ready(symbol, tf)
                    logger.debug(
                        f"trades_backfill - {symbol}/{tf}: notified coordinator bars ready"
                    )

            # Store trades in BootstrapCoordinator for FactorEngine to consume
            # Use UNFILTERED trades so tick indicators (trade_rate) get full
            # coverage without the ~17s gap between REST and WS streams.
            if self._coordinator:
                source = "polygon" if self.run_mode == "live" else "parquet"
                first_ws_ts = self._first_ws_tick_ts.get(symbol)
                trades_key = self._coordinator.store_trades(
                    symbol=symbol,
                    trades=tick_warmup_trades,  # unfiltered for tick warmup
                    from_ms=from_ms,
                    first_ws_ts=first_ws_ts,
                )
                logger.info(
                    f"trades_backfill - {symbol}: stored {len(tick_warmup_trades)} trades "
                    f"in coordinator at {trades_key}"
                )

            # Publish TradesBackfillCompleted event to EventBus (legacy)
            if self._event_bus:
                source = "polygon" if self.run_mode == "live" else "parquet"
                first_ws_ts = self._first_ws_tick_ts.get(symbol)
                self._event_bus.publish(
                    BusEvent.trades_backfill_completed(
                        symbol=symbol,
                        trade_count=len(trades),
                        source=source,
                        from_ms=from_ms,
                        first_ws_ts=first_ws_ts,
                    )
                )
        finally:
            # Signal that bootstrap is complete (success or failure)
            # so tickdata_server REST handler can stop waiting.
            evt = self._bootstrap_events.pop(symbol, None)
            if evt:
                evt.set()

    def pre_register_bootstrap(self, symbol: str) -> None:
        """Pre-create a bootstrap Event for this ticker.

        Called by tickdata_server BEFORE the Redis XADD so that any
        concurrent REST request will block on wait_for_bootstrap
        even if bars_builder hasn't picked up the subscription yet.

        If add_ticker later creates its own Event, it will overwrite
        this one — which is fine because add_ticker's Event is the
        one that trades_backfill will set().

        If the ticker is already active (re-subscribe), add_ticker
        will create a fresh Event anyway, so this pre-registered one
        will be replaced.
        """
        if symbol not in self._bootstrap_events:
            self._bootstrap_events[symbol] = Event()
            logger.debug(
                f"pre_register_bootstrap - {symbol}: "
                f"Event pre-created for wait_for_bootstrap"
            )

    def wait_for_bootstrap(self, symbol: str, timeout: float = 3.0) -> bool:
        """Wait until trades_backfill completes for this ticker.

        Called by tickdata_server before serving bar REST responses to
        avoid returning stale data with gap from unsubscribe period.

        Args:
            symbol: Ticker symbol.
            timeout: Max seconds to wait (default 3s).

        Returns:
            True if bootstrap completed (or no bootstrap in progress).
            False if timed out.
        """
        evt = self._bootstrap_events.get(symbol)
        if evt is None:
            return True  # no bootstrap in progress
        return evt.wait(timeout=timeout)

    # Timezone helpers — thin wrappers around jerry_trader.shared.utils.timezone
    # so existing call sites (self._ms_to_et, etc.) keep working.
    _ms_to_et = staticmethod(ms_to_et)
    _utc_ms_to_et_ms = staticmethod(utc_ms_to_et_ms)
    _et_ms_to_utc_ms = staticmethod(et_ms_to_utc_ms)
    _convert_bar_et_to_utc = staticmethod(convert_bar_et_to_utc)

    # ════════════════════════════════════════════════════════════════════
    # Tick consumption (async, runs on ws_loop)
    # ════════════════════════════════════════════════════════════════════

    async def _consume_stream_key(self, stream_key: str, symbol: str) -> None:
        """Read from the per-client asyncio.Queue and feed to BarBuilder."""
        logger.debug(f"_consume_stream_key - {stream_key}: starting")

        # Retry: subscribe() is async and may not have completed yet
        q = None
        for attempt in range(10):
            q = self.ws_manager.get_client_queue(CLIENT_ID, stream_key)
            if q is not None:
                break
            logger.debug(
                f"_consume_stream_key - {stream_key}: queue not ready, "
                f"retrying ({attempt + 1}/10)..."
            )
            await asyncio.sleep(0.3)

        if q is None:
            logger.warning(
                f"_consume_stream_key - {stream_key}: no queue found after retries"
            )
            return

        while True:
            try:
                data = await q.get()
                normalized = self.ws_manager.normalize_data(data)
                if normalized is None:
                    continue  # Filtered out (e.g., delayed TRF trade)
                self._on_trade(normalized)
            except asyncio.CancelledError:
                logger.info(f"_consume_stream_key - {stream_key}: cancelled")
                break
            except Exception as e:
                logger.error(f"_consume_stream_key - {stream_key}: error - {e}")

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

        # Convert UTC → ET for Rust BarBuilder
        ts_et_ms = self._utc_ms_to_et_ms(ts_ms)

        # Feed to Rust BarBuilder (completed bars are drained by _flush_loop)
        self.bar_builder.ingest_trade(symbol, float(price), float(size), ts_et_ms)
        self._stats["ticks_ingested"] += 1

        # On first WS tick: record timestamp in Rust and detect meeting bars.
        # The Rust BarBuilder handles all meeting bar merge logic internally.
        if (
            symbol not in self._first_ws_tick_ts
            and symbol not in self._bootstrap_done
            and self._bootstrap_tfs_set
        ):
            self._first_ws_tick_ts[symbol] = ts_ms
            logger.info(
                f"_on_trade - FIRST_WS_TICK - {symbol}: "
                f"ts={self._ms_to_et(ts_ms)}, price={price}, size={size}"
            )

            # Record first WS tick in Rust BarBuilder (ET timestamp)
            is_new = self.bar_builder.set_first_ws_tick_ts(symbol, ts_et_ms)
            if is_new:
                # Detect which bars straddle the REST→WS boundary
                meeting = self.bar_builder.detect_meeting_bars(symbol)
                if meeting:
                    logger.info(
                        f"_on_trade - MEETING_BAR - {symbol}: "
                        f"detected meeting bars at {meeting}"
                    )

    # ════════════════════════════════════════════════════════════════════
    # Redis pub/sub: broadcast completed bars
    # ════════════════════════════════════════════════════════════════════

    def _publish_bar(self, bar: dict) -> None:
        """Publish a completed bar to a Redis channel for WebSocket fan-out.

        Also publishes BarClosed event to EventBus for inter-service communication
        (e.g., FactorEngine for bar-based factor computation).
        """
        import json

        ticker = bar.get("ticker", "")
        timeframe = bar.get("timeframe", "")
        channel = f"bars:{ticker}:{timeframe}"

        # Publish to Redis pub/sub (for ChartBFF WebSocket fan-out)
        try:
            self.redis_client.publish(channel, json.dumps(bar))
        except Exception as e:
            logger.error(f"Failed to publish bar on {channel}: {e}")

        # Publish BarClosed event to EventBus (for FactorEngine, etc.)
        if self._event_bus:
            try:
                self._event_bus.publish(
                    BusEvent.bar_closed(
                        symbol=ticker,
                        timeframe=timeframe,
                        bar_start=bar.get("bar_start", 0),
                        open_price=bar.get("open", 0.0),
                        high=bar.get("high", 0.0),
                        low=bar.get("low", 0.0),
                        close=bar.get("close", 0.0),
                        volume=bar.get("volume", 0),
                    )
                )
            except Exception as e:
                logger.error(f"Failed to publish BarClosed event: {e}")

    # ════════════════════════════════════════════════════════════════════
    # ClickHouse flush loop
    # ════════════════════════════════════════════════════════════════════

    def _flush_loop(self) -> None:
        """Periodically advance bar state and flush to ClickHouse.

        Polls every 10 ms real-time.  When virtual time crosses a 100 ms
        boundary we run ``advance`` and drain completed bars; any completed bars are flushed
        to ClickHouse immediately so writes land right at :00, :10, …
        """
        POLL_SEC = 0.01  # 10 ms real-time poll

        logger.info("Flush loop started")
        # Align to the current virtual-time 100 ms slot.
        last_slot = clock_mod.now_ms() // 100

        while self._running:
            time.sleep(POLL_SEC)

            now_ms = clock_mod.now_ms()
            cur_slot = now_ms // 100

            if cur_slot <= last_slot:
                continue  # haven't crossed a 100 ms boundary yet
            last_slot = cur_slot

            # Wall-time bar completion: close any bars whose boundary
            # has passed, even if no new trade arrived.
            # Convert UTC → ET for Rust BarBuilder
            now_et_ms = self._utc_ms_to_et_ms(now_ms)
            advanced = self.bar_builder.advance(now_et_ms)
            if advanced:
                logger.debug(f"advance closed {len(advanced)} bars (now_ms={now_ms})")

            completed = self.bar_builder.drain_completed()
            if completed:
                # Rust BarBuilder already handles meeting bar merge internally.
                # Deferred bars (REST not yet available) are held in Rust and
                # will be returned on the next drain after set_rest_partial().
                bars = [
                    Bar.from_rust_dict(self._convert_bar_et_to_utc(d))
                    for d in completed
                ]
                self._stats["bars_completed"] += len(bars)

                # Convert to dicts for ClickHouse
                bar_dicts = [b.to_clickhouse_dict() for b in bars]
                self._enqueue_bars(bar_dicts, source="flush_loop/drain_completed")

                # Publish completed bars to Redis pub/sub
                for bar in bars:
                    self._publish_bar(bar.to_event_dict())

            # Flush to ClickHouse immediately when bars are pending.
            if self._has_pending_bars():
                self._flush_to_clickhouse(caller="flush_loop")

        # Final flush on shutdown
        self._flush_to_clickhouse(caller="flush_loop/shutdown")
        logger.info("Flush loop stopped")

    def _flush_to_clickhouse(self, caller: str = "unknown") -> None:
        """Insert pending bars into ClickHouse via the shared ohlcv_writer."""
        bars = self._drain_pending_bars()
        if not bars:
            return

        # for bar in bars:
        #     logger.debug(
        #         f"_flush_to_clickhouse [{caller}]: writing "
        #         f"{bar.get('ticker')}/{bar.get('timeframe')} "
        #         f"bar_start={self._ms_to_et(bar.get('bar_start', 0))} "
        #         f"bar_end={self._ms_to_et(bar.get('bar_end', 0))} "
        #         f"trades={bar.get('trade_count')}"
        #     )

        try:
            # Use per-thread client via config to avoid concurrent query errors
            n = write_bars(
                self._ch_config,
                bars,
                source="bars_builder",
                filter_closed=False,  # Rust BarBuilder already filters closed session
            )
            self._stats["bars_written"] += n
            logger.debug(
                f"_flush_to_clickhouse: flushed {n} bars to ClickHouse "
                f"(total: {self._stats['bars_written']})"
            )
        except Exception as e:
            logger.error(
                f"_flush_to_clickhouse: failed to insert {len(bars)} bars - {e}"
            )
            # Re-queue failed bars for retry
            self._enqueue_bars(bars, source="flush_to_clickhouse/retry")

    def _enqueue_bars(self, bars: List[Dict], source: str = "unknown") -> None:
        if not bars:
            return
        # for bar in bars:
        #     logger.debug(
        #         f"_enqueue_bars [{source}]: {bar.get('ticker')}/{bar.get('timeframe')} "
        #         f"bar_start={self._ms_to_et(bar.get('bar_start', 0))} "
        #         f"bar_end={self._ms_to_et(bar.get('bar_end', 0))} "
        #         f"trades={bar.get('trade_count')}"
        #     )
        with self._pending_lock:
            self._pending_bars.extend(bars)

    def _has_pending_bars(self) -> bool:
        with self._pending_lock:
            return bool(self._pending_bars)

    def _drain_pending_bars(self) -> List[Dict]:
        with self._pending_lock:
            if not self._pending_bars:
                return []
            bars = self._pending_bars
            self._pending_bars = []
            return bars

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
        """Get the current in-progress bar from the Rust BarBuilder (UTC)."""
        bar = self.bar_builder.get_current_bar(ticker, timeframe)
        if bar is not None:
            bar = self._convert_bar_et_to_utc(bar)
        return bar

    def get_pending_bars(self, ticker: str, timeframe: str) -> List[Dict]:
        """Return completed bars not yet flushed to ClickHouse.

        These bars exist in ``_pending_bars`` for up to ~50 ms before
        the flush loop writes them to ClickHouse.  Including them in
        REST responses prevents a gap between ClickHouse and the
        partial (in-progress) bar.
        """
        with self._pending_lock:
            pending = list(self._pending_bars)
        return [
            bar
            for bar in pending
            if bar.get("ticker") == ticker and bar.get("timeframe") == timeframe
        ]
