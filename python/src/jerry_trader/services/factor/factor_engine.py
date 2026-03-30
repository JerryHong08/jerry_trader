"""Hybrid factor computation engine.

Supports both bar-based and tick-based indicators:
- Bar indicators (EMA): updated on bar close from Redis pub/sub
- Tick indicators (TradeRate): updated on ticks, computed every 1s

Publishes factors to Redis pub/sub for SignalsEngine and ChartBFF.
"""

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import redis

import jerry_trader.clock as clock_mod
from jerry_trader._rust import load_trades_from_parquet
from jerry_trader.domain.factor import FactorSnapshot
from jerry_trader.domain.market import Bar
from jerry_trader.platform.config.config import lake_data_dir
from jerry_trader.platform.storage.clickhouse import (
    get_clickhouse_client,
    query_ohlcv_bars,
)
from jerry_trader.services.bar_builder.chart_data_service import REPLAY_LOOKBACK
from jerry_trader.services.factor.factor_storage import FactorStorage
from jerry_trader.services.factor.indicators import (
    EMA,
    BarIndicator,
    QuoteIndicator,
    TickIndicator,
    TradeRate,
)
from jerry_trader.services.market_data.bootstrap.polygon_fetcher import (
    fetch_polygon_trades,
)
from jerry_trader.services.market_data.feeds.unified_tick_manager import (
    UnifiedTickManager,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("factor_engine", log_to_file=True, level=logging.DEBUG)

# Constants
COMPUTE_INTERVAL_SEC = 1.0  # Tick indicator compute interval


@dataclass
class TimeframeState:
    """Per-timeframe indicator state for a ticker."""

    timeframe: str
    bar_indicators: list[BarIndicator] = field(default_factory=list)
    last_bar_start_ms: int = 0  # Track last processed bar to avoid duplicates

    def reset(self) -> None:
        """Reset all bar indicators."""
        for ind in self.bar_indicators:
            ind.reset()
        self.last_bar_start_ms = 0


@dataclass
class TickerState:
    """Per-ticker indicator state across all timeframes."""

    symbol: str
    timeframe_states: dict[str, TimeframeState] = field(default_factory=dict)
    tick_indicators: list[TickIndicator] = field(default_factory=list)
    quote_indicators: list[QuoteIndicator] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_bootstrap_trade_ms: int = (
        0  # Track last trade from bootstrap to avoid duplicates
    )

    def reset(self) -> None:
        """Reset all indicators."""
        for tf_state in self.timeframe_states.values():
            tf_state.reset()
        for ind in self.tick_indicators:
            ind.reset()
        for ind in self.quote_indicators:
            ind.reset()


class FactorEngine:
    """Hybrid factor computation engine.

    Architecture:
        UnifiedTickManager → ticks → TickIndicators (trade_rate)
        BarsBuilder → Redis pub/sub → BarIndicators (EMA)
        FactorEngine → ClickHouse (via FactorStorage)
    """

    def __init__(
        self,
        session_id: str,
        redis_config: dict[str, Any] | None = None,
        ws_manager: UnifiedTickManager | None = None,
        ws_loop: asyncio.AbstractEventLoop | None = None,
        timeframe: str | None = None,  # Deprecated, kept for compatibility
        clickhouse_config: dict[str, Any] | None = None,
        # Unused params for backend_starter compatibility
        manager_type: str | None = None,
    ):
        """Initialize the factor engine.

        Args:
            session_id: Session identifier
            redis_config: Redis connection config
            ws_manager: Shared UnifiedTickManager for tick data
            ws_loop: Event loop for async tick consumption
            timeframe: Deprecated - timeframes now registered per-ticker
            clickhouse_config: ClickHouse connection config
        """
        self.session_id = session_id
        # Default timeframe for backward compatibility
        self._default_timeframe = timeframe or "1m"

        # Redis client
        redis_config = redis_config or {}
        self.redis_client = redis.Redis(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            db=redis_config.get("db", 0),
            decode_responses=True,
        )

        # ClickHouse storage - uses per-thread clients to avoid concurrent query issues
        self._ch_config = clickhouse_config
        self.storage = (
            FactorStorage(
                session_id=session_id,
                clickhouse_config=clickhouse_config,
            )
            if clickhouse_config
            else None
        )

        # Tick manager (shared with ChartBFF/BarsBuilder)
        self.ws_manager = ws_manager
        self.ws_loop = ws_loop
        self._owns_manager = ws_manager is None

        if self._owns_manager:
            self.ws_manager = UnifiedTickManager(provider=manager_type)
            self.ws_loop = asyncio.new_event_loop()
            self._ws_thread = threading.Thread(
                target=self._run_ws_loop, daemon=True, name="FactorEngine-WS"
            )

        # Per-ticker state
        self.ticker_states: dict[str, TickerState] = {}
        self._states_lock = threading.Lock()

        # Async consumers for tick streams
        self._tick_consumers: dict[str, asyncio.Future] = {}

        # Lifecycle
        self._running = False
        self._pubsub = None
        self._bars_thread: threading.Thread | None = None
        self._tasks_thread: threading.Thread | None = None
        self._compute_thread: threading.Thread | None = None

        # Bootstrap state
        self._bootstrap_events: dict[str, threading.Event] = {}
        self._bootstrap_done: set[str] = set()
        self._bars_builder = None  # set by backend_starter via set_bars_builder()
        # Track tick bootstrap completion per ticker (EventBus TradesBackfillCompleted)
        self._tick_bootstrap_events: dict[str, threading.Event] = {}
        # Track timeframes that failed bootstrap (no bars found) so we can retry
        # when bar backfill completes. Structure: {symbol: {timeframe1, timeframe2, ...}}
        self._failed_bootstrap_timeframes: dict[str, set[str]] = {}

    # ═══════════════════════════════════════════════════════════════════════════
    # EventBus handlers
    # ═══════════════════════════════════════════════════════════════════════════

    def _on_event_bar_backfill_completed(self, event) -> None:
        """Handle BarBackfillCompleted event from EventBus.

        Args:
            event: Event with symbol, timeframe, bar_count in event.data
        """
        symbol = event.symbol
        timeframe = event.timeframe
        bar_count = event.data.get("bar_count", 0)

        # Check if this timeframe failed bootstrap for this symbol
        failed_tfs = self._failed_bootstrap_timeframes.get(symbol, set())
        if timeframe not in failed_tfs:
            return

        logger.info(
            f"_on_event_bar_backfill_completed - {symbol}/{timeframe}: "
            f"{bar_count} bars backfilled, retrying bootstrap"
        )

        # Get the timeframe state
        state = self.ticker_states.get(symbol)
        if not state:
            logger.debug(
                f"_on_event_bar_backfill_completed - {symbol}/{timeframe}: "
                f"TickerState not found, skipping"
            )
            return

        tf_state = state.timeframe_states.get(timeframe)
        if not tf_state:
            logger.debug(
                f"_on_event_bar_backfill_completed - {symbol}/{timeframe}: "
                f"TimeframeState not found, skipping"
            )
            return

        # Retry bootstrap for this timeframe
        self._bootstrap_bars_for_timeframe(symbol, timeframe, tf_state)

        # Remove from failed set if bootstrap succeeded
        failed_tfs.discard(timeframe)
        if not failed_tfs:
            self._failed_bootstrap_timeframes.pop(symbol, None)
            logger.info(
                f"_on_event_bar_backfill_completed - {symbol}/{timeframe}: "
                f"bootstrap retry complete"
            )

    def _on_event_trades_backfill_completed(self, event) -> None:
        """Handle TradesBackfillCompleted event from EventBus.

        Fetches historical trades from the same source (Polygon/parquet) and
        feeds them to tick indicators for warmup. This replaces the old
        _on_bootstrap_trades callback.

        Args:
            event: Event with symbol, trade_count, source, from_ms, first_ws_ts in event.data
        """
        symbol = event.symbol
        trade_count = event.data.get("trade_count", 0)
        source = event.data.get("source", "unknown")
        from_ms = event.data.get("from_ms")
        first_ws_ts = event.data.get("first_ws_ts")

        logger.info(
            f"_on_event_trades_backfill_completed - {symbol}: "
            f"{trade_count} trades from {source}, processing for tick warmup"
        )

        # Fetch trades from the same source
        trades = self._fetch_trades_for_symbol(symbol, source, from_ms, first_ws_ts)
        if not trades:
            logger.warning(
                f"_on_event_trades_backfill_completed - {symbol}: no trades fetched"
            )
            # Signal completion anyway so we don't block
            tick_evt = self._tick_bootstrap_events.get(symbol)
            if tick_evt:
                tick_evt.set()
            return

        # Process trades for tick indicator warmup
        self._process_bootstrap_trades(symbol, trades)

    def _on_event_bar_closed(self, event) -> None:
        """Handle BarClosed event from EventBus.

        Called when BarsBuilder publishes a completed bar. This replaces
        the Redis pub/sub _bars_listener for bar-based factor updates.

        Args:
            event: Event with symbol, timeframe, bar data in event.data
        """
        symbol = event.symbol
        timeframe = event.timeframe

        # Skip processing until bootstrap is complete
        if symbol not in self._bootstrap_done:
            return

        state = self.ticker_states.get(symbol)
        if not state:
            return

        tf_state = state.timeframe_states.get(timeframe)
        if not tf_state:
            return

        # Check for duplicate bars
        bar_start_ms = event.data.get("bar_start", 0)
        if bar_start_ms <= tf_state.last_bar_start_ms:
            logger.debug(
                f"_on_event_bar_closed: skipping duplicate bar for {symbol}/{timeframe}"
            )
            return

        # Update last processed bar timestamp
        tf_state.last_bar_start_ms = bar_start_ms

        # Convert event data to Bar domain model
        bar = Bar(
            symbol=symbol,
            timestamp_ns=bar_start_ms * 1_000_000,  # ms to ns
            timespan=timeframe,
            open=event.data.get("open", 0.0),
            high=event.data.get("high", 0.0),
            low=event.data.get("low", 0.0),
            close=event.data.get("close", 0.0),
            volume=int(event.data.get("volume", 0)),
        )

        # Update bar indicators for this timeframe
        factors: dict[str, float] = {}
        for ind in tf_state.bar_indicators:
            value = ind.update(bar)
            if value is not None:
                factors[ind.name] = value

        # Write bar-based factors to ClickHouse
        if factors:
            self._write_factors(symbol, bar.timestamp_ns, factors, timeframe)

    def _fetch_trades_for_symbol(
        self, symbol: str, source: str, from_ms: int | None, first_ws_ts: int | None
    ) -> list[tuple[int, float, int]]:
        """Fetch trades from source for tick indicator warmup.

        Args:
            symbol: Ticker symbol
            source: Data source ("polygon" or "parquet")
            from_ms: Start timestamp (UTC ms)
            first_ws_ts: End timestamp (UTC ms) - only trades before this

        Returns:
            List of (timestamp_ms, price, size) tuples
        """
        from jerry_trader.platform.config.session import parse_session_id

        trades: list[tuple[int, float, int]] = []

        if source == "live":
            trades = fetch_polygon_trades(symbol, from_ms=from_ms)
        else:  # replay mode - load from parquet
            db_date, _ = parse_session_id(self.session_id)
            end_ts_ms = first_ws_ts or clock_mod.now_ms()
            trades = load_trades_from_parquet(
                lake_data_dir, symbol, db_date, end_ts_ms, from_ms or 0
            )

        # Filter to only trades before first_ws_ts
        if first_ws_ts and trades:
            trades = [(t, p, s) for t, p, s in trades if t < first_ws_ts]

        return trades

    def _process_bootstrap_trades(
        self, symbol: str, trades: list[tuple[int, float, int]]
    ) -> None:
        """Process historical trades for tick indicator warmup.

        This replaces the old _on_bootstrap_trades callback.

        Args:
            symbol: Ticker symbol
            trades: List of (timestamp_ms, price, size) tuples
        """
        # Wait up to 2s for TickerState to exist (BarsBuilder may process
        # "add" before FactorEngine)
        state = None
        for _ in range(20):
            state = self.ticker_states.get(symbol)
            if state:
                break
            time.sleep(0.1)

        if not state:
            logger.warning(
                f"_process_bootstrap_trades - {symbol}: TickerState not found after 2s, skipping"
            )
            # Signal completion anyway
            tick_evt = self._tick_bootstrap_events.get(symbol)
            if tick_evt:
                tick_evt.set()
            return

        logger.info(
            f"_process_bootstrap_trades - {symbol}: feeding {len(trades)} trades to tick indicators"
        )

        # Feed trades and simulate 1s compute intervals
        snapshots: list[FactorSnapshot] = []
        last_compute_ms = 0
        max_ts_ms = 0

        for ts_ms, price, size in trades:
            ts_ms = int(ts_ms)
            max_ts_ms = max(max_ts_ms, ts_ms)
            for ind in state.tick_indicators:
                ind.on_tick(ts_ms, float(price), int(size))

            # Compute every 1000ms
            if last_compute_ms == 0:
                last_compute_ms = ts_ms
            elif ts_ms - last_compute_ms >= 1000:
                factors: dict[str, float] = {}
                for ind in state.tick_indicators:
                    value = ind.compute(ts_ms)
                    if value is not None:
                        factors[ind.name] = value
                if factors:
                    snapshots.append(
                        FactorSnapshot(
                            symbol=symbol,
                            timestamp_ns=ts_ms * 1_000_000,
                            factors=factors,
                        )
                    )
                last_compute_ms = ts_ms

        # Track the last trade timestamp to avoid duplicate processing
        state.last_bootstrap_trade_ms = max_ts_ms
        logger.debug(
            f"_process_bootstrap_trades - {symbol}: last_bootstrap_trade_ms set to {max_ts_ms}"
        )

        # Signal that tick bootstrap is complete
        tick_evt = self._tick_bootstrap_events.get(symbol)
        if tick_evt:
            tick_evt.set()
            logger.debug(
                f"_process_bootstrap_trades - {symbol}: tick bootstrap event set"
            )

        # Batch write tick-based factor snapshots to ClickHouse
        if snapshots and self.storage:
            snapshots_with_tf = [(s, "tick") for s in snapshots]
            count = self.storage.write_batch(snapshots_with_tf)
            logger.info(
                f"_process_bootstrap_trades - {symbol}: wrote {count} tick factor rows "
                f"from {len(snapshots)} snapshots"
            )

    def _run_ws_loop(self) -> None:
        """Run the WebSocket event loop (only if we own the manager)."""
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

    # ═══════════════════════════════════════════════════════════════════════════
    # Ticker Management
    # ═══════════════════════════════════════════════════════════════════════════

    def add_ticker(self, symbol: str, timeframes: list[str] | None = None) -> None:
        """Start tracking a ticker for factor computation.

        Args:
            symbol: Ticker symbol to track
            timeframes: List of timeframes to compute bar-based factors for.
                       Defaults to [self._default_timeframe] if not specified.
        """
        timeframes = timeframes or [self._default_timeframe]

        with self._states_lock:
            if symbol in self.ticker_states:
                logger.debug(f"FactorEngine: {symbol} already tracked")
                # Add any new timeframes to existing ticker
                state = self.ticker_states[symbol]
                for tf in timeframes:
                    if tf not in state.timeframe_states:
                        state.timeframe_states[tf] = TimeframeState(
                            timeframe=tf, bar_indicators=[EMA(period=20)]
                        )
                        self._subscribe_bars_for_timeframe(symbol, tf)
                return

            # Create timeframe states for each timeframe
            timeframe_states: dict[str, TimeframeState] = {}
            for tf in timeframes:
                timeframe_states[tf] = TimeframeState(
                    timeframe=tf, bar_indicators=[EMA(period=20)]
                )

            # Create indicator instances
            state = TickerState(
                symbol=symbol,
                timeframe_states=timeframe_states,
                tick_indicators=[TradeRate(window_ms=20_000, min_trades=5)],
                quote_indicators=[],
            )
            self.ticker_states[symbol] = state

        # Create bootstrap Events
        if symbol not in self._bootstrap_events:
            self._bootstrap_events[symbol] = threading.Event()
        if symbol not in self._tick_bootstrap_events:
            self._tick_bootstrap_events[symbol] = threading.Event()

        # Subscribe to bar pub/sub channels for all timeframes
        for tf in timeframes:
            self._subscribe_bars_for_timeframe(symbol, tf)

        # Subscribe to tick stream (timeframe-agnostic)
        self._subscribe_ticks(symbol)

        # Start bootstrap in background thread
        if self._bars_builder is not None:
            t = threading.Thread(
                target=self._run_bootstrap,
                args=(symbol,),
                daemon=True,
                name=f"FactorEngine-Bootstrap-{symbol}",
            )
            t.start()

        logger.info(f"FactorEngine: tracking {symbol} with timeframes={timeframes}")

    def remove_ticker(self, symbol: str) -> None:
        """Stop tracking a ticker."""
        with self._states_lock:
            if symbol not in self.ticker_states:
                return
            state = self.ticker_states[symbol]
            timeframes = list(state.timeframe_states.keys())
            del self.ticker_states[symbol]

        # Unsubscribe from all bar channels
        if self._pubsub:
            for tf in timeframes:
                channel = f"bars:{symbol}:{tf}"
                try:
                    self._pubsub.unsubscribe(channel)
                except Exception:
                    pass

        # Cancel tick consumer
        self._unsubscribe_ticks(symbol)

        logger.info(f"FactorEngine: stopped tracking {symbol}")

    # ═══════════════════════════════════════════════════════════════════════════
    # Bootstrap
    # ═══════════════════════════════════════════════════════════════════════════

    def set_bars_builder(self, bars_builder) -> None:
        """Wire BarsBuilder reference for bootstrap wait functionality."""
        self._bars_builder = bars_builder

    def _bootstrap_bars_for_timeframe(
        self, symbol: str, timeframe: str, tf_state: "TimeframeState"
    ) -> None:
        """Warm up bar-based indicators for a specific timeframe from ClickHouse."""
        # Create per-thread ClickHouse client to avoid concurrent query issues
        ch_client = get_clickhouse_client(self._ch_config)
        if not ch_client:
            return

        # Query bars using same lookback as ChartDataService (REPLAY_LOOKBACK)
        # This ensures enough historical bars for EMA warmup without over-fetching
        end_ms = clock_mod.now_ms()
        lookback_days = REPLAY_LOOKBACK.get(timeframe, 5)  # default 5 days
        start_ms = end_ms - (lookback_days * 24 * 60 * 60 * 1000)

        result = query_ohlcv_bars(ch_client, symbol, timeframe, start_ms, end_ms)

        if not result or not result.get("bars"):
            logger.info(
                f"_bootstrap_bars - {symbol}/{timeframe}: no bars found for warmup"
            )
            # Track this timeframe as failed so we can retry when bar backfill completes
            if symbol not in self._failed_bootstrap_timeframes:
                self._failed_bootstrap_timeframes[symbol] = set()
            self._failed_bootstrap_timeframes[symbol].add(timeframe)
            return

        bars = result["bars"]
        logger.info(
            f"_bootstrap_bars - {symbol}/{timeframe}: feeding {len(bars)} bars to bar indicators"
        )

        snapshots: list[FactorSnapshot] = []
        for bar_dict in bars:
            # query_ohlcv_bars returns bars with "time" (epoch seconds)
            bar = Bar(
                symbol=symbol,
                timestamp_ns=bar_dict["time"] * 1_000_000_000,  # seconds to ns
                timespan=timeframe,
                open=bar_dict["open"],
                high=bar_dict["high"],
                low=bar_dict["low"],
                close=bar_dict["close"],
                volume=int(bar_dict.get("volume", 0)),
                vwap=bar_dict.get("vwap"),
                trade_count=bar_dict.get("trade_count"),
            )

            factors: dict[str, float] = {}
            for ind in tf_state.bar_indicators:
                value = ind.update(bar)
                if value is not None:
                    factors[ind.name] = value

            if factors:
                snapshots.append(
                    FactorSnapshot(
                        symbol=symbol,
                        timestamp_ns=bar.timestamp_ns,
                        factors=factors,
                    )
                )

        # Batch write bar-based factor snapshots for this timeframe
        if snapshots and self.storage:
            # Create tuples with timeframe for write_batch
            snapshots_with_tf = [(s, timeframe) for s in snapshots]
            count = self.storage.write_batch(snapshots_with_tf)
            logger.info(
                f"_bootstrap_bars - {symbol}/{timeframe}: wrote {count} bar factor rows "
                f"from {len(snapshots)} snapshots"
            )

        # Track the last bar timestamp to avoid duplicate processing
        # when real-time bars arrive via Redis pub/sub
        if bars:
            last_bar = bars[-1]
            tf_state.last_bar_start_ms = last_bar["time"] * 1000  # seconds to ms

    def _bootstrap_bars(self, symbol: str) -> None:
        """Warm up bar-based indicators (EMA) from today's completed bars in ClickHouse.

        Bootstraps each timeframe separately.
        """
        if not self._ch_config:
            logger.warning(
                f"_bootstrap_bars - {symbol}: no ClickHouse config, skipping"
            )
            return

        state = self.ticker_states.get(symbol)
        if not state:
            return

        # Bootstrap each timeframe separately
        # Create a snapshot to avoid "dictionary changed size during iteration"
        with state.lock:
            timeframe_items = list(state.timeframe_states.items())
        for timeframe, tf_state in timeframe_items:
            self._bootstrap_bars_for_timeframe(symbol, timeframe, tf_state)

    def _run_bootstrap(self, symbol: str) -> None:
        """Orchestrate factor bootstrap for a ticker."""
        try:
            # 1. Wait for BarsBuilder bootstrap to complete (bars in ClickHouse)
            if self._bars_builder is not None:
                logger.info(
                    f"_run_bootstrap - {symbol}: waiting for bars_builder bootstrap"
                )
                # Pre-register to ensure Event exists (otherwise wait_for_bootstrap
                # returns immediately if bootstrap hasn't started yet)
                self._bars_builder.pre_register_bootstrap(symbol)
                ok = self._bars_builder.wait_for_bootstrap(symbol, timeout=60.0)
                if not ok:
                    logger.warning(
                        f"_run_bootstrap - {symbol}: bars_builder bootstrap timed out"
                    )

            # 2. Bar-based indicator warmup from ClickHouse bars
            self._bootstrap_bars(symbol)

            # 3. Wait for tick bootstrap to complete (via EventBus TradesBackfillCompleted)
            # This ensures tick indicators are fully warmed up before we start
            # processing real-time ticks
            tick_evt = self._tick_bootstrap_events.get(symbol)
            if tick_evt:
                logger.info(f"_run_bootstrap - {symbol}: waiting for tick bootstrap")
                if not tick_evt.wait(timeout=30.0):
                    # Timeout - no historical trades received, set event anyway
                    # so we don't block forever
                    logger.warning(
                        f"_run_bootstrap - {symbol}: tick bootstrap timed out, "
                        "proceeding without tick warmup"
                    )
                    tick_evt.set()
                else:
                    logger.info(f"_run_bootstrap - {symbol}: tick bootstrap complete")

            logger.info(f"_run_bootstrap - {symbol}: bootstrap complete")
        except Exception as e:
            logger.error(f"_run_bootstrap - {symbol}: error - {e}")
        finally:
            self._bootstrap_done.add(symbol)
            evt = self._bootstrap_events.get(symbol)
            if evt:
                evt.set()

    def pre_register_bootstrap(self, symbol: str) -> None:
        """Pre-create bootstrap Events for this ticker.

        Called by ChartBFF BEFORE the Redis XADD so that any concurrent
        REST request will block on wait_for_bootstrap.
        """
        if symbol not in self._bootstrap_events:
            self._bootstrap_events[symbol] = threading.Event()
            logger.debug(
                f"pre_register_bootstrap - {symbol}: bootstrap event pre-created"
            )
        if symbol not in self._tick_bootstrap_events:
            self._tick_bootstrap_events[symbol] = threading.Event()
            logger.debug(
                f"pre_register_bootstrap - {symbol}: tick bootstrap event pre-created"
            )

    def wait_for_bootstrap(self, symbol: str, timeout: float = 10.0) -> bool:
        """Wait until factor bootstrap completes for this ticker.

        Waits for both bar-based and tick-based bootstrap to complete.
        Returns True if bootstrap completed, False if timed out or bootstrap
        was not pre-registered (indicating FactorEngine hasn't started processing).
        """
        if symbol in self._bootstrap_done:
            return True

        # Check if bootstrap was pre-registered (events exist)
        # If not, FactorEngine hasn't started processing this ticker yet
        evt = self._bootstrap_events.get(symbol)
        tick_evt = self._tick_bootstrap_events.get(symbol)

        if evt is None and tick_evt is None:
            # Bootstrap not pre-registered yet, wait briefly for it to appear
            logger.debug(
                f"wait_for_bootstrap - {symbol}: events not created yet, waiting..."
            )
            for _ in range(50):  # 5 seconds total
                time.sleep(0.1)
                evt = self._bootstrap_events.get(symbol)
                tick_evt = self._tick_bootstrap_events.get(symbol)
                if evt is not None or tick_evt is not None:
                    break
            if evt is None and tick_evt is None:
                logger.warning(
                    f"wait_for_bootstrap - {symbol}: events never created, returning False"
                )
                return False

        # Wait for bar bootstrap
        if evt is not None:
            if not evt.wait(timeout=timeout):
                logger.warning(
                    f"wait_for_bootstrap - {symbol}: bar bootstrap timed out"
                )
                return False

        # Wait for tick bootstrap (trade observer callback)
        if tick_evt is not None:
            if not tick_evt.wait(timeout=timeout):
                logger.warning(
                    f"wait_for_bootstrap - {symbol}: tick bootstrap timed out"
                )
                return False

        logger.debug(f"wait_for_bootstrap - {symbol}: bootstrap complete")
        return True

    # ═══════════════════════════════════════════════════════════════════════════
    # Bar Subscription (Redis pub/sub)
    # ═══════════════════════════════════════════════════════════════════════════

    def _subscribe_bars_for_timeframe(self, symbol: str, timeframe: str) -> None:
        """Subscribe to Redis pub/sub channel for bars of a specific timeframe."""
        if not self._pubsub:
            return

        channel = f"bars:{symbol}:{timeframe}"
        try:
            self._pubsub.subscribe(channel)
            logger.debug(f"FactorEngine: subscribed to {channel}")
        except Exception as e:
            logger.error(f"FactorEngine: Failed to subscribe to {channel} - {e}")

    def _bars_listener(self) -> None:
        """Redis pub/sub listener for completed bars."""
        logger.info("FactorEngine: bars listener started")

        while self._running:
            try:
                message = self._pubsub.get_message(timeout=1.0)
                if message and message["type"] == "message":
                    try:
                        bar_dict = json.loads(message["data"])
                        self._on_bar(bar_dict)
                    except json.JSONDecodeError as e:
                        logger.warning(f"FactorEngine: Invalid JSON - {e}")
                    except Exception as e:
                        logger.error(f"FactorEngine: Error processing bar - {e}")
            except Exception as e:
                if self._running:
                    logger.error(f"FactorEngine: Bars listener error - {e}")
                    time.sleep(1.0)

        logger.info("FactorEngine: bars listener stopped")

    def _on_bar(self, bar_dict: dict) -> None:
        """Handle a completed bar."""
        symbol = bar_dict.get("ticker")
        if not symbol:
            return

        # Skip processing until bootstrap is complete to ensure
        # indicator state is fully warmed up before real-time bars
        if symbol not in self._bootstrap_done:
            # logger.debug(
            #     f"_on_bar: skipping bar for {symbol} - bootstrap not complete"
            # )
            return

        # Get timeframe from bar data
        timeframe = bar_dict.get("timeframe")
        if not timeframe:
            logger.warning(f"_on_bar: bar for {symbol} missing timeframe, skipping")
            return

        with self._states_lock:
            state = self.ticker_states.get(symbol)

        if not state:
            return

        # Get the timeframe-specific state
        tf_state = state.timeframe_states.get(timeframe)
        if not tf_state:
            logger.debug(f"_on_bar: no factor state for {symbol}/{timeframe}, skipping")
            return

        # Check for duplicate bars (already processed during bootstrap)
        bar_start_ms = bar_dict.get("bar_start", 0)
        if bar_start_ms <= tf_state.last_bar_start_ms:
            logger.debug(
                f"_on_bar: skipping duplicate bar for {symbol}/{timeframe} "
                f"at {bar_start_ms} (last processed: {tf_state.last_bar_start_ms})"
            )
            return

        # Update last processed bar timestamp
        tf_state.last_bar_start_ms = bar_start_ms

        logger.debug(
            f"_on_bar: received bar for {symbol}/{timeframe} at {bar_start_ms}"
        )
        # Convert to Bar domain model
        bar = Bar(
            symbol=symbol,
            timestamp_ns=bar_dict["bar_start"] * 1_000_000,  # ms to ns
            timespan=timeframe,
            open=bar_dict["open"],
            high=bar_dict["high"],
            low=bar_dict["low"],
            close=bar_dict["close"],
            volume=int(bar_dict.get("volume", 0)),
            vwap=bar_dict.get("vwap"),
            trade_count=bar_dict.get("trade_count"),
        )

        # Update bar indicators for this timeframe
        factors: dict[str, float] = {}
        for ind in tf_state.bar_indicators:
            logger.debug(
                f"_on_bar: calculate {symbol}/{timeframe} {ind.name} with bar at {bar_dict.get('bar_start')}"
            )
            value = ind.update(bar)

            logger.debug(
                f"_on_bar: calculated {symbol}/{timeframe} {ind.name} = {value}"
            )

            if value is not None:
                factors[ind.name] = value

        # Write bar-based factors to ClickHouse with timeframe
        if factors:
            self._write_factors(symbol, bar.timestamp_ns, factors, timeframe)

    # ═══════════════════════════════════════════════════════════════════════════
    # Tick Subscription (UnifiedTickManager)
    # ═══════════════════════════════════════════════════════════════════════════

    def _subscribe_ticks(self, symbol: str) -> None:
        """Subscribe to tick stream via UnifiedTickManager."""
        if not self.ws_manager or not self.ws_loop:
            return

        # Subscribe to trades and quotes
        asyncio.run_coroutine_threadsafe(
            self.ws_manager.subscribe(
                websocket_client="factor_engine",
                symbols=[symbol],
                events=["T", "Q"],
            ),
            self.ws_loop,
        )

        # Start consumer for this stream
        stream_keys = self.ws_manager.generate_stream_keys(
            symbols=[symbol], events=["T", "Q"]
        )
        for stream_key in stream_keys:
            future = asyncio.run_coroutine_threadsafe(
                self._consume_ticks(stream_key), self.ws_loop
            )
            old = self._tick_consumers.pop(stream_key, None)
            if old:
                old.cancel()
            self._tick_consumers[stream_key] = future
            logger.debug(f"FactorEngine: tick consumer started for {stream_key}")

    def _unsubscribe_ticks(self, symbol: str) -> None:
        """Unsubscribe from tick stream."""
        if not self.ws_manager or not self.ws_loop:
            return

        asyncio.run_coroutine_threadsafe(
            self.ws_manager.unsubscribe(
                websocket_client="factor_engine",
                symbol=symbol,
                events=["T", "Q"],
            ),
            self.ws_loop,
        )

        stream_keys = self.ws_manager.generate_stream_keys(
            symbols=[symbol], events=["T", "Q"]
        )
        for stream_key in stream_keys:
            future = self._tick_consumers.pop(stream_key, None)
            if future:
                future.cancel()

    async def _consume_ticks(self, stream_key: str) -> None:
        """Async consumer for tick data from UnifiedTickManager."""
        q = self.ws_manager.get_client_queue("factor_engine", stream_key)
        if q is None:
            logger.warning(f"FactorEngine: No queue for {stream_key}")
            return

        is_quote = ".Q." in stream_key or stream_key.endswith(".Q")

        while True:
            try:
                data = await q.get()
                normalized = self.ws_manager.normalize_data(data)
                if is_quote:
                    self._on_quote(normalized)
                else:
                    self._on_tick(normalized)
            except asyncio.CancelledError:
                logger.debug(f"FactorEngine: tick consumer cancelled for {stream_key}")
                break
            except Exception as e:
                logger.error(f"FactorEngine: tick consumer error - {e}")

    def _on_tick(self, data: dict) -> None:
        """Handle a trade tick."""
        symbol = data.get("symbol")
        if not symbol:
            return

        # Skip processing until bootstrap is complete to ensure
        # tick indicators are warmed up with historical trades
        if symbol not in self._bootstrap_done:
            # logger.debug(
            #     f"_on_tick: skipping tick for {symbol} - bootstrap not complete"
            # )
            return

        with self._states_lock:
            state = self.ticker_states.get(symbol)

        if not state:
            return

        ts_ms = data.get("timestamp")
        price = data.get("price")
        size = data.get("size", 0)

        if ts_ms is None or price is None:
            return

        ts_ms = int(ts_ms)

        # Skip trades that were already processed during bootstrap
        # to avoid duplicate counting in tick indicators
        if ts_ms <= state.last_bootstrap_trade_ms:
            logger.debug(
                f"_on_tick: skipping duplicate trade for {symbol} "
                f"at {ts_ms} (last bootstrap: {state.last_bootstrap_trade_ms})"
            )
            return

        # Feed to tick indicators
        for ind in state.tick_indicators:
            ind.on_tick(ts_ms, float(price), int(size))

    def _on_quote(self, data: dict) -> None:
        """Handle a quote tick."""
        symbol = data.get("symbol")
        if not symbol:
            return

        # Skip processing until bootstrap is complete
        if symbol not in self._bootstrap_done:
            return

        state = self.ticker_states.get(symbol)
        if not state or not state.quote_indicators:
            return

        ts_ms = data.get("timestamp")
        bid = data.get("bid_price") or data.get("bid")
        ask = data.get("ask_price") or data.get("ask")
        if ts_ms is None or bid is None or ask is None:
            return

        for ind in state.quote_indicators:
            ind.on_quote(
                int(ts_ms),
                float(bid),
                float(ask),
                int(data.get("bid_size", 0)),
                int(data.get("ask_size", 0)),
            )

    # ═══════════════════════════════════════════════════════════════════════════
    # Tick Indicator Compute Loop (every 1s)
    # ═══════════════════════════════════════════════════════════════════════════

    def _compute_loop(self) -> None:
        """Compute tick indicators every COMPUTE_INTERVAL_SEC."""
        logger.info("FactorEngine: compute loop started")

        while self._running:
            ts_ms = clock_mod.now_ms()

            with self._states_lock:
                states = list(self.ticker_states.values())

            for state in states:
                # Skip tick factor publishing until bootstrap is complete
                # to avoid mixing real-time data with historical bootstrap data
                if state.symbol not in self._bootstrap_done:
                    continue

                try:
                    factors: dict[str, float] = {}
                    for ind in state.tick_indicators:
                        value = ind.compute(ts_ms)
                        if value is not None:
                            factors[ind.name] = value

                    for ind in state.quote_indicators:
                        value = ind.compute(ts_ms)
                        if value is not None:
                            factors[ind.name] = value

                    if factors:
                        self._write_factors(
                            state.symbol,
                            ts_ms * 1_000_000,  # ms to ns
                            factors,
                            timeframe="tick",  # Tick-based indicators
                        )
                except Exception as e:
                    logger.error(
                        f"FactorEngine: compute error for {state.symbol} - {e}"
                    )

            time.sleep(COMPUTE_INTERVAL_SEC)

        logger.info("FactorEngine: compute loop stopped")

    # ═══════════════════════════════════════════════════════════════════════════
    # Factor Storage (ClickHouse)
    # ═══════════════════════════════════════════════════════════════════════════

    def _write_factors(
        self,
        symbol: str,
        timestamp_ns: int,
        factors: dict[str, float],
        timeframe: str = "tick",
    ) -> None:
        """Write factors to ClickHouse via FactorStorage and publish to Redis pub/sub.

        Args:
            symbol: Ticker symbol
            timestamp_ns: Timestamp in nanoseconds
            factors: Dict of factor_name -> value
            timeframe: Timeframe for these factors (e.g., '1m', '5m', 'tick')
                      Use 'tick' for tick-based indicators like TradeRate
        """
        if not self.storage:
            return

        snapshot = FactorSnapshot(
            symbol=symbol,
            timestamp_ns=timestamp_ns,
            factors=factors,
        )

        try:
            count = self.storage.write_factor_snapshot(snapshot, timeframe)
            if count > 0:
                logger.debug(
                    f"FactorEngine: wrote {count} factors for {symbol}/{timeframe} "
                    f"({list(factors.keys())})"
                )

                # Publish to timeframe-specific Redis channel for real-time streaming
                try:
                    channel = f"factors:{symbol}:{timeframe}"
                    message = json.dumps(
                        {
                            "symbol": symbol,
                            "timestamp_ns": timestamp_ns,
                            "timestamp_ms": timestamp_ns // 1_000_000,
                            "timeframe": timeframe,
                            "factors": factors,
                        }
                    )
                    self.redis_client.publish(channel, message)
                    # logger.debug(
                    #     f"FactorEngine: published {len(factors)} factors to {channel}"
                    # )
                except Exception as e:
                    logger.error(f"FactorEngine: Redis publish failed - {e}")

        except Exception as e:
            logger.error(f"FactorEngine: write failed for {symbol} - {e}")

    # ═══════════════════════════════════════════════════════════════════════════
    # Tasks Listener (Redis stream for add/remove ticker)
    # ═══════════════════════════════════════════════════════════════════════════

    def _tasks_listener(self) -> None:
        """Listen to factor_tasks Redis stream for ticker commands."""
        stream_key = f"factor_tasks:{self.session_id}"
        last_id = "0"

        logger.info(f"FactorEngine: tasks listener started on {stream_key}")

        while self._running:
            try:
                result = self.redis_client.xread(
                    {stream_key: last_id}, count=10, block=1000
                )

                if not result:
                    continue

                for stream_name, messages in result:
                    for msg_id, msg_data in messages:
                        last_id = msg_id
                        try:
                            action = msg_data.get("action")
                            ticker = msg_data.get("ticker")

                            if not ticker:
                                continue

                            if action == "add":
                                # Parse timeframes from message (e.g., "10s,1m,5m")
                                timeframes_str = msg_data.get("timeframes", "")
                                if timeframes_str:
                                    timeframes = [
                                        tf.strip()
                                        for tf in timeframes_str.split(",")
                                        if tf.strip()
                                    ]
                                else:
                                    timeframes = None  # Use default
                                self.add_ticker(ticker, timeframes)
                            elif action == "remove":
                                self.remove_ticker(ticker)
                            else:
                                logger.warning(f"FactorEngine: Unknown action {action}")
                        except Exception as e:
                            logger.error(f"FactorEngine: task error - {e}")

            except Exception as e:
                if self._running:
                    logger.error(f"FactorEngine: tasks listener error - {e}")
                    time.sleep(1.0)

        logger.info("FactorEngine: tasks listener stopped")

    # ═══════════════════════════════════════════════════════════════════════════
    # Lifecycle
    # ═══════════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        """Start the factor engine."""
        if self._running:
            return

        self._running = True

        # Start WS loop if we own the manager
        if self._owns_manager:
            self._ws_thread.start()

        # Initialize bar pub/sub
        self._pubsub = self.redis_client.pubsub()

        # Subscribe to bars for existing tickers (all their timeframes)
        with self._states_lock:
            ticker_states_snapshot = list(self.ticker_states.items())
        for symbol, state in ticker_states_snapshot:
            # Create a copy of timeframe keys to avoid "dictionary changed size during iteration"
            with state.lock:
                timeframes = list(state.timeframe_states.keys())
            for timeframe in timeframes:
                self._subscribe_bars_for_timeframe(symbol, timeframe)

        # Start bars listener thread
        self._bars_thread = threading.Thread(
            target=self._bars_listener,
            name="FactorEngine-Bars",
            daemon=True,
        )
        self._bars_thread.start()

        # Start tasks listener thread
        self._tasks_thread = threading.Thread(
            target=self._tasks_listener,
            name="FactorEngine-Tasks",
            daemon=True,
        )
        self._tasks_thread.start()

        # Start compute loop thread
        self._compute_thread = threading.Thread(
            target=self._compute_loop,
            name="FactorEngine-Compute",
            daemon=True,
        )
        self._compute_thread.start()

        logger.info("FactorEngine started")

    def stop(self) -> None:
        """Stop the factor engine."""
        if not self._running:
            return

        self._running = False

        # Cancel tick consumers
        for future in self._tick_consumers.values():
            future.cancel()
        self._tick_consumers.clear()

        # Close pub/sub
        if self._pubsub:
            try:
                self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None

        # Join threads
        for thread in [self._bars_thread, self._tasks_thread, self._compute_thread]:
            if thread and thread.is_alive():
                thread.join(timeout=5.0)

        # Stop WS loop if we own it
        if self._owns_manager and self.ws_loop and self.ws_loop.is_running():
            self.ws_loop.call_soon_threadsafe(self.ws_loop.stop)
            if hasattr(self, "_ws_thread"):
                self._ws_thread.join(timeout=2.0)

        logger.info("FactorEngine stopped")

    @property
    def tracked_tickers(self) -> list[str]:
        """Get list of tracked tickers."""
        with self._states_lock:
            return list(self.ticker_states.keys())
