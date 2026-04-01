"""Hybrid factor computation engine.

Supports both bar-based and tick-based indicators:
- Bar indicators (EMA): updated on bar close from Redis pub/sub
- Tick indicators (TradeRate): updated on ticks, computed every 1s

Publishes factors to Redis pub/sub for SignalsEngine and ChartBFF.
"""

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import redis
from redis.exceptions import ResponseError

import jerry_trader.clock as clock_mod
from jerry_trader.domain.factor import FactorSnapshot
from jerry_trader.domain.market import Bar, BarPeriod
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
from jerry_trader.services.market_data.feeds.unified_tick_manager import (
    UnifiedTickManager,
)
from jerry_trader.services.orchestration.bootstrap_coordinator import (
    BOOTSTRAP_TIMEFRAMES,
    BootstrapCoordinator,
    TimeframeState,
    TradesBootstrapState,
    get_coordinator,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("factor_engine", log_to_file=True, level=logging.INFO)

# Constants
COMPUTE_INTERVAL_SEC = 1.0  # Tick indicator compute interval


@dataclass
class FactorTimeframeState:
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
    timeframe_states: dict[str, FactorTimeframeState] = field(default_factory=dict)
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

        # BootstrapCoordinator integration (new orchestration)
        self._coordinator: BootstrapCoordinator | None = (
            None  # set via set_coordinator()
        )

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
            timeframe=timeframe,
            open=event.data.get("open", 0.0),
            high=event.data.get("high", 0.0),
            low=event.data.get("low", 0.0),
            close=event.data.get("close", 0.0),
            volume=float(event.data.get("volume", 0)),
            trade_count=event.data.get("trade_count", 0),
            vwap=event.data.get("vwap", 0.0),
            bar_start=bar_start_ms,
            bar_end=BarPeriod(timeframe, bar_start_ms).end_ms,
            session=event.data.get("session", "regular"),
        )

        # Update bar indicators for this timeframe
        factors: dict[str, float] = {}
        for ind in tf_state.bar_indicators:
            value = ind.update(bar)
            if value is not None:
                factors[ind.name] = value

        # Write bar-based factors to ClickHouse
        if factors:
            self._write_factors(symbol, bar.bar_start * 1_000_000, factors, timeframe)

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

    def is_ready(self, symbol: str) -> bool:
        """Check if ticker has completed bootstrap.

        Part of BootstrapableService protocol.
        """
        return symbol in self._bootstrap_done

    def add_ticker(self, symbol: str, timeframes: list[str] | None = None) -> bool:
        """Start tracking a ticker for factor computation.

        Part of BootstrapableService protocol. Called by Coordinator directly.
        Queries ClickHouse first to check if bars exist - if so, skips waiting
        for bootstrap and warms up immediately.

        Args:
            symbol: Ticker symbol to track
            timeframes: List of timeframes to compute bar-based factors for.
                       If None, uses Coordinator's timeframes or falls back to default.

        Returns:
            True if successfully started (or immediately complete)
        """
        # If no timeframes provided, use all bootstrap timeframes from config
        if timeframes is None:
            timeframes = list(BOOTSTRAP_TIMEFRAMES)
            logger.info(
                f"add_ticker - {symbol}: using all bootstrap timeframes: {timeframes}"
            )

        # Fallback to default if still None
        timeframes = timeframes or [self._default_timeframe]

        # Track new timeframes that need bootstrap for existing tickers
        new_timeframes_for_existing: list[str] = []

        with self._states_lock:
            if symbol in self.ticker_states:
                logger.debug(f"FactorEngine: {symbol} already tracked")
                # Add any new timeframes to existing ticker
                state = self.ticker_states[symbol]
                for tf in timeframes:
                    if tf not in state.timeframe_states:
                        state.timeframe_states[tf] = FactorTimeframeState(
                            timeframe=tf, bar_indicators=[EMA(period=20)]
                        )
                        self._subscribe_bars_for_timeframe(symbol, tf)
                        new_timeframes_for_existing.append(tf)

                # If no new timeframes, just return
                if not new_timeframes_for_existing:
                    return True

                # Continue to bootstrap logic for new timeframes only
                logger.info(
                    f"FactorEngine: {symbol} adding new timeframes {new_timeframes_for_existing}"
                )

                # Only bootstrap the new timeframes, not all of them
                tfs_to_process = new_timeframes_for_existing
                state = self.ticker_states[symbol]

                # Create bootstrap events if needed (might be already done)
                if symbol not in self._bootstrap_events:
                    self._bootstrap_events[symbol] = threading.Event()
                    self._bootstrap_events[
                        symbol
                    ].set()  # Already done for existing ticker
                if symbol not in self._tick_bootstrap_events:
                    self._tick_bootstrap_events[symbol] = threading.Event()
                    self._tick_bootstrap_events[symbol].set()  # Already done
            else:
                # Create timeframe states for each timeframe (new ticker case)
                timeframe_states: dict[str, FactorTimeframeState] = {}
                for tf in timeframes:
                    timeframe_states[tf] = FactorTimeframeState(
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

                tfs_to_process = timeframes

                # Create bootstrap Events (new ticker case)
                if symbol not in self._bootstrap_events:
                    self._bootstrap_events[symbol] = threading.Event()
                if symbol not in self._tick_bootstrap_events:
                    self._tick_bootstrap_events[symbol] = threading.Event()

                # Subscribe to tick stream (timeframe-agnostic) - only for new tickers
                self._subscribe_ticks(symbol)

            # Subscribe to bar pub/sub channels for timeframes being processed
            for tf in tfs_to_process:
                self._subscribe_bars_for_timeframe(symbol, tf)

            # Check if bars already exist in ClickHouse - if so, warmup immediately
            all_tfs_have_bars = True
            for tf in tfs_to_process:
                if not self._check_bars_exist(symbol, tf):
                    all_tfs_have_bars = False
                    logger.info(
                        f"add_ticker - {symbol}/{tf}: no bars found, will wait for bootstrap"
                    )
                    break

            if all_tfs_have_bars:
                logger.info(
                    f"add_ticker - {symbol}: all TFs have bars, running immediate warmup"
                )
                # Run warmup immediately without waiting for coordinator
                for tf in tfs_to_process:
                    tf_state = state.timeframe_states.get(tf)
                    if tf_state and tf_state.bar_indicators:
                        self._bootstrap_bars_for_timeframe(symbol, tf, tf_state)
                # For new tickers, mark as done since we've warmed up from existing bars
                if not new_timeframes_for_existing:
                    self._bootstrap_done.add(symbol)
                    evt = self._bootstrap_events.get(symbol)
                    if evt:
                        evt.set()
                    tick_evt = self._tick_bootstrap_events.get(symbol)
                    if tick_evt:
                        tick_evt.set()
                logger.info(f"add_ticker - {symbol}: immediate warmup complete")
            else:
                # Start bootstrap in background thread using BootstrapCoordinator
                # For existing tickers with new TFs, we need to wait for bars
                t = threading.Thread(
                    target=self._run_bootstrap,
                    args=(symbol, tfs_to_process),
                    daemon=True,
                    name=f"FactorEngine-Bootstrap-{symbol}",
                )
                t.start()

        logger.info(f"FactorEngine: tracking {symbol} with timeframes={timeframes}")
        return True

    def _check_bars_exist(self, symbol: str, timeframe: str) -> bool:
        """Check if bars exist in ClickHouse for this symbol/timeframe.

        Args:
            symbol: Ticker symbol
            timeframe: Timeframe string

        Returns:
            True if at least one bar exists
        """
        try:
            ch_client = get_clickhouse_client(self._ch_config)
            if not ch_client:
                return False

            # Query for any bars today
            import jerry_trader.clock as clock_mod
            from jerry_trader.platform.storage.clickhouse import query_ohlcv_bars

            end_ms = clock_mod.now_ms()
            start_ms = end_ms - (24 * 60 * 60 * 1000)  # Look back 1 day

            result = query_ohlcv_bars(ch_client, symbol, timeframe, start_ms, end_ms)
            has_bars = result is not None and len(result.get("bars", [])) > 0

            if has_bars:
                logger.debug(
                    f"_check_bars_exist - {symbol}/{timeframe}: found {len(result['bars'])} bars"
                )
            else:
                logger.debug(f"_check_bars_exist - {symbol}/{timeframe}: no bars found")

            return has_bars
        except Exception as e:
            logger.warning(
                f"_check_bars_exist - {symbol}/{timeframe}: error checking - {e}"
            )
            return False

    def remove_ticker(self, symbol: str) -> bool:
        """Stop tracking a ticker.

        Part of BootstrapableService protocol.

        Returns:
            True if successfully removed (or wasn't tracked)
        """
        with self._states_lock:
            if symbol not in self.ticker_states:
                return True
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
        return True

    # ═══════════════════════════════════════════════════════════════════════════
    # Bootstrap
    # ═══════════════════════════════════════════════════════════════════════════

    def set_coordinator(self, coordinator: BootstrapCoordinator | None) -> None:
        """Wire BootstrapCoordinator for unified bootstrap orchestration.

        When coordinator is set, FactorEngine will use the new orchestration flow:
        - Register as consumer for tick_warmup and per-timeframe bar_warmup
        - Read trades from coordinator (instead of re-fetching)
        - Report completion via coordinator.report_done()
        """
        self._coordinator = coordinator
        logger.info(f"FactorEngine: coordinator {'set' if coordinator else 'cleared'}")

    def _bootstrap_bars_for_timeframe(
        self, symbol: str, timeframe: str, tf_state: "FactorTimeframeState"
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
            bar_start_ms = bar_dict["time"] * 1000  # seconds to ms
            bar = Bar(
                symbol=symbol,
                timeframe=timeframe,
                open=bar_dict["open"],
                high=bar_dict["high"],
                low=bar_dict["low"],
                close=bar_dict["close"],
                volume=float(bar_dict.get("volume", 0)),
                trade_count=bar_dict.get("trade_count", 0),
                vwap=bar_dict.get("vwap", 0.0),
                bar_start=bar_start_ms,
                bar_end=BarPeriod(timeframe, bar_start_ms).end_ms,
                session=bar_dict.get("session", "regular"),
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
                        timestamp_ns=bar.bar_start * 1_000_000,  # ms to ns
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

    def _run_bootstrap(self, symbol: str, timeframes: list[str]) -> None:
        """Orchestrate factor bootstrap using BootstrapCoordinator.

        Uses the Coordinator for:
        - Reading trades from coordinator (tick warmup)
        - Reading bars from ClickHouse (bar warmup per timeframe)
        - Reporting completion via coordinator.report_done()

        Args:
            symbol: Ticker symbol
            timeframes: List of timeframes to bootstrap
        """
        coordinator = self._coordinator
        if not coordinator:
            logger.error(f"_run_bootstrap - {symbol}: no coordinator, cannot bootstrap")
            return

        try:
            # Wait for coordinator to start bootstrap (it may not have started yet)
            # ChartBFF and FactorEngine run in parallel, so we need to wait
            tf_bootstrap_info = None
            for _ in range(100):  # 10 seconds (100ms * 100)
                tf_bootstrap_info = coordinator.get_bootstrap(symbol)
                if tf_bootstrap_info:
                    break
                time.sleep(0.1)

            if not tf_bootstrap_info:
                logger.error(
                    f"_run_bootstrap - {symbol}: no bootstrap info in coordinator after timeout"
                )
                return

            # Use coordinator's timeframes, not FactorEngine's timeframes
            all_timeframes = list(tf_bootstrap_info.timeframes.keys())
            logger.info(
                f"_run_bootstrap - {symbol}: registering consumers for {all_timeframes}"
            )

            # Register tick warmup consumer if trades are needed
            needs_tick_warmup = (
                tf_bootstrap_info.trades_state != TradesBootstrapState.NOT_NEEDED
            )

            if needs_tick_warmup:
                coordinator.register_consumer(symbol, "tick_warmup", "factor_engine")
                logger.info(
                    f"_run_bootstrap - {symbol}: registered tick_warmup consumer"
                )

            # Register bar warmup consumers for ALL coordinator timeframes
            for tf in all_timeframes:
                coordinator.register_consumer(
                    symbol, f"bar_warmup:{tf}", "factor_engine"
                )
                logger.info(
                    f"_run_bootstrap - {symbol}/{tf}: registered bar_warmup consumer"
                )

            # 2. Wait for trades to be ready (if needed) and process tick warmup
            if needs_tick_warmup:
                logger.info(f"_run_bootstrap - {symbol}: waiting for trades")
                # Poll until trades are available or timeout
                trades = []
                for _ in range(300):  # 30 seconds (100ms * 300)
                    trades = coordinator.get_trades(symbol)
                    if trades:
                        break
                    time.sleep(0.1)

                if trades:
                    logger.info(
                        f"_run_bootstrap - {symbol}: got {len(trades)} trades, processing tick warmup"
                    )
                    self._process_bootstrap_trades(symbol, trades)
                else:
                    logger.warning(
                        f"_run_bootstrap - {symbol}: no trades available after timeout"
                    )

                # Report tick warmup done
                coordinator.report_done(symbol, "tick_warmup", "factor_engine")
                logger.info(f"_run_bootstrap - {symbol}: tick_warmup complete")

            # 3. Wait for bars ready and process bar warmup per timeframe
            state = self.ticker_states.get(symbol)
            if state:
                for tf in all_timeframes:
                    # Check if FactorEngine has indicators for this timeframe
                    tf_state = state.timeframe_states.get(tf)
                    if not tf_state or not tf_state.bar_indicators:
                        # No bar indicators for this timeframe, just report done
                        coordinator.report_done(
                            symbol, f"bar_warmup:{tf}", "factor_engine"
                        )
                        logger.info(
                            f"_run_bootstrap - {symbol}/{tf}: no bar indicators, skipped"
                        )
                        continue

                    # Wait for bars to be ready (poll bootstrap state)
                    logger.info(f"_run_bootstrap - {symbol}/{tf}: waiting for bars")
                    bars_ready = False
                    for _ in range(600):  # 60 seconds
                        tf_bootstrap = coordinator.get_bootstrap(symbol)
                        if tf_bootstrap and tf in tf_bootstrap.timeframes:
                            if tf_bootstrap.timeframes[tf].state in (
                                TimeframeState.READY,
                                TimeframeState.DONE,
                            ):
                                bars_ready = True
                                break
                        time.sleep(0.1)

                    if bars_ready:
                        logger.info(
                            f"_run_bootstrap - {symbol}/{tf}: bars ready, running bar warmup"
                        )
                        self._bootstrap_bars_for_timeframe(symbol, tf, tf_state)

                    # Report bar warmup done for this timeframe
                    coordinator.report_done(symbol, f"bar_warmup:{tf}", "factor_engine")
                    logger.info(f"_run_bootstrap - {symbol}/{tf}: bar_warmup complete")

            logger.info(f"_run_bootstrap - {symbol}: all bootstrap complete")
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
            logger.debug(f"wait_for_bootstrap - {symbol}: already in _bootstrap_done")
            return True

        # Check if bootstrap was pre-registered (events exist)
        # If not, FactorEngine hasn't started processing this ticker yet
        evt = self._bootstrap_events.get(symbol)
        tick_evt = self._tick_bootstrap_events.get(symbol)

        logger.debug(
            f"wait_for_bootstrap - {symbol}: initial check evt={evt is not None}, "
            f"tick_evt={tick_evt is not None}, _bootstrap_done={symbol in self._bootstrap_done}"
        )

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
                    logger.debug(
                        f"wait_for_bootstrap - {symbol}: events appeared after wait, "
                        f"evt={evt is not None}, tick_evt={tick_evt is not None}"
                    )
                    break
            if evt is None and tick_evt is None:
                logger.warning(
                    f"wait_for_bootstrap - {symbol}: events never created, returning False"
                )
                return False

        # Wait for bar bootstrap
        if evt is not None:
            logger.debug(
                f"wait_for_bootstrap - {symbol}: waiting for bar bootstrap (timeout={timeout}s)"
            )
            if not evt.wait(timeout=timeout):
                logger.warning(
                    f"wait_for_bootstrap - {symbol}: bar bootstrap timed out after {timeout}s, "
                    f"evt.is_set()={evt.is_set()}"
                )
                return False
            logger.debug(f"wait_for_bootstrap - {symbol}: bar bootstrap completed")

        # Wait for tick bootstrap (trade observer callback)
        if tick_evt is not None:
            logger.debug(
                f"wait_for_bootstrap - {symbol}: waiting for tick bootstrap (timeout={timeout}s)"
            )
            if not tick_evt.wait(timeout=timeout):
                logger.warning(
                    f"wait_for_bootstrap - {symbol}: tick bootstrap timed out after {timeout}s, "
                    f"tick_evt.is_set()={tick_evt.is_set()}"
                )
                return False
            logger.debug(f"wait_for_bootstrap - {symbol}: tick bootstrap completed")

        logger.debug(
            f"wait_for_bootstrap - {symbol}: all bootstrap completed successfully"
        )
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
            timeframe=timeframe,
            open=bar_dict["open"],
            high=bar_dict["high"],
            low=bar_dict["low"],
            close=bar_dict["close"],
            volume=float(bar_dict.get("volume", 0)),
            trade_count=bar_dict.get("trade_count", 0),
            vwap=bar_dict.get("vwap", 0.0),
            bar_start=bar_start_ms,
            bar_end=BarPeriod(timeframe, bar_start_ms).end_ms,
            session=bar_dict.get("session", "regular"),
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
            self._write_factors(symbol, bar.bar_start * 1_000_000, factors, timeframe)

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

        # Register with coordinator if available (new design: direct calls instead of Redis Stream)
        if self._coordinator:
            self._coordinator.register_service("factor_engine", self)

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
