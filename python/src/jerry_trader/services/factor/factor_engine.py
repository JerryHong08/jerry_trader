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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import redis
from redis.exceptions import ResponseError

import jerry_trader.clock as clock_mod
from jerry_trader._rust import PyBar, PyFactorEngine, PyTrade
from jerry_trader.domain.factor import FactorSnapshot
from jerry_trader.domain.market import Bar, BarPeriod
from jerry_trader.platform.storage.clickhouse import (
    get_clickhouse_client,
    query_ohlcv_bars,
)
from jerry_trader.services.bar_builder.chart_data_service import REPLAY_LOOKBACK
from jerry_trader.services.factor.factor_registry import FactorSpec, get_factor_registry
from jerry_trader.services.factor.factor_storage import FactorStorage
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
from jerry_trader.shared.time.timezone import ms_to_readable

logger = setup_logger("factor_engine", log_to_file=True, level=logging.INFO)

# Constants
COMPUTE_INTERVAL_SEC = 1.0  # Tick indicator compute interval


@dataclass
class FactorTimeframeState:
    """Per-timeframe bar factor state for a ticker.

    Bar factor instances live in the ticker's PyFactorEngine with
    key "{symbol}:{timeframe}". This dataclass tracks metadata only.
    """

    timeframe: str
    bar_factor_names: list[str] = field(
        default_factory=list
    )  # spec.id → ClickHouse/Redis
    bar_factor_rust_keys: dict[str, str] = field(
        default_factory=dict
    )  # spec.id → rust_key
    last_bar_start_ms: int = 0  # Track last processed bar to avoid duplicates

    def _rust_key(self, name: str) -> str:
        """Get the Rust registry key for a factor display name."""
        return self.bar_factor_rust_keys.get(name, name)

    def reset(self, rust_engine, symbol: str) -> None:
        """Reset all bar factors for this timeframe."""
        ticker_key = f"{symbol}:{self.timeframe}"
        for name in self.bar_factor_names:
            try:
                rust_engine.reset_factor(ticker_key, self._rust_key(name))
            except Exception:
                pass
        self.last_bar_start_ms = 0


@dataclass
class TickerState:
    """Per-ticker factor state across all timeframes.

    All factor computation is delegated to PyFactorEngine (Rust).
    Python only manages lifecycle, Redis pub/sub, and ClickHouse I/O.
    """

    symbol: str
    rust_engine: Any = None  # PyFactorEngine, set in __post_init__ or add_ticker
    timeframe_states: dict[str, FactorTimeframeState] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_bootstrap_trade_ms: int = 0
    last_bootstrap_quote_ms: int = 0
    last_tick_ts_ms: int = 0
    last_publish_ms: int = 0
    # Cached factor names for iteration (set in add_ticker)
    trade_factor_names: list[str] = field(default_factory=list)
    quote_factor_names: list[str] = field(default_factory=list)

    def reset(self) -> None:
        """Reset all Rust factor instances."""
        for tf_state in self.timeframe_states.values():
            tf_state.reset(self.rust_engine, self.symbol)
        for name in self.trade_factor_names:
            try:
                self.rust_engine.reset_factor(self.symbol, name)
            except Exception:
                pass
        for name in self.quote_factor_names:
            try:
                self.rust_engine.reset_factor(self.symbol, name)
            except Exception:
                pass
        self.last_publish_ms = 0


class FactorEngine:
    """Hybrid factor computation engine.

    Architecture:
        UnifiedTickManager → ticks → TradeIndicators (trade_rate)
        BarsBuilder → Redis pub/sub → BarIndicators (EMA)
        FactorEngine → ClickHouse (via FactorStorage)
    """

    _ms_to_readable = staticmethod(ms_to_readable)

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
        self._write_executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="FactorEngine-Write"
        )

        # Bootstrap state
        # Use composite key "symbol:timeframe" for per-timeframe bootstrap tracking
        self._bootstrap_events: dict[str, threading.Event] = {}
        self._bootstrap_done: set[str] = set()  # Stores "symbol:timeframe" keys
        self._bars_builder = None  # set by backend_starter via set_bars_builder()
        # Track tick bootstrap completion per ticker (EventBus TradesBackfillCompleted)
        # Trade-based factors don't have timeframe, use "symbol:tick" as key
        self._trade_bootstrap_events: dict[str, threading.Event] = {}
        # Track quote bootstrap completion per ticker, keyed "symbol:quote"
        self._quote_bootstrap_events: dict[str, threading.Event] = {}
        # Track timeframes that failed bootstrap (no bars found) so we can retry
        # when bar backfill completes. Structure: {symbol: {timeframe1, timeframe2, ...}}
        self._failed_bootstrap_timeframes: dict[str, set[str]] = {}

        # Tick buffer: holds WS ticks that arrive DURING bootstrap so they
        # aren't lost.  Replayed after trade warmup completes.
        self._trade_buffer: dict[str, list[tuple[int, float, int]]] = {}
        self._trade_buffer_lock = threading.Lock()

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

        # Skip processing until bootstrap is complete for this timeframe
        bar_key = f"{symbol}:{timeframe}"
        if bar_key not in self._bootstrap_done:
            logger.debug(
                f"_on_event_bar_closed: skipping {symbol}/{timeframe} - bootstrap not done "
                f"(done keys: {list(self._bootstrap_done)})"
            )
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

        # Update bar factors via Rust engine
        ticker_key = f"{symbol}:{timeframe}"
        py_bar = PyBar(
            bar_start_ms,
            event.data.get("open", 0.0),
            event.data.get("high", 0.0),
            event.data.get("low", 0.0),
            event.data.get("close", 0.0),
            int(event.data.get("volume", 0)),
        )
        factors: dict[str, float] = {}
        for name in tf_state.bar_factor_names:
            value = state.rust_engine.update_bar(
                ticker_key, tf_state._rust_key(name), py_bar
            )
            if value is not None:
                factors[name] = value

        # Write bar-based factors to ClickHouse
        if factors:
            self._write_factors(symbol, bar_start_ms * 1_000_000, factors, timeframe)

    def _process_bootstrap_trades(
        self, symbol: str, trades: list[tuple[int, float, int]]
    ) -> None:
        """Process historical trades for tick indicator warmup.

        Uses Rust batch processing (bootstrap_trade_rate) for all tick
        indicators — processes 200k+ trades in ~50ms instead of ~13s
        via Python loop.  Follows the same pattern as BarsBuilder's
        ingest_trades_batch: single Rust call, fast results.

        For each tick indicator with a window, this:
        1. Extracts timestamps from trades
        2. Calls Rust bootstrap function (single batch call)
        3. Gets back per-second snapshots + remaining window state
        4. Creates FactorSnapshot objects and batch-writes to ClickHouse
        5. Loads remaining timestamps into the indicator for seamless live merge

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
            trade_key = f"{symbol}:trade"
            trade_evt = self._trade_bootstrap_events.get(trade_key)
            if trade_evt:
                trade_evt.set()
            return

        if not state.trade_factor_names:
            # No trade factors — just set watermark and return
            if trades:
                state.last_bootstrap_trade_ms = int(trades[-1][0])
            trade_key = f"{symbol}:trade"
            trade_evt = self._trade_bootstrap_events.get(trade_key)
            if trade_evt:
                trade_evt.set()
            return

        t0 = time.perf_counter()
        logger.info(
            f"_process_bootstrap_trades - {symbol}: processing {len(trades)} trades "
            f"via Rust batch warmup"
        )

        max_ts_ms = max(int(t[0]) for t in trades) if trades else 0

        # Build compute grid: every 1s from first aligned boundary to last trade
        first_ts = min(int(t[0]) for t in trades)
        grid_start = first_ts - (first_ts % 1000) + 1000
        compute_ts = list(range(grid_start, max_ts_ms + 1000, 1000))

        all_snapshots: list[tuple[str, int, float]] = []

        # Use a throwaway engine for batch computation (stateless)
        batch_engine = PyFactorEngine()
        for name in state.trade_factor_names:
            try:
                spec = get_factor_registry().get_spec(name)
                rust_params = {
                    k: float(v) for k, v in (spec.params if spec else {}).items()
                }
                rust_params.setdefault("window_ms", 20000.0)
                rust_params.setdefault("min_trades", 5.0)

                py_trades = [PyTrade(int(t[0]), float(t[1]), int(t[2])) for t in trades]
                values = batch_engine.compute_batch_trade(
                    name,
                    py_trades,
                    compute_ts,
                    rust_params,
                )
                for ts_ms, value in zip(compute_ts, values):
                    if value is not None:
                        all_snapshots.append((name, ts_ms, value))

                logger.info(
                    f"_process_bootstrap_trades - {symbol}/{name}: "
                    f"batch produced {len(values)} values, {sum(1 for v in values if v is not None)} valid"
                )
            except Exception as e:
                logger.error(
                    f"_process_bootstrap_trades - {symbol}/{name}: "
                    f"batch warmup failed: {e}"
                )

        # Prime the per-ticker Rust engine with last window's trades
        # so real-time on_trade() picks up from the correct state
        if trades:
            window_ms = 20000  # Default trade rate window
            if state.trade_factor_names:
                spec = get_factor_registry().get_spec(state.trade_factor_names[0])
                if spec and "window_ms" in spec.params:
                    window_ms = int(spec.params["window_ms"])
            cutoff = max_ts_ms - window_ms
            recent = [t for t in trades if int(t[0]) > cutoff]
            for t_ts, t_price, t_size in recent:
                for name in state.trade_factor_names:
                    try:
                        state.rust_engine.on_trade(
                            symbol, name, int(t_ts), float(t_price), int(t_size)
                        )
                    except Exception:
                        pass

        # Track the last trade timestamp for dedup
        state.last_bootstrap_trade_ms = max_ts_ms

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info(
            f"_process_bootstrap_trades - {symbol}: Rust batch warmup done "
            f"in {elapsed:.0f}ms, {len(all_snapshots)} total snapshots, "
            f"last_trade_ts={max_ts_ms}"
        )

        # Signal that tick bootstrap is complete
        trade_key = f"{symbol}:trade"
        trade_evt = self._trade_bootstrap_events.get(trade_key)
        if trade_evt:
            trade_evt.set()

        # Batch write tick-based factor snapshots to ClickHouse
        if all_snapshots and self.storage:
            # Group by timestamp to create FactorSnapshots with multiple factors
            from collections import defaultdict

            ts_factors: dict[int, dict[str, float]] = defaultdict(dict)
            for ind_name, ts_ms, value in all_snapshots:
                ts_factors[ts_ms][ind_name] = value

            snapshots = [
                FactorSnapshot(
                    symbol=symbol,
                    timestamp_ns=ts_ms * 1_000_000,
                    factors=factors,
                )
                for ts_ms, factors in sorted(ts_factors.items())
            ]

            snapshots_with_tf = [(s, "tick") for s in snapshots]
            count = self.storage.write_batch(snapshots_with_tf)
            logger.info(
                f"_process_bootstrap_trades - {symbol}: wrote {count} trade factor rows "
                f"from {len(snapshots)} snapshots"
            )

    def _process_bootstrap_quotes(
        self, symbol: str, quotes: list[tuple[int, float, float, int, int]]
    ) -> None:
        """Process historical quotes for quote indicator warmup.

        Uses Rust batch processing via compute_batch_quote for all quote
        indicators — delegates to the same true-batch path as backtest.

        For each quote indicator with a window, this:
        1. Calls Rust PyFactorEngine.compute_batch_quote (single batch call)
        2. Gets back per-second (ts_ms, value) pairs
        3. Primes the Python indicator state from recent quotes for live merge
        4. Creates FactorSnapshot objects and batch-writes to ClickHouse

        Args:
            symbol: Ticker symbol
            quotes: List of (ts_ms, bid, ask, bid_size, ask_size) tuples
        """
        state = None
        for _ in range(20):
            state = self.ticker_states.get(symbol)
            if state:
                break
            time.sleep(0.1)

        if not state:
            logger.warning(
                f"_process_bootstrap_quotes - {symbol}: TickerState not found after 2s, skipping"
            )
            quote_key = f"{symbol}:quote"
            quote_evt = self._quote_bootstrap_events.get(quote_key)
            if quote_evt:
                quote_evt.set()
            return

        if not state.quote_factor_names:
            quote_key = f"{symbol}:quote"
            quote_evt = self._quote_bootstrap_events.get(quote_key)
            if quote_evt:
                quote_evt.set()
            return

        t0 = time.perf_counter()
        logger.info(
            f"_process_bootstrap_quotes - {symbol}: processing {len(quotes)} quotes "
            f"via Rust batch warmup"
        )

        batch_engine = PyFactorEngine()
        all_snapshots: list[tuple[str, int, float]] = []
        max_ts_ms = max(q[0] for q in quotes) if quotes else 0

        for name in state.quote_factor_names:
            try:
                spec = get_factor_registry().get_spec(name)
                rust_params = {
                    k: float(v) for k, v in (spec.params if spec else {}).items()
                }
                rust_params.setdefault("window_ms", 20000.0)
                rust_params.setdefault("min_quotes", 5.0)

                result = batch_engine.compute_batch_quote(name, quotes, rust_params)
                for ts_ms, value in result:
                    if value is not None:
                        all_snapshots.append((name, ts_ms, value))

                logger.info(
                    f"_process_bootstrap_quotes - {symbol}/{name}: "
                    f"Rust batch produced {len(result)} (ts_ms, value) pairs"
                )
            except Exception as e:
                logger.error(
                    f"_process_bootstrap_quotes - {symbol}/{name}: "
                    f"batch warmup failed: {e}"
                )

        # Prime the per-ticker Rust engine with last window's quotes
        # so real-time on_quote() picks up from the correct state
        if quotes and state.quote_factor_names:
            window_ms = 20000
            spec = get_factor_registry().get_spec(state.quote_factor_names[0])
            if spec and "window_ms" in spec.params:
                window_ms = int(spec.params["window_ms"])
            cutoff = max_ts_ms - window_ms
            recent = [q for q in quotes if q[0] > cutoff]
            for q_ts, bid, ask, bid_sz, ask_sz in recent:
                for name in state.quote_factor_names:
                    try:
                        state.rust_engine.on_quote(
                            symbol,
                            name,
                            int(q_ts),
                            float(bid),
                            float(ask),
                            int(bid_sz),
                            int(ask_sz),
                        )
                    except Exception:
                        pass

        state.last_bootstrap_quote_ms = max_ts_ms

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info(
            f"_process_bootstrap_quotes - {symbol}: Rust batch warmup done "
            f"in {elapsed:.0f}ms, {len(all_snapshots)} total snapshots, "
            f"last_quote_ts={max_ts_ms}"
        )

        # Signal that quote bootstrap is complete
        quote_key = f"{symbol}:quote"
        quote_evt = self._quote_bootstrap_events.get(quote_key)
        if quote_evt:
            quote_evt.set()

        # Batch write quote-based factor snapshots to ClickHouse
        if all_snapshots and self.storage:
            from collections import defaultdict

            ts_factors: dict[int, dict[str, float]] = defaultdict(dict)
            for ind_name, ts_ms, value in all_snapshots:
                ts_factors[ts_ms][ind_name] = value

            snapshots = [
                FactorSnapshot(
                    symbol=symbol,
                    timestamp_ns=ts_ms * 1_000_000,
                    factors=factors,
                )
                for ts_ms, factors in sorted(ts_factors.items())
            ]

            snapshots_with_tf = [(s, "tick") for s in snapshots]
            count = self.storage.write_batch(snapshots_with_tf)
            logger.info(
                f"_process_bootstrap_quotes - {symbol}: wrote {count} quote factor rows "
                f"from {len(snapshots)} snapshots"
            )

    def _trade_warmup_background(
        self, symbol: str, coordinator: BootstrapCoordinator
    ) -> None:
        """Wait for trades from coordinator and run trade warmup.

        Used when bars already exist (immediate bar warmup path) but tick
        indicators still need trades from BarsBuilder's trades_backfill.
        Runs in a background thread to avoid blocking the coordinator.
        """
        trades = []
        for _ in range(300):  # 30 s timeout (same as _run_bootstrap)
            trades = coordinator.get_trades(symbol)
            if trades:
                break
            time.sleep(0.1)

        if trades:
            logger.info(
                f"_trade_warmup_background - {symbol}: got {len(trades)} trades"
            )
            self._process_bootstrap_trades(symbol, trades)
        else:
            logger.warning(
                f"_trade_warmup_background - {symbol}: no trades after 30s timeout"
            )

        coordinator.report_done(symbol, "trade_warmup", "factor_engine")

        # Mark tick bootstrap done and replay buffered ticks
        trade_key = f"{symbol}:trade"
        self._bootstrap_done.add(trade_key)
        trade_evt = self._trade_bootstrap_events.get(trade_key)
        if trade_evt:
            trade_evt.set()
        self._replay_trade_buffer(symbol)

        logger.info(f"_trade_warmup_background - {symbol}: complete")

    def _quote_warmup_background(
        self, symbol: str, coordinator: BootstrapCoordinator
    ) -> None:
        """Wait for quotes from coordinator and run quote warmup.

        Used when bars already exist (immediate bar warmup path) but quote
        indicators still need quotes from BarsBuilder's quotes_backfill.
        Runs in a background thread to avoid blocking the coordinator.
        """
        quotes = []
        for _ in range(300):  # 30 s timeout
            quotes = coordinator.get_quotes(symbol)
            if quotes:
                break
            time.sleep(0.1)

        if quotes:
            logger.info(
                f"_quote_warmup_background - {symbol}: got {len(quotes)} quotes"
            )
            self._process_bootstrap_quotes(symbol, quotes)
        else:
            logger.warning(
                f"_quote_warmup_background - {symbol}: no quotes after 30s timeout"
            )

        coordinator.report_done(symbol, "quote_warmup", "factor_engine")

        # Mark quote bootstrap done
        quote_key = f"{symbol}:quote"
        self._bootstrap_done.add(quote_key)
        quote_evt = self._quote_bootstrap_events.get(quote_key)
        if quote_evt:
            quote_evt.set()

        logger.info(f"_quote_warmup_background - {symbol}: complete")

    def _replay_trade_buffer(self, symbol: str) -> None:
        """Replay WS ticks that were buffered during bootstrap.

        Called after trade warmup completes.  Drains the per-symbol buffer
        and feeds ticks to indicators, bridging the gap between the last
        bootstrap trade and the first live WS tick.

        Computes factors directly and batch-writes to ClickHouse (synchronous)
        rather than going through the live _on_trade path which publishes to
        Redis — no consumer is listening yet during replay, so Redis publishes
        would be lost.
        """
        with self._trade_buffer_lock:
            buffered = self._trade_buffer.pop(symbol, [])

        if not buffered:
            return

        state = self.ticker_states.get(symbol)
        if not state:
            return

        cutoff = state.last_bootstrap_trade_ms
        to_replay = [(ts, p, s) for ts, p, s in buffered if ts > cutoff]

        if not to_replay:
            logger.info(
                f"_replay_trade_buffer - {symbol}: no ticks to replay "
                f"(buffered={len(buffered)}, all <= cutoff={cutoff})"
            )
            return

        logger.info(
            f"_replay_trade_buffer - {symbol}: replaying {len(to_replay)} ticks "
            f"(buffered={len(buffered)}, cutoff={cutoff})"
        )

        # Feed ticks to Rust engine and collect factor values every 1s
        snapshots: list[FactorSnapshot] = []
        last_compute_ms = 0
        max_ts_ms = 0

        for ts_ms, price, size in to_replay:
            ts_ms = int(ts_ms)
            max_ts_ms = max(max_ts_ms, ts_ms)
            price_f = float(price)
            size_i = int(size)
            for name in state.trade_factor_names:
                state.rust_engine.on_trade(symbol, name, ts_ms, price_f, size_i)

            if ts_ms - last_compute_ms >= 1000:
                factors: dict[str, float] = {}
                for name in state.trade_factor_names:
                    value = state.rust_engine.get_value(symbol, name, "trade")
                    if value is not None:
                        factors[name] = value
                if factors:
                    snapshots.append(
                        FactorSnapshot(
                            symbol=symbol,
                            timestamp_ns=ts_ms * 1_000_000,
                            factors=factors,
                        )
                    )
                last_compute_ms = ts_ms

        # Synchronous batch write so REST query will find these rows
        if snapshots and self.storage:
            snapshots_with_tf = [(s, "tick") for s in snapshots]
            count = self.storage.write_batch(snapshots_with_tf)
            logger.info(
                f"_replay_trade_buffer - {symbol}: wrote {count} replay factor rows "
                f"from {len(snapshots)} snapshots"
            )

        # Update dedup watermark so live _on_trade skips these replayed ticks
        state.last_bootstrap_trade_ms = max_ts_ms
        state.last_publish_ms = max_ts_ms  # prevent throttle misfire

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
        Returns True if tick bootstrap is done (minimum for real-time processing).
        """
        trade_key = f"{symbol}:trade"
        return trade_key in self._bootstrap_done

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
                registry = get_factor_registry()
                for tf in timeframes:
                    if tf not in state.timeframe_states:
                        # Create Rust bar factors for this timeframe
                        bar_specs = registry.get_factors_by_type("bar")
                        bar_names, bar_rust_keys = self._create_bar_factors_in_engine(
                            state.rust_engine, symbol, tf, bar_specs
                        )
                        state.timeframe_states[tf] = FactorTimeframeState(
                            timeframe=tf,
                            bar_factor_names=bar_names,
                            bar_factor_rust_keys=bar_rust_keys,
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
                trade_key = f"{symbol}:trade"
                if trade_key not in self._trade_bootstrap_events:
                    self._trade_bootstrap_events[trade_key] = threading.Event()
                    self._trade_bootstrap_events[trade_key].set()  # Already done
                quote_key = f"{symbol}:quote"
                if quote_key not in self._quote_bootstrap_events:
                    self._quote_bootstrap_events[quote_key] = threading.Event()
                    self._quote_bootstrap_events[quote_key].set()  # Already done
            else:
                # Create timeframe states for each timeframe (new ticker case)
                registry = get_factor_registry()
                rust_engine = PyFactorEngine()
                timeframe_states: dict[str, FactorTimeframeState] = {}
                for tf in timeframes:
                    bar_specs = registry.get_factors_by_type("bar")
                    bar_names, bar_rust_keys = self._create_bar_factors_in_engine(
                        rust_engine, symbol, tf, bar_specs
                    )
                    timeframe_states[tf] = FactorTimeframeState(
                        timeframe=tf,
                        bar_factor_names=bar_names,
                        bar_factor_rust_keys=bar_rust_keys,
                    )

                # Create Rust trade factors
                trade_specs = registry.get_factors_by_type("trade")
                if not trade_specs:
                    trade_specs = [
                        FactorSpec(
                            id="trade_rate",
                            class_name="TradeRate",
                            type="trade",
                            params={"window_ms": 20000, "min_trades": 5},
                        )
                    ]
                trade_factor_names = []
                for spec in trade_specs:
                    try:
                        rust_params = {k: float(v) for k, v in spec.params.items()}
                        rust_engine.create_trade_factor(
                            symbol, spec.rust_key, rust_params
                        )
                        trade_factor_names.append(spec.id)
                    except Exception as e:
                        logger.warning(
                            f"Failed to create trade factor {spec.id} for {symbol}: {e}"
                        )

                # Create Rust quote factors
                quote_specs = registry.get_factors_by_type("quote")
                quote_factor_names = []
                for spec in quote_specs:
                    try:
                        rust_params = {k: float(v) for k, v in spec.params.items()}
                        rust_engine.create_quote_factor(
                            symbol, spec.rust_key, rust_params
                        )
                        quote_factor_names.append(spec.id)
                    except Exception as e:
                        logger.warning(
                            f"Failed to create quote factor {spec.id} for {symbol}: {e}"
                        )

                state = TickerState(
                    symbol=symbol,
                    rust_engine=rust_engine,
                    timeframe_states=timeframe_states,
                    trade_factor_names=trade_factor_names,
                    quote_factor_names=quote_factor_names,
                )
                self.ticker_states[symbol] = state

                tfs_to_process = timeframes

                # Create bootstrap Events (new ticker case)
                if symbol not in self._bootstrap_events:
                    self._bootstrap_events[symbol] = threading.Event()
                trade_key = f"{symbol}:trade"
                if trade_key not in self._trade_bootstrap_events:
                    self._trade_bootstrap_events[trade_key] = threading.Event()
                quote_key = f"{symbol}:quote"
                if quote_key not in self._quote_bootstrap_events:
                    self._quote_bootstrap_events[quote_key] = threading.Event()

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
                # Run bar warmup immediately without waiting for coordinator
                for tf in tfs_to_process:
                    tf_state = state.timeframe_states.get(tf)
                    if tf_state and tf_state.bar_factor_names:
                        self._bootstrap_bars_for_timeframe(symbol, tf, tf_state)
                        # Mark this timeframe's bootstrap as done
                        bar_key = f"{symbol}:{tf}"
                        self._bootstrap_done.add(bar_key)
                        bar_evt = self._bootstrap_events.get(bar_key)
                        if bar_evt:
                            bar_evt.set()

                # Tick warmup: wait for trades from coordinator (BarsBuilder
                # runs trades_backfill in a parallel thread).
                # Bar warmup is done (bars exist in ClickHouse), but tick
                # indicators need trades from BarsBuilder's REST fetch.
                # Spawn background thread to avoid blocking coordinator.
                if self._coordinator is not None and state.trade_factor_names:
                    coordinator = self._coordinator
                    coordinator.register_consumer(
                        symbol, "trade_warmup", "factor_engine"
                    )
                    for tf in tfs_to_process:
                        coordinator.register_consumer(
                            symbol, f"bar_warmup:{tf}", "factor_engine"
                        )
                        coordinator.report_done(
                            symbol, f"bar_warmup:{tf}", "factor_engine"
                        )

                    t = threading.Thread(
                        target=self._trade_warmup_background,
                        args=(symbol, coordinator),
                        daemon=True,
                        name=f"FactorEngine-TickWarmup-{symbol}",
                    )
                    t.start()
                else:
                    # No trade indicators — mark tick bootstrap done immediately
                    trade_key = f"{symbol}:trade"
                    self._bootstrap_done.add(trade_key)
                    trade_evt = self._trade_bootstrap_events.get(trade_key)
                    if trade_evt:
                        trade_evt.set()
                    self._replay_trade_buffer(symbol)

                # Quote warmup: spawn alongside trade warmup if quote indicators exist
                if self._coordinator is not None and state.quote_factor_names:
                    coordinator = self._coordinator
                    coordinator.register_consumer(
                        symbol, "quote_warmup", "factor_engine"
                    )
                    t = threading.Thread(
                        target=self._quote_warmup_background,
                        args=(symbol, coordinator),
                        daemon=True,
                        name=f"FactorEngine-QuoteWarmup-{symbol}",
                    )
                    t.start()
                else:
                    # No quote indicators — mark quote bootstrap done immediately
                    quote_key = f"{symbol}:quote"
                    self._bootstrap_done.add(quote_key)
                    quote_evt = self._quote_bootstrap_events.get(quote_key)
                    if quote_evt:
                        quote_evt.set()

                logger.info(
                    f"add_ticker - {symbol}: immediate bar warmup complete, "
                    f"trade/quote warmup running in background"
                )
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

        # Clear bootstrap state so re-subscribe triggers fresh warmup
        for tf in timeframes:
            bar_key = f"{symbol}:{tf}"
            self._bootstrap_done.discard(bar_key)
            bar_evt = self._bootstrap_events.pop(bar_key, None)
            if bar_evt:
                bar_evt.clear()
        trade_key = f"{symbol}:trade"
        self._bootstrap_done.discard(trade_key)
        trade_evt = self._trade_bootstrap_events.pop(trade_key, None)
        if trade_evt:
            trade_evt.clear()

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

    def _create_bar_factors_in_engine(
        self,
        engine,
        symbol: str,
        timeframe: str,
        bar_specs: list,
    ) -> tuple[list[str], dict[str, str]]:
        """Create Rust bar factor instances for a ticker:timeframe pair.

        Returns (display_names, rust_key_map) where:
          - display_names: spec.id values for ClickHouse/Redis/API (e.g., "ema_20")
          - rust_key_map: mapping from spec.id → Rust registry key (e.g., {"ema_20": "ema"})
        """
        names: list[str] = []
        rust_keys: dict[str, str] = {}
        ticker_key = f"{symbol}:{timeframe}"
        if not bar_specs:
            # Fallback: EMA(20)
            try:
                engine.create_bar_factor(ticker_key, "ema", {"period": 20.0})
                names.append("ema_20")
                rust_keys["ema_20"] = "ema"
            except Exception as e:
                logger.warning(f"Fallback bar factor ema failed for {ticker_key}: {e}")
            return names, rust_keys

        for spec in bar_specs:
            if not isinstance(spec, FactorSpec):
                continue
            # spec.id has parameter suffixes (e.g., "ema_20") while spec.rust_key
            # is the canonical Rust registry key (e.g., "ema"). Both come from
            # FactorSpec which derives rust_key from factor_class._factor_name.
            try:
                rust_params = {k: float(v) for k, v in spec.params.items()}
                engine.create_bar_factor(ticker_key, spec.rust_key, rust_params)
                names.append(spec.id)
                rust_keys[spec.id] = spec.rust_key
            except Exception as e:
                logger.warning(
                    f"Failed to create bar factor {spec.id} (rust={spec.rust_key}) "
                    f"for {ticker_key}: {e}"
                )
        return names, rust_keys

    @staticmethod
    def _bar_dict_to_bar(symbol: str, timeframe: str, bar_dict: dict) -> Bar:
        """Convert a bar dict (from ClickHouse or pub/sub) to a Bar domain model."""
        bar_start_ms = bar_dict.get("bar_start", bar_dict.get("time", 0) * 1000)
        return Bar(
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

        state = self.ticker_states.get(symbol)
        if not state:
            return

        ticker_key = f"{symbol}:{timeframe}"
        snapshots: list[FactorSnapshot] = []
        for bar_dict in bars:
            # query_ohlcv_bars returns bars with "time" (epoch seconds)
            bar_start_ms = bar_dict["time"] * 1000  # seconds to ms
            bar = self._bar_dict_to_bar(symbol, timeframe, bar_dict)

            factors: dict[str, float] = {}
            py_bar = PyBar(
                bar.bar_start, bar.open, bar.high, bar.low, bar.close, int(bar.volume)
            )
            for name in tf_state.bar_factor_names:
                value = state.rust_engine.update_bar(
                    ticker_key, tf_state._rust_key(name), py_bar
                )
                if value is not None:
                    factors[name] = value

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
        - Reading trades from coordinator (trade warmup)
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

            # Register trade warmup consumer if trades are needed
            needs_trade_warmup = (
                tf_bootstrap_info.trades_state != TradesBootstrapState.NOT_NEEDED
            )

            if needs_trade_warmup:
                coordinator.register_consumer(symbol, "trade_warmup", "factor_engine")
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

            # 2. Wait for trades to be ready (if needed) and process trade warmup
            if needs_trade_warmup:
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
                        f"_run_bootstrap - {symbol}: got {len(trades)} trades, processing trade warmup"
                    )
                    self._process_bootstrap_trades(symbol, trades)
                else:
                    logger.warning(
                        f"_run_bootstrap - {symbol}: no trades available after timeout"
                    )

                # Report trade warmup done
                coordinator.report_done(symbol, "trade_warmup", "factor_engine")
                logger.info(f"_run_bootstrap - {symbol}: tick_warmup complete")

                # Mark tick bootstrap as done
                self._bootstrap_done.add(f"{symbol}:trade")
                trade_key = f"{symbol}:trade"
                trade_evt = self._trade_bootstrap_events.get(trade_key)
                if trade_evt:
                    trade_evt.set()

                # Replay WS ticks buffered during bootstrap
                self._replay_trade_buffer(symbol)

            # 3. Wait for quotes to be ready and process quote warmup
            state = self.ticker_states.get(symbol)
            if state and state.quote_factor_names:
                logger.info(f"_run_bootstrap - {symbol}: waiting for quotes")
                coordinator.register_consumer(symbol, "quote_warmup", "factor_engine")

                quotes = []
                for _ in range(300):  # 30 seconds
                    quotes = coordinator.get_quotes(symbol)
                    if quotes:
                        break
                    time.sleep(0.1)

                if quotes:
                    logger.info(
                        f"_run_bootstrap - {symbol}: got {len(quotes)} quotes, processing quote warmup"
                    )
                    self._process_bootstrap_quotes(symbol, quotes)
                else:
                    logger.warning(
                        f"_run_bootstrap - {symbol}: no quotes available after timeout"
                    )

                coordinator.report_done(symbol, "quote_warmup", "factor_engine")
                self._bootstrap_done.add(f"{symbol}:quote")
                quote_key_evt = self._quote_bootstrap_events.get(f"{symbol}:quote")
                if quote_key_evt:
                    quote_key_evt.set()

            state = self.ticker_states.get(symbol)
            if state:
                for tf in all_timeframes:
                    # Check if FactorEngine has indicators for this timeframe
                    tf_state = state.timeframe_states.get(tf)
                    if not tf_state or not tf_state.bar_factor_names:
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
                    wait_iterations = 0
                    for _ in range(600):  # 60 seconds
                        wait_iterations += 1
                        tf_bootstrap = coordinator.get_bootstrap(symbol)
                        if tf_bootstrap and tf in tf_bootstrap.timeframes:
                            tf_bs_state = tf_bootstrap.timeframes[tf].state
                            logger.debug(
                                f"_run_bootstrap - {symbol}/{tf}: tf_bs_state={tf_bs_state}"
                            )
                            if tf_bs_state in (
                                TimeframeState.READY,
                                TimeframeState.DONE,
                            ):
                                bars_ready = True
                                break
                        time.sleep(0.1)

                    logger.info(
                        f"_run_bootstrap - {symbol}/{tf}: bars_ready={bars_ready} "
                        f"after {wait_iterations * 100}ms"
                    )

                    if bars_ready:
                        logger.info(
                            f"_run_bootstrap - {symbol}/{tf}: bars ready, running bar warmup"
                        )
                        self._bootstrap_bars_for_timeframe(symbol, tf, tf_state)

                    # Report bar warmup done for this timeframe
                    coordinator.report_done(symbol, f"bar_warmup:{tf}", "factor_engine")
                    logger.info(f"_run_bootstrap - {symbol}/{tf}: bar_warmup complete")

                    # Mark this timeframe's bootstrap as done
                    bar_key = f"{symbol}:{tf}"
                    self._bootstrap_done.add(bar_key)
                    bar_evt = self._bootstrap_events.get(bar_key)
                    if bar_evt:
                        bar_evt.set()

            logger.info(f"_run_bootstrap - {symbol}: all bootstrap complete")
        except Exception as e:
            logger.error(f"_run_bootstrap - {symbol}: error - {e}")

    def pre_register_bootstrap(self, symbol: str, timeframe: str | None = None) -> None:
        """Pre-create bootstrap Events for this ticker and timeframe.

        Called by ChartBFF BEFORE the WebSocket subscription triggers FactorEngine
        to start processing. This ensures any concurrent REST request will block on
        wait_for_bootstrap.

        Args:
            symbol: Ticker symbol
            timeframe: Timeframe for bar-based factors (e.g., '1m', '5m').
                       'trade' or None for tick-based factors only.
        """
        # Always pre-register tick bootstrap (key format: "symbol:tick")
        trade_key = f"{symbol}:trade"
        if trade_key not in self._trade_bootstrap_events:
            self._trade_bootstrap_events[trade_key] = threading.Event()
            logger.debug(
                f"pre_register_bootstrap - {symbol}: tick bootstrap event pre-created"
            )

        # Pre-register bar bootstrap for specific timeframe (not for 'trade')
        if timeframe and timeframe != "tick":
            bar_key = f"{symbol}:{timeframe}"
            if bar_key not in self._bootstrap_events:
                self._bootstrap_events[bar_key] = threading.Event()
                logger.debug(
                    f"pre_register_bootstrap - {symbol}/{timeframe}: bar bootstrap event pre-created"
                )

    def wait_for_bootstrap(
        self, symbol: str, timeframe: str | None = None, timeout: float = 10.0
    ) -> bool:
        """Wait until factor bootstrap completes for this ticker and timeframe.

        Args:
            symbol: Ticker symbol
            timeframe: Timeframe for bar-based factors (e.g., '1m', '5m').
                       'trade' or None waits for tick bootstrap only.
            timeout: Maximum wait time in seconds

        Returns True if bootstrap completed, False if timed out or bootstrap
        was not pre-registered (indicating FactorEngine hasn't started processing).
        """
        # If ticker is not tracked, no bootstrap will happen - return immediately
        with self._states_lock:
            if symbol not in self.ticker_states:
                logger.warning(
                    f"wait_for_bootstrap - {symbol}: ticker not tracked, "
                    "no bootstrap will happen"
                )
                return False

        # For tick-based factors (timeframe='trade' or None), only check tick bootstrap
        if timeframe == "tick" or timeframe is None:
            trade_key = f"{symbol}:trade"
            if trade_key in self._bootstrap_done:
                logger.debug(
                    f"wait_for_bootstrap - {symbol}: tick bootstrap already done"
                )
                return True

            trade_evt = self._trade_bootstrap_events.get(trade_key)
            if trade_evt is None:
                # Bootstrap not pre-registered yet, wait briefly for it to appear
                logger.debug(
                    f"wait_for_bootstrap - {symbol}: tick bootstrap not pre-registered, "
                    "waiting for registration"
                )
                for _ in range(int(timeout * 10)):
                    trade_evt = self._trade_bootstrap_events.get(trade_key)
                    if trade_evt is not None:
                        break
                    time.sleep(0.1)

                if trade_evt is None:
                    logger.warning(
                        f"wait_for_bootstrap - {symbol}: tick bootstrap never registered"
                    )
                    return False

            if not trade_evt.wait(timeout=timeout):
                logger.warning(f"wait_for_bootstrap - {symbol}: tick bootstrap timeout")
                return False

            logger.debug(f"wait_for_bootstrap - {symbol}: tick bootstrap complete")
            return True

        # For bar-based factors, check tick bootstrap first, then bar bootstrap
        trade_key = f"{symbol}:trade"
        if trade_key not in self._bootstrap_done:
            trade_evt = self._trade_bootstrap_events.get(trade_key)
            if trade_evt:
                if not trade_evt.wait(timeout=timeout):
                    logger.warning(
                        f"wait_for_bootstrap - {symbol}: tick bootstrap timeout"
                    )
                    return False

        # Check bar bootstrap for specific timeframe
        bar_key = f"{symbol}:{timeframe}"
        if bar_key in self._bootstrap_done:
            logger.debug(
                f"wait_for_bootstrap - {symbol}/{timeframe}: bar bootstrap already done"
            )
            return True

        evt = self._bootstrap_events.get(bar_key)
        if evt is None:
            # Bootstrap not pre-registered yet, wait briefly for it to appear
            logger.debug(
                f"wait_for_bootstrap - {symbol}/{timeframe}: bootstrap not pre-registered, "
                "waiting for registration"
            )
            for _ in range(int(timeout * 10)):
                evt = self._bootstrap_events.get(bar_key)
                if evt is not None:
                    break
                time.sleep(0.1)

                if evt is None:
                    logger.warning(
                        f"wait_for_bootstrap - {symbol}/{timeframe}: bootstrap never registered"
                    )
                    return False

            if not evt.wait(timeout=timeout):
                logger.warning(f"wait_for_bootstrap - {symbol}/{timeframe}: timeout")
                return False

            logger.debug(f"wait_for_bootstrap - {symbol}/{timeframe}: complete")
            return True

        # No timeframe specified, just tick bootstrap was needed
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

        # Get timeframe from bar data
        timeframe = bar_dict.get("timeframe")
        if not timeframe:
            return

        # Skip processing until bootstrap is complete for this timeframe
        bar_key = f"{symbol}:{timeframe}"
        if bar_key not in self._bootstrap_done:
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

        # Update bar factors via Rust engine
        ticker_key = f"{symbol}:{timeframe}"
        py_bar = PyBar(
            bar_start_ms,
            bar_dict["open"],
            bar_dict["high"],
            bar_dict["low"],
            bar_dict["close"],
            int(bar_dict.get("volume", 0)),
        )
        factors: dict[str, float] = {}
        for name in tf_state.bar_factor_names:
            value = state.rust_engine.update_bar(
                ticker_key, tf_state._rust_key(name), py_bar
            )
            if value is not None:
                factors[name] = value

        # Write bar-based factors to ClickHouse with timeframe
        if factors:
            self._write_factors(symbol, bar_start_ms * 1_000_000, factors, timeframe)

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

        while True:
            try:
                data = await q.get()
                normalized = self.ws_manager.normalize_data(data)
                if normalized is None:
                    continue  # Filtered out (e.g., delayed TRF trade)
                # Use event_type from normalized data (authoritative), not stream key
                if normalized.get("event_type") == "Q":
                    self._on_quote(normalized)
                else:
                    self._on_trade(normalized)
            except asyncio.CancelledError:
                logger.debug(f"FactorEngine: tick consumer cancelled for {stream_key}")
                break
            except Exception as e:
                logger.error(f"FactorEngine: tick consumer error - {e}")

    def _on_trade(self, data: dict) -> None:
        """Handle a trade."""
        symbol = data.get("symbol")
        if not symbol:
            return

        # Buffer ticks until tick bootstrap is complete, then replay.
        trade_key = f"{symbol}:trade"
        if trade_key not in self._bootstrap_done:
            ts_ms_val = data.get("timestamp")
            price_val = data.get("price")
            size_val = data.get("size", 0)
            if ts_ms_val is not None and price_val is not None:
                with self._trade_buffer_lock:
                    self._trade_buffer.setdefault(symbol, []).append(
                        (int(ts_ms_val), float(price_val), int(size_val))
                    )
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
                f"_on_trade: skipping duplicate trade for {symbol} "
                f"at {ts_ms} (last bootstrap: {state.last_bootstrap_trade_ms})"
            )
            return

        # DEBUG: Log tick arrival time
        now_ms = clock_mod.now_ms()
        if now_ms - ts_ms > 2000:  # Only log if delay > 2s
            logger.warning(
                f"[DELAY] _on_trade: {symbol} tick_ts={ts_ms} clock_ts={now_ms} "
                f"delay={now_ms - ts_ms}ms"
            )

        # Feed to Rust trade factors, collect returned values
        price_f = float(price)
        size_i = int(size)
        trade_values: dict[str, float] = {}
        for name in state.trade_factor_names:
            value = state.rust_engine.on_trade(
                state.symbol, name, ts_ms, price_f, size_i
            )
            if value is not None:
                trade_values[name] = value

        # Track last tick timestamp for DEBUG
        state.last_tick_ts_ms = ts_ms

        # Compute and publish trade+quote factors immediately with throttle
        self._maybe_compute_trade_factors(state, ts_ms, price_f, trade_values)

    def _maybe_compute_trade_factors(
        self,
        state: "TickerState",
        ts_ms: int,
        price: float,
        trade_values: dict[str, float] | None = None,
    ) -> None:
        """Compute and publish trade+quote factors with throttle.

        Called on every trade to ensure low latency. Throttles to at most
        one publication per second per ticker.

        trade_values are already-collected from on_trade() calls.
        """
        # Throttle: only publish at most once per second
        if ts_ms - state.last_publish_ms < 1000:
            return

        state.last_publish_ms = ts_ms

        factors: dict[str, float] = dict(trade_values or {})

        # Read quote factor current values from Rust engine
        for name in state.quote_factor_names:
            value = state.rust_engine.get_value(state.symbol, name, "quote")
            if value is not None:
                factors[name] = value

        if factors:
            logger.info(
                f"FactorEngine: on-tick publish {len(factors)} factors for {state.symbol}/tick "
                f"at ts={self._ms_to_readable(ts_ms)} "
                f"({list(factors.keys())})"
            )
            # Publish to Redis immediately (non-blocking)
            try:
                channel = f"factors:{state.symbol}:tick"
                message = json.dumps(
                    {
                        "symbol": state.symbol,
                        "timestamp_ns": ts_ms * 1_000_000,
                        "timestamp_ms": ts_ms,
                        "timeframe": "tick",
                        "factors": factors,
                        "price": price,
                    }
                )
                self.redis_client.publish(channel, message)
            except Exception as e:
                logger.error(f"FactorEngine: Redis publish failed - {e}")

            # Write to ClickHouse in background thread (non-blocking)
            if self.storage:
                snapshot = FactorSnapshot(
                    symbol=state.symbol,
                    timestamp_ns=ts_ms * 1_000_000,
                    factors=factors,
                )
                # Submit to thread pool for async write
                self._write_executor.submit(
                    self._write_snapshot_bg,
                    snapshot,
                    "tick",
                )

    def _write_snapshot_bg(self, snapshot: FactorSnapshot, timeframe: str) -> None:
        """Background thread worker for writing factor snapshot to ClickHouse."""
        try:
            count = self.storage.write_factor_snapshot(snapshot, timeframe)
            if count > 0:
                logger.debug(
                    f"FactorEngine: bg write {count} rows for {snapshot.symbol}/{timeframe} "
                    f"({list(snapshot.factors.keys())})"
                )
        except Exception as e:
            logger.error(f"FactorEngine: Background write failed - {e}")

    def _on_quote(self, data: dict) -> None:
        """Handle a quote tick."""
        symbol = data.get("symbol")
        if not symbol:
            return

        # Skip processing until quote bootstrap is complete
        quote_key = f"{symbol}:quote"
        if quote_key not in self._bootstrap_done:
            return

        state = self.ticker_states.get(symbol)
        if not state or not state.quote_factor_names:
            return

        ts_ms = data.get("timestamp")
        bid = data.get("bid_price") or data.get("bid")
        ask = data.get("ask_price") or data.get("ask")
        if ts_ms is None or bid is None or ask is None:
            return

        ts = int(ts_ms)
        b = float(bid)
        a = float(ask)
        bs = int(data.get("bid_size", 0))
        az = int(data.get("ask_size", 0))
        for name in state.quote_factor_names:
            state.rust_engine.on_quote(state.symbol, name, ts, b, a, bs, az)

    # ═══════════════════════════════════════════════════════════════════════════
    # Tick Indicator Compute Loop (fallback for quote indicators)
    # ═══════════════════════════════════════════════════════════════════════════

    def _compute_loop(self) -> None:
        """Compute quote indicators every COMPUTE_INTERVAL_SEC.

        Note: Trade indicators are now computed on-tick via _maybe_compute_trade_factors
        for low latency. This loop serves as a fallback for quote indicators and
        ensures factors are published even if ticks stop arriving.
        """
        logger.info("FactorEngine: compute loop started (quote indicators fallback)")

        while self._running:
            ts_ms = clock_mod.now_ms()

            with self._states_lock:
                states = list(self.ticker_states.values())

            for state in states:
                # Skip tick factor publishing until tick bootstrap is complete
                # to avoid mixing real-time data with historical bootstrap data
                trade_key = f"{state.symbol}:trade"
                if trade_key not in self._bootstrap_done:
                    continue

                try:
                    # Only read quote factor values here (tick indicators are on-tick)
                    if not state.quote_factor_names:
                        continue

                    factors: dict[str, float] = {}
                    for name in state.quote_factor_names:
                        value = state.rust_engine.get_value(state.symbol, name, "quote")
                        if value is not None:
                            factors[name] = value

                    if factors:
                        logger.info(
                            f"FactorEngine: compute-loop publish {len(factors)} factors "
                            f"for {state.symbol}/tick at ts={self._ms_to_readable(ts_ms)} "
                            f"({list(factors.keys())})"
                        )
                        self._write_factors(
                            state.symbol,
                            ts_ms * 1_000_000,  # ms to ns
                            factors,
                            timeframe="tick",
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
            timeframe: Timeframe for these factors (e.g., '1m', '5m', 'trade')
                      Use 'trade' for trade-based indicators like TradeRate
        """
        if not self.storage:
            return

        snapshot = FactorSnapshot(
            symbol=symbol,
            timestamp_ns=timestamp_ns,
            factors=factors,
        )

        # Publish to Redis first (unconditional, matches _maybe_compute_trade_factors).
        # Real-time streaming must not depend on ClickHouse write returning > 0.
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
        except Exception as e:
            logger.error(f"FactorEngine: Redis publish failed - {e}")

        # Write to ClickHouse in background (fire-and-forget)
        try:
            self._write_executor.submit(
                self._write_snapshot_bg,
                snapshot,
                timeframe,
            )
        except Exception as e:
            logger.error(f"FactorEngine: write submit failed for {symbol} - {e}")

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

        # Shutdown write executor
        self._write_executor.shutdown(wait=False)

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
