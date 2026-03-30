"""Bootstrap Coordinator V2 - per-timeframe orchestration for ticker subscribe.

This module provides centralized coordination for the ticker subscription flow:
1. ChartBFF receives subscribe request from frontend
2. BootstrapCoordinator analyzes timeframe requirements
3. BarsBuilder fetches trades and builds bars (with meeting bar merge)
4. FactorEngine consumes trades for tick warmup, bars for bar warmup
5. Coordinator tracks per-timeframe state and notifies when ready

Key design decisions:
- Meeting bar logic stays in BarsBuilder (not exposed to Coordinator)
- Coordinator tracks "when to start", not "how to merge"
- Trades stored in Redis (gzip) for FactorEngine to consume
- Per-timeframe state tracking for complex dependencies

Usage:
    coordinator = BootstrapCoordinator(redis_client, event_bus)

    # ChartBFF calls this on subscribe
    plan = coordinator.start_bootstrap("AAPL", timeframes=["10s", "1m"])

    # BarsBuilder stores trades after fetch
    coordinator.store_trades("AAPL", trades)

    # BarsBuilder notifies when bars ready per timeframe
    coordinator.on_bars_ready("AAPL", "10s")
    coordinator.on_bars_ready("AAPL", "1m")

    # FactorEngine registers and reports
    coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")
    coordinator.register_consumer("AAPL", "bar_warmup:10s", "factor_engine")
    coordinator.report_done("AAPL", "tick_warmup", "factor_engine")

    # ChartBFF waits
    coordinator.wait_for_ticker_ready("AAPL", timeout=30.0)
"""

from __future__ import annotations

import gzip
import logging
import pickle
import threading
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable

import redis

from jerry_trader import clock as clock_mod
from jerry_trader.platform.messaging.event_bus import Event, EventBus, EventType
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("bootstrap_coordinator", log_to_file=True, level=logging.DEBUG)

# Intraday timeframes that support trades bootstrap
# 10s: only from trades
# 1m-4h: from trades (with meeting bar) or custom bar backfill
# 1d/1w: ws_only (real-time building, no bootstrap)
BOOTSTRAP_TIMEFRAMES = {"10s", "1m", "5m", "15m", "30m", "1h", "4h"}

TRADES_ONLY_TIMEFRAMES = {"10s"}  # Only trades can build these


class TimeframeState(Enum):
    """Per-timeframe bootstrap state."""

    PENDING = auto()  # Not started
    FETCHING = auto()  # BarsBuilder fetching/building
    READY = auto()  # Bars available in ClickHouse
    WARMUP = auto()  # FactorEngine warming up
    DONE = auto()  # Complete
    FAILED = auto()  # Error, terminal
    WS_ONLY = auto()  # No bootstrap needed (1d/1w)


class TradesBootstrapState(Enum):
    """Shared trades bootstrap state (across timeframes)."""

    NOT_NEEDED = auto()  # No timeframe needs trades (e.g., only 1d)
    PENDING = auto()  # Will start
    FETCHING = auto()  # BarsBuilder fetching
    READY = auto()  # Trades stored in Redis
    CONSUMING = auto()  # FactorEngine consuming
    DONE = auto()  # All consumers done


@dataclass
class TimeframeBootstrap:
    """Bootstrap status for a single timeframe."""

    timeframe: str
    state: TimeframeState = TimeframeState.PENDING
    needs_trades: bool = False  # Does this tf need trades (for meeting bar)?
    bar_source: str = "unknown"  # "trades_only", "trades_or_clickhouse", "ws_only"

    # Bar warmup consumers
    bar_consumers: dict[str, bool] = field(default_factory=dict)

    def all_bar_consumers_done(self) -> bool:
        return bool(self.bar_consumers) and all(self.bar_consumers.values())


@dataclass
class TickerBootstrap:
    """Complete bootstrap status for a ticker."""

    symbol: str
    timeframes: dict[str, TimeframeBootstrap] = field(default_factory=dict)
    trades_state: TradesBootstrapState = TradesBootstrapState.PENDING

    # Trades storage
    trades_key: str = ""
    trades: list[tuple[int, float, int]] = field(default_factory=list)
    from_ms: int | None = None
    first_ws_ts: int | None = None

    # Tick warmup consumers (shared across all timeframes)
    tick_consumers: dict[str, bool] = field(default_factory=dict)

    # Timing
    start_time_ns: int = 0
    end_time_ns: int = 0

    def all_tick_consumers_done(self) -> bool:
        """All tick consumers done (or no tick consumers registered)."""
        if not self.tick_consumers:
            return True
        return all(self.tick_consumers.values())

    def all_timeframes_done(self) -> bool:
        """All timeframe bar warmups done."""
        for tf in self.timeframes.values():
            if tf.state not in (TimeframeState.DONE, TimeframeState.WS_ONLY):
                return False
        return True

    def is_ready(self) -> bool:
        """Ticker fully ready: trades done + all timeframes done."""
        return self.all_tick_consumers_done() and self.all_timeframes_done()

    def to_dict(self) -> dict[str, Any]:
        elapsed_ms = 0
        if self.end_time_ns:
            elapsed_ms = (self.end_time_ns - self.start_time_ns) // 1_000_000
        elif self.start_time_ns:
            elapsed_ms = (clock_mod.now_ns() - self.start_time_ns) // 1_000_000

        return {
            "symbol": self.symbol,
            "trades_state": self.trades_state.name,
            "tick_consumers": {
                "total": len(self.tick_consumers),
                "done": sum(self.tick_consumers.values()),
            },
            "timeframes": {
                tf: {"state": status.state.name, "consumers": len(status.bar_consumers)}
                for tf, status in self.timeframes.items()
            },
            "elapsed_ms": elapsed_ms,
        }


class BootstrapCoordinator:
    """Central coordinator for per-timeframe bootstrap orchestration.

    Thread-safe: all state changes protected by _lock.
    """

    # TTL for trades in Redis (1 hour)
    TRADES_TTL_SECONDS = 3600

    def __init__(
        self,
        redis_client: redis.Redis,
        event_bus: EventBus | None = None,
        on_ticker_ready: Callable[[str], None] | None = None,
    ):
        self.redis = redis_client
        self.event_bus = event_bus
        self.on_ticker_ready = on_ticker_ready

        # State: symbol -> TickerBootstrap
        self._bootstraps: dict[str, TickerBootstrap] = {}
        self._lock = threading.Lock()

        # Completion events: symbol -> threading.Event
        self._ready_events: dict[str, threading.Event] = {}

        logger.info("BootstrapCoordinator V2 initialized")

    def start_bootstrap(self, symbol: str, timeframes: list[str]) -> TickerBootstrap:
        """Start bootstrap for a ticker.

        Analyzes timeframes to determine bootstrap strategy:
        - 10s: trades_only
        - 1m-4h: trades_or_clickhouse (meeting bar support)
        - 1d/1w: ws_only (no bootstrap)

        Args:
            symbol: Ticker symbol
            timeframes: List of requested timeframes

        Returns:
            TickerBootstrap plan
        """
        with self._lock:
            # Check if already in progress
            if symbol in self._bootstraps:
                bootstrap = self._bootstraps[symbol]
                if not bootstrap.is_ready():
                    logger.warning(f"start_bootstrap - {symbol}: already in progress")
                    return bootstrap

            # Create new bootstrap plan
            bootstrap = TickerBootstrap(
                symbol=symbol,
                start_time_ns=clock_mod.now_ns(),
            )

            # Analyze each timeframe
            needs_trades = False
            for tf in timeframes:
                if tf in TRADES_ONLY_TIMEFRAMES:
                    # 10s: only trades can build
                    tf_bootstrap = TimeframeBootstrap(
                        timeframe=tf,
                        state=TimeframeState.PENDING,
                        needs_trades=True,
                        bar_source="trades_only",
                    )
                    needs_trades = True
                elif tf in BOOTSTRAP_TIMEFRAMES:
                    # 1m-4h: trades (with meeting bar) or clickhouse
                    tf_bootstrap = TimeframeBootstrap(
                        timeframe=tf,
                        state=TimeframeState.PENDING,
                        needs_trades=True,
                        bar_source="trades_or_clickhouse",
                    )
                    needs_trades = True
                else:
                    # 1d/1w: ws_only
                    tf_bootstrap = TimeframeBootstrap(
                        timeframe=tf,
                        state=TimeframeState.WS_ONLY,
                        needs_trades=False,
                        bar_source="ws_only",
                    )

                bootstrap.timeframes[tf] = tf_bootstrap

            # Set trades state
            if not needs_trades:
                bootstrap.trades_state = TradesBootstrapState.NOT_NEEDED

            self._bootstraps[symbol] = bootstrap
            self._ready_events[symbol] = threading.Event()

        logger.info(
            f"start_bootstrap - {symbol}: started with timeframes={timeframes}, "
            f"needs_trades={needs_trades}"
        )

        # Publish event
        if self.event_bus:
            self.event_bus.publish(
                Event(
                    type=EventType.BOOTSTRAP_START,
                    symbol=symbol,
                    timestamp_ns=clock_mod.now_ns(),
                    data={"timeframes": timeframes, "needs_trades": needs_trades},
                )
            )

        return bootstrap

    def store_trades(
        self,
        symbol: str,
        trades: list[tuple[int, float, int]],
        from_ms: int | None = None,
        first_ws_ts: int | None = None,
    ) -> str:
        """Store trades for FactorEngine to consume.

        Called by BarsBuilder after fetching trades.
        Compresses and stores in Redis with TTL.

        Args:
            symbol: Ticker symbol
            trades: List of (timestamp_ms, price, size)
            from_ms: Start timestamp of fetch
            first_ws_ts: First WebSocket tick timestamp

        Returns:
            Redis key for stored trades
        """
        trades_key = f"bootstrap:trades:{symbol}:{uuid.uuid4().hex[:8]}"

        # Compress and store
        compressed = gzip.compress(pickle.dumps(trades))
        self.redis.setex(trades_key, self.TRADES_TTL_SECONDS, compressed)

        with self._lock:
            if symbol in self._bootstraps:
                bootstrap = self._bootstraps[symbol]
                bootstrap.trades_key = trades_key
                bootstrap.trades = trades  # Keep in memory for fast access
                bootstrap.from_ms = from_ms
                bootstrap.first_ws_ts = first_ws_ts

                # Transition state
                if bootstrap.trades_state in (
                    TradesBootstrapState.PENDING,
                    TradesBootstrapState.FETCHING,
                ):
                    bootstrap.trades_state = TradesBootstrapState.READY

        logger.info(
            f"store_trades - {symbol}: stored {len(trades)} trades at {trades_key}"
        )

        # Notify consumers
        if self.event_bus:
            self.event_bus.publish(
                Event(
                    type=EventType.TRADES_READY,
                    symbol=symbol,
                    timestamp_ns=clock_mod.now_ns(),
                    data={
                        "trades_key": trades_key,
                        "trade_count": len(trades),
                        "from_ms": from_ms,
                        "first_ws_ts": first_ws_ts,
                    },
                )
            )

        return trades_key

    def get_trades(self, symbol: str) -> list[tuple[int, float, int]]:
        """Get trades for tick warmup.

        Called by FactorEngine. First checks memory, then Redis.
        """
        with self._lock:
            if symbol in self._bootstraps:
                bootstrap = self._bootstraps[symbol]
                if bootstrap.trades:
                    return bootstrap.trades

                # Fallback to Redis
                if bootstrap.trades_key:
                    data = self.redis.get(bootstrap.trades_key)
                    if data and isinstance(data, bytes):
                        trades = pickle.loads(gzip.decompress(data))
                        bootstrap.trades = trades  # Cache in memory
                        return trades

        return []

    def on_bars_ready(self, symbol: str, timeframe: str) -> None:
        """Notify that bars are ready for a timeframe.

        Called by BarsBuilder when bars are built and stored in ClickHouse.
        """
        with self._lock:
            if symbol not in self._bootstraps:
                logger.warning(f"on_bars_ready - {symbol}: no active bootstrap")
                return

            bootstrap = self._bootstraps[symbol]
            if timeframe not in bootstrap.timeframes:
                logger.warning(
                    f"on_bars_ready - {symbol}/{timeframe}: unknown timeframe"
                )
                return

            tf_bootstrap = bootstrap.timeframes[timeframe]
            if tf_bootstrap.state == TimeframeState.PENDING:
                tf_bootstrap.state = TimeframeState.READY
                logger.info(f"on_bars_ready - {symbol}/{timeframe}: bars ready")

        # Notify consumers
        if self.event_bus:
            self.event_bus.publish(
                Event(
                    type=EventType.BAR_BACKFILL_COMPLETED,
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp_ns=clock_mod.now_ns(),
                    data={},
                )
            )

        self._check_ticker_ready(symbol)

    def register_consumer(self, symbol: str, phase: str, consumer_id: str) -> None:
        """Register a consumer for a phase.

        Phases:
        - "tick_warmup": For tick indicators (TradeRate)
        - "bar_warmup:{timeframe}": For bar indicators per timeframe
        """
        with self._lock:
            if symbol not in self._bootstraps:
                logger.warning(f"register_consumer - {symbol}: no active bootstrap")
                return

            bootstrap = self._bootstraps[symbol]

            if phase == "tick_warmup":
                bootstrap.tick_consumers[consumer_id] = False
                logger.debug(
                    f"register_consumer - {symbol}: {consumer_id} for tick_warmup"
                )
            elif phase.startswith("bar_warmup:"):
                tf = phase.split(":", 1)[1]
                if tf in bootstrap.timeframes:
                    bootstrap.timeframes[tf].bar_consumers[consumer_id] = False
                    logger.debug(
                        f"register_consumer - {symbol}/{tf}: {consumer_id} for bar_warmup"
                    )

    def report_done(self, symbol: str, phase: str, consumer_id: str) -> None:
        """Report a consumer has completed a phase."""
        with self._lock:
            if symbol not in self._bootstraps:
                return

            bootstrap = self._bootstraps[symbol]

            if phase == "tick_warmup":
                if consumer_id in bootstrap.tick_consumers:
                    bootstrap.tick_consumers[consumer_id] = True
                    logger.info(
                        f"report_done - {symbol}: {consumer_id} completed tick_warmup"
                    )
            elif phase.startswith("bar_warmup:"):
                tf = phase.split(":", 1)[1]
                if tf in bootstrap.timeframes:
                    tf_bootstrap = bootstrap.timeframes[tf]
                    if consumer_id in tf_bootstrap.bar_consumers:
                        tf_bootstrap.bar_consumers[consumer_id] = True
                        logger.info(
                            f"report_done - {symbol}/{tf}: {consumer_id} completed bar_warmup"
                        )

                        # Check if all bar consumers done for this tf
                        if tf_bootstrap.all_bar_consumers_done():
                            tf_bootstrap.state = TimeframeState.DONE
                            logger.info(
                                f"report_done - {symbol}/{tf}: all consumers done"
                            )

        self._check_ticker_ready(symbol)

    def wait_for_ticker_ready(self, symbol: str, timeout: float = 30.0) -> bool:
        """Wait for all bootstrap phases to complete."""
        event = self._ready_events.get(symbol)
        if event is None:
            return False

        ready = event.wait(timeout=timeout)
        if ready:
            logger.info(f"wait_for_ticker_ready - {symbol}: ready")
        else:
            logger.warning(
                f"wait_for_ticker_ready - {symbol}: timeout after {timeout}s"
            )

        return ready

    def get_bootstrap(self, symbol: str) -> TickerBootstrap | None:
        """Get current bootstrap status."""
        with self._lock:
            return self._bootstraps.get(symbol)

    def is_ready(self, symbol: str) -> bool:
        """Check if ticker bootstrap is complete."""
        bootstrap = self.get_bootstrap(symbol)
        return bootstrap is not None and bootstrap.is_ready()

    def cleanup(self, symbol: str) -> None:
        """Clean up bootstrap state and stored trades."""
        with self._lock:
            if symbol in self._bootstraps:
                bootstrap = self._bootstraps[symbol]

                # Delete trades from Redis
                if bootstrap.trades_key:
                    self.redis.delete(bootstrap.trades_key)

                del self._bootstraps[symbol]

        if symbol in self._ready_events:
            del self._ready_events[symbol]

        logger.debug(f"cleanup - {symbol}: bootstrap state cleaned")

    def _check_ticker_ready(self, symbol: str) -> None:
        """Check if ticker is fully ready and notify."""
        with self._lock:
            if symbol not in self._bootstraps:
                return

            bootstrap = self._bootstraps[symbol]
            if not bootstrap.is_ready():
                return

            bootstrap.end_time_ns = clock_mod.now_ns()

        logger.info(f"_check_ticker_ready - {symbol}: ticker fully ready")

        # Signal waiting threads
        if symbol in self._ready_events:
            self._ready_events[symbol].set()

        # Call callback
        if self.on_ticker_ready:
            try:
                self.on_ticker_ready(symbol)
            except Exception as e:
                logger.error(f"_check_ticker_ready - {symbol}: callback error: {e}")

        # Publish event
        if self.event_bus:
            self.event_bus.publish(
                Event(
                    type=EventType.TICKER_READY,
                    symbol=symbol,
                    timestamp_ns=clock_mod.now_ns(),
                    data=bootstrap.to_dict(),
                )
            )


# Global coordinator instance (set by backend_starter)
_global_coordinator: BootstrapCoordinator | None = None


def get_coordinator() -> BootstrapCoordinator | None:
    """Get the global coordinator instance."""
    return _global_coordinator


def set_coordinator(coordinator: BootstrapCoordinator) -> None:
    """Set the global coordinator instance."""
    global _global_coordinator
    _global_coordinator = coordinator
    logger.info("Global coordinator instance set")
