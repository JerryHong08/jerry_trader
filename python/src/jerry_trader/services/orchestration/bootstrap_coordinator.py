"""Bootstrap Coordinator - unified orchestration for ticker subscribe pipeline.

This module provides centralized coordination for the ticker subscription flow:
1. ChartBFF receives subscribe request from frontend
2. BootstrapCoordinator manages the orchestration:
   - State machine: PENDING → FETCHING → TRADES_READY → WARMUP → READY
   - Tracks consumers (FactorEngine, SignalEngine, etc.)
   - Ensures single IO (trades fetched once by BarsBuilder, shared via Redis)
3. Services report completion via events
4. Coordinator notifies ChartBFF when ticker is fully ready

Usage:
    coordinator = BootstrapCoordinator(redis_client, event_bus)

    # ChartBFF calls this on subscribe
    coordinator.start_bootstrap("AAPL", timeframes=["1m", "5m"])

    # Services register as consumers
    coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")

    # Services report completion
    coordinator.report_done("AAPL", "tick_warmup", "factor_engine")

    # ChartBFF waits for completion
    coordinator.wait_for_ready("AAPL", timeout=30.0)
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


class BootstrapState(Enum):
    """Bootstrap state machine states."""

    PENDING = auto()  # Initial state, waiting to start
    FETCHING = auto()  # BarsBuilder fetching trades
    TRADES_READY = auto()  # Trades available in Redis, consumers can start
    TICK_WARMUP = auto()  # FactorEngine doing tick indicator warmup
    BAR_WARMUP = auto()  # FactorEngine doing bar indicator warmup
    SIGNAL_WARMUP = auto()  # SignalEngine doing signal warmup (optional)
    READY = auto()  # All warmups complete, ticker fully ready
    FAILED = auto()  # Bootstrap failed, terminal state


@dataclass
class BootstrapStatus:
    """Status of a single ticker bootstrap."""

    symbol: str
    state: BootstrapState = BootstrapState.PENDING
    timeframes: list[str] = field(default_factory=list)
    start_time_ns: int = 0
    end_time_ns: int = 0

    # Consumer tracking: phase -> {consumer_id -> done}
    consumers: dict[str, dict[str, bool]] = field(default_factory=dict)

    # Trades storage info
    trades_key: str = ""
    trade_count: int = 0
    from_ms: int | None = None
    first_ws_ts: int | None = None

    # Error info
    error_message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "state": self.state.name,
            "timeframes": self.timeframes,
            "elapsed_ms": (
                (self.end_time_ns - self.start_time_ns) // 1_000_000
                if self.end_time_ns
                else (clock_mod.now_ns() - self.start_time_ns) // 1_000_000
            ),
            "consumers": {
                phase: {"total": len(consumers), "done": sum(consumers.values())}
                for phase, consumers in self.consumers.items()
            },
            "trades": (
                {
                    "key": self.trades_key,
                    "count": self.trade_count,
                }
                if self.trades_key
                else None
            ),
        }


class BootstrapCoordinator:
    """Central coordinator for ticker bootstrap orchestration.

    Manages the state machine and coordinates between services:
    - ChartBFF: initiates bootstrap, waits for completion
    - BarsBuilder: fetches trades, stores in Redis
    - FactorEngine: consumes trades for warmup
    - SignalEngine: consumes data for signal warmup (future)

    Thread-safe: all state changes are protected by _lock.
    """

    # TTL for trades storage in Redis (1 hour)
    TRADES_TTL_SECONDS = 3600

    # Timeout defaults
    DEFAULT_FETCH_TIMEOUT = 60.0
    DEFAULT_WARMUP_TIMEOUT = 30.0

    def __init__(
        self,
        redis_client: redis.Redis,
        event_bus: EventBus | None = None,
        on_ticker_ready: Callable[[str], None] | None = None,
    ):
        """Initialize BootstrapCoordinator.

        Args:
            redis_client: Redis client for state and trades storage
            event_bus: EventBus for publishing bootstrap events
            on_ticker_ready: Callback when ticker becomes ready
        """
        self.redis = redis_client
        self.event_bus = event_bus
        self.on_ticker_ready = on_ticker_ready

        # State tracking: symbol -> BootstrapStatus
        self._status: dict[str, BootstrapStatus] = {}
        self._status_lock = threading.Lock()

        # Completion events: symbol -> threading.Event
        self._ready_events: dict[str, threading.Event] = {}

        logger.info("BootstrapCoordinator initialized")

    def start_bootstrap(
        self, symbol: str, timeframes: list[str], timeout: float = DEFAULT_FETCH_TIMEOUT
    ) -> BootstrapStatus:
        """Start bootstrap for a ticker.

        This is called by ChartBFF when frontend subscribes to a ticker.
        Creates the bootstrap state and notifies BarsBuilder to start fetching.

        Args:
            symbol: Ticker symbol
            timeframes: List of timeframes to subscribe
            timeout: Max time to wait for bootstrap

        Returns:
            BootstrapStatus for tracking

        Raises:
            RuntimeError: If bootstrap already in progress for this symbol
        """
        with self._status_lock:
            if symbol in self._status:
                status = self._status[symbol]
                if status.state not in (BootstrapState.READY, BootstrapState.FAILED):
                    raise RuntimeError(
                        f"Bootstrap already in progress for {symbol}: {status.state.name}"
                    )

            # Create new status
            status = BootstrapStatus(
                symbol=symbol,
                state=BootstrapState.PENDING,
                timeframes=timeframes,
                start_time_ns=clock_mod.now_ns(),
            )
            self._status[symbol] = status
            self._ready_events[symbol] = threading.Event()

        logger.info(
            f"start_bootstrap - {symbol}: starting with timeframes={timeframes}"
        )

        # Transition to FETCHING and notify BarsBuilder
        self._transition(symbol, BootstrapState.FETCHING)

        # Publish event to trigger BarsBuilder
        if self.event_bus:
            self.event_bus.publish(
                self._create_event(
                    "bootstrap_start", symbol, {"timeframes": timeframes}
                )
            )

        return status

    def register_consumer(self, symbol: str, phase: str, consumer_id: str) -> None:
        """Register a consumer for a warmup phase.

        Services call this to indicate they will participate in a phase.
        The coordinator waits for all registered consumers to report done.

        Args:
            symbol: Ticker symbol
            phase: Warmup phase (e.g., "tick_warmup", "bar_warmup")
            consumer_id: Unique consumer identifier (e.g., "factor_engine")
        """
        with self._status_lock:
            if symbol not in self._status:
                logger.warning(
                    f"register_consumer - {symbol}: no active bootstrap, ignoring"
                )
                return

            status = self._status[symbol]
            if phase not in status.consumers:
                status.consumers[phase] = {}
            status.consumers[phase][consumer_id] = False

        logger.debug(
            f"register_consumer - {symbol}: {consumer_id} registered for {phase}"
        )

    def unregister_consumer(self, symbol: str, phase: str, consumer_id: str) -> None:
        """Unregister a consumer (e.g., service crashed)."""
        with self._status_lock:
            if symbol not in self._status:
                return

            status = self._status[symbol]
            if phase in status.consumers and consumer_id in status.consumers[phase]:
                del status.consumers[phase][consumer_id]
                logger.warning(
                    f"unregister_consumer - {symbol}: {consumer_id} unregistered from {phase}"
                )

                # Check if phase is now complete
                self._check_phase_complete(symbol, phase)

    def report_done(self, symbol: str, phase: str, consumer_id: str) -> None:
        """Report a consumer has completed a phase.

        Args:
            symbol: Ticker symbol
            phase: Warmup phase completed
            consumer_id: Consumer that completed
        """
        with self._status_lock:
            if symbol not in self._status:
                logger.warning(f"report_done - {symbol}: no active bootstrap, ignoring")
                return

            status = self._status[symbol]

            if phase not in status.consumers:
                logger.warning(f"report_done - {symbol}: unknown phase {phase}")
                return

            if consumer_id not in status.consumers[phase]:
                logger.warning(
                    f"report_done - {symbol}: {consumer_id} not registered for {phase}"
                )
                return

            # Mark as done
            status.consumers[phase][consumer_id] = True
            logger.info(
                f"report_done - {symbol}: {consumer_id} completed {phase} "
                f"({sum(status.consumers[phase].values())}/{len(status.consumers[phase])})"
            )

            # Check if phase is complete
            self._check_phase_complete(symbol, phase)

    def store_trades(
        self,
        symbol: str,
        trades: list[tuple[int, float, int]],
        from_ms: int | None = None,
        first_ws_ts: int | None = None,
    ) -> str:
        """Store trades in Redis for consumers to read.

        Called by BarsBuilder after fetching trades.
        Returns the key for consumers to read from.

        Args:
            symbol: Ticker symbol
            trades: List of (timestamp_ms, price, size) tuples
            from_ms: Start timestamp of fetch
            first_ws_ts: First WebSocket tick timestamp

        Returns:
            Redis key where trades are stored
        """
        # Generate unique key
        trades_key = f"bootstrap:trades:{symbol}:{uuid.uuid4().hex[:8]}"

        # Compress and store trades
        compressed = gzip.compress(pickle.dumps(trades))
        self.redis.setex(trades_key, self.TRADES_TTL_SECONDS, compressed)

        # Update status
        with self._status_lock:
            if symbol in self._status:
                status = self._status[symbol]
                status.trades_key = trades_key
                status.trade_count = len(trades)
                status.from_ms = from_ms
                status.first_ws_ts = first_ws_ts

        logger.info(
            f"store_trades - {symbol}: stored {len(trades)} trades at {trades_key}"
        )

        # Transition to TRADES_READY and notify consumers
        self._transition(symbol, BootstrapState.TRADES_READY)

        # Publish event for consumers
        if self.event_bus:
            self.event_bus.publish(
                self._create_event(
                    "trades_ready",
                    symbol,
                    {
                        "trades_key": trades_key,
                        "trade_count": len(trades),
                        "from_ms": from_ms,
                        "first_ws_ts": first_ws_ts,
                    },
                )
            )

        return trades_key

    def get_trades(self, trades_key: str) -> list[tuple[int, float, int]] | None:
        """Get trades from Redis storage.

        Called by consumers (FactorEngine) to read the trades.

        Args:
            trades_key: Redis key returned by store_trades

        Returns:
            List of trades or None if not found/expired
        """
        data = self.redis.get(trades_key)
        if data is None:
            return None

        trades = pickle.loads(gzip.decompress(data))
        return trades

    def delete_trades(self, trades_key: str) -> None:
        """Delete trades from Redis (cleanup after all consumers done)."""
        self.redis.delete(trades_key)
        logger.debug(f"delete_trades: deleted {trades_key}")

    def wait_for_ready(self, symbol: str, timeout: float = 30.0) -> bool:
        """Wait for ticker bootstrap to complete.

        Called by ChartBFF to block until ticker is fully ready.

        Args:
            symbol: Ticker symbol
            timeout: Max seconds to wait

        Returns:
            True if ready, False if timeout
        """
        event = self._ready_events.get(symbol)
        if event is None:
            logger.warning(f"wait_for_ready - {symbol}: no bootstrap in progress")
            return False

        logger.info(f"wait_for_ready - {symbol}: waiting up to {timeout}s")
        ready = event.wait(timeout=timeout)

        if ready:
            logger.info(f"wait_for_ready - {symbol}: ready")
        else:
            logger.warning(f"wait_for_ready - {symbol}: timeout after {timeout}s")

        return ready

    def get_status(self, symbol: str) -> BootstrapStatus | None:
        """Get current bootstrap status for a ticker."""
        with self._status_lock:
            return self._status.get(symbol)

    def is_ready(self, symbol: str) -> bool:
        """Check if ticker bootstrap is complete."""
        status = self.get_status(symbol)
        return status is not None and status.state == BootstrapState.READY

    def fail_bootstrap(self, symbol: str, error_message: str) -> None:
        """Mark bootstrap as failed."""
        with self._status_lock:
            if symbol in self._status:
                status = self._status[symbol]
                status.state = BootstrapState.FAILED
                status.error_message = error_message
                status.end_time_ns = clock_mod.now_ns()

        logger.error(f"fail_bootstrap - {symbol}: {error_message}")

        # Signal ready (with failure) so waiters don't block forever
        if symbol in self._ready_events:
            self._ready_events[symbol].set()

    def cleanup(self, symbol: str) -> None:
        """Clean up bootstrap state for a ticker."""
        with self._status_lock:
            if symbol in self._status:
                status = self._status[symbol]

                # Clean up trades storage
                if status.trades_key:
                    self.delete_trades(status.trades_key)

                del self._status[symbol]

        if symbol in self._ready_events:
            del self._ready_events[symbol]

        logger.debug(f"cleanup - {symbol}: bootstrap state cleaned")

    def _transition(self, symbol: str, new_state: BootstrapState) -> None:
        """Transition state machine to new state."""
        with self._status_lock:
            if symbol not in self._status:
                return

            status = self._status[symbol]
            old_state = status.state
            status.state = new_state

        logger.info(f"_transition - {symbol}: {old_state.name} → {new_state.name}")

        # Publish state change event
        if self.event_bus:
            self.event_bus.publish(
                self._create_event(
                    "state_change",
                    symbol,
                    {"from": old_state.name, "to": new_state.name},
                )
            )

        # Handle state-specific logic
        if new_state == BootstrapState.READY:
            self._on_ready(symbol)

    def _check_phase_complete(self, symbol: str, phase: str) -> None:
        """Check if all consumers for a phase are done."""
        with self._status_lock:
            if symbol not in self._status:
                return

            status = self._status[symbol]
            if phase not in status.consumers:
                return

            consumers = status.consumers[phase]
            if not consumers:
                return

            all_done = all(consumers.values())
            if not all_done:
                return

        logger.info(f"_check_phase_complete - {symbol}: phase {phase} complete")

        # Transition to next phase
        phase_transitions = {
            "tick_warmup": BootstrapState.BAR_WARMUP,
            "bar_warmup": BootstrapState.READY,
        }

        if phase in phase_transitions:
            self._transition(symbol, phase_transitions[phase])

    def _on_ready(self, symbol: str) -> None:
        """Handle ticker becoming ready."""
        with self._status_lock:
            if symbol in self._status:
                self._status[symbol].end_time_ns = clock_mod.now_ns()

        # Signal waiting threads
        if symbol in self._ready_events:
            self._ready_events[symbol].set()

        # Call callback
        if self.on_ticker_ready:
            try:
                self.on_ticker_ready(symbol)
            except Exception as e:
                logger.error(f"_on_ready - {symbol}: callback error: {e}")

        # Publish final event
        if self.event_bus:
            self.event_bus.publish(self._create_event("ticker_ready", symbol, {}))

        logger.info(f"_on_ready - {symbol}: ticker fully ready")

    def _create_event(self, event_type: str, symbol: str, data: dict) -> Any:
        """Create an event for EventBus."""
        # Import here to avoid circular dependency
        from jerry_trader.platform.messaging.event_bus import Event

        return Event(
            type=event_type,  # Using string for bootstrap events
            symbol=symbol,
            timestamp_ns=clock_mod.now_ns(),
            data=data,
        )


# Convenience functions for service integration


def get_coordinator() -> BootstrapCoordinator | None:
    """Get the global coordinator instance (if initialized)."""
    # This is set by backend_starter when wiring services
    return getattr(get_coordinator, "_instance", None)


def set_coordinator(coordinator: BootstrapCoordinator) -> None:
    """Set the global coordinator instance."""
    get_coordinator._instance = coordinator
