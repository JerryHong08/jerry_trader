"""
Event Bus - Redis Streams-based inter-service communication.

Replaces callback-based observer pattern with message-based decoupling.
Services publish typed events to a unified stream and subscribe to react
to events from other services.

Usage:
    bus = EventBus(redis_client, session_id)

    # Subscribe to events
    bus.subscribe(EventType.BAR_BACKFILL_COMPLETED, self._on_bar_backfill)
    bus.subscribe(EventType.BAR_CLOSED, self._on_bar)

    # Start consumer thread
    bus.start()

    # Publish events
    bus.publish(Event.back_backfill_completed("AAPL", "5m", bar_count=1588))

    # Stop on shutdown
    bus.stop()
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

import redis

from jerry_trader.shared.ids.redis_keys import events_stream
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)


class EventType(str, Enum):
    """Event types for inter-service communication."""

    # Bar events
    BAR_BACKFILL_COMPLETED = "bar_backfill_completed"
    BAR_CLOSED = "bar_closed"

    # Tick events
    TRADES_BACKFILL_COMPLETED = "trades_backfill_completed"

    # Bootstrap events (BootstrapCoordinator)
    BOOTSTRAP_START = "bootstrap_start"
    TRADES_READY = "trades_ready"
    TICK_WARMUP_DONE = "tick_warmup_done"
    BAR_WARMUP_DONE = "bar_warmup_done"
    TICKER_READY = "ticker_ready"

    # Future: more event types
    # TICK_SNAPSHOT = "tick_snapshot"


@dataclass
class Event:
    """Typed event for inter-service communication."""

    type: EventType
    symbol: str
    timestamp_ns: int
    timeframe: str | None = None
    data: dict[str, Any] = field(default_factory=dict)

    def to_redis_fields(self) -> dict[str, str]:
        """Convert to Redis stream field dict."""
        return {
            "type": self.type.value,
            "symbol": self.symbol,
            "timeframe": self.timeframe or "",
            "timestamp_ns": str(self.timestamp_ns),
            "data": json.dumps(self.data),
        }

    @classmethod
    def from_redis_fields(cls, fields: dict[str, Any]) -> "Event":
        """Parse from Redis stream field dict."""
        return cls(
            type=EventType(fields["type"]),
            symbol=fields["symbol"],
            timeframe=fields["timeframe"] or None,
            timestamp_ns=int(fields["timestamp_ns"]),
            data=json.loads(fields["data"]),
        )

    # ─────────────────────────────────────────────────────────────────────
    # Factory methods for common events
    # ─────────────────────────────────────────────────────────────────────

    @classmethod
    def bar_backfill_completed(
        cls,
        symbol: str,
        timeframe: str,
        bar_count: int,
        source: str = "unknown",
        timestamp_ns: int | None = None,
    ) -> "Event":
        """Create BarBackfillCompleted event."""
        from jerry_trader import clock

        return cls(
            type=EventType.BAR_BACKFILL_COMPLETED,
            symbol=symbol,
            timeframe=timeframe,
            timestamp_ns=timestamp_ns or clock.now_ns(),
            data={"bar_count": bar_count, "source": source},
        )

    @classmethod
    def trades_backfill_completed(
        cls,
        symbol: str,
        trade_count: int,
        source: str = "unknown",
        from_ms: int | None = None,
        first_ws_ts: int | None = None,
        timestamp_ns: int | None = None,
    ) -> "Event":
        """Create TradesBackfillCompleted event.

        Args:
            symbol: Ticker symbol
            trade_count: Number of trades backfilled
            source: Data source ("polygon", "parquet", etc.)
            from_ms: Start timestamp of trade range (UTC ms)
            first_ws_ts: First WebSocket tick timestamp (UTC ms) - end of trade range
        """
        from jerry_trader import clock

        data = {"trade_count": trade_count, "source": source}
        if from_ms is not None:
            data["from_ms"] = from_ms
        if first_ws_ts is not None:
            data["first_ws_ts"] = first_ws_ts

        return cls(
            type=EventType.TRADES_BACKFILL_COMPLETED,
            symbol=symbol,
            timestamp_ns=timestamp_ns or clock.now_ns(),
            data=data,
        )

    @classmethod
    def bar_closed(
        cls,
        symbol: str,
        timeframe: str,
        bar_start: int,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        timestamp_ns: int | None = None,
    ) -> "Event":
        """Create BarClosed event."""
        from jerry_trader import clock

        return cls(
            type=EventType.BAR_CLOSED,
            symbol=symbol,
            timeframe=timeframe,
            timestamp_ns=timestamp_ns or clock.now_ns(),
            data={
                "bar_start": bar_start,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            },
        )


# Handler type: function that receives an Event
EventHandler = Callable[[Event], None]


class EventBus:
    """Redis Streams-based event bus for inter-service communication.

    Services publish typed events to a unified stream and subscribe to
    react to events from other services. This decouples services via
    message-based communication instead of direct callback calls.

    Thread-safe: Consumer runs in a background thread, handlers are
    called from that thread. Use thread-safe patterns in handlers.

    Example:
        bus = EventBus(redis_client, session_id)
        bus.subscribe(EventType.BAR_CLOSED, self._on_bar)
        bus.start()
        bus.publish(Event.bar_closed(...))
        bus.stop()
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        session_id: str,
        consumer_name: str = "default",
    ):
        """Initialize EventBus.

        Args:
            redis_client: Redis client for stream operations
            session_id: Session ID for stream scoping
            consumer_name: Unique name for this consumer (for debugging)
        """
        self.redis = redis_client
        self.session_id = session_id
        self.consumer_name = consumer_name
        self.stream_key = events_stream(session_id)

        # Event type → list of handlers
        self._handlers: dict[EventType, list[EventHandler]] = {
            et: [] for et in EventType
        }
        self._handlers_lock = threading.Lock()

        # Consumer thread state
        self._running = False
        self._consumer_thread: threading.Thread | None = None

        # Last processed message ID
        self._last_id: str = "0"

        logger.info(
            f"EventBus initialized: stream={self.stream_key}, consumer={consumer_name}"
        )

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Register a handler for an event type.

        Multiple handlers can be registered for the same event type.
        Handlers are called in registration order.

        Args:
            event_type: Type of event to handle
            handler: Function to call when event is received
        """
        with self._handlers_lock:
            self._handlers[event_type].append(handler)
        logger.debug(
            f"EventBus: registered handler for {event_type.value} "
            f"(total: {len(self._handlers[event_type])})"
        )

    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """Remove a handler for an event type.

        Args:
            event_type: Type of event
            handler: Handler to remove
        """
        with self._handlers_lock:
            try:
                self._handlers[event_type].remove(handler)
                logger.debug(f"EventBus: unregistered handler for {event_type.value}")
            except ValueError:
                logger.warning(f"EventBus: handler not found for {event_type.value}")

    def publish(self, event: Event) -> str:
        """Publish an event to the stream.

        Args:
            event: Event to publish

        Returns:
            Message ID from Redis
        """
        msg_id = self.redis.xadd(self.stream_key, event.to_redis_fields())
        logger.debug(
            f"EventBus: published {event.type.value} for {event.symbol} "
            f"(timeframe={event.timeframe}, id={msg_id})"
        )
        return msg_id

    def start(self) -> None:
        """Start the consumer thread.

        The consumer thread reads events from the stream and dispatches
        them to registered handlers.
        """
        if self._running:
            logger.warning("EventBus: already running")
            return

        self._running = True
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            name=f"EventBus-{self.consumer_name}",
            daemon=True,
        )
        self._consumer_thread.start()
        logger.info(f"EventBus: consumer started for {self.stream_key}")

    def stop(self) -> None:
        """Stop the consumer thread."""
        if not self._running:
            return

        self._running = False
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5.0)
        logger.info(f"EventBus: consumer stopped for {self.stream_key}")

    def _consumer_loop(self) -> None:
        """Consumer thread main loop.

        Reads events from the stream using XREAD and dispatches to handlers.
        """
        logger.info(f"EventBus: consumer loop started for {self.stream_key}")

        while self._running:
            try:
                # Block for up to 1 second waiting for new events
                result = self.redis.xread(
                    {self.stream_key: self._last_id},
                    count=10,
                    block=1000,
                )

                if not result:
                    continue

                for stream_name, messages in result:
                    for msg_id, fields in messages:
                        if not self._running:
                            break

                        # Update last processed ID
                        self._last_id = msg_id

                        try:
                            # Parse and dispatch event
                            event = Event.from_redis_fields(fields)
                            self._dispatch_event(event)
                        except Exception as e:
                            logger.error(
                                f"EventBus: error processing message {msg_id}: {e}"
                            )

            except redis.ConnectionError as e:
                logger.error(f"EventBus: Redis connection error: {e}")
                time.sleep(1.0)  # Wait before retrying
            except Exception as e:
                if self._running:
                    logger.error(f"EventBus: consumer error: {e}")
                    time.sleep(0.5)

        logger.info(f"EventBus: consumer loop stopped for {self.stream_key}")

    def _dispatch_event(self, event: Event) -> None:
        """Dispatch an event to registered handlers."""
        with self._handlers_lock:
            handlers = list(self._handlers.get(event.type, []))

        if not handlers:
            logger.debug(
                f"EventBus: no handlers for {event.type.value} "
                f"(symbol={event.symbol})"
            )
            return

        logger.debug(
            f"EventBus: dispatching {event.type.value} for {event.symbol} "
            f"to {len(handlers)} handler(s)"
        )

        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"EventBus: handler error for {event.type.value}: {e}")
