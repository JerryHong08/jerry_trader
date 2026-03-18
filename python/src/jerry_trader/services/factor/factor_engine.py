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
from jerry_trader.domain.factor import FactorSnapshot
from jerry_trader.domain.market import Bar
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.factor.factor_storage import FactorStorage
from jerry_trader.services.factor.indicators import (
    EMA,
    BarIndicator,
    TickIndicator,
    TradeRate,
)
from jerry_trader.services.market_data.feeds.unified_tick_manager import (
    UnifiedTickManager,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("factor_engine", log_to_file=True, level=logging.DEBUG)

# Constants
COMPUTE_INTERVAL_SEC = 1.0  # Tick indicator compute interval


@dataclass
class TickerState:
    """Per-ticker indicator state."""

    symbol: str
    bar_indicators: list[BarIndicator] = field(default_factory=list)
    tick_indicators: list[TickIndicator] = field(default_factory=list)

    def reset(self) -> None:
        """Reset all indicators."""
        for ind in self.bar_indicators:
            ind.reset()
        for ind in self.tick_indicators:
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
        timeframe: str = "1m",
        clickhouse_config: dict[str, Any] | None = None,
        # Unused params for backend_starter compatibility
        manager_type: str | None = None,
        influxdb_config: dict | None = None,
    ):
        """Initialize the factor engine.

        Args:
            session_id: Session identifier
            redis_config: Redis connection config
            ws_manager: Shared UnifiedTickManager for tick data
            ws_loop: Event loop for async tick consumption
            timeframe: Bar timeframe to subscribe to (default: 1m)
            clickhouse_config: ClickHouse connection config
        """
        self.session_id = session_id
        self.timeframe = timeframe

        # Redis client
        redis_config = redis_config or {}
        self.redis_client = redis.Redis(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            db=redis_config.get("db", 0),
            decode_responses=True,
        )

        # ClickHouse storage
        ch_client = get_clickhouse_client(clickhouse_config)
        self.storage = FactorStorage(ch_client, session_id) if ch_client else None

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

        logger.info(
            f"FactorEngine initialized: session={session_id}, timeframe={timeframe}, "
            f"shared_manager={not self._owns_manager}, storage={'enabled' if self.storage else 'disabled'}"
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

    def add_ticker(self, symbol: str) -> None:
        """Start tracking a ticker for factor computation."""
        with self._states_lock:
            if symbol in self.ticker_states:
                logger.debug(f"FactorEngine: {symbol} already tracked")
                return

            # Create indicator instances
            state = TickerState(
                symbol=symbol,
                bar_indicators=[EMA(period=1)],
                tick_indicators=[TradeRate(window_ms=20_000, min_trades=5)],
            )
            self.ticker_states[symbol] = state

        # Subscribe to bar pub/sub channel
        self._subscribe_bars(symbol)

        # Subscribe to tick stream
        self._subscribe_ticks(symbol)

        logger.info(f"FactorEngine: tracking {symbol}")

    def remove_ticker(self, symbol: str) -> None:
        """Stop tracking a ticker."""
        with self._states_lock:
            if symbol not in self.ticker_states:
                return
            del self.ticker_states[symbol]

        # Unsubscribe from bar channel
        if self._pubsub:
            channel = f"bars:{symbol}:{self.timeframe}"
            try:
                self._pubsub.unsubscribe(channel)
            except Exception:
                pass

        # Cancel tick consumer
        self._unsubscribe_ticks(symbol)

        logger.info(f"FactorEngine: stopped tracking {symbol}")

    # ═══════════════════════════════════════════════════════════════════════════
    # Bar Subscription (Redis pub/sub)
    # ═══════════════════════════════════════════════════════════════════════════

    def _subscribe_bars(self, symbol: str) -> None:
        """Subscribe to Redis pub/sub channel for bars."""
        if not self._pubsub:
            return

        channel = f"bars:{symbol}:{self.timeframe}"
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

        with self._states_lock:
            state = self.ticker_states.get(symbol)

        if not state:
            return

        logger.debug(
            f"_on_bar: received bar for {symbol} at {bar_dict.get('bar_start')}"
        )
        # Convert to Bar domain model
        bar = Bar(
            symbol=symbol,
            timestamp_ns=bar_dict["bar_start"] * 1_000_000,  # ms to ns
            timespan=bar_dict.get("timeframe", self.timeframe),
            open=bar_dict["open"],
            high=bar_dict["high"],
            low=bar_dict["low"],
            close=bar_dict["close"],
            volume=int(bar_dict.get("volume", 0)),
            vwap=bar_dict.get("vwap"),
            trade_count=bar_dict.get("trade_count"),
        )

        # Update bar indicators
        factors: dict[str, float] = {}
        for ind in state.bar_indicators:
            logger.debug(
                f"_on_bar: calculate {symbol} {ind.name} with bar at {bar_dict.get('bar_start')}"
            )
            value = ind.update(bar)

            logger.debug(
                f"_on_bar: calculated {symbol} {ind.name} = {value} with bar at {bar_dict.get('bar_start')}"
            )

            if value is not None:
                factors[ind.name] = value

        # Write bar-based factors to ClickHouse
        if factors:
            self._write_factors(symbol, bar.timestamp_ns, factors)

    # ═══════════════════════════════════════════════════════════════════════════
    # Tick Subscription (UnifiedTickManager)
    # ═══════════════════════════════════════════════════════════════════════════

    def _subscribe_ticks(self, symbol: str) -> None:
        """Subscribe to tick stream via UnifiedTickManager."""
        if not self.ws_manager or not self.ws_loop:
            return

        # Subscribe to trades only (not quotes)
        asyncio.run_coroutine_threadsafe(
            self.ws_manager.subscribe(
                websocket_client="factor_engine",
                symbols=[symbol],
                events=["T"],
            ),
            self.ws_loop,
        )

        # Start consumer for this stream
        stream_keys = self.ws_manager.generate_stream_keys(
            symbols=[symbol], events=["T"]
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
                events=["T"],
            ),
            self.ws_loop,
        )

        stream_keys = self.ws_manager.generate_stream_keys(
            symbols=[symbol], events=["T"]
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

        with self._states_lock:
            state = self.ticker_states.get(symbol)

        if not state:
            return

        ts_ms = data.get("timestamp")
        price = data.get("price")
        size = data.get("size", 0)

        if ts_ms is None or price is None:
            return

        # Feed to tick indicators
        for ind in state.tick_indicators:
            ind.on_tick(int(ts_ms), float(price), int(size))

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
                try:
                    factors: dict[str, float] = {}
                    for ind in state.tick_indicators:
                        value = ind.compute(ts_ms)
                        if value is not None:
                            factors[ind.name] = value

                    if factors:
                        self._write_factors(
                            state.symbol,
                            ts_ms * 1_000_000,  # ms to ns
                            factors,
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
    ) -> None:
        """Write factors to ClickHouse via FactorStorage and publish to Redis pub/sub.

        Args:
            symbol: Ticker symbol
            timestamp_ns: Timestamp in nanoseconds
            factors: Dict of factor_name -> value
        """
        if not self.storage:
            return

        snapshot = FactorSnapshot(
            symbol=symbol,
            timestamp_ns=timestamp_ns,
            factors=factors,
        )

        try:
            count = self.storage.write_factor_snapshot(snapshot)
            if count > 0:
                logger.debug(
                    f"FactorEngine: wrote {count} factors for {symbol} "
                    f"({list(factors.keys())})"
                )

                # Publish to Redis pub/sub for real-time streaming
                try:
                    channel = f"factors:{symbol}"
                    message = json.dumps(
                        {
                            "symbol": symbol,
                            "timestamp_ns": timestamp_ns,
                            "timestamp_ms": timestamp_ns // 1_000_000,
                            "factors": factors,
                        }
                    )
                    self.redis_client.publish(channel, message)
                    logger.debug(
                        f"FactorEngine: published {len(factors)} factors to {channel}"
                    )
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
                                self.add_ticker(ticker)
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

        # Subscribe to bars for existing tickers
        with self._states_lock:
            for symbol in self.ticker_states:
                self._subscribe_bars(symbol)

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
