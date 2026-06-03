"""Signal Engine — real-time event evaluation.

Subscribes to FactorEngine Redis channels, evaluates events from events.yaml
against incoming factor updates, and emits trigger events.

Unified with Backtest: Both use events.yaml as the single source of truth.
EventEvaluator handles the condition matching logic.

ML Integration (roadmap/ml-event-architecture.md):
- ML-based events use ModelRegistry for prediction
- hard_constraints checked before ML prediction
- Returns (should_enter, expected_return, confidence) for ML events

Usage:
    engine = SignalEngine(redis_client, events_config_path="config/events.yaml")
    engine.start()  # starts background thread
    engine.stop()   # graceful shutdown
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import redis

from jerry_trader.domain.event import Event
from jerry_trader.services.backtest.event_evaluator import (
    EventEvaluator,
    MLEvaluationResult,
    TickerState,
)
from jerry_trader.services.model_registry import get_model_registry
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("signal_engine", log_to_file=True)


# ─────────────────────────────────────────────────────────────────────────────
# Signal Engine
# ─────────────────────────────────────────────────────────────────────────────


class SignalEngine:
    """Real-time event evaluation engine.

    Subscribes to FactorEngine Redis pub/sub channels and evaluates
    events from events.yaml against incoming factor updates.

    Unified with Backtest: uses the same EventEvaluator and events.yaml
    for consistent signal matching logic.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        events_config_path: str = "config/events.yaml",
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        storage: Any | None = None,
        clickhouse_config: dict | None = None,
        return_fill_interval_sec: float = 120.0,
    ):
        self.redis_client = redis_client
        self.events_config_path = events_config_path
        self.symbols = [s.upper() for s in symbols] if symbols else []
        self.timeframes = timeframes or ["tick", "10s", "1m", "5m"]
        self._storage = storage
        self._clickhouse_config = clickhouse_config
        self._return_fill_interval_sec = return_fill_interval_sec

        # Event evaluator (shared logic with backtest)
        self._evaluator: EventEvaluator | None = None

        # Ticker state tracking for stage-based evaluation
        self._ticker_states: dict[str, TickerState] = {}
        self._states_lock = threading.Lock()

        # Subscription state
        self._pubsub: redis.client.PubSub | None = None
        self._thread: threading.Thread | None = None
        self._running = threading.Event()

        # Return fill background thread
        self._return_fill_thread: threading.Thread | None = None

        # Trigger dedup: prevent logging same trigger within cooldown period
        # Key: (event_name, symbol), Value: last_trigger_time_ms
        self._last_trigger: dict[tuple[str, str], int] = {}
        self._trigger_cooldown_sec: float = 60.0

        self._trigger_count: int = 0

    @property
    def trigger_count(self) -> int:
        return self._trigger_count

    @property
    def events(self) -> list[Event]:
        """Current loaded events (copy)."""
        if self._evaluator is None:
            return []
        return list(self._evaluator.events)

    @property
    def anti_patterns(self) -> list[Event]:
        """Current loaded anti-patterns (copy)."""
        if self._evaluator is None:
            return []
        return list(self._evaluator.anti_patterns)

    # ─────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    def load_events(self) -> int:
        """Load events from events.yaml config file.

        Returns:
            Number of events loaded.
        """
        self._evaluator = EventEvaluator(config_path=self.events_config_path)

        event_count = len(self._evaluator.events)
        anti_count = len(self._evaluator.anti_patterns)

        logger.info(
            f"SignalEngine: loaded {event_count} events, {anti_count} anti-patterns "
            f"from {self.events_config_path}"
        )
        for event in self._evaluator.events:
            logger.info(
                f"  Event: {event.name} stage={event.stage.value} "
                f"trigger={event.trigger.value}"
            )

        return event_count

    def start(self) -> None:
        """Start the signal engine in a background thread."""
        if self._running.is_set():
            logger.warning("SignalEngine: already running")
            return

        self.load_events()

        self._running.set()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="signal-engine",
            daemon=True,
        )
        self._thread.start()
        logger.info("SignalEngine: started")

        # Start return fill background thread in live mode only.
        if self._clickhouse_config and not self._is_replay_mode():
            self._return_fill_thread = threading.Thread(
                target=self._return_fill_loop,
                name="signal-return-fill",
                daemon=True,
            )
            self._return_fill_thread.start()
            logger.info(
                f"SignalEngine: return fill thread started "
                f"(interval={self._return_fill_interval_sec}s)"
            )
        elif self._clickhouse_config and self._is_replay_mode():
            logger.info(
                "SignalEngine: replay mode — return fill deferred to post-replay batch"
            )

    def stop(self) -> None:
        """Stop the signal engine."""
        self._running.clear()
        if self._pubsub:
            try:
                self._pubsub.punsubscribe()
                self._pubsub.unsubscribe()
                self._pubsub.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        if self._return_fill_thread and self._return_fill_thread.is_alive():
            self._return_fill_thread.join(timeout=5.0)
        logger.info(f"SignalEngine: stopped (triggers fired: {self._trigger_count})")

    # ─────────────────────────────────────────────────────────────────────────
    # Main Loop
    # ─────────────────────────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        """Background thread: subscribe to Redis channels and process messages."""
        self._pubsub = self.redis_client.pubsub()

        # Subscribe to factor channels for configured symbols and timeframes
        channels: list[str] = []
        use_pattern = False
        if self.symbols:
            for sym in self.symbols:
                for tf in self.timeframes:
                    channels.append(f"factors:{sym}:{tf}")
        else:
            # Subscribe to all factor channels via pattern
            channels.append("factors:*")
            use_pattern = True

        try:
            if channels:
                if use_pattern:
                    self._pubsub.psubscribe(*channels)
                else:
                    self._pubsub.subscribe(*channels)
                logger.info(f"SignalEngine: subscribed to {len(channels)} channels")
            else:
                logger.warning("SignalEngine: no channels to subscribe to")
                return
        except Exception as e:
            logger.error(f"SignalEngine: subscribe failed - {e}")
            return

        while self._running.is_set():
            try:
                message = self._pubsub.get_message(timeout=0.1)
                if message and message["type"] in ("message", "pmessage"):
                    self._process_message(message)
            except redis.ConnectionError:
                logger.warning("SignalEngine: Redis connection lost, reconnecting...")
                time.sleep(1.0)
            except Exception as e:
                logger.error(f"SignalEngine: error in message loop - {e}")

        # Cleanup
        try:
            if use_pattern:
                self._pubsub.punsubscribe()
            else:
                self._pubsub.unsubscribe()
            self._pubsub.close()
        except Exception:
            pass

    def _return_fill_loop(self) -> None:
        """Background thread: periodically run ReturnFiller to backfill returns."""
        from jerry_trader.services.signal.return_fill import ReturnFiller

        filler = ReturnFiller(clickhouse_config=self._clickhouse_config)

        # Initial delay — wait for bars to accumulate before first run
        self._running.wait(timeout=60.0)

        while self._running.is_set():
            try:
                filler.run()
            except Exception as e:
                logger.error(f"SignalEngine: return fill error - {e}")
            self._running.wait(timeout=self._return_fill_interval_sec)

    @staticmethod
    def _is_replay_mode() -> bool:
        """Check if running in replay mode."""
        from jerry_trader import clock as clock_mod

        return clock_mod.is_replay()

    def _process_message(self, message: dict[str, Any]) -> None:
        """Process a single Redis pub/sub message."""
        try:
            data = json.loads(message["data"])
        except (json.JSONDecodeError, TypeError):
            return

        symbol = data.get("symbol", "").upper()
        timeframe = data.get("timeframe", "tick")
        factors = data.get("factors", {})
        timestamp_ms = data.get("timestamp_ms", 0)

        if not symbol or not factors:
            return

        price = data.get("price")
        if price is not None:
            price = float(price)

        self._evaluate_events(symbol, timeframe, factors, timestamp_ms, price)

    def _evaluate_events(
        self,
        symbol: str,
        timeframe: str,
        factors: dict[str, float],
        timestamp_ms: int,
        price: float | None = None,
    ) -> None:
        """Evaluate all events against current factor snapshot.

        Supports both boolean and ML-based events:
        - Boolean: EventEvaluator.match_signal()
        - ML: EventEvaluator.match_signal_with_ml()
        """
        if self._evaluator is None:
            return

        # Build signal dict for matching
        signal = {
            "trigger_time_ns": timestamp_ms * 1_000_000,
            "trigger_time_ms": timestamp_ms,
            "symbol": symbol,
            "trigger_price": price,
            **factors,
        }

        # Use hybrid matching (supports both boolean and ML events)
        matched_event, ml_result = self._evaluator.match_signal_with_ml(signal)

        if matched_event is None:
            return

        # Trigger fired! Check cooldown using global clock
        import jerry_trader.clock as clock_mod

        now_ms = clock_mod.now_ms()
        cooldown_key = (matched_event.name, symbol)
        last_ms = self._last_trigger.get(cooldown_key, 0)
        if now_ms - last_ms < self._trigger_cooldown_sec * 1000:
            return

        self._last_trigger[cooldown_key] = now_ms
        self._trigger_count += 1

        self._on_trigger(
            matched_event,
            symbol,
            timeframe,
            factors,
            timestamp_ms,
            price,
            ml_result=ml_result,
        )

    def _on_trigger(
        self,
        event: Event,
        symbol: str,
        timeframe: str,
        factors: dict[str, float],
        timestamp_ms: int,
        price: float | None = None,
        ml_result: MLEvaluationResult | None = None,
    ) -> None:
        """Handle a triggered event — log and persist to ClickHouse.

        Args:
            event: Matched event
            symbol: Ticker symbol
            timeframe: Timeframe string
            factors: Factor values
            timestamp_ms: Trigger timestamp
            price: Trigger price
            ml_result: ML evaluation result (for ML-based events)
        """
        factor_summary = ", ".join(f"{k}={v:.4f}" for k, v in factors.items())

        # Build log message
        log_msg = (
            f"SIGNAL TRIGGERED: event={event.name} symbol={symbol} "
            f"tf={timeframe} ts={timestamp_ms} stage={event.stage.value} "
            f"factors=[{factor_summary}]"
        )

        # Add ML info if available
        if ml_result is not None:
            log_msg += (
                f" expected_return={ml_result.expected_return:.2%} "
                f"confidence={ml_result.confidence:.2f}"
            )

        logger.info(log_msg)

        # Persist to ClickHouse via SignalStorage
        if self._storage:
            self._storage.write_signal_event(
                rule_id=event.name,  # Use event name as rule_id for compatibility
                rule_version=1,  # Events don't have versions
                symbol=symbol,
                timeframe=timeframe,
                trigger_time_ns=timestamp_ms * 1_000_000,
                factors=factors,
                trigger_price=price,
            )
