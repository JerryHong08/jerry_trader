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

from jerry_trader.domain.event import Event, TriggerType
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
        session_id: str = "",
        notification_manager: Any | None = None,
        event_names: list[str] | None = None,
        source_redis_clients: dict[str, redis.Redis] | None = None,
    ):
        self.redis_client = redis_client
        self.events_config_path = events_config_path
        self.symbols = [s.upper() for s in symbols] if symbols else []
        self.timeframes = timeframes or ["tick", "10s", "1m", "5m"]
        self._storage = storage
        self._clickhouse_config = clickhouse_config
        self._return_fill_interval_sec = return_fill_interval_sec
        self._session_id = session_id
        self._notification_manager = notification_manager

        # Event whitelist — if set, only these event names are active
        self._event_names: list[str] | None = event_names

        # Per-source Redis clients for pub/sub.
        # Maps source type ("factors", "catalyst") → Redis client.
        # Sources not in this dict fall back to self.redis_client.
        self._source_redis_clients = source_redis_clients or {}

        # Event evaluator (shared logic with backtest)
        self._evaluator: EventEvaluator | None = None

        # Ticker state tracking for stage-based evaluation
        self._ticker_states: dict[str, TickerState] = {}
        self._states_lock = threading.Lock()

        # Subscription state — one pubsub per unique Redis client
        self._pubsubs: list[redis.client.PubSub] = []
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

        If _event_names is set, only those events are kept active.
        Anti-patterns are always loaded (not affected by event filter).

        Returns:
            Number of events loaded.
        """
        self._evaluator = EventEvaluator(config_path=self.events_config_path)

        # Filter events by whitelist if configured
        if self._event_names:
            whitelist = set(self._event_names)
            self._evaluator.events = [
                e for e in self._evaluator.events if e.name in whitelist
            ]
            missing = whitelist - {e.name for e in self._evaluator.events}
            if missing:
                logger.warning(
                    f"SignalEngine: event_names not found in config: {missing}"
                )

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
        for ps in self._pubsubs:
            try:
                ps.punsubscribe()
                ps.close()
            except Exception:
                pass
        self._pubsubs.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        if self._return_fill_thread and self._return_fill_thread.is_alive():
            self._return_fill_thread.join(timeout=5.0)
        logger.info(f"SignalEngine: stopped (triggers fired: {self._trigger_count})")

    # ─────────────────────────────────────────────────────────────────────────
    # Main Loop
    # ─────────────────────────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        """Background thread: subscribe to configured sources and process messages.

        Each trigger source ("factors", "catalyst") subscribes to
        ``{source}:*`` on its own Redis client (from source_redis_clients,
        falling back to self.redis_client). Pub/sub connections are
        grouped by Redis client to minimise connections.
        """
        # Resolve which sources to subscribe to.
        # When subscriptions are explicitly configured, only subscribe to
        # those sources. Otherwise subscribe to all on the main Redis.
        if self._source_redis_clients:
            source_types = list(self._source_redis_clients.keys())
        else:
            source_types = ["factors", "catalyst"]  # backward compat

        # Group patterns by Redis client (dedup connections to the same Redis)
        # redis_client → list of (source_type, pattern)
        groups: dict[int, list[tuple[str, str]]] = {}
        for src in source_types:
            rc = self._source_redis_clients.get(src, self.redis_client)
            key = id(rc)
            if key not in groups:
                groups[key] = []
            groups[key].append((src, f"{src}:*"))

        # Create one pubsub per unique Redis client
        self._pubsubs = []
        for rc_key, patterns in groups.items():
            # Get the Redis client (any pattern's client works — same key = same rc)
            rc = self._source_redis_clients.get(patterns[0][0], self.redis_client)
            ps = rc.pubsub()
            ps.psubscribe(*[p for _, p in patterns])
            self._pubsubs.append(ps)
            host = (
                rc.get_connection_kwargs().get("host", "?")
                if hasattr(rc, "get_connection_kwargs")
                else "?"
            )
            src_names = [s for s, _ in patterns]
            logger.info(f"SignalEngine: subscribed to {src_names} on Redis {host}")

        if not self._pubsubs:
            logger.warning("SignalEngine: no subscriptions configured")
            return

        while self._running.is_set():
            for ps in self._pubsubs:
                try:
                    message = ps.get_message(timeout=0.05)
                    if message and message["type"] == "pmessage":
                        self._route_message(message)
                except redis.ConnectionError:
                    logger.warning("SignalEngine: Redis connection lost")
                    time.sleep(1.0)
                except Exception as e:
                    logger.error(f"SignalEngine: error in message loop - {e}")

        # Cleanup
        for ps in self._pubsubs:
            try:
                ps.punsubscribe()
                ps.close()
            except Exception:
                pass
        self._pubsubs.clear()

    def _route_message(self, message: dict[str, Any]) -> None:
        """Route a pmessage to the correct handler based on channel prefix."""
        channel = message.get("channel", b"")
        if isinstance(channel, bytes):
            channel = channel.decode()
        if channel.startswith("catalyst:"):
            self._handle_catalyst_message(message)
        else:
            self._process_message(message)

    def _return_fill_loop(self) -> None:
        """Background thread: periodically run ReturnFiller to backfill returns."""
        from jerry_trader.services.signal.return_fill import ReturnFiller

        filler = ReturnFiller(
            clickhouse_config=self._clickhouse_config, session_id=self._session_id
        )

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

    def _handle_catalyst_message(self, message: dict[str, Any]) -> None:
        """Handle a catalyst pub/sub message from NewsProcessor.

        Fires CATALYST-trigger events directly — no factor data needed.
        Each catalyst message maps to one _on_trigger call per matching event.

        Passes news metadata (title, classification, score) as extra context
        so notification emails can include full news details.
        """
        try:
            data = json.loads(message["data"])
        except (json.JSONDecodeError, TypeError):
            return

        symbol = data.get("symbol", "").upper()
        if not symbol:
            return

        timestamp_ms = data.get("timestamp_ms", int(time.time() * 1000))

        if self._evaluator is None:
            return

        # Build extra context with news details for notification templates
        extra: dict[str, Any] = {
            "catalyst_title": data.get("title", ""),
            "catalyst_classification": data.get("classification", ""),
            "catalyst_score_raw": data.get("score", ""),
            "catalyst_url": data.get("url", ""),
            "catalyst_content": data.get("content_preview", ""),
            "catalyst_source": data.get("source_from", ""),
        }

        # Find events with CATALYST trigger and fire _on_trigger
        for event in self._evaluator.events:
            if event.trigger != TriggerType.CATALYST:
                continue

            # Cooldown check
            cooldown_key = (event.name, symbol)
            last_ms = self._last_trigger.get(cooldown_key, 0)
            if timestamp_ms - last_ms < self._trigger_cooldown_sec * 1000:
                continue

            self._last_trigger[cooldown_key] = timestamp_ms
            self._trigger_count += 1

            # Build minimal factors dict from catalyst payload
            factors: dict[str, float] = {}
            score = data.get("score", "")
            if score:
                try:
                    # "7/10" → 7.0
                    factors["catalyst_score"] = float(score.split("/")[0])
                except (ValueError, IndexError):
                    pass

            self._on_trigger(
                event=event,
                symbol=symbol,
                timeframe="catalyst",
                factors=factors,
                timestamp_ms=timestamp_ms,
                price=None,
                extra=extra,
            )

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
        extra: dict[str, Any] | None = None,
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
            extra: Additional context (e.g., catalyst news details)
        """
        # Build log message — include extra context for catalyst events
        parts = [f"event={event.name}", f"symbol={symbol}", f"tf={timeframe}"]
        if extra:
            title = extra.get("catalyst_title", "")
            classification = extra.get("catalyst_classification", "")
            score = extra.get("catalyst_score_raw", "")
            if title:
                parts.append(f"title={title[:80]}")
            if classification:
                parts.append(f"classification={classification}")
            if score:
                parts.append(f"score={score}")

        factor_summary = ", ".join(f"{k}={v:.4f}" for k, v in factors.items())
        log_msg = (
            f"SIGNAL TRIGGERED: {' '.join(parts)} "
            f"ts={timestamp_ms} stage={event.stage.value} "
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

        # Email notification (if event has notify config)
        if self._notification_manager:
            try:
                self._notification_manager.evaluate(
                    event=event,
                    symbol=symbol,
                    timeframe=timeframe,
                    factors=factors,
                    price=price,
                    ml_result=ml_result,
                    extra=extra,
                )
            except Exception as e:
                logger.error(
                    f"Notification error for event={event.name} symbol={symbol}: {e}"
                )
