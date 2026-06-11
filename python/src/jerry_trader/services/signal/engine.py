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
import socket as _socket
import threading
import time
import uuid as _uuid
from typing import Any

import redis

from jerry_trader.domain.event import Event, TriggerType
from jerry_trader.services.backtest.event_evaluator import (
    EventEvaluator,
    MLEvaluationResult,
    TickerState,
)
from jerry_trader.services.model_registry import get_model_registry
from jerry_trader.shared.ids.redis_keys import news_processor_results_stream
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import ms_to_et, parse_to_et_datetime

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

        # Subscription state — worker threads from run_in_thread
        self._pubsubs: list[redis.client.PubSub] = []
        self._pubsub_workers: list[redis.client.PubSubWorkerThread] = []
        self._thread: threading.Thread | None = None
        self._running = threading.Event()
        self._reconnect_event = threading.Event()

        # Return fill background thread
        self._return_fill_thread: threading.Thread | None = None

        # Catalyst stream consumer thread (replaces Pub/Sub for reliability)
        self._catalyst_stream_thread: threading.Thread | None = None

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

        # Catalyst stream consumer (replaces Pub/Sub for reliability)
        self._catalyst_stream_thread = threading.Thread(
            target=self._catalyst_stream_loop,
            name="signal-catalyst-stream",
            daemon=True,
        )
        self._catalyst_stream_thread.start()

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
        self._cleanup_pubsubs()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
        if self._catalyst_stream_thread and self._catalyst_stream_thread.is_alive():
            self._catalyst_stream_thread.join(timeout=3.0)
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
        falling back to self.redis_client).

        Uses redis-py 7.x callback pattern: registers a message handler
        for each pattern and lets ``run_in_thread`` manage the read loop
        with built-in health checks and reconnection.
        """
        # Resolve which sources to subscribe to.
        # Catalyst is handled via Stream (persistent, auto-catchup), not Pub/Sub.
        if self._source_redis_clients:
            source_types = [s for s in self._source_redis_clients if s != "catalyst"]
        else:
            source_types = ["factors"]

        # Group patterns by Redis client (dedup connections to the same Redis)
        groups: dict[int, list[tuple[str, str]]] = {}
        for src in source_types:
            rc = self._source_redis_clients.get(src, self.redis_client)
            groups.setdefault(id(rc), []).append((src, f"{src}:*"))

        if not groups:
            logger.info(
                "SignalEngine: no Pub/Sub sources configured (catalyst uses Stream)"
            )
            return

        retry_delay = 1.0
        while self._running.is_set():
            # ── Create pubsub + worker threads ─────────────────────────
            self._pubsubs = []
            self._pubsub_workers = []
            try:
                for rc_key, patterns in groups.items():
                    rc = self._source_redis_clients.get(
                        patterns[0][0], self.redis_client
                    )
                    ps = rc.pubsub()

                    # Register pattern callbacks via psubscribe kwargs
                    # redis-py 7.x dispatches pmessages to these callbacks
                    # when run_in_thread() is active.
                    pattern_map = {}
                    for src_name, pattern in patterns:
                        pattern_map[pattern] = self._make_handler(src_name)
                    if pattern_map:
                        ps.psubscribe(**pattern_map)

                    # Start the worker thread (redis-py manages the read loop)
                    worker = ps.run_in_thread(
                        sleep_time=0.1,
                        daemon=True,
                        exception_handler=self._on_pubsub_error,
                    )
                    self._pubsubs.append(ps)
                    self._pubsub_workers.append(worker)

                    host = (
                        rc.get_connection_kwargs().get("host", "?")
                        if hasattr(rc, "get_connection_kwargs")
                        else "?"
                    )
                    src_names = [s for s, _ in patterns]
                    logger.info(
                        f"SignalEngine: subscribed to {src_names} on Redis {host}"
                    )
                retry_delay = 1.0  # Reset on successful connection

            except (redis.ConnectionError, redis.TimeoutError, OSError) as e:
                logger.warning(
                    f"SignalEngine: failed to connect (retry in {retry_delay:.1f}s): {e}"
                )
                self._cleanup_pubsubs()
                self._running.wait(timeout=retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)
                continue

            # ── Wait while workers are alive ───────────────────────────
            while self._running.is_set():
                # Check if any worker thread died unexpectedly, or if a
                # connection error forced a reconnect (subscriptions are
                # lost on reconnect — redis-py doesn't re-issue PSUBSCRIBE).
                all_alive = True
                for w in self._pubsub_workers:
                    if not w.is_alive():
                        logger.warning(
                            "SignalEngine: pubsub worker thread died, reconnecting..."
                        )
                        all_alive = False
                        break
                if not all_alive or self._reconnect_event.is_set():
                    if self._reconnect_event.is_set():
                        logger.warning(
                            "SignalEngine: forced reconnect due to connection error"
                        )
                    self._reconnect_event.clear()
                    self._cleanup_pubsubs()
                    break
                self._running.wait(timeout=1.0)

        self._cleanup_pubsubs()

    def _make_handler(self, source_name: str):
        """Return a callback that routes pmessages to _route_message.

        redis-py 7.x passes the message dict directly to the callback.
        """

        def handler(message: dict[str, Any]) -> None:
            logger.debug(
                f"SignalEngine: [{source_name}] received message "
                f"channel={message.get('channel', '?')}"
            )
            self._route_message(message)

        return handler

    def _on_pubsub_error(
        self,
        error: Exception,
        pubsub: redis.client.PubSub,
        worker: redis.client.PubSubWorkerThread,
    ) -> None:
        """Exception handler for PubSubWorkerThread.

        Only logs when the engine is still running — errors during shutdown
        (e.g. closed socket) are expected and ignored.
        """
        if self._running.is_set():
            logger.warning(f"SignalEngine: pubsub worker error (will retry): {error}")
            # redis-py reconnects the socket on connection errors but does
            # NOT re-issue PSUBSCRIBE — the subscription is lost server-side.
            # Signal the monitoring loop to force a full reconnect cycle.
            self._reconnect_event.set()

    def _cleanup_pubsubs(self) -> None:
        """Stop all worker threads and close pubsub connections."""
        for w in self._pubsub_workers:
            try:
                w.stop()
                w.join(timeout=2.0)
            except Exception:
                pass
        self._pubsub_workers.clear()
        for ps in self._pubsubs:
            try:
                ps.close()
            except Exception:
                pass
        self._pubsubs.clear()

    @staticmethod
    def _normalize_pubsub_message(message: dict) -> dict[str, Any]:
        """Convert pubsub message keys from bytes to str.

        When the Redis client has ``decode_responses=False`` (the default),
        pubsub message dicts use bytes keys (``b"channel"``, ``b"data"``).
        Normalize them to string keys so downstream handlers work uniformly.
        """
        if isinstance(next(iter(message.keys())), bytes):
            return {
                k.decode() if isinstance(k, bytes) else k: (
                    v.decode() if isinstance(v, bytes) else v
                )
                for k, v in message.items()
            }
        return message

    def _route_message(self, message: dict[str, Any]) -> None:
        """Route a pmessage to the correct handler based on channel prefix."""
        msg = self._normalize_pubsub_message(message)
        channel = msg.get("channel", "")
        logger.debug(f"SignalEngine: routing message channel={channel}")
        if channel.startswith("catalyst:"):
            logger.info(f"SignalEngine: routing to catalyst handler channel={channel}")
            self._handle_catalyst_message(msg)
        else:
            self._process_message(msg)

    def _catalyst_stream_loop(self) -> None:
        """Background thread: consume catalyst results from Redis Stream.

        Uses a consumer group on ``news_processor_results_stream`` so that
        after any disconnection we resume from the last acknowledged position
        — no messages are lost.  This replaces Pub/Sub for catalyst delivery.
        """
        rc = self._source_redis_clients.get("catalyst")
        if rc is None:
            logger.warning(
                "SignalEngine: no catalyst Redis client, catalyst stream disabled"
            )
            return

        stream_key = news_processor_results_stream(self._session_id)
        consumer_group = f"signal_engine_{_socket.gethostname()}"
        consumer_name = f"{consumer_group}_{_uuid.uuid4().hex[:8]}"

        # Create consumer group (idempotent)
        try:
            rc.xgroup_create(stream_key, consumer_group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        logger.info(
            f"SignalEngine: catalyst stream consumer started "
            f"group={consumer_group} stream={stream_key}"
        )

        retry_delay = 1.0
        while self._running.is_set():
            try:
                messages = rc.xreadgroup(
                    consumer_group,
                    consumer_name,
                    {stream_key: ">"},
                    count=20,
                    block=1000,
                )
                retry_delay = 1.0  # Reset on success

                if messages:
                    for _, entries in messages:
                        for msg_id, msg_data in entries:
                            if msg_data.get("is_catalyst") != "true":
                                rc.xack(stream_key, consumer_group, msg_id)
                                continue

                            # Build data dict matching pub/sub payload format
                            data: dict[str, Any] = {
                                "symbol": msg_data.get("symbol", ""),
                                "classification": msg_data.get("classification", ""),
                                "score": msg_data.get("score", ""),
                                "title": msg_data.get("title", ""),
                                "url": msg_data.get("url", ""),
                                "content_preview": msg_data.get("content_preview", ""),
                                "source_from": msg_data.get("source_from", ""),
                                "published_time": msg_data.get("published_time", ""),
                            }
                            # current_time = NewsProcessor processing time
                            current_time = msg_data.get("current_time", "")
                            data["current_time"] = current_time
                            if current_time:
                                try:
                                    from datetime import datetime

                                    dt = datetime.fromisoformat(str(current_time))
                                    data["timestamp_ms"] = int(dt.timestamp() * 1000)
                                except (ValueError, TypeError):
                                    data["timestamp_ms"] = int(time.time() * 1000)
                            else:
                                data["timestamp_ms"] = int(time.time() * 1000)

                            self._handle_catalyst_data(data)
                            rc.xack(stream_key, consumer_group, msg_id)

            except (redis.ConnectionError, redis.TimeoutError, OSError) as e:
                logger.warning(
                    f"SignalEngine: catalyst stream connection error "
                    f"(retry in {retry_delay:.1f}s): {e}"
                )
                self._running.wait(timeout=retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)
            except Exception as e:
                logger.error(f"SignalEngine: catalyst stream error: {e}")
                self._running.wait(timeout=1.0)

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
        """Handle a catalyst pub/sub message (legacy path)."""
        try:
            data = json.loads(message["data"])
        except (json.JSONDecodeError, TypeError):
            return
        self._handle_catalyst_data(data)

    def _handle_catalyst_data(self, data: dict[str, Any]) -> None:
        """Process catalyst data from any source (Pub/Sub or Stream).

        Fires CATALYST-trigger events directly — no factor data needed.
        Passes news metadata (title, classification, score) as extra context
        so notification emails can include full news details.
        """
        symbol = data.get("symbol", "").upper()
        if not symbol:
            return

        ts_ms = data.get("timestamp_ms", 0)
        ts_str = ms_to_et(ts_ms) if ts_ms else "N/A"
        title = data.get("title", "")[:60]
        score = data.get("score", "")
        logger.info(f'SignalEngine: catalyst {symbol} {score} "{title}" ts={ts_str}')

        timestamp_ms = data.get("timestamp_ms", int(time.time() * 1000))

        if self._evaluator is None:
            return

        # Format timestamps as ET for human-readable display
        published_et = ""
        fetched_et = ""
        try:
            published_raw = data.get("published_time", "")
            if published_raw:
                published_et = parse_to_et_datetime(published_raw).strftime(
                    "%Y-%m-%d %H:%M ET"
                )
        except Exception:
            pass
        try:
            current_raw = data.get("current_time", "")
            if current_raw:
                fetched_et = parse_to_et_datetime(current_raw).strftime(
                    "%Y-%m-%d %H:%M ET"
                )
        except Exception:
            pass

        # Build extra context with news details for notification templates
        extra: dict[str, Any] = {
            "catalyst_title": data.get("title", ""),
            "catalyst_classification": data.get("classification", ""),
            "catalyst_score_raw": data.get("score", ""),
            "catalyst_url": data.get("url", ""),
            "catalyst_content": data.get("content_preview", ""),
            "catalyst_source": data.get("source_from", ""),
            "catalyst_published_time": published_et,
            "catalyst_fetched_time": fetched_et,
        }

        catalyst_events = [
            e for e in self._evaluator.events if e.trigger == TriggerType.CATALYST
        ]
        for event in catalyst_events:

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
        # Build concise trigger log — full details are in the notification log
        fields = [f"event={event.name}", f"symbol={symbol}"]
        if extra:
            score = extra.get("catalyst_score_raw", "")
            if score:
                fields.append(f"score={score}")
            classification = extra.get("catalyst_classification", "")
            if classification:
                fields.append(f"class={classification}")
            title = extra.get("catalyst_title", "")
            if title:
                fields.append(f"title={title}")
        fields.append(f"ts={ms_to_et(timestamp_ms)}")
        if price is not None:
            fields.append(f"price={price:.4f}")
        if ml_result is not None:
            fields.append(
                f"exp_ret={ml_result.expected_return:.2%} "
                f"conf={ml_result.confidence:.2f}"
            )

        logger.info("SIGNAL TRIGGERED: " + " | ".join(fields))

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
