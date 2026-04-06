"""Signal Engine — real-time DSL rule evaluation.

Subscribes to FactorEngine Redis channels, evaluates loaded DSL rules
against incoming factor updates, and emits trigger events.

Usage:
    engine = SignalEngine(redis_client, rules_dir="config/rules/")
    engine.start()  # starts background thread
    engine.stop()   # graceful shutdown
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import redis

from jerry_trader.domain.strategy.rule import ComparisonOp, Condition, Rule, TriggerType
from jerry_trader.domain.strategy.rule_parser import load_rules_from_dir
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("signal_engine", log_to_file=True)


# ─────────────────────────────────────────────────────────────────────────────
# Condition Evaluator
# ─────────────────────────────────────────────────────────────────────────────


def evaluate_condition(
    condition: Condition,
    factors: dict[str, float],
) -> bool:
    """Evaluate a single condition against current factor values.

    Args:
        condition: The condition to evaluate.
        factors: Current factor values {name: value}.

    Returns:
        True if condition is satisfied.
    """
    value = factors.get(condition.factor)
    if value is None:
        return False

    if condition.op in (
        ComparisonOp.GT,
        ComparisonOp.LT,
        ComparisonOp.GTE,
        ComparisonOp.LTE,
        ComparisonOp.EQ,
    ):
        assert condition.value is not None
        if condition.op == ComparisonOp.GT:
            return value > condition.value
        elif condition.op == ComparisonOp.LT:
            return value < condition.value
        elif condition.op == ComparisonOp.GTE:
            return value >= condition.value
        elif condition.op == ComparisonOp.LTE:
            return value <= condition.value
        else:  # EQ
            return abs(value - condition.value) < 1e-9
    elif condition.op == ComparisonOp.BETWEEN:
        assert condition.lo is not None and condition.hi is not None
        return condition.lo <= value <= condition.hi
    elif condition.op in (ComparisonOp.CROSS_ABOVE, ComparisonOp.CROSS_BELOW):
        # Cross requires historical buffer — not supported in Phase 1
        logger.debug(f"Cross operator not yet supported: {condition.op}")
        return False
    else:
        return False


def evaluate_trigger(
    conditions: list[Condition],
    trigger_type: TriggerType,
    factors: dict[str, float],
) -> bool:
    """Evaluate a trigger's conditions against factor values.

    Args:
        conditions: List of conditions.
        trigger_type: AND or OR composition.
        factors: Current factor values.

    Returns:
        True if trigger fires.
    """
    if not conditions:
        return False

    results = [evaluate_condition(c, factors) for c in conditions]

    if trigger_type == TriggerType.AND:
        return all(results)
    else:  # OR
        return any(results)


# ─────────────────────────────────────────────────────────────────────────────
# Signal Engine
# ─────────────────────────────────────────────────────────────────────────────


class SignalEngine:
    """Real-time DSL rule evaluation engine.

    Subscribes to FactorEngine Redis pub/sub channels and evaluates
    loaded DSL rules against incoming factor updates.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        rules_dir: str = "config/rules/",
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        storage: Any | None = None,
        clickhouse_config: dict | None = None,
        return_fill_interval_sec: float = 120.0,
    ):
        self.redis_client = redis_client
        self.rules_dir = rules_dir
        self.symbols = [s.upper() for s in symbols] if symbols else []
        self.timeframes = timeframes or ["trade", "10s", "1m", "5m"]
        self._storage = storage
        self._clickhouse_config = clickhouse_config
        self._return_fill_interval_sec = return_fill_interval_sec

        # Loaded rules
        self._rules: list[Rule] = []
        self._rules_lock = threading.Lock()

        # Subscription state
        self._pubsub: redis.client.PubSub | None = None
        self._thread: threading.Thread | None = None
        self._running = threading.Event()

        # Return fill background thread
        self._return_fill_thread: threading.Thread | None = None

        # Trigger dedup: prevent logging same trigger within cooldown period
        # Key: (rule_id, symbol), Value: last_trigger_time_ms
        self._last_trigger: dict[tuple[str, str], int] = {}
        self._trigger_cooldown_sec: float = 60.0  # Minimum 60s between same trigger

        self._trigger_count: int = 0

    @property
    def trigger_count(self) -> int:
        return self._trigger_count

    @property
    def rules(self) -> list[Rule]:
        """Current loaded rules (copy)."""
        with self._rules_lock:
            return list(self._rules)

    # ─────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    def load_rules(self) -> int:
        """Load DSL rules from rules directory.

        Returns:
            Number of rules loaded.
        """
        rules = load_rules_from_dir(self.rules_dir)
        enabled = [r for r in rules if r.enabled]

        with self._rules_lock:
            self._rules = enabled

        logger.info(
            f"SignalEngine: loaded {len(enabled)} enabled rules "
            f"from {self.rules_dir}"
        )
        for rule in enabled:
            logger.info(f"  Rule: {rule.id} v{rule.version} — {rule.name}")

        return len(enabled)

    def start(self) -> None:
        """Start the signal engine in a background thread."""
        if self._running.is_set():
            logger.warning("SignalEngine: already running")
            return

        self.load_rules()

        self._running.set()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="signal-engine",
            daemon=True,
        )
        self._thread.start()
        logger.info("SignalEngine: started")

        # Start return fill background thread in live mode only.
        # In replay mode, run ReturnFiller once after replay completes.
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
        timeframe = data.get("timeframe", "trade")
        factors = data.get("factors", {})
        timestamp_ms = data.get("timestamp_ms", 0)

        if not symbol or not factors:
            return

        price = data.get("price")
        if price is not None:
            price = float(price)

        self._evaluate_rules(symbol, timeframe, factors, timestamp_ms, price)

    def _evaluate_rules(
        self,
        symbol: str,
        timeframe: str,
        factors: dict[str, float],
        timestamp_ms: int,
        price: float | None = None,
    ) -> None:
        """Evaluate all rules against current factor snapshot."""
        with self._rules_lock:
            rules = list(self._rules)

        for rule in rules:
            # Check if rule has conditions for this timeframe
            rule_timeframes = {c.timeframe for c in rule.trigger.conditions}
            if timeframe not in rule_timeframes:
                continue

            # Only evaluate conditions relevant to this timeframe
            matching_conditions = [
                c for c in rule.trigger.conditions if c.timeframe == timeframe
            ]

            # For AND trigger: we need ALL conditions met.
            # If some conditions are for different timeframes, we can't evaluate
            # them here — skip full evaluation until we have multi-timeframe support.
            if rule.trigger.type == TriggerType.AND:
                # Phase 1: only evaluate if ALL conditions match current timeframe
                if len(matching_conditions) != len(rule.trigger.conditions):
                    continue

            if not evaluate_trigger(matching_conditions, rule.trigger.type, factors):
                continue

            # Trigger fired! Check cooldown using global clock
            import jerry_trader.clock as clock_mod

            now_ms = clock_mod.now_ms()
            cooldown_key = (rule.id, symbol)
            last_ms = self._last_trigger.get(cooldown_key, 0)
            if now_ms - last_ms < self._trigger_cooldown_sec * 1000:
                continue

            self._last_trigger[cooldown_key] = now_ms
            self._trigger_count += 1

            self._on_trigger(rule, symbol, timeframe, factors, timestamp_ms, price)

    def _on_trigger(
        self,
        rule: Rule,
        symbol: str,
        timeframe: str,
        factors: dict[str, float],
        timestamp_ms: int,
        price: float | None = None,
    ) -> None:
        """Handle a triggered rule — log and persist to ClickHouse."""
        factor_summary = ", ".join(f"{k}={v:.4f}" for k, v in factors.items())

        logger.info(
            f"SIGNAL TRIGGERED: rule={rule.id} symbol={symbol} "
            f"tf={timeframe} ts={timestamp_ms} factors=[{factor_summary}]"
        )

        # Persist to ClickHouse via SignalStorage
        if self._storage:
            self._storage.write_signal_event(
                rule_id=rule.id,
                rule_version=rule.version,
                symbol=symbol,
                timeframe=timeframe,
                trigger_time_ns=timestamp_ms * 1_000_000,
                factors=factors,
                trigger_price=price,
            )
