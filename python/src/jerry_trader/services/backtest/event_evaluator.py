"""Event Evaluator - Boolean signal selection.

Based on Event Framework Validation:
- Use Boolean filter (accept/reject), not factor ranking
- Match signals to predefined events
- Validate with Avg Return + Win Rate (not IC)

Stage Architecture (roadmap/event-stage-architecture.md):
- WATCH: 选股阶段 - FIRST_ENTRY trigger
- ENTRY: 入场阶段 - CONTINUOUS trigger + pre_condition
- EXIT: 出场阶段 - CONTINUOUS trigger + has_position

ML Integration (roadmap/ml-event-architecture.md):
- ML-based events use ReturnPredictor for entry decisions
- hard_constraints always checked before ML prediction
- Returns (should_enter, expected_return, confidence) tuple
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from jerry_trader.domain.event import (
    Condition,
    Event,
    EventAction,
    EventStage,
    ModelConfig,
    TriggerType,
)
from jerry_trader.domain.session import SessionPhase, get_session_phase_from_ns
from jerry_trader.services.backtest.ml_model import ReturnPredictor
from jerry_trader.services.model_registry import get_model_registry

logger = logging.getLogger(__name__)


@dataclass
class MLEvaluationResult:
    """Result from ML-based event evaluation.

    For ML-based events, returns prediction results instead of boolean match.
    """

    should_enter: bool
    expected_return: float
    confidence: float
    event: Event
    signal: dict
    reason: str = ""


@dataclass
class TickerState:
    """State tracking for a ticker during stage-based evaluation.

    Tracks the progress through WATCH → ENTRY → EXIT flow:
    - watch_triggered: 是否触发了 WATCH 事件（进入观察列表）
    - entry_triggered: 是否触发了 ENTRY 事件（已入场）
    - has_position: 是否有持仓

    This enables pre_condition checking for ENTRY/EXIT events.
    """

    symbol: str
    watch_triggered: bool = False
    watch_time_ms: int = 0
    watch_event_name: str = ""
    entry_triggered: bool = False
    entry_time_ms: int = 0
    entry_price: float = 0.0
    entry_event_name: str = ""
    has_position: bool = False
    triggered_events: dict[str, int] = field(
        default_factory=dict
    )  # event_name -> trigger_time_ms
    abandoned: bool = False  # 是否已放弃监听（超时或其他原因）


# 超时配置: 进入 WATCH list 后 30 分钟无入场机会则放弃
WATCH_TIMEOUT_MS = 30 * 60 * 1000  # 30 min


def compute_signal_density(
    factor_ts: dict[int, dict[str, float]],
    base_factor: str = "trade_rate",
    threshold: float = 150.0,
    window_minutes: int = 5,
) -> dict[int, int]:
    """Compute signal density for each timestamp.

    Signal density = number of times base_factor > threshold in rolling window.

    Args:
        factor_ts: Factor timeseries dict[ts_ms, dict[factor_name, value]]
        base_factor: Factor to check (e.g., "trade_rate")
        threshold: Threshold for counting as a signal
        window_minutes: Rolling window in minutes

    Returns:
        Dict[ts_ms, signal_density] for each timestamp
    """
    window_ms = window_minutes * 60 * 1000
    signal_times: list[int] = []
    results: dict[int, int] = {}

    for ts_ms in sorted(factor_ts.keys()):
        factors = factor_ts[ts_ms]
        factor_value = factors.get(base_factor, 0)

        # Check if base condition met
        if factor_value > threshold:
            signal_times.append(ts_ms)

        # Remove old signals outside window
        cutoff = ts_ms - window_ms
        signal_times = [t for t in signal_times if t > cutoff]

        results[ts_ms] = len(signal_times)

    return results


def add_signal_density_to_factor_ts(
    factor_ts: dict[int, dict[str, float]],
    configs: list[dict] | None = None,
) -> dict[int, dict[str, float]]:
    """Add signal_density factors to factor timeseries.

    Computes multiple signal_density variants and adds them to factor_ts.

    Args:
        factor_ts: Factor timeseries dict[ts_ms, dict[factor_name, value]]
        configs: List of signal_density configs, each with:
            - base_factor: Factor to check (default: "trade_rate")
            - threshold: Threshold for counting (default: 150.0)
            - window_minutes: Rolling window (default: 5)
            - name: Output factor name (default: "signal_density_{window}m")

    Returns:
        Updated factor_ts with signal_density factors added
    """
    if configs is None:
        # Default configs: two variants
        configs = [
            {"base_factor": "trade_rate", "threshold": 150.0, "window_minutes": 5},
            {"base_factor": "trade_rate", "threshold": 100.0, "window_minutes": 3},
        ]

    for cfg in configs:
        base_factor = cfg.get("base_factor", "trade_rate")
        threshold = cfg.get("threshold", 150.0)
        window_minutes = cfg.get("window_minutes", 5)
        name = cfg.get("name", f"signal_density_{window_minutes}m")

        density_ts = compute_signal_density(
            factor_ts,
            base_factor=base_factor,
            threshold=threshold,
            window_minutes=window_minutes,
        )

        # Add to factor_ts
        for ts_ms, density in density_ts.items():
            if ts_ms in factor_ts:
                factor_ts[ts_ms][name] = float(density)

    return factor_ts


class EventEvaluator:
    """Match signals to events using Boolean filters.

    Usage:
        evaluator = EventEvaluator()

        # Match single signal
        signal = {"trade_rate": 12, "price_direction": 0.4, "gap_percent": 0.1, ...}
        event = evaluator.match_signal(signal)
        if event:
            print(f"Matched: {event.name}")

        # Filter batch of signals
        matched = evaluator.filter_signals(signals_list)
    """

    events: list[Event]
    anti_patterns: list[Event]

    def __init__(
        self,
        config_path: Optional[str] = None,
        events: Optional[list[Event]] = None,
        anti_patterns: Optional[list[Event]] = None,
    ):
        """Initialize event evaluator.

        Args:
            config_path: Path to events.yaml config file
            events: Custom events list (overrides config)
            anti_patterns: Custom anti-patterns list (overrides config)
        """
        if events is not None:
            self.events = events
        elif config_path is not None:
            self.events = self._load_events_from_yaml(config_path)
        else:
            raise ValueError("Either config_path or events must be provided")

        if anti_patterns is not None:
            self.anti_patterns = anti_patterns
        elif config_path is not None:
            self.anti_patterns = self._load_anti_patterns_from_yaml(config_path)
        else:
            self.anti_patterns = []  # No anti-patterns if not specified

    def _load_events_from_yaml(self, config_path: str) -> list[Event]:
        """Load events from YAML config file."""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Event config file not found: {config_path}")

        config = yaml.safe_load(config_file.read_text())
        events_data = config.get("events", [])
        return [self._parse_event_yaml(e) for e in events_data]

    def _load_anti_patterns_from_yaml(self, config_path: str) -> list[Event]:
        """Load anti-patterns from YAML config file."""
        config_file = Path(config_path)
        if not config_file.exists():
            return []  # No anti-patterns if file doesn't exist

        config = yaml.safe_load(config_file.read_text())
        anti_data = config.get("anti_patterns", [])
        return [self._parse_event_yaml(e) for e in anti_data]

    def _parse_event_yaml(self, yaml_event: dict) -> Event:
        """Parse YAML event definition to Event object.

        Supports both boolean conditions and ML model configuration:
        - Boolean: conditions list with factor, op, value
        - ML: model config with name, min_expected_return, min_confidence
        - Hard constraints: always checked, even for ML-based events
        """
        # Parse boolean conditions (for backward compat)
        conditions = [
            Condition(
                factor=c["factor"],
                op=c["op"],
                value=c["value"],
            )
            for c in yaml_event.get("conditions", [])
        ]

        # Parse hard constraints (always checked, even for ML events)
        hard_constraints = [
            Condition(
                factor=c["factor"],
                op=c["op"],
                value=c["value"],
            )
            for c in yaml_event.get("hard_constraints", [])
        ]

        # Parse ML model configuration (optional)
        model_config = None
        model_data = yaml_event.get("model")
        if model_data:
            model_config = ModelConfig(
                name=model_data["name"],
                min_expected_return=model_data.get("min_expected_return", 0.02),
                min_confidence=model_data.get("min_confidence", 0.0),
            )

        validation = yaml_event.get("validation", {})

        action_str = yaml_event.get("action", "ACCEPT")
        action = EventAction.AVOID if action_str == "AVOID" else EventAction.ACCEPT

        # Parse trigger type (default: continuous for backward compat)
        trigger_str = yaml_event.get("trigger", "continuous")
        try:
            trigger = TriggerType(trigger_str)
        except ValueError:
            trigger = TriggerType.CONTINUOUS

        # Parse stage (default: entry for backward compat)
        stage_str = yaml_event.get("stage", "entry")
        try:
            stage = EventStage(stage_str)
        except ValueError:
            stage = EventStage.ENTRY

        # Parse pre_condition (default: None)
        pre_condition = yaml_event.get("pre_condition")

        # Parse notify config (optional)
        notify_config = None
        notify_data = yaml_event.get("notify")
        if notify_data and notify_data.get("enabled", False):
            from jerry_trader.domain.event import NotifyConfig

            extra_conditions = [
                Condition(factor=c["factor"], op=c["op"], value=c["value"])
                for c in notify_data.get("extra_conditions", [])
            ]
            notify_config = NotifyConfig(
                enabled=True,
                extra_conditions=extra_conditions,
                catalysts=notify_data.get("catalysts", False),
                cooldown_sec=notify_data.get("cooldown_sec", 300),
                subject_template=notify_data.get(
                    "subject_template", "JerryTrader: {event_name} on {symbol}"
                ),
                body_template=notify_data.get(
                    "body_template",
                    "Symbol: {symbol}\nEvent: {event_name}\nTimeframe: {timeframe}\nPrice: {price}\nFactors: {factors}",
                ),
            )

        return Event(
            name=yaml_event["name"],
            semantic=yaml_event.get("description", ""),
            trigger=trigger,
            stage=stage,
            pre_condition=pre_condition,
            conditions=conditions,
            hard_constraints=hard_constraints,
            model=model_config,
            expected_return=validation.get("expected_return", 0.0),
            min_win_rate=validation.get("min_win_rate", 0.0),
            signal_count=validation.get("signal_count", 0),
            action=action,
            notify=notify_config,
        )

    def match_signal(self, signal: dict) -> Optional[Event]:
        """Match signal to event using Boolean filters.

        Process:
        1. First check anti-patterns → reject if matched
        2. Then check valid events → accept if matched
        3. Return None if no match

        Args:
            signal: Dict with factor values (trade_rate, price_direction, etc.)

        Returns:
            Event if matched and ACCEPT, None if rejected or no match
        """
        # Ensure session_phase is computed
        if "session_phase" not in signal and "trigger_time_ns" in signal:
            phase = get_session_phase_from_ns(signal["trigger_time_ns"])
            signal["session_phase"] = phase.value

        # First check anti-patterns (explicit reject)
        for anti in self.anti_patterns:
            if anti.matches(signal):
                return None  # Explicit reject - anti-pattern matched

        # Then check valid events (accept)
        for event in self.events:
            if event.matches(signal):
                return event  # Accept - valid event matched

        return None  # No match

    def filter_signals(self, signals: list[dict]) -> list[tuple[dict, Event]]:
        """Filter signals to those matching valid events.

        Args:
            signals: List of signal dicts

        Returns:
            List of (signal, matched_event) tuples
        """
        matched = []
        for signal in signals:
            event = self.match_signal(signal)
            if event is not None and not event.is_anti_pattern():
                matched.append((signal, event))
        return matched

    def classify_signals(self, signals: list[dict]) -> dict[str, list[dict]]:
        """Classify signals by matched event name.

        Args:
            signals: List of signal dicts

        Returns:
            Dict mapping event_name -> list of matched signals
        """
        classified: dict[str, list[dict]] = {}
        for signal in signals:
            event = self.match_signal(signal)
            if event is not None:
                event_name = event.name
                if event_name not in classified:
                    classified[event_name] = []
                classified[event_name].append(signal)
        return classified

    def evaluate_ml_event(
        self,
        event: Event,
        signal: dict,
    ) -> MLEvaluationResult:
        """Evaluate ML-based event with prediction.

        For ML-based events (event.uses_ml() == True):
        1. Check hard_constraints first (explicit risk control)
        2. Load model from registry
        3. Predict expected_return
        4. Compute confidence
        5. Return (should_enter, expected_return, confidence)

        Args:
            event: Event with model configuration
            signal: Signal dict with factor values

        Returns:
            MLEvaluationResult with prediction results
        """
        if not event.uses_ml():
            raise ValueError(f"Event {event.name} does not use ML model")

        if event.model is None:
            raise ValueError(f"Event {event.name} has no model configuration")

        # Step 1: Check hard constraints (explicit risk control)
        if not event.matches_hard_constraints(signal):
            return MLEvaluationResult(
                should_enter=False,
                expected_return=0.0,
                confidence=0.0,
                event=event,
                signal=signal,
                reason="Hard constraints not satisfied",
            )

        # Step 2: Load model from registry
        registry = get_model_registry()
        try:
            predictor = registry.get(event.model.name)
        except KeyError:
            logger.error(f"Model '{event.model.name}' not found in registry")
            return MLEvaluationResult(
                should_enter=False,
                expected_return=0.0,
                confidence=0.0,
                event=event,
                signal=signal,
                reason=f"Model '{event.model.name}' not found",
            )

        # Step 3: Predict expected return
        # Extract factors from signal
        factors = {k: v for k, v in signal.items() if isinstance(v, (int, float))}

        try:
            expected_return = predictor.predict(factors)
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return MLEvaluationResult(
                should_enter=False,
                expected_return=0.0,
                confidence=0.0,
                event=event,
                signal=signal,
                reason=f"Prediction error: {e}",
            )

        # Step 4: Compute confidence
        # Use simple confidence based on distance from threshold
        distance = expected_return - event.model.min_expected_return
        if distance > 0:
            confidence = min(
                1.0, 0.5 + distance / (event.model.min_expected_return + 0.05) * 0.5
            )
        else:
            confidence = max(
                0.0, 0.5 + distance / event.model.min_expected_return * 0.5
            )

        # Step 5: Decision
        should_enter = (
            expected_return >= event.model.min_expected_return
            and confidence >= event.model.min_confidence
        )

        # Build reason
        if should_enter:
            reason = (
                f"ML prediction: expected_return={expected_return:.2%} >= "
                f"threshold={event.model.min_expected_return:.2%}, "
                f"confidence={confidence:.2f}"
            )
        else:
            if expected_return < event.model.min_expected_return:
                reason = (
                    f"ML prediction: expected_return={expected_return:.2%} < "
                    f"threshold={event.model.min_expected_return:.2%}"
                )
            else:
                reason = (
                    f"ML prediction: confidence={confidence:.2f} < "
                    f"min_confidence={event.model.min_confidence:.2f}"
                )

        return MLEvaluationResult(
            should_enter=should_enter,
            expected_return=expected_return,
            confidence=confidence,
            event=event,
            signal=signal,
            reason=reason,
        )

    def match_signal_with_ml(
        self,
        signal: dict,
    ) -> tuple[Optional[Event], Optional[MLEvaluationResult]]:
        """Match signal to event, supporting both boolean and ML events.

        Process:
        1. Check anti-patterns → reject if matched
        2. For each event:
           - If ML-based: evaluate_ml_event()
           - If boolean: event.matches()
        3. Return (event, ml_result) tuple

        Args:
            signal: Dict with factor values

        Returns:
            Tuple of (matched_event, ml_result)
            - ml_result is None for boolean events
            - Both None if no match
        """
        # Ensure session_phase is computed
        if "session_phase" not in signal and "trigger_time_ns" in signal:
            phase = get_session_phase_from_ns(signal["trigger_time_ns"])
            signal["session_phase"] = phase.value

        # First check anti-patterns (explicit reject)
        for anti in self.anti_patterns:
            if anti.matches(signal):
                return None, None  # Explicit reject

        # Then check valid events
        for event in self.events:
            if event.uses_ml():
                # ML-based event
                result = self.evaluate_ml_event(event, signal)
                if result.should_enter:
                    return event, result
            else:
                # Boolean event
                if event.matches(signal):
                    return event, None

        return None, None  # No match

    def evaluate_ticker(
        self,
        event: Event,
        symbol: str,
        factor_ts: dict[int, dict[str, float]],
        session_start_ms: int,
        session_end_ms: int,
        trigger_prices: dict[int, float] | None = None,
        first_entry_ms: int | None = None,
        ticker_state: TickerState | None = None,
    ) -> tuple[list[dict], TickerState]:
        """Evaluate single Event against ticker's factor timeseries.

        Stage-based evaluation flow:
        1. Check pre_condition (if any)
        2. Evaluate based on trigger type
        3. Update TickerState on match

        Args:
            event: Event definition with Boolean conditions.
            symbol: Ticker symbol.
            factor_ts: Factor timeseries dict[ts_ms, dict[factor_name, value]].
            session_start_ms: Session start epoch ms.
            session_end_ms: Session end epoch ms.
            trigger_prices: Optional dict[ts_ms, price] for trigger price lookup.
            first_entry_ms: Timestamp when ticker first entered top20.
            ticker_state: State tracking for stage-based evaluation.

        Returns:
            Tuple of (signals, updated_ticker_state).
        """
        if ticker_state is None:
            ticker_state = TickerState(symbol=symbol)

        # ── Signal Deduplication Guards ────────────────────────────────────────
        # Prevent re-evaluation after signal already triggered

        # Guard 1: ENTRY stage already triggered → skip entirely
        if event.stage == EventStage.ENTRY and ticker_state.entry_triggered:
            return [], ticker_state  # No more signals for this ticker

        # Guard 2: WATCH stage already triggered → skip (use FIRST_ENTRY trigger only)
        if event.stage == EventStage.WATCH and ticker_state.watch_triggered:
            return [], ticker_state

        # Guard 3: EXIT stage without position → skip
        if event.stage == EventStage.EXIT and not ticker_state.has_position:
            return [], ticker_state

        # ───────────────────────────────────────────────────────────────────────

        # Check pre_condition
        if not self._check_pre_condition(event, ticker_state):
            return [], ticker_state

        signals: list[dict] = []

        # Handle trigger-based evaluation
        if event.trigger == TriggerType.FIRST_ENTRY:
            # Only evaluate at the moment ticker enters top20
            if first_entry_ms is None or first_entry_ms <= 0:
                return [], ticker_state

            # Find the closest timestamp in factor_ts
            ts_ms = self._find_closest_timestamp(factor_ts, first_entry_ms)
            if ts_ms is None:
                return [], ticker_state

            # Get factors at trigger point
            factors = factor_ts.get(ts_ms)
            if factors is None:
                return [], ticker_state

            # Build signal and check conditions
            signal = self._build_signal(
                ts_ms, symbol, factors, trigger_prices, session_start_ms, session_end_ms
            )

            # Anti-patterns: 只适用于 ENTRY stage, 不阻止 WATCH stage 的选股
            # WATCH stage 是 FIRST_ENTRY trigger，用于筛选候选股，不应被 anti-patterns 阻止
            # ENTRY stage 是 CONTINUOUS trigger，用于入场决策，应该检查 anti-patterns
            if event.stage == EventStage.ENTRY and self._check_anti_patterns(signal):
                return [], ticker_state

            # Check event conditions
            if event.matches(signal):
                signal["event_name"] = event.name
                signal["event_stage"] = event.stage.value
                self._update_state_on_match(ticker_state, event, signal)
                return [signal], ticker_state

            return [], ticker_state

        # CONTINUOUS mode: evaluate all timestamps
        for ts_ms, factors in factor_ts.items():
            # Filter to session window
            if not session_start_ms <= ts_ms < session_end_ms:
                continue

            # ── Stage-specific early termination ─────────────────────
            # WATCH stage: 只触发一次 (first entry) → CONTINUOUS mode 不应该用于 WATCH
            if event.stage == EventStage.WATCH:
                # WATCH events should use FIRST_ENTRY trigger, not CONTINUOUS
                # If we're here, skip (logically shouldn't happen)
                continue

            # ENTRY stage: 检查超时和已入场
            if event.stage == EventStage.ENTRY:
                # 已放弃监听 → 停止
                if ticker_state.abandoned:
                    break

                # 已入场 → 停止监听（后续由 EXIT stage 处理）
                if ticker_state.entry_triggered:
                    break

                # 超时检查: 30 min 无入场机会 → 放弃
                if ticker_state.watch_triggered:
                    elapsed = ts_ms - ticker_state.watch_time_ms
                    if elapsed > WATCH_TIMEOUT_MS:
                        ticker_state.abandoned = True
                        logger.debug(
                            f"{symbol}: ENTRY abandoned after {elapsed/60000:.1f}min "
                            f"(watch_time={ticker_state.watch_time_ms})"
                        )
                        break

            # EXIT stage: 只在有持仓时监听
            if event.stage == EventStage.EXIT:
                if not ticker_state.has_position:
                    continue

            # ── Build signal and check conditions ─────────────────────
            signal = self._build_signal(
                ts_ms, symbol, factors, trigger_prices, session_start_ms, session_end_ms
            )

            # Anti-patterns: 只适用于 ENTRY stage, 不阻止 WATCH stage 的选股
            # WATCH stage 是 FIRST_ENTRY trigger，用于筛选候选股，不应被 anti-patterns 阻止
            # ENTRY stage 是 CONTINUOUS trigger，用于入场决策，应该检查 anti-patterns
            if event.stage == EventStage.ENTRY and self._check_anti_patterns(signal):
                continue

            # Check valid events (accept)
            for ev in self.events:
                if ev.name == event.name and ev.matches(signal):
                    signal["event_name"] = event.name
                    signal["event_stage"] = event.stage.value
                    signals.append(signal)
                    self._update_state_on_match(ticker_state, event, signal)
                    break

        return signals, ticker_state

    def _check_pre_condition(self, event: Event, state: TickerState) -> bool:
        """Check if pre_condition for this event is satisfied.

        Pre-condition logic:
        - WATCH stage: no pre_condition needed
        - ENTRY stage: requires specific WATCH event triggered (or any watch)
        - EXIT stage: requires has_position
        """
        if not event.requires_pre_condition():
            return True

        pre_cond = event.pre_condition

        if pre_cond == "has_position":
            return state.has_position

        # Check if the specific event was triggered
        if pre_cond in state.triggered_events:
            return True

        # For ENTRY stage, also check if ANY watch was triggered
        if event.stage == EventStage.ENTRY and state.watch_triggered:
            return True

        return False

    def _update_state_on_match(
        self, state: TickerState, event: Event, signal: dict
    ) -> None:
        """Update TickerState when an event matches."""
        ts_ms = signal.get("trigger_time_ms", 0)

        # Record triggered event
        if event.name not in state.triggered_events:
            state.triggered_events[event.name] = ts_ms

        # Update stage-specific state
        if event.stage == EventStage.WATCH:
            state.watch_triggered = True
            state.watch_time_ms = ts_ms
            state.watch_event_name = event.name
        elif event.stage == EventStage.ENTRY:
            state.entry_triggered = True
            state.entry_time_ms = ts_ms
            state.entry_event_name = event.name
            state.has_position = True
            trigger_price = signal.get("trigger_price", 0)
            if trigger_price and trigger_price > 0:
                state.entry_price = trigger_price

    def _find_closest_timestamp(
        self, factor_ts: dict[int, dict[str, float]], target_ms: int
    ) -> int | None:
        """Find the closest timestamp in factor_ts to target_ms."""
        if not factor_ts:
            return None

        timestamps = sorted(factor_ts.keys())
        # Find first timestamp >= target_ms
        for ts in timestamps:
            if ts >= target_ms:
                return ts

        # If target is after all timestamps, return last
        return timestamps[-1] if timestamps else None

    def _build_signal(
        self,
        ts_ms: int,
        symbol: str,
        factors: dict[str, float],
        trigger_prices: dict[int, float] | None,
        session_start_ms: int,
        session_end_ms: int,
    ) -> dict:
        """Build signal dict for Event matching."""
        from jerry_trader.domain.session import get_session_phase_from_ns

        phase = get_session_phase_from_ns(ts_ms * 1_000_000)

        signal = {
            "trigger_time_ns": ts_ms * 1_000_000,
            "trigger_time_ms": ts_ms,
            "symbol": symbol,
            "session_phase": phase.value,
            **factors,
        }

        if trigger_prices and ts_ms in trigger_prices:
            signal["trigger_price"] = trigger_prices[ts_ms]

        return signal

    def _check_anti_patterns(self, signal: dict) -> bool:
        """Check if signal matches any anti-pattern (should be rejected)."""
        for anti in self.anti_patterns:
            if anti.matches(signal):
                return True
        return False

    def get_rejected_signals(self, signals: list[dict]) -> list[dict]:
        """Get signals that matched anti-patterns (rejected).

        Args:
            signals: List of signal dicts

        Returns:
            List of rejected signals
        """
        rejected = []
        for signal in signals:
            # Check anti-patterns only
            for anti in self.anti_patterns:
                if anti.matches(signal):
                    rejected.append(signal)
                    break
        return rejected


__all__ = ["EventEvaluator", "TickerState", "MLEvaluationResult"]
