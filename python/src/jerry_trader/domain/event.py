"""Event Definition - Boolean signal selection with trigger-based architecture.

Based on Event Framework Validation (roadmap/event-framework-validation.md):
- Event Selection improves avg return (+4.29% vs -0.23%)
- Factor Ranking is ineffective (IC unstable, std=0.29)
- Use Boolean filter (accept/reject), not ranking

Trigger Architecture (roadmap/event-trigger-architecture.md):
- Events trigger at specific points (first_entry, price_accel, trade_rate_spike)
- Conditions are quality filters applied at trigger point
- Each ticker triggers once per trigger type (not every timestamp)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventAction(Enum):
    """Event action type"""

    ACCEPT = "ACCEPT"
    AVOID = "AVOID"


class TriggerType(Enum):
    """Event trigger type - when to evaluate conditions.

    TRIGGER vs CONDITION:
    - Trigger: defines WHEN to check (e.g., "ticker enters top20")
    - Conditions: define WHAT to check at that moment (quality filter)

    This ensures signals only fire at meaningful moments, not continuously.
    """

    FIRST_ENTRY = "first_entry"  # New ticker enters top20
    PRICE_ACCEL = "price_accel"  # Sudden price acceleration
    TRADE_RATE_SPIKE = "trade_rate_spike"  # Sudden volume spike
    GAP_BREAKOUT = "gap_breakout"  # Breakout from consolidation
    CONTINUOUS = "continuous"  # Legacy: every timestamp (deprecated)


class EventStage(Enum):
    """Event stage in trading flow.

    Defines the position of this event in the trading lifecycle:
    - WATCH: 选股阶段 - 决定是否关注这个 ticker
    - ENTRY: 入场阶段 - 决定是否买入
    - EXIT: 出场阶段 - 决定是否卖出

    Flow:
        FIRST_ENTRY trigger → WATCH event → (if matched) 进入观察
        CONTINUOUS trigger → ENTRY event → (if pre_condition matched) 入场
        CONTINUOUS trigger → EXIT event → (if has_position) 出场
    """

    WATCH = "watch"  # 选股：决定是否关注
    ENTRY = "entry"  # 入场：决定是否买入
    EXIT = "exit"  # 出场：决定是否卖出


@dataclass
class ModelConfig:
    """ML model configuration for event evaluation.

    Replaces boolean conditions with ML prediction.
    The event triggers when expected_return >= min_expected_return.

    Example:
        ModelConfig(
            name="return_predictor_v1",
            min_expected_return=0.02,  # 2% minimum
            min_confidence=0.5,
        )
    """

    name: str  # Model identifier in registry
    min_expected_return: float = 0.02  # Minimum expected return to trigger
    min_confidence: float = 0.0  # Minimum confidence (0-1)

    def __repr__(self) -> str:
        return f"ModelConfig({self.name}, min_return={self.min_expected_return:.2%})"


@dataclass
class Condition:
    """Single condition for event matching.

    Boolean check on factor value - no ranking, no normalization.
    """

    factor: str  # trade_rate, price_direction, gap_pct, session_phase
    op: str  # eq, gt, lt, between, ne, gte, lte
    value: Any  # threshold or range [lo, hi]

    def check(self, signal_value: Any) -> bool:
        """Boolean check - returns True if condition passes."""
        if signal_value is None:
            return False

        if self.op == "eq":
            return signal_value == self.value
        elif self.op == "gt":
            return float(signal_value) > float(self.value)
        elif self.op == "lt":
            return float(signal_value) < float(self.value)
        elif self.op == "gte":
            return float(signal_value) >= float(self.value)
        elif self.op == "lte":
            return float(signal_value) <= float(self.value)
        elif self.op == "between":
            lo, hi = self.value
            return float(lo) <= float(signal_value) <= float(hi)
        elif self.op == "ne":
            return signal_value != self.value
        else:
            raise ValueError(f"Unknown op: {self.op}")

    def __repr__(self) -> str:
        return f"Condition({self.factor} {self.op} {self.value})"


@dataclass
class Event:
    """Event definition - semantic signal category with trigger-based architecture.

    An Event fires at a specific trigger point (e.g., ticker enters top20),
    then checks quality conditions at that moment.

    Stage-based architecture (WATCH → ENTRY → EXIT):
    - WATCH: 选股阶段，FIRST_ENTRY trigger，决定是否关注
    - ENTRY: 入场阶段，CONTINUOUS trigger，决定是否买入（需要 pre_condition）
    - EXIT: 出场阶段，CONTINUOUS trigger，决定是否卖出（需要有持仓）

    Example:
        # Stage 1: WATCH (选股)
        Event(
            name="gap_up_watch",
            stage=EventStage.WATCH,
            trigger=TriggerType.FIRST_ENTRY,
            conditions=[...],
        )

        # Stage 2: ENTRY (入场)
        Event(
            name="momentum_entry",
            stage=EventStage.ENTRY,
            trigger=TriggerType.CONTINUOUS,
            pre_condition="gap_up_watch",  # 必须先触发 WATCH
            conditions=[...],
        )

        # Stage 3: EXIT (出场)
        Event(
            name="take_profit",
            stage=EventStage.EXIT,
            trigger=TriggerType.CONTINUOUS,
            pre_condition="has_position",  # 必须有持仓
            conditions=[...],
        )
    """

    name: str
    semantic: str  # "跳空整理突破"
    trigger: TriggerType = TriggerType.CONTINUOUS
    stage: EventStage = EventStage.ENTRY  # NEW: default ENTRY for backward compat
    pre_condition: str | None = None  # NEW: must trigger this event first
    conditions: list[Condition] = field(default_factory=list)

    # ML model configuration (replaces conditions for ENTRY stage)
    model: ModelConfig | None = None

    # Hard constraints (always checked, even with ML model)
    hard_constraints: list[Condition] = field(default_factory=list)

    expected_return: float = 0.0  # Historical avg_return from validation
    min_win_rate: float = 0.0  # Historical win_rate from validation
    signal_count: int = 0  # Historical sample count
    action: EventAction = EventAction.ACCEPT

    def matches(self, signal: dict) -> bool:
        """Check if signal matches all conditions (Boolean filter).

        Note: For ML-based events, use EventEvaluator.evaluate() instead.
        This method is for backward compatibility with boolean-only events.
        """
        for cond in self.conditions:
            value = signal.get(cond.factor)
            if not cond.check(value):
                return False
        return True

    def matches_hard_constraints(self, signal: dict) -> bool:
        """Check if signal passes all hard constraints.

        Hard constraints are always evaluated, even for ML-based events.
        They serve as explicit risk control.
        """
        for cond in self.hard_constraints:
            value = signal.get(cond.factor)
            if not cond.check(value):
                return False
        return True

    def uses_ml(self) -> bool:
        """Check if this event uses ML model instead of boolean conditions."""
        return self.model is not None

    def is_anti_pattern(self) -> bool:
        """Check if this event is an anti-pattern to avoid."""
        return self.action == EventAction.AVOID

    def is_trigger_based(self) -> bool:
        """Check if this event uses trigger-based evaluation (not continuous)."""
        return self.trigger != TriggerType.CONTINUOUS

    def is_watch_stage(self) -> bool:
        """Check if this is a WATCH stage event."""
        return self.stage == EventStage.WATCH

    def is_entry_stage(self) -> bool:
        """Check if this is an ENTRY stage event."""
        return self.stage == EventStage.ENTRY

    def is_exit_stage(self) -> bool:
        """Check if this is an EXIT stage event."""
        return self.stage == EventStage.EXIT

    def requires_pre_condition(self) -> bool:
        """Check if this event has a pre_condition requirement."""
        return self.pre_condition is not None

    def __repr__(self) -> str:
        return f"Event({self.name}, stage={self.stage.value}, trigger={self.trigger.value})"


__all__ = [
    "EventAction",
    "TriggerType",
    "EventStage",
    "Condition",
    "ModelConfig",
    "Event",
]
