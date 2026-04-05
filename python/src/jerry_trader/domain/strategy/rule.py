"""Strategy DSL Rule Models

Declarative rule definitions for the Signal Engine.
Rules define factor-based conditions that trigger signal recording.

Pure Pydantic models — no I/O, no service dependencies.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator

# ─────────────────────────────────────────────────────────────────────────────
# Operators
# ─────────────────────────────────────────────────────────────────────────────


class ComparisonOp(str, Enum):
    """Comparison operators for factor conditions."""

    GT = "gt"  # greater than
    LT = "lt"  # less than
    GTE = "gte"  # greater than or equal
    LTE = "lte"  # less than or equal
    EQ = "eq"  # equal
    BETWEEN = "between"  # in range [lo, hi]
    CROSS_ABOVE = "cross_above"  # price crosses above factor
    CROSS_BELOW = "cross_below"  # price crosses below factor


class TriggerType(str, Enum):
    """Logical composition of conditions."""

    AND = "AND"
    OR = "OR"


class ActionType(str, Enum):
    """Action to take when rule triggers."""

    RECORD = "record"  # Record trigger event and track returns
    NOTIFY = "notify"  # Send notification (future)
    ALERT = "alert"  # Set price/volume alert (future)


# ─────────────────────────────────────────────────────────────────────────────
# Condition
# ─────────────────────────────────────────────────────────────────────────────


class Condition(BaseModel):
    """A single factor condition.

    Examples:
        trade_rate > 3.0:
            factor: trade_rate
            op: gt
            value: 3.0
            timeframe: trade

        ema_20 cross_above close:
            factor: ema_20
            op: cross_above
            target: close
            timeframe: 10s
    """

    factor: str = Field(description="Factor name (must exist in FactorRegistry)")
    op: ComparisonOp = Field(description="Comparison operator")
    timeframe: str = Field(default="trade", description="Factor timeframe")
    value: float | None = Field(
        default=None, description="Threshold value (for gt/lt/gte/lte/eq)"
    )
    target: str | None = Field(
        default=None, description="Target field name (for cross_above/cross_below)"
    )
    lo: float | None = Field(default=None, description="Lower bound (for between)")
    hi: float | None = Field(default=None, description="Upper bound (for between)")

    @model_validator(mode="after")
    def validate_op_params(self) -> "Condition":
        """Ensure the right parameters are provided for each operator."""
        threshold_ops = {
            ComparisonOp.GT,
            ComparisonOp.LT,
            ComparisonOp.GTE,
            ComparisonOp.LTE,
            ComparisonOp.EQ,
        }
        cross_ops = {ComparisonOp.CROSS_ABOVE, ComparisonOp.CROSS_BELOW}

        if self.op in threshold_ops and self.value is None:
            raise ValueError(f"op={self.op.value} requires 'value'")

        if self.op in cross_ops and self.target is None:
            raise ValueError(f"op={self.op.value} requires 'target'")

        if self.op == ComparisonOp.BETWEEN:
            if self.lo is None or self.hi is None:
                raise ValueError("op=between requires 'lo' and 'hi'")
            if self.lo >= self.hi:
                raise ValueError(f"'lo' ({self.lo}) must be less than 'hi' ({self.hi})")

        return self


# ─────────────────────────────────────────────────────────────────────────────
# Trigger (recursive for nested AND/OR)
# ─────────────────────────────────────────────────────────────────────────────


class Trigger(BaseModel):
    """A group of conditions composed with AND/OR.

    Supports nesting: a condition can itself be a Trigger for recursive logic.
    Phase 1: single-level AND only. Nested triggers for Phase 2.

    Examples:
        Single-level AND:
          type: AND
          conditions:
            - factor: trade_rate
              op: gt
              value: 3.0
            - factor: volume_ratio_5m
              op: gt
              value: 2.0
    """

    type: TriggerType = Field(
        default=TriggerType.AND, description="Logical composition"
    )
    conditions: list[Condition] = Field(description="List of conditions")

    @field_validator("conditions")
    @classmethod
    def conditions_not_empty(cls, v: list[Condition]) -> list[Condition]:
        if not v:
            raise ValueError("Trigger must have at least one condition")
        return v


# ─────────────────────────────────────────────────────────────────────────────
# Action
# ─────────────────────────────────────────────────────────────────────────────


class Action(BaseModel):
    """Action to execute when rule triggers."""

    type: ActionType = Field(default=ActionType.RECORD)
    track: list[str] = Field(
        default_factory=lambda: ["returns_1m", "returns_5m", "returns_15m"],
        description="Metrics to track after trigger (for record action)",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Rule
# ─────────────────────────────────────────────────────────────────────────────


class Rule(BaseModel):
    """A complete trading signal rule.

    Defines factor conditions and what to do when they're met.

    Example YAML:
        id: premarket_momentum_v1
        name: "Pre-market momentum spike"
        version: 1
        trigger:
          type: AND
          conditions:
            - factor: trade_rate
              timeframe: trade
              op: gt
              value: 3.0
        actions:
          - type: record
            track: [returns_1m, returns_5m, returns_15m]
    """

    id: str = Field(description="Unique rule identifier")
    name: str = Field(default="", description="Human-readable rule name")
    version: int = Field(default=1, ge=1, description="Rule version")
    trigger: Trigger = Field(description="Trigger conditions")
    actions: list[Action] = Field(
        default_factory=lambda: [Action()],
        description="Actions to execute on trigger",
    )
    enabled: bool = Field(default=True, description="Whether rule is active")

    @field_validator("id")
    @classmethod
    def id_format(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Rule id cannot be empty")
        # Allow alphanumeric, underscores, hyphens
        cleaned = v.strip()
        if not all(c.isalnum() or c in "_-" for c in cleaned):
            raise ValueError(f"Rule id must be alphanumeric/underscore/hyphen: {v}")
        return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# Validation Result
# ─────────────────────────────────────────────────────────────────────────────


class ValidationIssue(BaseModel):
    """A validation issue found during rule checking."""

    path: str = Field(
        description="JSON path to the issue (e.g., 'trigger.conditions[0].factor')"
    )
    message: str = Field(description="Human-readable description")
    severity: str = Field(default="error", description="'error' or 'warning'")


class RuleValidationResult(BaseModel):
    """Result of validating a rule against the factor registry."""

    rule_id: str
    valid: bool
    issues: list[ValidationIssue] = Field(default_factory=list)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]
