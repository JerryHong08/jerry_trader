"""Unit tests for Signal Engine rule evaluation."""

import pytest

from jerry_trader.domain.strategy.rule import (
    ComparisonOp,
    Condition,
    Rule,
    Trigger,
    TriggerType,
)
from jerry_trader.services.signal.engine import evaluate_condition, evaluate_trigger

# ─────────────────────────────────────────────────────────────────────────────
# Condition Evaluation
# ─────────────────────────────────────────────────────────────────────────────


class TestEvaluateCondition:
    def test_gt_true(self):
        c = Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0)
        assert evaluate_condition(c, {"trade_rate": 5.0}) is True

    def test_gt_false(self):
        c = Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0)
        assert evaluate_condition(c, {"trade_rate": 2.0}) is False

    def test_lt_true(self):
        c = Condition(factor="spread", op=ComparisonOp.LT, value=0.05)
        assert evaluate_condition(c, {"spread": 0.01}) is True

    def test_gte(self):
        c = Condition(factor="x", op=ComparisonOp.GTE, value=10.0)
        assert evaluate_condition(c, {"x": 10.0}) is True
        assert evaluate_condition(c, {"x": 9.9}) is False

    def test_lte(self):
        c = Condition(factor="x", op=ComparisonOp.LTE, value=10.0)
        assert evaluate_condition(c, {"x": 10.0}) is True
        assert evaluate_condition(c, {"x": 10.1}) is False

    def test_between(self):
        c = Condition(factor="vol", op=ComparisonOp.BETWEEN, lo=2.0, hi=5.0)
        assert evaluate_condition(c, {"vol": 3.0}) is True
        assert evaluate_condition(c, {"vol": 2.0}) is True  # inclusive
        assert evaluate_condition(c, {"vol": 5.0}) is True  # inclusive
        assert evaluate_condition(c, {"vol": 1.9}) is False
        assert evaluate_condition(c, {"vol": 5.1}) is False

    def test_missing_factor(self):
        c = Condition(factor="nonexistent", op=ComparisonOp.GT, value=3.0)
        assert evaluate_condition(c, {"trade_rate": 5.0}) is False

    def test_cross_not_supported(self):
        c = Condition(factor="ema_20", op=ComparisonOp.CROSS_ABOVE, target="close")
        assert evaluate_condition(c, {"ema_20": 100.0, "close": 99.0}) is False


# ─────────────────────────────────────────────────────────────────────────────
# Trigger Evaluation
# ─────────────────────────────────────────────────────────────────────────────


class TestEvaluateTrigger:
    def test_and_all_true(self):
        conditions = [
            Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0),
            Condition(factor="volume", op=ComparisonOp.GT, value=100.0),
        ]
        assert (
            evaluate_trigger(
                conditions, TriggerType.AND, {"trade_rate": 5.0, "volume": 200.0}
            )
            is True
        )

    def test_and_one_false(self):
        conditions = [
            Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0),
            Condition(factor="volume", op=ComparisonOp.GT, value=100.0),
        ]
        assert (
            evaluate_trigger(
                conditions, TriggerType.AND, {"trade_rate": 5.0, "volume": 50.0}
            )
            is False
        )

    def test_or_one_true(self):
        conditions = [
            Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0),
            Condition(factor="volume", op=ComparisonOp.GT, value=100.0),
        ]
        assert (
            evaluate_trigger(
                conditions, TriggerType.OR, {"trade_rate": 5.0, "volume": 50.0}
            )
            is True
        )

    def test_or_all_false(self):
        conditions = [
            Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0),
            Condition(factor="volume", op=ComparisonOp.GT, value=100.0),
        ]
        assert (
            evaluate_trigger(
                conditions, TriggerType.OR, {"trade_rate": 1.0, "volume": 50.0}
            )
            is False
        )

    def test_empty_conditions(self):
        assert evaluate_trigger([], TriggerType.AND, {"x": 1.0}) is False
