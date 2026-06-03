"""Tests for domain/event.py — Condition, Event, ModelConfig, TriggerType, EventStage.

Covers:
  - Condition.check() for all operators (gt, lt, eq, ne, gte, lte, between)
  - None handling (signal_value=None returns False)
  - Invalid op raises ValueError
  - ModelConfig construction and repr
  - Event.matches() boolean filter
  - Event.matches_hard_constraints()
  - Event helper methods: uses_ml, is_anti_pattern, is_trigger_based
  - Event stage helpers: is_watch_stage, is_entry_stage, is_exit_stage
  - Event.requires_pre_condition
  - TriggerType and EventStage enum values
"""

import pytest

from jerry_trader.domain.event import (
    Condition,
    Event,
    EventAction,
    EventStage,
    ModelConfig,
    TriggerType,
)


class TestTriggerType:
    def test_trigger_values(self):
        assert TriggerType.FIRST_ENTRY.value == "first_entry"
        assert TriggerType.PRICE_ACCEL.value == "price_accel"
        assert TriggerType.TRADE_RATE_SPIKE.value == "trade_rate_spike"
        assert TriggerType.GAP_BREAKOUT.value == "gap_breakout"
        assert TriggerType.CONTINUOUS.value == "continuous"

    def test_trigger_from_string(self):
        assert TriggerType("first_entry") == TriggerType.FIRST_ENTRY
        assert TriggerType("continuous") == TriggerType.CONTINUOUS


class TestEventStage:
    def test_stage_values(self):
        assert EventStage.WATCH.value == "watch"
        assert EventStage.ENTRY.value == "entry"
        assert EventStage.EXIT.value == "exit"


class TestEventAction:
    def test_action_values(self):
        assert EventAction.ACCEPT.value == "ACCEPT"
        assert EventAction.AVOID.value == "AVOID"


class TestModelConfig:
    def test_defaults(self):
        mc = ModelConfig(name="return_predictor_v1")
        assert mc.name == "return_predictor_v1"
        assert mc.min_expected_return == 0.02
        assert mc.min_confidence == 0.0

    def test_custom_thresholds(self):
        mc = ModelConfig(name="v2", min_expected_return=0.05, min_confidence=0.7)
        assert mc.min_expected_return == 0.05
        assert mc.min_confidence == 0.7

    def test_repr(self):
        mc = ModelConfig(name="test_model", min_expected_return=0.03)
        r = repr(mc)
        assert "test_model" in r
        assert "3.00%" in r


# ══════════════════════════════════════════════════════════════════════
# Condition.check()
# ══════════════════════════════════════════════════════════════════════


class TestConditionCheckNone:
    """signal_value=None always returns False."""

    def test_none_returns_false_gt(self):
        c = Condition(factor="trade_rate", op="gt", value=100)
        assert c.check(None) is False

    def test_none_returns_false_eq(self):
        c = Condition(factor="price_direction", op="eq", value=0.5)
        assert c.check(None) is False

    def test_none_returns_false_between(self):
        c = Condition(factor="gap_pct", op="between", value=[0.01, 0.10])
        assert c.check(None) is False


class TestConditionCheckGt:
    def test_gt_true(self):
        c = Condition(factor="trade_rate", op="gt", value=100)
        assert c.check(150) is True

    def test_gt_false(self):
        c = Condition(factor="trade_rate", op="gt", value=100)
        assert c.check(50) is False

    def test_gt_equal_is_false(self):
        c = Condition(factor="trade_rate", op="gt", value=100)
        assert c.check(100) is False

    def test_gt_with_float(self):
        c = Condition(factor="price_direction", op="gt", value=0.0)
        assert c.check(0.5) is True
        assert c.check(-0.1) is False

    def test_gt_string_value_cast(self):
        """Values are cast to float, so string '150' works."""
        c = Condition(factor="trade_rate", op="gt", value=100)
        assert c.check("150") is True

    def test_gt_invalid_string_raises(self):
        c = Condition(factor="trade_rate", op="gt", value=100)
        with pytest.raises(ValueError):
            c.check("not_a_number")


class TestConditionCheckLt:
    def test_lt_true(self):
        c = Condition(factor="trade_rate", op="lt", value=100)
        assert c.check(50) is True

    def test_lt_false(self):
        c = Condition(factor="trade_rate", op="lt", value=100)
        assert c.check(150) is False


class TestConditionCheckGte:
    def test_gte_true(self):
        c = Condition(factor="trade_rate", op="gte", value=100)
        assert c.check(150) is True

    def test_gte_equal_is_true(self):
        c = Condition(factor="trade_rate", op="gte", value=100)
        assert c.check(100) is True

    def test_gte_false(self):
        c = Condition(factor="trade_rate", op="gte", value=100)
        assert c.check(50) is False


class TestConditionCheckLte:
    def test_lte_true(self):
        c = Condition(factor="trade_rate", op="lte", value=100)
        assert c.check(50) is True

    def test_lte_equal_is_true(self):
        c = Condition(factor="trade_rate", op="lte", value=100)
        assert c.check(100) is True

    def test_lte_false(self):
        c = Condition(factor="trade_rate", op="lte", value=100)
        assert c.check(150) is False


class TestConditionCheckEq:
    def test_eq_true(self):
        c = Condition(factor="session_phase", op="eq", value="mid")
        assert c.check("mid") is True

    def test_eq_false(self):
        c = Condition(factor="session_phase", op="eq", value="mid")
        assert c.check("early") is False

    def test_eq_int(self):
        c = Condition(factor="count", op="eq", value=5)
        assert c.check(5) is True
        assert c.check(6) is False


class TestConditionCheckNe:
    def test_ne_false_when_equal(self):
        c = Condition(factor="session_phase", op="ne", value="early")
        assert c.check("early") is False

    def test_ne_true_when_different(self):
        c = Condition(factor="session_phase", op="ne", value="early")
        assert c.check("mid") is True


class TestConditionCheckBetween:
    def test_between_inside(self):
        c = Condition(factor="gap_pct", op="between", value=[0.01, 0.10])
        assert c.check(0.05) is True

    def test_between_at_lower_bound(self):
        c = Condition(factor="gap_pct", op="between", value=[0.01, 0.10])
        assert c.check(0.01) is True

    def test_between_at_upper_bound(self):
        c = Condition(factor="gap_pct", op="between", value=[0.01, 0.10])
        assert c.check(0.10) is True

    def test_between_below(self):
        c = Condition(factor="gap_pct", op="between", value=[0.01, 0.10])
        assert c.check(0.001) is False

    def test_between_above(self):
        c = Condition(factor="gap_pct", op="between", value=[0.01, 0.10])
        assert c.check(0.20) is False

    def test_between_with_negative_range(self):
        c = Condition(factor="delta", op="between", value=[-5.0, 5.0])
        assert c.check(-3.0) is True
        assert c.check(0.0) is True
        assert c.check(-10.0) is False


class TestConditionCheckInvalidOp:
    def test_unknown_op_raises(self):
        c = Condition(factor="x", op="unknown_op", value=1)
        with pytest.raises(ValueError, match="Unknown op"):
            c.check(1)


class TestConditionRepr:
    def test_repr(self):
        c = Condition(factor="trade_rate", op="gt", value=100)
        r = repr(c)
        assert "trade_rate" in r
        assert "gt" in r
        assert "100" in r


# ══════════════════════════════════════════════════════════════════════
# Event.matches() — Boolean filter
# ══════════════════════════════════════════════════════════════════════


class TestEventMatches:
    def test_single_condition_matches(self):
        event = Event(
            name="high_trade_rate",
            semantic="high volume",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        assert event.matches({"trade_rate": 150}) is True
        assert event.matches({"trade_rate": 50}) is False

    def test_multiple_conditions_all_must_match(self):
        event = Event(
            name="momentum",
            semantic="momentum signal",
            conditions=[
                Condition(factor="trade_rate", op="gt", value=100),
                Condition(factor="price_direction", op="gt", value=0.0),
            ],
        )
        assert event.matches({"trade_rate": 150, "price_direction": 0.5}) is True
        assert event.matches({"trade_rate": 150, "price_direction": -0.5}) is False

    def test_missing_factor_in_signal(self):
        """When a required factor is not in the signal dict, .get() returns None → False."""
        event = Event(
            name="momentum",
            semantic="momentum signal",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
        )
        assert event.matches({}) is False
        assert event.matches({"other_factor": 999}) is False

    def test_no_conditions_always_matches(self):
        event = Event(name="always", semantic="always matches")
        assert event.matches({}) is True
        assert event.matches({"anything": "value"}) is True

    def test_empty_conditions_always_matches(self):
        event = Event(name="always", semantic="always matches", conditions=[])
        assert event.matches({}) is True


class TestEventMatchesHardConstraints:
    def test_passes_when_all_constraints_met(self):
        event = Event(
            name="entry",
            semantic="entry with constraints",
            conditions=[Condition(factor="trade_rate", op="gt", value=100)],
            hard_constraints=[Condition(factor="gap_pct", op="lt", value=0.50)],
        )
        assert event.matches_hard_constraints({"gap_pct": 0.10}) is True

    def test_fails_when_constraint_not_met(self):
        event = Event(
            name="entry",
            semantic="entry with constraints",
            hard_constraints=[Condition(factor="gap_pct", op="lt", value=0.50)],
        )
        assert event.matches_hard_constraints({"gap_pct": 0.80}) is False

    def test_no_hard_constraints_always_passes(self):
        event = Event(name="entry", semantic="no constraints")
        assert event.matches_hard_constraints({}) is True


# ══════════════════════════════════════════════════════════════════════
# Event helper methods
# ══════════════════════════════════════════════════════════════════════


class TestEventUsesMl:
    def test_uses_ml_false_by_default(self):
        event = Event(name="boolean_event", semantic="boolean")
        assert event.uses_ml() is False

    def test_uses_ml_true_with_model(self):
        event = Event(
            name="ml_event",
            semantic="ml-based",
            model=ModelConfig(name="predictor_v1"),
        )
        assert event.uses_ml() is True


class TestEventIsAntiPattern:
    def test_not_anti_by_default(self):
        event = Event(name="valid", semantic="valid")
        assert event.is_anti_pattern() is False

    def test_is_anti_with_avoid_action(self):
        event = Event(name="bad_pattern", semantic="avoid", action=EventAction.AVOID)
        assert event.is_anti_pattern() is True


class TestEventIsTriggerBased:
    def test_continuous_is_not_trigger_based(self):
        event = Event(name="legacy", semantic="legacy", trigger=TriggerType.CONTINUOUS)
        assert event.is_trigger_based() is False

    def test_first_entry_is_trigger_based(self):
        event = Event(
            name="gap_watch", semantic="watch", trigger=TriggerType.FIRST_ENTRY
        )
        assert event.is_trigger_based() is True

    def test_price_accel_is_trigger_based(self):
        event = Event(name="accel", semantic="accel", trigger=TriggerType.PRICE_ACCEL)
        assert event.is_trigger_based() is True


class TestEventStageHelpers:
    def test_default_stage_is_entry(self):
        event = Event(name="legacy", semantic="legacy")
        assert event.is_entry_stage() is True
        assert event.is_watch_stage() is False
        assert event.is_exit_stage() is False

    def test_watch_stage(self):
        event = Event(
            name="watch_event",
            semantic="watch",
            stage=EventStage.WATCH,
            trigger=TriggerType.FIRST_ENTRY,
        )
        assert event.is_watch_stage() is True
        assert event.is_entry_stage() is False
        assert event.is_exit_stage() is False

    def test_entry_stage(self):
        event = Event(name="entry_event", semantic="entry", stage=EventStage.ENTRY)
        assert event.is_entry_stage() is True
        assert event.is_watch_stage() is False
        assert event.is_exit_stage() is False

    def test_exit_stage(self):
        event = Event(name="exit_event", semantic="exit", stage=EventStage.EXIT)
        assert event.is_exit_stage() is True
        assert event.is_watch_stage() is False
        assert event.is_entry_stage() is False


class TestEventPreCondition:
    def test_no_pre_condition_by_default(self):
        event = Event(name="simple", semantic="simple")
        assert event.requires_pre_condition() is False

    def test_has_pre_condition(self):
        event = Event(
            name="entry",
            semantic="entry",
            stage=EventStage.ENTRY,
            pre_condition="gap_up_watch",
        )
        assert event.requires_pre_condition() is True


class TestEventRepr:
    def test_repr_includes_stage_and_trigger(self):
        event = Event(
            name="test_event",
            semantic="test",
            stage=EventStage.ENTRY,
            trigger=TriggerType.CONTINUOUS,
        )
        r = repr(event)
        assert "test_event" in r
        assert "entry" in r
        assert "continuous" in r
