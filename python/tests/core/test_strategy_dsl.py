"""Unit tests for Strategy DSL rule models and parser.

Tests:
- Rule model validation (valid/invalid conditions)
- YAML parsing
- Factor reference validation (mock checker)
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from jerry_trader.domain.strategy.rule import (
    Action,
    ActionType,
    ComparisonOp,
    Condition,
    Rule,
    RuleValidationResult,
    Trigger,
    TriggerType,
    ValidationIssue,
)
from jerry_trader.domain.strategy.rule_parser import (
    load_rules_from_dir,
    parse_rule,
    parse_rule_file,
    validate_rule_factors,
)

# ─────────────────────────────────────────────────────────────────────────────
# Condition Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestCondition:
    def test_gt_with_value(self):
        c = Condition(
            factor="trade_rate", op=ComparisonOp.GT, value=3.0, timeframe="trade"
        )
        assert c.factor == "trade_rate"
        assert c.value == 3.0

    def test_gt_requires_value(self):
        with pytest.raises(ValueError, match="requires 'value'"):
            Condition(factor="trade_rate", op=ComparisonOp.GT, timeframe="trade")

    def test_lt_with_value(self):
        c = Condition(
            factor="spread", op=ComparisonOp.LT, value=0.05, timeframe="trade"
        )
        assert c.value == 0.05

    def test_between_valid(self):
        c = Condition(
            factor="volume_ratio",
            op=ComparisonOp.BETWEEN,
            lo=2.0,
            hi=5.0,
            timeframe="1m",
        )
        assert c.lo == 2.0
        assert c.hi == 5.0

    def test_between_requires_lo_hi(self):
        with pytest.raises(ValueError, match="requires 'lo' and 'hi'"):
            Condition(factor="volume_ratio", op=ComparisonOp.BETWEEN, timeframe="1m")

    def test_between_lo_lt_hi(self):
        with pytest.raises(ValueError, match="must be less than"):
            Condition(
                factor="volume_ratio",
                op=ComparisonOp.BETWEEN,
                lo=5.0,
                hi=2.0,
                timeframe="1m",
            )

    def test_cross_above_requires_target(self):
        with pytest.raises(ValueError, match="requires 'target'"):
            Condition(factor="ema_20", op=ComparisonOp.CROSS_ABOVE, timeframe="10s")

    def test_cross_above_with_target(self):
        c = Condition(
            factor="ema_20",
            op=ComparisonOp.CROSS_ABOVE,
            target="close",
            timeframe="10s",
        )
        assert c.target == "close"


# ─────────────────────────────────────────────────────────────────────────────
# Trigger Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestTrigger:
    def test_single_condition(self):
        t = Trigger(
            type=TriggerType.AND,
            conditions=[Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0)],
        )
        assert len(t.conditions) == 1

    def test_multiple_conditions(self):
        t = Trigger(
            type=TriggerType.AND,
            conditions=[
                Condition(factor="trade_rate", op=ComparisonOp.GT, value=3.0),
                Condition(factor="volume_ratio", op=ComparisonOp.LT, value=0.05),
            ],
        )
        assert len(t.conditions) == 2

    def test_empty_conditions_rejected(self):
        with pytest.raises(ValueError, match="at least one condition"):
            Trigger(type=TriggerType.AND, conditions=[])


# ─────────────────────────────────────────────────────────────────────────────
# Rule Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestRule:
    def _make_rule(self, **overrides):
        defaults = {
            "id": "test_rule",
            "name": "Test rule",
            "trigger": {
                "type": "AND",
                "conditions": [{"factor": "trade_rate", "op": "gt", "value": 3.0}],
            },
        }
        defaults.update(overrides)
        return parse_rule(defaults)

    def test_valid_minimal_rule(self):
        rule = self._make_rule()
        assert rule.id == "test_rule"
        assert rule.version == 1
        assert rule.enabled is True

    def test_empty_id_rejected(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            self._make_rule(id="")

    def test_invalid_id_chars_rejected(self):
        with pytest.raises(ValueError, match="alphanumeric"):
            self._make_rule(id="bad rule id!")

    def test_valid_id_formats(self):
        for id_str in ["my_rule", "rule-1", "Rule123"]:
            rule = self._make_rule(id=id_str)
            assert rule.id == id_str

    def test_default_action_is_record(self):
        rule = self._make_rule()
        assert len(rule.actions) == 1
        assert rule.actions[0].type == ActionType.RECORD

    def test_custom_actions(self):
        rule = self._make_rule(
            actions=[{"type": "record", "track": ["returns_1m", "returns_5m"]}]
        )
        assert rule.actions[0].track == ["returns_1m", "returns_5m"]


# ─────────────────────────────────────────────────────────────────────────────
# Parser Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestParser:
    def test_parse_rule_from_dict(self):
        data = {
            "id": "test",
            "trigger": {
                "type": "AND",
                "conditions": [{"factor": "trade_rate", "op": "gt", "value": 5.0}],
            },
        }
        rule = parse_rule(data)
        assert rule.id == "test"
        assert rule.trigger.conditions[0].value == 5.0

    def test_parse_rule_file(self):
        rule_data = {
            "id": "file_test",
            "name": "From file",
            "version": 2,
            "trigger": {
                "type": "AND",
                "conditions": [
                    {
                        "factor": "trade_rate",
                        "op": "gt",
                        "value": 3.0,
                        "timeframe": "trade",
                    },
                ],
            },
            "actions": [{"type": "record", "track": ["returns_5m"]}],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(rule_data, f)
            path = f.name

        try:
            rule = parse_rule_file(path)
            assert rule.id == "file_test"
            assert rule.version == 2
            assert rule.trigger.conditions[0].timeframe == "trade"
            assert rule.actions[0].track == ["returns_5m"]
        finally:
            Path(path).unlink()

    def test_parse_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            parse_rule_file("/nonexistent/rule.yaml")

    def test_load_rules_from_dir(self):
        rules_dir = Path(tempfile.mkdtemp())
        for i in range(3):
            data = {
                "id": f"rule_{i}",
                "trigger": {
                    "type": "AND",
                    "conditions": [
                        {"factor": "trade_rate", "op": "gt", "value": float(i)}
                    ],
                },
            }
            (rules_dir / f"rule_{i}.yaml").write_text(yaml.dump(data))

        rules = load_rules_from_dir(rules_dir)
        assert len(rules) == 3
        assert {r.id for r in rules} == {"rule_0", "rule_1", "rule_2"}

    def test_load_rules_skips_invalid(self):
        rules_dir = Path(tempfile.mkdtemp())
        # Valid rule
        data = {
            "id": "valid",
            "trigger": {
                "type": "AND",
                "conditions": [{"factor": "x", "op": "gt", "value": 1.0}],
            },
        }
        (rules_dir / "valid.yaml").write_text(yaml.dump(data))
        # Invalid YAML (just a string)
        (rules_dir / "invalid.yaml").write_text("not a dict")

        rules = load_rules_from_dir(rules_dir)
        assert len(rules) == 1
        assert rules[0].id == "valid"


# ─────────────────────────────────────────────────────────────────────────────
# Factor Validation Tests
# ─────────────────────────────────────────────────────────────────────────────


class TestFactorValidation:
    def _make_checker(self, known_factors: dict[str, list[str]]):
        """Create a mock checker. known_factors: {factor_id: [timeframes]}"""

        def checker(factor_id: str, timeframe: str) -> bool:
            if factor_id not in known_factors:
                return False
            if not timeframe:
                return True
            return timeframe in known_factors[factor_id]

        return checker

    def test_all_factors_known(self):
        rule = parse_rule(
            {
                "id": "test",
                "trigger": {
                    "type": "AND",
                    "conditions": [
                        {
                            "factor": "trade_rate",
                            "op": "gt",
                            "value": 3.0,
                            "timeframe": "trade",
                        },
                    ],
                },
            }
        )
        checker = self._make_checker({"trade_rate": ["trade"]})
        result = validate_rule_factors(rule, checker)
        assert result.valid
        assert len(result.issues) == 0

    def test_unknown_factor(self):
        rule = parse_rule(
            {
                "id": "test",
                "trigger": {
                    "type": "AND",
                    "conditions": [
                        {
                            "factor": "nonexistent",
                            "op": "gt",
                            "value": 3.0,
                            "timeframe": "trade",
                        },
                    ],
                },
            }
        )
        checker = self._make_checker({"trade_rate": ["trade"]})
        result = validate_rule_factors(rule, checker)
        assert not result.valid
        assert any("Unknown factor" in i.message for i in result.issues)

    def test_wrong_timeframe(self):
        rule = parse_rule(
            {
                "id": "test",
                "trigger": {
                    "type": "AND",
                    "conditions": [
                        {
                            "factor": "ema_20",
                            "op": "gt",
                            "value": 100.0,
                            "timeframe": "1h",
                        },
                    ],
                },
            }
        )
        checker = self._make_checker({"ema_20": ["10s", "1m", "5m"]})
        result = validate_rule_factors(rule, checker)
        assert not result.valid
        assert any("not available" in i.message for i in result.issues)

    def test_cross_target_unknown(self):
        rule = parse_rule(
            {
                "id": "test",
                "trigger": {
                    "type": "AND",
                    "conditions": [
                        {
                            "factor": "ema_20",
                            "op": "cross_above",
                            "target": "nonexistent",
                            "timeframe": "10s",
                        },
                    ],
                },
            }
        )
        checker = self._make_checker({"ema_20": ["10s"]})
        result = validate_rule_factors(rule, checker)
        assert not result.valid
        assert any("Unknown cross target" in i.message for i in result.issues)
