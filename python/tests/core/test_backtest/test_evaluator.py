"""Tests for SignalEvaluator — DSL rule evaluation against FactorTimeseries."""

from jerry_trader.domain.strategy.rule import Rule
from jerry_trader.services.backtest.batch_engine import FactorTimeseries
from jerry_trader.services.backtest.evaluator import (
    EvalResult,
    SignalEvaluator,
    TriggerPoint,
    evaluate_ticker,
)


def _make_rule(
    rule_id: str = "test_rule",
    factor: str = "trade_rate",
    op: str = "gt",
    value: float = 3.0,
) -> Rule:
    """Create a simple single-condition rule."""
    return Rule.model_validate(
        {
            "id": rule_id,
            "name": "Test rule",
            "version": 1,
            "trigger": {
                "type": "AND",
                "conditions": [
                    {"factor": factor, "op": op, "value": value, "timeframe": "trade"}
                ],
            },
        }
    )


class TestSignalEvaluator:
    def test_evaluate_finds_trigger(self):
        ts: FactorTimeseries = {
            1000: {"trade_rate": 1.0},
            2000: {"trade_rate": 4.0},  # triggers: > 3.0
            3000: {"trade_rate": 5.0},  # triggers: > 3.0
            4000: {"trade_rate": 2.0},
        }

        evaluator = SignalEvaluator()
        evaluator._rules = [_make_rule()]

        result = evaluator.evaluate("TEST", ts)
        assert result.symbol == "TEST"
        assert len(result.triggers) == 2
        assert result.triggers[0].trigger_time_ms == 2000
        assert result.triggers[1].trigger_time_ms == 3000

    def test_evaluate_no_triggers(self):
        ts: FactorTimeseries = {
            1000: {"trade_rate": 1.0},
            2000: {"trade_rate": 2.0},
        }

        evaluator = SignalEvaluator()
        evaluator._rules = [_make_rule(value=10.0)]

        result = evaluator.evaluate("TEST", ts)
        assert len(result.triggers) == 0

    def test_evaluate_empty_timeseries(self):
        ts: FactorTimeseries = {}

        evaluator = SignalEvaluator()
        evaluator._rules = [_make_rule()]

        result = evaluator.evaluate("TEST", ts)
        assert len(result.triggers) == 0

    def test_evaluate_no_rules_loaded(self):
        ts: FactorTimeseries = {1000: {"trade_rate": 5.0}}

        evaluator = SignalEvaluator()
        # No rules loaded
        result = evaluator.evaluate("TEST", ts)
        assert len(result.triggers) == 0

    def test_evaluate_multiple_rules(self):
        ts: FactorTimeseries = {
            1000: {"trade_rate": 4.0, "volume": 100},
            2000: {"trade_rate": 2.0, "volume": 200},
        }

        rule_a = _make_rule(rule_id="rule_a", factor="trade_rate", value=3.0)
        rule_b = _make_rule(rule_id="rule_b", factor="volume", value=150.0)

        evaluator = SignalEvaluator()
        evaluator._rules = [rule_a, rule_b]

        result = evaluator.evaluate("TEST", ts)
        assert (
            len(result.triggers) == 2
        )  # rule_a@1000 + rule_b@2000 (volume=100 at t=1000 does NOT trigger > 150)
        rule_ids = {t.rule_id for t in result.triggers}
        assert rule_ids == {"rule_a", "rule_b"}

    def test_evaluate_and_rule_all_conditions(self):
        """AND rule requires ALL conditions met."""
        ts: FactorTimeseries = {
            1000: {"trade_rate": 4.0, "volume": 50},  # volume too low
            2000: {"trade_rate": 4.0, "volume": 200},  # both met
        }

        rule = Rule.model_validate(
            {
                "id": "and_rule",
                "trigger": {
                    "type": "AND",
                    "conditions": [
                        {
                            "factor": "trade_rate",
                            "op": "gt",
                            "value": 3.0,
                            "timeframe": "trade",
                        },
                        {
                            "factor": "volume",
                            "op": "gt",
                            "value": 100.0,
                            "timeframe": "trade",
                        },
                    ],
                },
            }
        )

        evaluator = SignalEvaluator()
        evaluator._rules = [rule]

        result = evaluator.evaluate("TEST", ts)
        assert len(result.triggers) == 1
        assert result.triggers[0].trigger_time_ms == 2000

    def test_evaluate_or_rule(self):
        """OR rule fires if ANY condition met."""
        ts: FactorTimeseries = {
            1000: {"trade_rate": 4.0, "volume": 50},
            2000: {"trade_rate": 1.0, "volume": 200},
        }

        rule = Rule.model_validate(
            {
                "id": "or_rule",
                "trigger": {
                    "type": "OR",
                    "conditions": [
                        {
                            "factor": "trade_rate",
                            "op": "gt",
                            "value": 3.0,
                            "timeframe": "trade",
                        },
                        {
                            "factor": "volume",
                            "op": "gt",
                            "value": 100.0,
                            "timeframe": "trade",
                        },
                    ],
                },
            }
        )

        evaluator = SignalEvaluator()
        evaluator._rules = [rule]

        result = evaluator.evaluate("TEST", ts)
        assert len(result.triggers) == 2

    def test_trigger_point_has_price(self):
        ts: FactorTimeseries = {
            1000: {"trade_rate": 5.0, "close": 10.50},
        }

        evaluator = SignalEvaluator()
        evaluator._rules = [_make_rule()]

        result = evaluator.evaluate("TEST", ts, price_source="close")
        assert result.triggers[0].trigger_price == 10.50

    def test_trigger_point_captures_factors(self):
        ts: FactorTimeseries = {
            1000: {"trade_rate": 5.0, "momentum": 0.03},
        }

        evaluator = SignalEvaluator()
        evaluator._rules = [_make_rule()]

        result = evaluator.evaluate("TEST", ts)
        assert result.triggers[0].factors["trade_rate"] == 5.0
        assert result.triggers[0].factors["momentum"] == 0.03


class TestEvaluateTicker:
    """Test the stateless evaluate_ticker convenience function."""

    def test_stateless_evaluation(self):
        ts: FactorTimeseries = {
            1000: {"trade_rate": 4.0},
        }
        rules = [_make_rule()]

        result = evaluate_ticker("TEST", ts, rules)
        assert len(result.triggers) == 1
        assert result.triggers[0].rule_id == "test_rule"

    def test_between_operator(self):
        ts: FactorTimeseries = {
            1000: {"trade_rate": 2.5},
            2000: {"trade_rate": 3.5},
            3000: {"trade_rate": 6.0},
        }

        rule = Rule.model_validate(
            {
                "id": "between_rule",
                "trigger": {
                    "type": "AND",
                    "conditions": [
                        {
                            "factor": "trade_rate",
                            "op": "between",
                            "lo": 3.0,
                            "hi": 5.0,
                            "timeframe": "trade",
                        },
                    ],
                },
            }
        )

        result = evaluate_ticker("TEST", ts, [rule])
        assert len(result.triggers) == 1
        assert result.triggers[0].trigger_time_ms == 2000
