"""Signal evaluator for backtest — evaluates DSL rules against FactorTimeseries.

Walks the unified factor timeseries chronologically, evaluates each loaded rule
at every timestamp, and collects trigger events with the factor snapshot and
price at trigger time.

Reuses evaluate_condition/evaluate_trigger from SignalEngine — same logic as live.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from jerry_trader.domain.strategy.rule import Rule
from jerry_trader.domain.strategy.rule_parser import load_rules_from_dir
from jerry_trader.services.backtest.batch_engine import FactorTimeseries
from jerry_trader.services.signal.engine import evaluate_trigger
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.evaluator", log_to_file=True)


@dataclass(frozen=True, slots=True)
class TriggerPoint:
    """A signal trigger event found during backtest evaluation.

    Captures the exact state when a rule fired: which rule, which ticker,
    when it fired, what the factors were, and what the price was.
    """

    rule_id: str
    symbol: str
    trigger_time_ms: int
    trigger_price: float | None
    factors: dict[str, float]


@dataclass
class EvalResult:
    """Result of evaluating all rules against one ticker's factor timeseries.

    Contains all trigger points found for this ticker across all rules.
    """

    symbol: str
    triggers: list[TriggerPoint] = field(default_factory=list)


class SignalEvaluator:
    """Evaluates DSL rules against a FactorTimeseries for backtest.

    Loads rules from YAML (same format as live SignalEngine), walks the
    factor timeseries chronologically, and fires triggers using the exact
    same evaluate_trigger logic.

    Unlike live SignalEngine, this:
    - Does not need Redis or WebSocket
    - Has no cooldown/dedup (every timestamp is evaluated independently)
    - Collects all trigger points in memory
    """

    def __init__(self, rules_dir: str = "config/rules/"):
        self._rules_dir = rules_dir
        self._rules: list[Rule] = []

    def load_rules(self) -> list[Rule]:
        """Load DSL rules from directory.

        Returns:
            List of enabled Rule objects.
        """
        all_rules = load_rules_from_dir(self._rules_dir)
        self._rules = [r for r in all_rules if r.enabled]

        logger.info(
            f"SignalEvaluator: loaded {len(self._rules)} enabled rules "
            f"from {self._rules_dir}"
        )
        for rule in self._rules:
            logger.info(f"  Rule: {rule.id} v{rule.version} — {rule.name}")

        return self._rules

    @property
    def rules(self) -> list[Rule]:
        return list(self._rules)

    def evaluate(
        self,
        symbol: str,
        ts: FactorTimeseries,
        *,
        price_source: str = "close",
    ) -> EvalResult:
        """Evaluate all rules against a ticker's factor timeseries.

        Args:
            symbol: Ticker symbol.
            ts: FactorTimeseries = {timestamp_ms: {factor_name: value}}.
            price_source: Factor name to use as trigger price (default "close").

        Returns:
            EvalResult with all trigger points found.
        """
        if not self._rules:
            logger.warning("SignalEvaluator: no rules loaded, call load_rules() first")
            return EvalResult(symbol=symbol)

        result = EvalResult(symbol=symbol)
        sorted_times = sorted(ts.keys())

        for ts_ms in sorted_times:
            factors = ts[ts_ms]

            for rule in self._rules:
                # Phase 1: evaluate all conditions against the merged factor dict
                if not evaluate_trigger(
                    rule.trigger.conditions, rule.trigger.type, factors
                ):
                    continue

                # Get price from factors if available
                trigger_price = factors.get(price_source)

                result.triggers.append(
                    TriggerPoint(
                        rule_id=rule.id,
                        symbol=symbol,
                        trigger_time_ms=ts_ms,
                        trigger_price=trigger_price,
                        factors=dict(factors),
                    )
                )

        logger.debug(
            f"Evaluator: {symbol} — {len(result.triggers)} triggers "
            f"from {len(sorted_times)} timestamps across {len(self._rules)} rules"
        )

        return result


def evaluate_ticker(
    symbol: str,
    ts: FactorTimeseries,
    rules: list[Rule],
    *,
    price_source: str = "close",
) -> EvalResult:
    """Convenience function: evaluate rules against a ticker's factors.

    Stateless alternative to SignalEvaluator — useful for one-shot evaluation
    without instantiating the class.
    """
    result = EvalResult(symbol=symbol)
    sorted_times = sorted(ts.keys())

    for ts_ms in sorted_times:
        factors = ts[ts_ms]

        for rule in rules:
            if not evaluate_trigger(
                rule.trigger.conditions, rule.trigger.type, factors
            ):
                continue

            trigger_price = factors.get(price_source)
            result.triggers.append(
                TriggerPoint(
                    rule_id=rule.id,
                    symbol=symbol,
                    trigger_time_ms=ts_ms,
                    trigger_price=trigger_price,
                    factors=dict(factors),
                )
            )

    return result
