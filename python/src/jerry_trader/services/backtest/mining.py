"""Strategy Mining Framework

Automatically discovers profitable trading strategies through parameter grid search.

Approach:
1. Analyze factor distributions from historical data
2. Generate candidate rule variations
3. Run backtests for each candidate (direct Rule objects, no file I/O)
4. Rank by win rate, profit factor, avg return
5. Record to experiment logs (optional)
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from jerry_trader.domain.strategy.rule import (
    Action,
    ActionType,
    ComparisonOp,
    Condition,
    Rule,
    Trigger,
    TriggerType,
)
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.experiment_logger import ExperimentLogger
from jerry_trader.services.backtest.runner import BacktestRunner
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("strategy_mining", log_to_file=True)

# Default experiments directory
import os

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", os.getcwd()))
EXPERIMENTS_DIR = PROJECT_ROOT / "experiments"


# ─────────────────────────────────────────────────────────────────────────────
# Mining Config
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class MiningConfig:
    """Configuration for strategy mining."""

    # Factor thresholds to explore
    trade_rate_range: tuple[float, float, float] = (
        100.0,
        400.0,
        50.0,
    )  # (min, max, step)
    ema_period_range: tuple[int, int, int] = (10, 30, 5)  # (min, max, step)

    # Backtest parameters
    gain_threshold: float = 2.0
    top_n: int = 20
    slippage: float = 0.001

    # Evaluation criteria
    min_signals: int = 3  # Minimum signals to consider strategy valid
    min_win_rate: float = 0.3  # Minimum 30% win rate
    min_profit_factor: float = 0.5  # Minimum profit factor


@dataclass
class MiningResult:
    """Result of mining a single strategy candidate."""

    rule_id: str
    rule: Rule  # Keep the actual Rule object for reference
    total_signals: int
    win_rate_1m: float
    win_rate_5m: float
    win_rate_15m: float
    avg_return_1m: float
    avg_return_5m: float
    avg_return_15m: float
    profit_factor: float
    avg_mfe: float
    avg_mae: float
    factors: dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy Candidates Generator
# ─────────────────────────────────────────────────────────────────────────────


def generate_trade_rate_candidates(config: MiningConfig) -> list[Rule]:
    """Generate trade_rate threshold candidates."""
    min_val, max_val, step = config.trade_rate_range
    rules = []

    for threshold in range(int(min_val), int(max_val) + 1, int(step)):
        rule = Rule(
            id=f"trade_rate_gt_{threshold}",
            name=f"Trade rate > {threshold}",
            version=1,
            trigger=Trigger(
                type=TriggerType.AND,
                conditions=[
                    Condition(
                        factor="trade_rate",
                        timeframe="trade",
                        op=ComparisonOp.GT,
                        value=float(threshold),
                    )
                ],
            ),
            actions=[
                Action(
                    type=ActionType.RECORD,
                    track=["returns_1m", "returns_5m", "returns_15m"],
                )
            ],
        )
        rules.append(rule)

    return rules


def generate_ema_cross_candidates(config: MiningConfig) -> list[Rule]:
    """Generate EMA cross candidates (price crosses above/below EMA)."""
    # Note: Cross operators need prev_factors, so this is for future implementation
    # For now, generate EMA distance strategies
    rules = []

    # EMA distance: close > EMA by X%
    for pct in [0.5, 1.0, 2.0, 3.0]:
        rule = Rule(
            id=f"price_above_ema_pct_{pct}",
            name=f"Price > EMA by {pct}%",
            version=1,
            trigger=Trigger(
                type=TriggerType.AND,
                conditions=[
                    Condition(
                        factor="close",
                        timeframe="1m",
                        op=ComparisonOp.GT,
                        value=0,  # Will need custom handling for relative comparison
                    )
                ],
            ),
            actions=[Action(type=ActionType.RECORD)],
        )
        rules.append(rule)

    return rules


def generate_combined_candidates(config: MiningConfig) -> list[Rule]:
    """Generate combined factor strategies."""
    min_rate, max_rate, step_rate = config.trade_rate_range
    rules = []

    # High trade rate + price relative to EMA
    # Note: Current factor set only has trade_rate and ema_20
    # Future: add more factors for combination strategies

    for threshold in [150.0, 200.0, 250.0, 300.0]:
        rule = Rule(
            id=f"high_rate_{threshold}",
            name=f"High trade rate ({threshold}) momentum",
            version=1,
            trigger=Trigger(
                type=TriggerType.AND,
                conditions=[
                    Condition(
                        factor="trade_rate",
                        timeframe="trade",
                        op=ComparisonOp.GT,
                        value=threshold,
                    )
                ],
            ),
            actions=[Action(type=ActionType.RECORD)],
        )
        rules.append(rule)

    return rules


# ─────────────────────────────────────────────────────────────────────────────
# Strategy Mining Runner
# ─────────────────────────────────────────────────────────────────────────────


class StrategyMiner:
    """Run strategy mining for a given date.

    Uses BacktestConfig.rules directly — no file I/O needed.
    """

    def __init__(self, config: MiningConfig | None = None):
        self.config = config or MiningConfig()

    def mine(self, date: str, record_experiment: bool = False) -> list[MiningResult]:
        """Run mining for a single date.

        Args:
            date: Backtest date YYYY-MM-DD
            record_experiment: If True, save results to experiments/ directory
        """
        logger.info(f"Strategy mining for {date}")

        # Generate candidates
        candidates = generate_trade_rate_candidates(self.config)
        logger.info(f"Generated {len(candidates)} trade_rate candidates")

        results: list[MiningResult] = []

        for rule in candidates:
            try:
                result = self._evaluate_candidate(rule, date)
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Failed to evaluate {rule.id}: {e}")

        # Sort by profit factor
        results.sort(key=lambda r: r.profit_factor, reverse=True)

        # Record to experiment log if requested
        if record_experiment and results:
            self._record_mining_experiment(date, results)

        return results

    def _evaluate_candidate(self, rule: Rule, date: str) -> MiningResult | None:
        """Evaluate a single candidate rule directly (no file I/O).

        Each rule is tested INDIVIDUALLY by passing it directly to BacktestConfig.
        This ensures isolated evaluation without the workaround of saving to temp files.
        """
        # Create config with the rule directly (no rules_dir needed)
        config = BacktestConfig(
            date=date,
            rules=[rule],  # Direct Rule object — no file I/O
            gain_threshold=self.config.gain_threshold,
            pre_filter=PreFilterConfig(
                top_n=self.config.top_n,
                min_gain_pct=self.config.gain_threshold,
            ),
            slippage_buffer=self.config.slippage,
            output_clickhouse=False,  # Don't persist mining results
            output_console=False,  # No console output during mining
        )

        # Run backtest with this rule
        ch_client = get_clickhouse_client()
        runner = BacktestRunner(config, ch_client=ch_client)

        try:
            bt_result = runner.run()
        except Exception as e:
            logger.error(f"Backtest failed for {rule.id}: {e}")
            return None

        # Filter by minimum signals
        if bt_result.total_signals < self.config.min_signals:
            logger.debug(f"{rule.id}: skipped (only {bt_result.total_signals} signals)")
            return None

        # Extract metrics
        result = MiningResult(
            rule_id=rule.id,
            rule=rule,
            total_signals=bt_result.total_signals,
            win_rate_1m=bt_result.win_rate.get("1m", 0.0),
            win_rate_5m=bt_result.win_rate.get("5m", 0.0),
            win_rate_15m=bt_result.win_rate.get("15m", 0.0),
            avg_return_1m=bt_result.avg_return.get("1m", 0.0),
            avg_return_5m=bt_result.avg_return.get("5m", 0.0),
            avg_return_15m=bt_result.avg_return.get("15m", 0.0),
            profit_factor=bt_result.profit_factor,
            avg_mfe=bt_result.avg_mfe,
            avg_mae=bt_result.avg_mae,
            factors={"trade_rate_threshold": rule.trigger.conditions[0].value},
        )

        logger.info(
            f"{rule.id}: {result.total_signals} signals, "
            f"win_rate_5m={result.win_rate_5m:.2%}, "
            f"profit_factor={result.profit_factor:.2f}"
        )

        return result

    def _record_mining_experiment(self, date: str, results: list[MiningResult]) -> str:
        """Record mining results to experiment logs.

        Creates a mining experiment file in experiments/YYYY-MM-DD/
        with all candidate results.

        Returns:
            experiment_id
        """
        # Create daily directory
        today = datetime.now().strftime("%Y-%m-%d")
        daily_dir = EXPERIMENTS_DIR / today
        daily_dir.mkdir(parents=True, exist_ok=True)

        # Get next experiment ID
        existing = list(daily_dir.glob("mining_*.yaml"))
        next_id = len(existing) + 1
        exp_id = f"mining_{datetime.now().strftime('%Y%m%d')}_{next_id:03d}"

        # Build experiment record
        best = results[0] if results else None
        experiment = {
            "id": exp_id,
            "type": "parameter_sweep",
            "date": date,
            "hypothesis": f"trade_rate threshold sweep ({self.config.trade_rate_range[0]}-{self.config.trade_rate_range[1]})",
            "candidates_tested": len(results),
            "best_strategy": {
                "rule_id": best.rule_id if best else None,
                "signals": best.total_signals if best else 0,
                "win_rate_5m": f"{best.win_rate_5m * 100:.1f}%" if best else "N/A",
                "profit_factor": round(best.profit_factor, 2) if best else 0,
                "avg_return_5m": f"{best.avg_return_5m * 100:.2f}%" if best else "N/A",
            },
            "all_candidates": [
                {
                    "rule_id": r.rule_id,
                    "threshold": r.factors.get("trade_rate_threshold"),
                    "signals": r.total_signals,
                    "win_rate_5m": f"{r.win_rate_5m * 100:.1f}%",
                    "profit_factor": round(r.profit_factor, 2),
                    "avg_return_5m": f"{r.avg_return_5m * 100:.2f}%",
                }
                for r in results
            ],
            "lessons": self._extract_lessons(results),
            "validation": "quick_check",
            "next_steps": [
                "Run multi-date validation if best profit_factor > 1.0",
                "Test combined conditions (trade_rate + price_direction)",
            ],
        }

        # Write to file
        exp_file = daily_dir / f"{exp_id}.yaml"
        with open(exp_file, "w") as f:
            yaml.dump(experiment, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Recorded mining experiment: {exp_id}")

        # Update knowledge.yaml
        self._update_knowledge(exp_id, date, results)

        return exp_id

    def _extract_lessons(self, results: list[MiningResult]) -> list[str]:
        """Extract lessons from mining results."""
        lessons = []

        if not results:
            lessons.append(
                "No valid strategies found — all candidates had < min_signals"
            )
            return lessons

        best = results[0]

        # Best threshold lesson
        threshold = best.factors.get("trade_rate_threshold")
        lessons.append(
            f"Best threshold: trade_rate > {threshold} "
            f"(PF={best.profit_factor:.2f}, win={best.win_rate_5m:.1%})"
        )

        # Threshold trend
        if len(results) >= 2:
            low = results[-1]
            high = results[0]
            if low.factors.get("trade_rate_threshold") < high.factors.get(
                "trade_rate_threshold"
            ):
                if low.profit_factor > high.profit_factor:
                    lessons.append("Lower threshold yields better profit factor")
                else:
                    lessons.append("Higher threshold yields better profit factor")

        # Sample size warning
        total_signals = sum(r.total_signals for r in results)
        if total_signals < 30:
            lessons.append(
                f"Small sample ({total_signals} signals) — need multi-date validation"
            )

        return lessons

    def _update_knowledge(
        self, exp_id: str, date: str, results: list[MiningResult]
    ) -> None:
        """Update knowledge.yaml with mining insights."""
        knowledge_file = EXPERIMENTS_DIR / "knowledge.yaml"

        if knowledge_file.exists():
            with open(knowledge_file) as f:
                knowledge = yaml.safe_load(f) or {"experiments": [], "lessons": []}
        else:
            knowledge = {"experiments": [], "lessons": []}

        # Add experiment reference
        best = results[0] if results else None
        knowledge["experiments"].append(
            {
                "id": exp_id,
                "type": "mining",
                "date": date,
                "hypothesis": "trade_rate threshold sweep",
                "best_strategy": best.rule_id if best else None,
            }
        )

        # Add lessons
        for lesson in self._extract_lessons(results):
            knowledge["lessons"].append({"exp_id": exp_id, "lesson": lesson})

        with open(knowledge_file, "w") as f:
            yaml.dump(knowledge, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Updated knowledge.yaml from {exp_id}")

    def report(self, results: list[MiningResult]) -> str:
        """Generate a summary report."""
        if not results:
            return "No valid strategies found."

        lines = [
            "# Strategy Mining Results",
            "",
            f"Total candidates tested: {len(results)}",
            "",
            "## Top Strategies by Profit Factor",
            "",
            "| Rule | Signals | Win Rate 5m | Profit Factor | Avg Return 5m |",
            "|------|---------|-------------|---------------|---------------|",
        ]

        for r in results[:10]:
            lines.append(
                f"| {r.rule_id} | {r.total_signals} | {r.win_rate_5m:.1%} | "
                f"{r.profit_factor:.2f} | {r.avg_return_5m:.2%} |"
            )

        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────────────────────────────────────


def main():
    """Run strategy mining.

    Results are:
    - Printed to console (summary table)
    - Recorded to experiments/YYYY-MM-DD/mining_*.yaml (if --record-experiment)
    - Knowledge extracted to experiments/knowledge.yaml
    """
    import argparse

    parser = argparse.ArgumentParser(description="Strategy Mining")
    parser.add_argument("--date", required=True, help="Backtest date (YYYY-MM-DD)")
    parser.add_argument(
        "--record-experiment",
        action="store_true",
        default=True,
        help="Record results to experiments/ directory (default: True)",
    )
    parser.add_argument(
        "--no-record",
        action="store_true",
        help="Skip experiment recording (just print results)",
    )
    args = parser.parse_args()

    miner = StrategyMiner()
    results = miner.mine(
        args.date, record_experiment=args.record_experiment and not args.no_record
    )

    # Print summary to console
    report = miner.report(results)
    print(report)

    if args.record_experiment and not args.no_record and results:
        print(
            f"\nResults recorded to experiments/{datetime.now().strftime('%Y-%m-%d')}/"
        )
        print("See knowledge.yaml for accumulated lessons")


if __name__ == "__main__":
    main()
