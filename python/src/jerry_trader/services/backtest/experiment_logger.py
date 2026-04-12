"""Experiment Logger for Backtest Pipeline (Simplified)

Records experiment results in simplified format for knowledge transfer.

Usage:
    logger = ExperimentLogger()
    exp_id = logger.record_experiment(
        hypothesis="...",
        date="2026-03-13",
        cli_command="...",
        results=BacktestResult,
        lessons=["..."]
    )
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.experiment", log_to_file=True)

# Default experiments directory
import os

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", os.getcwd()))
EXPERIMENTS_DIR = PROJECT_ROOT / "experiments"


class ExperimentLogger:
    """Record and query experiment logs (simplified format)."""

    experiments_dir: Path
    daily_dir: Path
    knowledge_file: Path
    _next_id: int

    def __init__(self, experiments_dir: Path | None = None):
        self.experiments_dir = experiments_dir or EXPERIMENTS_DIR
        self.experiments_dir.mkdir(parents=True, exist_ok=True)

        # Today's directory
        self.daily_dir = self.experiments_dir / datetime.now().strftime("%Y-%m-%d")
        self.daily_dir.mkdir(parents=True, exist_ok=True)

        # Knowledge base (single file)
        self.knowledge_file = self.experiments_dir / "knowledge.yaml"

        # Counter for daily experiments
        self._next_id = self._get_next_id()

    def _get_next_id(self) -> int:
        """Get next experiment ID for today."""
        existing = list(self.daily_dir.glob("exp_*.yaml"))
        if not existing:
            return 1

        ids = []
        for f in existing:
            try:
                id_part = f.stem.split("_")[-1]
                ids.append(int(id_part))
            except ValueError:
                continue

        return max(ids) + 1 if ids else 1

    def record_experiment(
        self,
        hypothesis: str,
        date: str,
        cli_command: str,
        result: Any,  # BacktestResult
        lessons: list[str],
    ) -> str:
        """Record experiment in simplified format.

        Args:
            hypothesis: What we're testing (1-2 sentences)
            date: Backtest date YYYY-MM-DD
            cli_command: Exact CLI command
            result: BacktestResult object (includes run_id)
            lessons: What we learned

        Returns:
            experiment_id
        """
        exp_id = f"exp_{datetime.now().strftime('%Y%m%d')}_{self._next_id:03d}"

        # Get run_id from result (links to ClickHouse)
        run_id = getattr(result, "run_id", None)

        # Compute per-ticker stats from signals
        per_ticker = self._compute_per_ticker(result)

        # Build simplified experiment
        experiment = {
            "id": exp_id,
            "date": date,
            "hypothesis": hypothesis,
            "cli_command": cli_command,
            # Link to ClickHouse
            "run_id": run_id,
            # Results
            "signals": result.total_signals,
            "win_rate_10m": f"{result.win_rate.get('10m', 0) * 100:.0f}%",
            "avg_return_10m": f"{result.avg_return.get('10m', 0) * 100:.2f}%",
            "profit_factor": round(result.profit_factor, 2),
            "mfe": f"+{result.avg_mfe * 100:.2f}%",
            "mae": f"{result.avg_mae * 100:.2f}%",
            # Ticker breakdown
            "per_ticker": per_ticker,
            # Lessons
            "lessons": lessons,
            # Validation status
            "validation": "quick_check",
            "blockers": self._check_blockers(result),
        }

        # Write to file
        exp_file = self.daily_dir / f"{exp_id}.yaml"
        with open(exp_file, "w") as f:
            yaml.dump(experiment, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Created experiment: {exp_id}")
        self._next_id += 1

        # Extract to knowledge base
        self._extract_to_knowledge(exp_id, experiment)

        return exp_id

    def _compute_per_ticker(self, result: Any) -> dict[str, dict[str, Any]]:
        """Compute per-ticker statistics from signals."""
        from collections import defaultdict

        per_ticker: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"signals": 0, "wins": 0, "returns": [], "mfe": [], "mae": []}
        )

        for sig in result.signals:
            ticker = sig.symbol
            per_ticker[ticker]["signals"] += 1

            if sig.returns:
                ret_10m = sig.returns.get("10m", 0)
                if ret_10m > 0:
                    per_ticker[ticker]["wins"] += 1
                per_ticker[ticker]["returns"].append(ret_10m)

            if sig.mfe is not None:
                per_ticker[ticker]["mfe"].append(sig.mfe)
            if sig.mae is not None:
                per_ticker[ticker]["mae"].append(sig.mae)

        # Aggregate
        aggregated = {}
        for ticker, stats in per_ticker.items():
            if stats["signals"] > 0:
                win_rate = stats["wins"] / stats["signals"] * 100
                avg_ret = (
                    sum(stats["returns"]) / len(stats["returns"])
                    if stats["returns"]
                    else 0
                )
                avg_mfe = sum(stats["mfe"]) / len(stats["mfe"]) if stats["mfe"] else 0
                avg_mae = sum(stats["mae"]) / len(stats["mae"]) if stats["mae"] else 0

                aggregated[ticker] = {
                    "signals": stats["signals"],
                    "win": f"{win_rate:.0f}%",
                    "avg_ret": f"{avg_ret * 100:.2f}%",
                    "mfe": f"+{avg_mfe * 100:.2f}%",
                    "mae": f"{avg_mae * 100:.2f}%",
                }

        return aggregated

    def _check_blockers(self, result: Any) -> list[str]:
        """Check validation blockers."""
        blockers = []

        if result.total_signals < 5:
            blockers.append("insufficient_sample_size")

        win_rate_10m = result.win_rate.get("10m", 0) * 100
        if win_rate_10m < 30:
            blockers.append("low_win_rate")

        return blockers

    def _extract_to_knowledge(self, exp_id: str, experiment: dict[str, Any]) -> None:
        """Extract lessons to knowledge.yaml."""
        # Load existing knowledge
        if self.knowledge_file.exists():
            with open(self.knowledge_file) as f:
                knowledge = yaml.safe_load(f) or {"experiments": [], "lessons": []}
        else:
            knowledge = {"experiments": [], "lessons": []}

        # Add experiment reference
        knowledge["experiments"].append(
            {
                "id": exp_id,
                "date": experiment["date"],
                "hypothesis": experiment["hypothesis"],
                "signals": experiment["signals"],
                "win_rate": experiment["win_rate_10m"],
            }
        )

        # Add lessons
        for lesson in experiment["lessons"]:
            knowledge["lessons"].append({"exp_id": exp_id, "lesson": lesson})

        # Write back
        with open(self.knowledge_file, "w") as f:
            yaml.dump(knowledge, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Extracted knowledge from {exp_id}")

    def get_experiment(self, exp_id: str) -> dict[str, Any] | None:
        """Get experiment by ID."""
        exp_file = self._find_experiment_file(exp_id)
        if not exp_file:
            return None

        with open(exp_file) as f:
            return yaml.safe_load(f)

    def _find_experiment_file(self, exp_id: str) -> Path | None:
        """Find experiment file by ID."""
        try:
            date_part = exp_id.split("_")[1]
            date_str = datetime.strptime(date_part, "%Y%m%d").strftime("%Y-%m-%d")
            exp_file = self.experiments_dir / date_str / f"{exp_id}.yaml"
            if exp_file.exists():
                return exp_file
        except (ValueError, IndexError):
            pass

        return None

    def list_experiments(self, date: str | None = None) -> list[dict[str, Any]]:
        """List all experiments."""
        experiments = []

        for date_dir in self.experiments_dir.iterdir():
            if not date_dir.is_dir() or date_dir.name in [
                "knowledge_base",
                "reproducibility",
                "validation_gates",
            ]:
                continue

            if date and date_dir.name != date:
                continue

            for exp_file in date_dir.glob("exp_*.yaml"):
                with open(exp_file) as f:
                    exp = yaml.safe_load(f)
                    experiments.append(exp)

        return experiments

    def get_knowledge(self) -> dict[str, Any]:
        """Get accumulated knowledge."""
        if not self.knowledge_file.exists():
            return {"experiments": [], "lessons": []}

        with open(self.knowledge_file) as f:
            return yaml.safe_load(f) or {"experiments": [], "lessons": []}


# Convenience functions for agent queries
def find_experiments_by_hypothesis(keyword: str) -> list[dict[str, Any]]:
    """Find experiments with hypothesis containing keyword."""
    logger = ExperimentLogger()
    all_exps = logger.list_experiments()
    return [e for e in all_exps if keyword.lower() in e.get("hypothesis", "").lower()]


def get_all_lessons() -> list[str]:
    """Get all accumulated lessons."""
    logger = ExperimentLogger()
    knowledge = logger.get_knowledge()
    return [l["lesson"] for l in knowledge.get("lessons", [])]


def get_ticker_insights(ticker: str) -> list[dict[str, Any]]:
    """Get all insights for a specific ticker."""
    logger = ExperimentLogger()
    all_exps = logger.list_experiments()

    insights = []
    for exp in all_exps:
        per_ticker = exp.get("per_ticker", {})
        if ticker in per_ticker:
            insights.append(
                {
                    "exp_id": exp["id"],
                    "date": exp["date"],
                    "stats": per_ticker[ticker],
                }
            )

    return insights
