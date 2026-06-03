#!/usr/bin/env python3
"""Conditional Probability Matrix & Pareto Frontier Analysis.

Enumerates factor threshold combinations and computes precision/recall for each.
Finds the Pareto frontier between coverage (recall) and quality (precision).

Answers: Is our current threshold on the Pareto frontier? Can we get more
coverage without sacrificing quality, or more quality without losing coverage?

Usage:
    poetry run python -m jerry_trader.scripts.condition_probability_matrix \
        --input data/dense_signals/explore.parquet

    # Compare current vs best on Pareto frontier
    poetry run python -m jerry_trader.scripts.condition_probability_matrix \
        --input data/dense_signals/explore.parquet --compare-current
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("condition_matrix", log_to_file=True)

CURRENT_CONDITIONS = {
    "gap_pct": 4.0,
    "rel_vol_20": 2.0,
    "vol_accel_5_15": 1.0,
    "trade_rate": 100,
    "session_phase": "mid",  # not a numeric filter but noted
}

# Threshold sweeps
THRESHOLD_GRID = {
    "gap_pct": [2.0, 3.0, 4.0, 5.0, 6.0],
    "rel_vol_20": [1.0, 1.5, 2.0, 2.5, 3.0],
    "vol_accel_5_15": [0.6, 0.8, 1.0, 1.2, 1.5],
    "trade_rate": [200, 150, 100, 75, 50],
}


def compute_condition_matrix(
    df: pd.DataFrame,
    factors: list[str],
    target_tiers: list[str] | None = None,
) -> pd.DataFrame:
    """Enumerate threshold combinations and compute precision/recall.

    Args:
        df: DataFrame from dense_signal_batch with factor columns and max_return_pct.
        factors: Factors to enumerate thresholds for (e.g., ["gap_pct", "rel_vol_20"]).
        target_tiers: Tiers to count as "winner" (default: ["big", "medium"]).

    Returns:
        DataFrame with columns: factor thresholds, precision, recall, avg_return, n_signals.
    """
    if target_tiers is None:
        target_tiers = ["big", "medium"]

    df = df.copy()

    # Classify tier if not already present
    if "tier" not in df.columns:
        df["tier"] = pd.cut(
            df["max_return_pct"],
            bins=[-float("inf"), 0, 10, 20, 50, float("inf")],
            labels=["loser", "flat", "small", "medium", "big"],
        )

    is_winner = df["tier"].isin(target_tiers)
    total_winners = is_winner.sum()

    if total_winners == 0:
        logger.warning("No winners found in dataset")
        return pd.DataFrame()

    # Generate threshold combinations
    thresholds = {}
    for f in factors:
        if f in THRESHOLD_GRID and f in df.columns:
            thresholds[f] = THRESHOLD_GRID[f]
        elif f in df.columns:
            # Auto-generate thresholds from quantiles
            thresholds[f] = [
                float(df[f].quantile(q)) for q in [0.1, 0.25, 0.5, 0.75, 0.9]
            ]
        else:
            logger.warning(f"Factor {f} not in dataset, skipping")

    if not thresholds:
        return pd.DataFrame()

    # Enumerate all combinations
    import itertools

    factor_names = list(thresholds.keys())
    threshold_values = [thresholds[f] for f in factor_names]

    total_combos = 1
    for vals in threshold_values:
        total_combos *= len(vals)
    logger.info(f"Enumerating {total_combos} combinations across {factor_names}")

    results = []
    for combo in itertools.product(*threshold_values):
        # Build mask for this combination
        mask = pd.Series(True, index=df.index)
        for factor, thresh in zip(factor_names, combo):
            mask &= df[factor] > thresh

        n_signals = mask.sum()
        if n_signals < 5:
            continue

        winners_captured = (mask & is_winner).sum()
        precision = winners_captured / n_signals
        recall = winners_captured / total_winners
        avg_return = float(df.loc[mask, "max_return_pct"].mean())
        median_return = float(df.loc[mask, "max_return_pct"].median())

        row = {
            "n_signals": int(n_signals),
            "precision": float(precision),
            "recall": float(recall),
            "avg_return": avg_return,
            "median_return": median_return,
        }
        for factor, thresh in zip(factor_names, combo):
            row[factor] = thresh

        results.append(row)

    logger.info(f"Generated {len(results)} valid combinations")
    return pd.DataFrame(results)


def find_pareto_frontier(
    matrix: pd.DataFrame,
    x_col: str = "recall",
    y_col: str = "precision",
) -> pd.DataFrame:
    """Find Pareto-optimal points: no other point has both higher x and higher y.

    For recall (x) and precision (y): a point dominates another if it has
    strictly higher precision at the same recall, or higher recall at the
    same precision.
    """
    if matrix.empty:
        return matrix

    points = matrix[[x_col, y_col]].values
    n = len(points)

    # A point i is Pareto-optimal if no point j has (x_j >= x_i AND y_j >= y_i AND not equal)
    pareto_mask = np.ones(n, dtype=bool)
    for i in range(n):
        if not pareto_mask[i]:
            continue
        for j in range(n):
            if i == j:
                continue
            # j dominates i: j is strictly better in one dimension and at least as good in the other
            if (
                points[j, 0] >= points[i, 0]
                and points[j, 1] >= points[i, 1]
                and (points[j, 0] > points[i, 0] or points[j, 1] > points[i, 1])
            ):
                pareto_mask[i] = False
                break

    frontier = matrix[pareto_mask].sort_values(x_col)
    return frontier


def find_current_point(
    matrix: pd.DataFrame,
    conditions: dict[str, float],
) -> pd.Series | None:
    """Find the row in the matrix that matches current conditions."""
    mask = pd.Series(True, index=matrix.index)
    for factor, thresh in conditions.items():
        if factor in matrix.columns:
            mask &= matrix[factor] == thresh

    matches = matrix[mask]
    if len(matches) > 0:
        return matches.iloc[0]
    return None


def format_report(
    matrix: pd.DataFrame,
    frontier: pd.DataFrame,
    current_point=None,
    factors: list[str] | None = None,
) -> str:
    """Format condition matrix results as a markdown report."""
    lines = [
        "# Conditional Probability Matrix",
        "",
        f"- Total combinations evaluated: {len(matrix)}",
        f"- Pareto-optimal points: {len(frontier)}",
        "",
    ]

    if current_point is not None:
        lines.extend(
            [
                "## Current Configuration",
                "",
                f"| Factor | Threshold |",
                f"|--------|-----------|",
            ]
        )
        for factor in CURRENT_CONDITIONS:
            if factor in current_point.index:
                lines.append(f"| {factor} | {current_point[factor]} |")

        lines.extend(
            [
                "",
                f"- Precision: {current_point.get('precision', 0):.1%}",
                f"- Recall: {current_point.get('recall', 0):.1%}",
                f"- Avg Return: {current_point.get('avg_return', 0):.1f}%",
                f"- Signals: {int(current_point.get('n_signals', 0))}",
                "",
            ]
        )

    lines.extend(
        [
            "## Pareto Frontier (sorted by recall)",
            "",
            "| gap_pct | rel_vol_20 | vol_accel | trade_rate | Precision | Recall | AvgReturn | N |",
            "|---------|------------|-----------|------------|-----------|--------|-----------|---|",
        ]
    )

    for _, row in frontier.iterrows():
        gap = row.get("gap_pct", "-")
        rv = row.get("rel_vol_20", "-")
        va = row.get("vol_accel_5_15", "-")
        tr = row.get("trade_rate", "-")
        prec = row.get("precision", 0)
        rec = row.get("recall", 0)
        avg = row.get("avg_return", 0)
        n = int(row.get("n_signals", 0))

        # Mark current config
        is_current = False
        if current_point is not None:
            is_current = all(
                abs(row.get(f, -999) - current_point.get(f, -998)) < 0.01
                for f in CURRENT_CONDITIONS
                if f in row.index and f in current_point.index
            )

        marker = " ← CURRENT" if is_current else ""

        gap_str = f"{gap:.1f}" if isinstance(gap, float) else str(gap)
        rv_str = f"{rv:.1f}" if isinstance(rv, float) else str(rv)
        va_str = f"{va:.1f}" if isinstance(va, float) else str(va)
        tr_str = f"{tr:.0f}" if isinstance(tr, float) else str(tr)

        lines.append(
            f"| {gap_str} | {rv_str} | {va_str} | {tr_str} | "
            f"{prec:.1%} | {rec:.1%} | {avg:.1f}% | {n}{marker} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Each row on the Pareto frontier is a non-dominated choice: no other combination gets both more coverage AND higher precision",
            "- Moving right (higher recall): captures more winners but with lower precision (more false positives)",
            "- Moving up (higher precision): more selective signals but misses more winners",
            "- **Decision**: Choose the point that balances your risk tolerance with expected return",
            "",
            "If the CURRENT configuration is NOT on the Pareto frontier, there exists a strictly better combination.",
        ]
    )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Conditional Probability Matrix & Pareto Frontier"
    )
    parser.add_argument(
        "--input",
        default="data/dense_signals/all_dates.parquet",
        help="Path to parquet file from dense_signal_batch.py",
    )
    parser.add_argument(
        "--factors",
        nargs="*",
        default=["gap_pct", "rel_vol_20", "vol_accel_5_15", "trade_rate"],
        help="Factors to enumerate (default: gap_pct, rel_vol_20, vol_accel_5_15, trade_rate)",
    )
    parser.add_argument(
        "--output",
        help="Output path for markdown report",
    )
    parser.add_argument(
        "--csv-output",
        help="Output path for full matrix CSV",
    )
    parser.add_argument(
        "--frontier-only",
        action="store_true",
        help="Show only Pareto frontier points",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {args.input}")
        return

    df = pd.read_parquet(args.input)
    logger.info(f"Loaded {len(df):,} samples")

    matrix = compute_condition_matrix(df, factors=args.factors)

    if matrix.empty:
        logger.error("No valid combinations found")
        return

    frontier = find_pareto_frontier(matrix)
    current_point = find_current_point(matrix, CURRENT_CONDITIONS)

    # Print summary
    print(f"\n{'='*70}")
    print(f"Conditional Probability Matrix: {len(frontier)} Pareto-optimal points")
    print(f"{'='*70}")

    if current_point is not None:
        print(
            f"Current config  → precision={current_point.precision:.1%}, "
            f"recall={current_point.recall:.1%}, "
            f"avg_return={current_point.avg_return:.1f}%, "
            f"n={int(current_point.n_signals)}"
        )

    # Find better alternatives
    if current_point is not None:
        better = matrix[
            (matrix["precision"] > current_point.precision)
            & (matrix["recall"] > current_point.recall)
        ]
        if len(better) > 0:
            print(f"\nFound {len(better)} combinations that dominate current config!")
            best_alt = better.loc[better["avg_return"].idxmax()]
            print(
                f"Best alternative → precision={best_alt.precision:.1%}, "
                f"recall={best_alt.recall:.1%}, "
                f"avg_return={best_alt.avg_return:.1f}%"
            )
            for f in args.factors:
                if f in best_alt.index:
                    print(f"  {f} > {best_alt[f]:.2f}")
        else:
            print("Current config is on the Pareto frontier.")

    # Show frontier
    print(f"\nPareto Frontier:")
    cols = args.factors + ["precision", "recall", "avg_return", "n_signals"]
    print(frontier[cols].to_string(index=False))

    if args.csv_output:
        matrix.to_csv(args.csv_output, index=False)
        logger.info(f"Full matrix saved to {args.csv_output}")

    if args.output:
        report = format_report(matrix, frontier, current_point, args.factors)
        Path(args.output).write_text(report)
        logger.info(f"Report saved to {args.output}")


if __name__ == "__main__":
    main()
