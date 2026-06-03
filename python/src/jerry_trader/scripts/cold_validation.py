#!/usr/bin/env python3
"""Cold Validation: Train/Test split to prevent data snooping.

Phase 1 (Explore): Discover patterns and choose thresholds on EXPLORE_DATES.
Phase 2 (Validate): ONE-TIME evaluation on VALIDATE_DATES with frozen thresholds.

Usage:
    # Run cold validation
    poetry run python -m jerry_trader.scripts.cold_validation \
        --explore data/dense_signals/explore.parquet \
        --validate data/dense_signals/validate.parquet

    # With custom target tier
    poetry run python -m jerry_trader.scripts.cold_validation \
        --explore data/dense_signals/explore.parquet \
        --validate data/dense_signals/validate.parquet \
        --target big,medium
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("cold_validation", log_to_file=True)

# Condition format: (factor, threshold, direction)
# direction: ">" means factor > threshold, "<" means factor < threshold
Condition = tuple[str, float, str]

CURRENT_CONDITIONS: list[Condition] = [
    ("trade_rate", 10, ">"),
    ("rel_vol_20", 2.0, ">"),
]

THRESHOLD_GRID = {
    "trade_rate": [10, 20, 50],
    "rel_vol_20": [1.5, 2.0, 3.0],
    "entry_gap_pct": [4.0, 8.0],
    "quote_rate": [10, 20],
}

STRATEGY_VARIANTS: dict[str, list[Condition]] = {
    # ── events.yaml entries (current strategy) ──
    "momentum_entry": [
        ("trade_rate", 10, ">"),
        ("rel_vol_20", 2.0, ">"),
    ],
    "activity_entry": [
        ("trade_rate", 20, ">"),
        ("rel_vol_20", 1.5, ">"),
    ],
    "high_conviction": [
        ("trade_rate", 50, ">"),
        ("rel_vol_20", 3.0, ">"),
    ],
    "momentum_strict": [
        ("rel_vol_20", 3.0, ">"),
        ("price_direction", 0.8, ">"),
        ("bid_ask_spread", 30, "<"),
    ],
    # ── volume-first strategies (rel_vol primary, trade_rate secondary) ──
    "volume_entry": [
        ("rel_vol_20", 3.0, ">"),
        ("trade_rate", 5, ">"),
    ],
    "volume_strong": [
        ("rel_vol_20", 5.0, ">"),
        ("trade_rate", 5, ">"),
    ],
    # ── single-factor baselines ──
    "trade_rate_10_only": [
        ("trade_rate", 10, ">"),
    ],
    "trade_rate_20_only": [
        ("trade_rate", 20, ">"),
    ],
    "trade_rate_50_only": [
        ("trade_rate", 50, ">"),
    ],
    "rel_vol_2_only": [
        ("rel_vol_20", 2.0, ">"),
    ],
    "rel_vol_3_only": [
        ("rel_vol_20", 3.0, ">"),
    ],
    # ── data-driven: trade_rate + quote_rate (top 2 discriminative factors) ──
    "trade_10_quote_10": [
        ("trade_rate", 10, ">"),
        ("quote_rate", 10, ">"),
    ],
    "trade_20_quote_20": [
        ("trade_rate", 20, ">"),
        ("quote_rate", 20, ">"),
    ],
    # ── WATCH-only (gap filter) ──
    "entry_gap_4_only": [
        ("entry_gap_pct", 4.0, ">"),
    ],
    "entry_gap_8_only": [
        ("entry_gap_pct", 8.0, ">"),
    ],
}


def evaluate_conditions(
    df: pd.DataFrame,
    conditions: list[Condition],
    target_tiers: list[str],
) -> dict:
    """Evaluate a set of conditions on a dataset.

    Args:
        df: DataFrame with factor columns.
        conditions: List of (factor, threshold, direction) tuples.
        target_tiers: Tiers to count as "winner".

    Returns precision, recall, avg_return, n_signals, and per-date breakdown.
    """
    mask = pd.Series(True, index=df.index)
    for factor, thresh, direction in conditions:
        if factor not in df.columns:
            continue
        if direction == ">":
            mask &= df[factor] > thresh
        elif direction == "<":
            mask &= df[factor] < thresh
        else:
            mask &= df[factor] >= thresh

    if mask.sum() < 5:
        return {
            "n_signals": 0,
            "precision": 0,
            "recall": 0,
            "avg_return": 0,
            "win_rate": 0,
            "by_date": {},
            "error": "insufficient signals",
        }

    if "tier" not in df.columns:
        df = df.copy()
        df["tier"] = pd.cut(
            df["max_return_pct"],
            bins=[-float("inf"), 0, 10, 20, 50, float("inf")],
            labels=["loser", "flat", "small", "medium", "big"],
        )

    is_winner = df["tier"].isin(target_tiers)
    total_winners = is_winner.sum()

    signals = df[mask]
    winners_captured = signals["tier"].isin(target_tiers).sum()
    precision = winners_captured / len(signals) if len(signals) > 0 else 0
    recall = winners_captured / total_winners if total_winners > 0 else 0
    avg_return = float(signals["max_return_pct"].mean())
    win_rate = float((signals["max_return_pct"] > 0).mean())

    # Per-date breakdown
    by_date = {}
    if "date" in signals.columns:
        for date in signals["date"].unique():
            day_signals = signals[signals["date"] == date]
            day_winners = day_signals["tier"].isin(target_tiers).sum()
            by_date[date] = {
                "n_signals": int(len(day_signals)),
                "precision": float(day_winners / len(day_signals)),
                "avg_return": float(day_signals["max_return_pct"].mean()),
            }

    return {
        "n_signals": int(len(signals)),
        "precision": float(precision),
        "recall": float(recall),
        "avg_return": avg_return,
        "win_rate": win_rate,
        "by_date": by_date,
    }


def find_best_thresholds(
    df: pd.DataFrame,
    factors: list[str],
    target_tiers: list[str],
    metric: str = "precision",
    direction: str = ">",
) -> list[Condition]:
    """Find the best threshold combination on exploration set.

    Args:
        metric: Metric to optimize — "precision", "recall", or "avg_return".
        direction: ">" or "<" for all factors.
    """
    import itertools

    factor_names = [f for f in factors if f in THRESHOLD_GRID and f in df.columns]
    threshold_values = [THRESHOLD_GRID[f] for f in factor_names]

    total_combos = 1
    for vals in threshold_values:
        total_combos *= len(vals)

    logger.info(f"Exploring {total_combos} combinations for best {metric}...")

    best_score = -float("inf")
    best_combo = None
    best_result = None

    for combo in itertools.product(*threshold_values):
        conditions = [(f, t, direction) for f, t in zip(factor_names, combo)]
        result = evaluate_conditions(df, conditions, target_tiers)

        if result["n_signals"] < 10:
            continue

        score = result.get(metric, 0)
        if score > best_score:
            best_score = score
            best_combo = conditions
            best_result = result

    logger.info(f"Best {metric}: {best_score:.3f} " f"with {best_combo}")
    return best_combo or []


def compare_variants(
    explore_df: pd.DataFrame,
    validate_df: pd.DataFrame,
    target_tiers: list[str],
) -> dict:
    """Compare all strategy variants on both explore and validate sets."""
    results = {}

    for variant_name, conditions in STRATEGY_VARIANTS.items():
        logger.info(f"Evaluating {variant_name}...")
        explore_result = evaluate_conditions(explore_df, conditions, target_tiers)
        validate_result = evaluate_conditions(validate_df, conditions, target_tiers)

        results[variant_name] = {
            "conditions": conditions,
            "explore": explore_result,
            "validate": validate_result,
            "delta": {
                "precision": validate_result["precision"] - explore_result["precision"],
                "recall": validate_result["recall"] - explore_result["recall"],
                "avg_return": validate_result["avg_return"]
                - explore_result["avg_return"],
                "win_rate": validate_result["win_rate"] - explore_result["win_rate"],
            },
        }

    return results


def significance_test(
    explore_df: pd.DataFrame,
    validate_df: pd.DataFrame,
    conditions: list[Condition],
    target_tiers: list[str],
) -> dict:
    """Test if explore and validate results are significantly different."""
    ex_result = evaluate_conditions(explore_df, conditions, target_tiers)
    val_result = evaluate_conditions(validate_df, conditions, target_tiers)

    # Extract per-date avg_returns if available
    ex_returns = []
    val_returns = []
    for date_info in ex_result.get("by_date", {}).values():
        ex_returns.append(date_info.get("avg_return", 0))
    for date_info in val_result.get("by_date", {}).values():
        val_returns.append(date_info.get("avg_return", 0))

    t_test = None
    if len(ex_returns) >= 3 and len(val_returns) >= 3:
        t_stat, p_value = stats.ttest_ind(ex_returns, val_returns)
        t_test = {"t_stat": float(t_stat), "p_value": float(p_value)}

    return {
        "explore_n_signals": ex_result["n_signals"],
        "validate_n_signals": val_result["n_signals"],
        "explore_precision": ex_result["precision"],
        "validate_precision": val_result["precision"],
        "delta_precision": val_result["precision"] - ex_result["precision"],
        "t_test": t_test,
        "interpretation": (
            f"No significant difference (p={t_test['p_value']:.3f})"
            if t_test and t_test["p_value"] > 0.05
            else (
                f"Significant difference (p={t_test['p_value']:.3f}) — out-of-sample degradation"
                if t_test
                else "Insufficient data for significance test"
            )
        ),
    }


def format_report(results: dict, sig_test: dict | None = None) -> str:
    """Format cold validation results as a markdown report."""
    lines = [
        "# Cold Validation Report",
        "",
        "## Strategy Variant Comparison",
        "",
        "| Variant | IS N | OOS N | IS Prec | OOS Prec | IS MedRet | OOS MedRet | Δ Prec |",
        "|---------|------|-------|---------|----------|-----------|------------|--------|",
    ]

    for variant_name, result in results.items():
        ex = result["explore"]
        val = result["validate"]
        delta = result["delta"]

        lines.append(
            f"| {variant_name} | {ex['n_signals']:,} | {val['n_signals']:,} | "
            f"{ex['precision']:.1%} | {val['precision']:.1%} | "
            f"{ex['avg_return']:.1f}% | {val['avg_return']:.1f}% | "
            f"{delta['precision']:+.1%} |"
        )

    lines.extend(
        [
            "",
            "**IS = In-Sample (explore), OOS = Out-of-Sample (validate)**",
            "",
            "## Key Findings",
            "",
        ]
    )

    # Find best OOS variant
    best_oos = max(
        results.items(),
        key=lambda x: x[1]["validate"]["avg_return"],
    )
    best_oos_name = best_oos[0]
    best_oos_result = best_oos[1]["validate"]

    standard = results.get("standard", {})
    standard_oos = standard.get("validate", {})

    lines.append(
        f"- **Best OOS variant**: {best_oos_name} "
        f"(precision={best_oos_result['precision']:.1%}, "
        f"avg_return={best_oos_result['avg_return']:.1f}%)"
    )

    if standard_oos:
        lines.append(
            f"- **Standard variant OOS**: precision={standard_oos['precision']:.1%}, "
            f"avg_return={standard_oos['avg_return']:.1f}%"
        )

    # Check if any relaxed variant beats standard OOS
    for name in ["relaxed_relvol", "relaxed_gap", "relaxed_all"]:
        if name in results:
            variant = results[name]
            v_oos = variant["validate"]
            standard_oos = results.get("standard", {}).get("validate", {})
            if standard_oos and v_oos["avg_return"] > standard_oos["avg_return"]:
                lines.append(
                    f"- **{name} beats standard OOS** "
                    f"(avg_return: {v_oos['avg_return']:.1f}% vs {standard_oos['avg_return']:.1f}%)"
                )

    lines.extend(
        [
            "",
            "## Degradation Analysis",
            "",
            "Δ = OOS − IS. Negative Δ means out-of-sample performance is worse than in-sample.",
            "Large negative Δ suggests overfitting in the exploration phase.",
            "",
        ]
    )

    for variant_name, result in results.items():
        delta = result["delta"]
        lines.append(
            f"- **{variant_name}**: precision {delta['precision']:+.1%}, "
            f"recall {delta['recall']:+.1%}, avg_return {delta['avg_return']:+.1f}%"
        )

    if sig_test:
        lines.extend(
            [
                "",
                "## Statistical Significance",
                "",
                (
                    f"- T-test: p = {sig_test.get('p_value', 'N/A'):.4f}"
                    if sig_test.get("p_value") is not None
                    and isinstance(sig_test.get("p_value"), float)
                    else f"- T-test: p = {sig_test.get('p_value', 'N/A')}"
                ),
                f"- Interpretation: {sig_test.get('interpretation', 'N/A')}",
            ]
        )

    lines.extend(
        [
            "",
            "## Decision Guidance",
            "",
            "- If |Δ| < 2% on precision: thresholds generalize well, no overfitting",
            "- If Δ < -5% on precision: thresholds overfit exploration set, relax conditions",
            "- If a relaxed variant matches standard OOS precision but with higher recall: adopt it",
            "",
            "**Warning**: Do NOT iterate on VALIDATE_DATES. Once validation is done,",
            "either accept the results or collect NEW out-of-sample data for further testing.",
        ]
    )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Cold Validation: Train/Test split evaluation"
    )
    parser.add_argument(
        "--explore",
        required=True,
        help="Path to exploration set parquet",
    )
    parser.add_argument(
        "--validate",
        required=True,
        help="Path to validation set parquet",
    )
    parser.add_argument(
        "--target",
        default="big,medium",
        help="Target tiers (comma-separated, default: big,medium)",
    )
    parser.add_argument(
        "--output",
        help="Output path for markdown report",
    )
    parser.add_argument(
        "--find-best",
        action="store_true",
        help="Also search for best threshold combination on explore set",
    )

    args = parser.parse_args()

    explore_path = Path(args.explore)
    validate_path = Path(args.validate)

    for p in [explore_path, validate_path]:
        if not p.exists():
            logger.error(f"File not found: {p}")
            return

    target_tiers = [t.strip() for t in args.target.split(",")]

    explore_df = pd.read_parquet(explore_path)
    validate_df = pd.read_parquet(validate_path)

    logger.info(
        f"Explore: {len(explore_df):,} samples, {explore_df['date'].nunique()} dates"
    )
    logger.info(
        f"Validate: {len(validate_df):,} samples, {validate_df['date'].nunique()} dates"
    )

    # Compare all variants
    results = compare_variants(explore_df, validate_df, target_tiers)

    # Significance test on standard variant
    sig_test = significance_test(
        explore_df, validate_df, CURRENT_CONDITIONS, target_tiers
    )

    # Find best thresholds if requested
    best_thresholds = None
    if args.find_best:
        logger.info("Searching for best thresholds on explore set...")
        factor_names = [c[0] for c in CURRENT_CONDITIONS if c[0] in THRESHOLD_GRID]
        best_thresholds = find_best_thresholds(
            explore_df,
            factor_names,
            target_tiers,
            metric="precision",
        )

    # Print summary
    print(f"\n{'='*85}")
    print("Cold Validation Results")
    print(f"{'='*85}")
    print(
        f"{'Variant':<22} {'IS N':>6} {'OOS N':>6} {'IS Prec':>8} {'OOS Prec':>8} {'IS Ret':>8} {'OOS Ret':>8} {'Δ Prec':>8}"
    )
    print("-" * 85)

    for name, result in results.items():
        ex = result["explore"]
        val = result["validate"]
        delta = result["delta"]
        print(
            f"{name:<22} {ex['n_signals']:>6,} {val['n_signals']:>6,} "
            f"{ex['precision']:>8.1%} {val['precision']:>8.1%} "
            f"{ex['avg_return']:>8.1f}% {val['avg_return']:>8.1f}% "
            f"{delta['precision']:>+8.1%}"
        )

    print(f"{'='*85}")
    print(f"Significance: {sig_test.get('interpretation', 'N/A')}")

    if best_thresholds:
        print(f"\nBest thresholds found on explore set:")
        for factor, thresh, direction in best_thresholds:
            print(f"  {factor} {direction} {thresh:.2f}")

    if args.output:
        report = format_report(results, sig_test)
        Path(args.output).write_text(report)
        logger.info(f"Report saved to {args.output}")


if __name__ == "__main__":
    main()
