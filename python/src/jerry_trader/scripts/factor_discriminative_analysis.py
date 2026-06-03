#!/usr/bin/env python3
"""Factor Discriminative Power Analysis.

For each factor, measures how well it separates big winners from losers.
Answers: Which factors actually predict returns? Are current thresholds optimal?

Usage:
    # Run on all dates
    poetry run python -m jerry_trader.scripts.factor_discriminative_analysis \
        --input data/dense_signals/all_dates.parquet

    # Run on exploration set only
    poetry run python -m jerry_trader.scripts.factor_discriminative_analysis \
        --input data/dense_signals/explore.parquet

    # Output to file
    poetry run python -m jerry_trader.scripts.factor_discriminative_analysis \
        --input data/dense_signals/all_dates.parquet --output reports/factor_analysis.md
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("factor_analysis", log_to_file=True)

FACTOR_NAMES = [
    "rel_vol_20",
    "price_direction",
    "trade_rate",
    "bid_ask_spread",
    "entry_gap_pct",
    "gap_pct",
    "order_imbalance",
    "quote_rate",
    "vol_accel_5_15",
    "ema_20",
]

TIER_BINS = [-float("inf"), 0, 10, 20, 50, float("inf")]
TIER_LABELS = ["loser", "flat", "small", "medium", "big"]

CURRENT_THRESHOLDS = {
    "gap_pct": 4.0,
    "rel_vol_20": 2.0,
    "trade_rate": 100,
    "price_direction": 0,
}


def classify_tier(max_return: float) -> str:
    """Classify a sample into winner tier based on max_return_pct.

    Consistent with pd.cut bins=[-inf, 0, 10, 20, 50, inf] right=True.
    """
    if max_return > 50:
        return "big"
    elif max_return > 20:
        return "medium"
    elif max_return > 10:
        return "small"
    elif max_return > 0:
        return "flat"
    else:
        return "loser"


def analyze_factor_distribution(df: pd.DataFrame, factor: str) -> dict:
    """Compute distribution statistics for a factor by winner tier."""
    stats = {}
    for tier in TIER_LABELS:
        subset = df[df["tier"] == tier][factor].dropna()
        if len(subset) == 0:
            continue
        stats[tier] = {
            "n": len(subset),
            "mean": float(subset.mean()),
            "median": float(subset.median()),
            "std": float(subset.std()),
            "q25": float(subset.quantile(0.25)),
            "q75": float(subset.quantile(0.75)),
        }
    return stats


def ks_test_big_vs_loser(df: pd.DataFrame, factor: str) -> dict:
    """Two-sample KS test: does factor distribution differ between big winners and losers?"""
    from scipy.stats import ks_2samp

    big = df[df["tier"] == "big"][factor].dropna()
    loser = df[df["tier"] == "loser"][factor].dropna()

    if len(big) < 5 or len(loser) < 5:
        return {"ks_stat": 0, "p_value": 1.0, "note": "insufficient samples"}

    ks_stat, p_value = ks_2samp(big, loser)
    return {"ks_stat": float(ks_stat), "p_value": float(p_value)}


def single_factor_auc(df: pd.DataFrame, factor: str) -> float:
    """Compute AUC for single-factor classification of big winner vs not."""
    from sklearn.metrics import roc_auc_score

    subset = df[df[factor].notna()]
    if len(subset) < 10:
        return 0.5

    y_true = (subset["tier"].isin(["big", "medium"])).astype(int)
    if y_true.sum() < 3 or (1 - y_true).sum() < 3:
        return 0.5

    try:
        auc = roc_auc_score(y_true, subset[factor].values)
        # AUC < 0.5 means the factor is negatively correlated; flip it
        return float(max(auc, 1 - auc))
    except Exception:
        return 0.5


def threshold_sensitivity(
    df: pd.DataFrame,
    factor: str,
    thresholds: list[float],
    target_tiers: list[str] = None,
) -> list[dict]:
    """For each threshold, compute P(winner | factor > threshold) and related metrics."""
    if target_tiers is None:
        target_tiers = ["big", "medium"]

    total_winners = len(df[df["tier"].isin(target_tiers)])

    results = []
    for thresh in thresholds:
        above = df[df[factor] > thresh]
        if len(above) < 5:
            continue

        winners_above = len(above[above["tier"].isin(target_tiers)])
        precision = winners_above / len(above) if len(above) > 0 else 0
        recall = winners_above / total_winners if total_winners > 0 else 0
        avg_return = float(above["max_return_pct"].mean())

        results.append(
            {
                "threshold": thresh,
                "n_signals": len(above),
                "precision": float(precision),
                "recall": float(recall),
                "avg_return": avg_return,
            }
        )

    return results


def compute_all_factor_stats(df: pd.DataFrame) -> dict:
    """Compute full discriminative power analysis for all factors."""
    # Add tier classification (also used by format_report)
    df["tier"] = df["max_return_pct"].apply(classify_tier)

    # Threshold sweep definitions per factor
    threshold_sweeps = {
        "rel_vol_20": [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0],
        "price_direction": [-2.0, -1.0, -0.5, 0, 0.5, 1.0, 2.0],
        "trade_rate": [10, 25, 50, 75, 100, 150, 200, 300],
        "bid_ask_spread": [0.001, 0.005, 0.01, 0.02, 0.05],
        "entry_gap_pct": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0],
        "gap_pct": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0],
        "order_imbalance": [0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        "quote_rate": [10, 25, 50, 100, 200, 500],
    }

    results = {}
    for factor in FACTOR_NAMES:
        if factor not in df.columns:
            logger.warning(f"Factor {factor} not in dataset")
            continue

        logger.info(f"Analyzing {factor}...")

        distribution = analyze_factor_distribution(df, factor)
        ks_result = ks_test_big_vs_loser(df, factor)
        auc = single_factor_auc(df, factor)
        thresholds = threshold_sweeps.get(factor, [])
        sensitivity = threshold_sensitivity(df, factor, thresholds)

        results[factor] = {
            "distribution": distribution,
            "ks_test": ks_result,
            "auc": auc,
            "threshold_sensitivity": sensitivity,
            "current_threshold": CURRENT_THRESHOLDS.get(factor),
        }

    return results


def format_report(results: dict, df: pd.DataFrame) -> str:
    """Format factor analysis results as a markdown report."""
    # Ensure tier column exists
    if "tier" not in df.columns:
        df = df.copy()
        df["tier"] = df["max_return_pct"].apply(classify_tier)

    lines = [
        "# Factor Discriminative Power Analysis",
        "",
        f"## Dataset Summary",
        f"- Total samples: {len(df):,}",
        f"- Dates: {df['date'].nunique() if 'date' in df.columns else 'N/A'}",
        f"- Tickers: {df['ticker'].nunique() if 'ticker' in df.columns else 'N/A'}",
        "",
        "## Tier Distribution",
    ]

    tier_counts = df["tier"].value_counts()
    for tier in TIER_LABELS:
        count = tier_counts.get(tier, 0)
        pct = count / len(df) * 100
        lines.append(f"- {tier}: {count:,} ({pct:.1f}%)")

    lines.extend(
        [
            "",
            "## Factor Rankings",
            "",
            "| Factor | KS Stat | AUC | Big Mean | Loser Mean | Current Threshold | Best Threshold |",
            "|--------|---------|-----|----------|------------|-------------------|----------------|",
        ]
    )

    # Sort by KS stat descending
    ranked = sorted(
        results.items(),
        key=lambda x: x[1].get("ks_test", {}).get("ks_stat", 0),
        reverse=True,
    )

    for factor, info in ranked:
        dist = info.get("distribution", {})
        big_mean = dist.get("big", {}).get("mean", 0)
        loser_mean = dist.get("loser", {}).get("mean", 0)
        ks = info.get("ks_test", {}).get("ks_stat", 0)
        auc = info.get("auc", 0)
        curr = info.get("current_threshold", "-")

        # Find best threshold (highest precision while keeping n_signals > 20)
        sensitivity = info.get("threshold_sensitivity", [])
        best_t = "-"
        if sensitivity:
            valid = [s for s in sensitivity if s["n_signals"] > 20]
            if valid:
                best = max(valid, key=lambda s: s["precision"])
                best_t = f"{best['threshold']:.2f}"

        lines.append(
            f"| {factor} | {ks:.3f} | {auc:.3f} | "
            f"{big_mean:.2f} | {loser_mean:.2f} | {curr} | {best_t} |"
        )

    lines.extend(
        [
            "",
            "## Factor Details",
            "",
        ]
    )

    for factor, info in results.items():
        lines.extend(
            [
                f"### {factor}",
                "",
                "**Distribution by Tier:**",
                "",
                "| Tier | N | Mean | Median | Q25 | Q75 |",
                "|------|---|------|--------|-----|-----|",
            ]
        )

        for tier in TIER_LABELS:
            if tier in info.get("distribution", {}):
                d = info["distribution"][tier]
                lines.append(
                    f"| {tier} | {d['n']} | {d['mean']:.3f} | "
                    f"{d['median']:.3f} | {d['q25']:.3f} | {d['q75']:.3f} |"
                )

        ks = info.get("ks_test", {})
        lines.extend(
            [
                "",
                (
                    f"- **KS stat (big vs loser):** {ks.get('ks_stat', 'N/A'):.3f}"
                    if isinstance(ks.get("ks_stat"), float)
                    else f"- **KS stat (big vs loser):** {ks.get('ks_stat', 'N/A')}"
                ),
                (
                    f"- **p-value:** {ks.get('p_value', 'N/A'):.6f}"
                    if isinstance(ks.get("p_value"), float)
                    else f"- **p-value:** {ks.get('p_value', 'N/A')}"
                ),
                f"- **AUC (medium+ vs rest):** {info.get('auc', 'N/A'):.3f}",
                "",
            ]
        )

        # Threshold sensitivity (top 5)
        sensitivity = info.get("threshold_sensitivity", [])
        if sensitivity:
            lines.extend(
                [
                    "**Threshold Sensitivity (top by precision):**",
                    "",
                    "| Thresh | Signals | Precision | Recall | Avg Return |",
                    "|--------|---------|-----------|--------|------------|",
                ]
            )
            top5 = sorted(sensitivity, key=lambda s: s["precision"], reverse=True)[:5]
            for s in top5:
                lines.append(
                    f"| {s['threshold']:.2f} | {s['n_signals']} | "
                    f"{s['precision']:.2%} | {s['recall']:.2%} | {s['avg_return']:.1f}% |"
                )
            lines.append("")

    lines.extend(
        [
            "## Interpretation Guide",
            "",
            "- **KS stat > 0.2**: Factor has meaningful separation between big winners and losers",
            "- **KS stat < 0.1**: Factor adds little discriminative power",
            "- **AUC > 0.6**: Factor has predictive power for medium+ winners",
            "- **AUC ~ 0.5**: Factor is essentially random",
            "- Current threshold should be near the precision knee in the sensitivity table",
            "",
            "If a factor has low KS stat AND its current threshold filters many samples,",
            "that threshold is likely doing more harm (reducing coverage) than good (improving quality).",
        ]
    )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Factor Discriminative Power Analysis")
    parser.add_argument(
        "--input",
        default="data/dense_signals/all_dates.parquet",
        help="Path to parquet file from dense_signal_batch.py",
    )
    parser.add_argument(
        "--output",
        help="Output path for markdown report",
    )
    parser.add_argument(
        "--json-output",
        help="Output path for JSON results",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print summary table only, no per-factor details",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {args.input}")
        logger.info("Run dense_signal_batch.py first to collect data.")
        return

    df = pd.read_parquet(args.input)
    logger.info(f"Loaded {len(df):,} samples from {args.input}")

    results = compute_all_factor_stats(df)

    # Print summary
    print(f"\n{'='*70}")
    print("Factor Discriminative Power Summary")
    print(f"{'='*70}")
    print(
        f"{'Factor':<20} {'KS':>6} {'AUC':>6} {'BigMean':>8} {'LoserMean':>10} {'BestThr':>8}"
    )
    print("-" * 70)

    for factor, info in sorted(
        results.items(),
        key=lambda x: x[1].get("ks_test", {}).get("ks_stat", 0),
        reverse=True,
    ):
        ks = info.get("ks_test", {}).get("ks_stat", 0)
        auc = info.get("auc", 0)
        dist = info.get("distribution", {})
        big_mean = dist.get("big", {}).get("mean", 0)
        loser_mean = dist.get("loser", {}).get("mean", 0)
        sensitivity = info.get("threshold_sensitivity", [])
        best_t = "-"
        if sensitivity:
            valid = [s for s in sensitivity if s["n_signals"] > 20]
            if valid:
                best_t = f"{max(valid, key=lambda s: s['precision'])['threshold']:.2f}"

        print(
            f"{factor:<20} {ks:>6.3f} {auc:>6.3f} "
            f"{big_mean:>8.2f} {loser_mean:>10.2f} {best_t:>8}"
        )

    print(f"{'='*70}\n")

    # Save outputs
    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"JSON results saved to {args.json_output}")

    if args.output:
        report = format_report(results, df)
        Path(args.output).write_text(report)
        logger.info(f"Report saved to {args.output}")


if __name__ == "__main__":
    main()
