#!/usr/bin/env python3
"""Batch dense signal collection across multiple dates.

Runs collect_signals() for each date and outputs a merged parquet file.
Supports incremental updates — skips dates already present in the output.

Usage:
    # Collect all available dates
    poetry run python -m jerry_trader.scripts.dense_signal_batch

    # Collect specific date range
    poetry run python -m jerry_trader.scripts.dense_signal_batch \
        --dates 2026-03-02 2026-03-03 2026-03-04 2026-03-05 2026-03-06 \
        --output data/dense_signals/explore.parquet

    # Incremental update (skip already-collected dates)
    poetry run python -m jerry_trader.scripts.dense_signal_batch --incremental
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from jerry_trader.services.backtest.dense_signal_collector import collect_signals
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("dense_signal_batch", log_to_file=True)

DEFAULT_DATES = [
    "2026-03-02",
    "2026-03-03",
    "2026-03-04",
    "2026-03-05",
    "2026-03-06",
    "2026-03-09",
    "2026-03-10",
    "2026-03-11",
    "2026-03-12",
    "2026-03-13",
]

EXPLORE_DATES = [
    "2026-03-02",
    "2026-03-03",
    "2026-03-04",
    "2026-03-05",
    "2026-03-06",
]

VALIDATE_DATES = [
    "2026-03-09",
    "2026-03-10",
    "2026-03-11",
    "2026-03-12",
    "2026-03-13",
]

OUTPUT_DIR = Path("data/dense_signals")


def collect_all_dates(
    dates: list[str],
    output_path: str | None = None,
    incremental: bool = False,
) -> pd.DataFrame:
    """Run collect_signals() across all dates, return concatenated DataFrame.

    Args:
        dates: List of date strings YYYY-MM-DD.
        output_path: Optional path to save merged parquet.
        incremental: If True, skip dates already in the output file.

    Returns:
        Concatenated DataFrame with a 'date' column added.
    """
    # Check for already-collected dates
    existing_dates: set[str] = set()
    if incremental and output_path and Path(output_path).exists():
        existing = pd.read_parquet(output_path)
        if "date" in existing.columns:
            existing_dates = set(existing["date"].unique())
            logger.info(f"Found {len(existing_dates)} existing dates in {output_path}")

    remaining = [d for d in dates if d not in existing_dates]
    if not remaining:
        logger.info("All dates already collected, nothing to do")
        if output_path:
            return pd.read_parquet(output_path)
        return pd.DataFrame()

    logger.info(f"Collecting {len(remaining)} dates: {remaining}")

    all_dfs = []
    for date in remaining:
        logger.info(f"  {date}...")
        try:
            df = collect_signals(date)
            if not df.empty:
                df["date"] = date
                all_dfs.append(df)
                logger.info(f"  {date}: {len(df)} samples")
            else:
                logger.warning(f"  {date}: no samples collected")
        except Exception as e:
            logger.error(f"  {date}: FAILED — {e}")

    if not all_dfs:
        logger.warning("No new samples collected")
        return pd.DataFrame()

    new_df = pd.concat(all_dfs, ignore_index=True)

    # Merge with existing if incremental
    if incremental and output_path and Path(output_path).exists():
        existing = pd.read_parquet(output_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
        if output_path:
            combined.to_parquet(output_path, index=False)
            logger.info(
                f"Saved {len(combined)} total samples ({len(new_df)} new) to {output_path}"
            )
        return combined

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        new_df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(new_df)} samples to {output_path}")

    return new_df


def get_stats(df: pd.DataFrame) -> dict:
    """Compute quick stats for collected data."""
    if df.empty:
        return {"n_samples": 0}

    tiers = pd.cut(
        df["max_return_pct"],
        bins=[-float("inf"), 0, 10, 20, 50, float("inf")],
        labels=["loser", "flat", "small", "medium", "big"],
    )

    return {
        "n_samples": len(df),
        "n_dates": df["date"].nunique(),
        "n_tickers": df["ticker"].nunique(),
        "tier_distribution": tiers.value_counts().to_dict(),
        "mean_max_return": float(df["max_return_pct"].mean()),
        "median_max_return": float(df["max_return_pct"].median()),
        "factor_columns": [
            c
            for c in df.columns
            if c
            not in {
                "ticker",
                "entry_time_ms",
                "return_pct",
                "max_return_pct",
                "entry_price",
                "exit_price",
                "max_price",
                "min_price",
                "session_phase",
                "date",
            }
        ],
    }


def main():
    parser = argparse.ArgumentParser(description="Batch dense signal collection")
    parser.add_argument(
        "--dates",
        nargs="*",
        help="Dates to collect (YYYY-MM-DD). Default: all 10 available dates.",
    )
    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR / "all_dates.parquet"),
        help="Output parquet path",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip dates already in output file",
    )
    parser.add_argument(
        "--explore",
        action="store_true",
        help="Collect only exploration dates (03-02 to 03-06)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Collect only validation dates (03-09 to 03-13)",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Print stats for existing output file without collecting",
    )

    args = parser.parse_args()

    if args.stats_only:
        if Path(args.output).exists():
            df = pd.read_parquet(args.output)
            stats = get_stats(df)
            print_stats(stats)
        else:
            logger.error(f"No file at {args.output}")
        return

    # Determine dates and output path
    if args.explore:
        dates = EXPLORE_DATES
        if args.output == str(OUTPUT_DIR / "all_dates.parquet"):
            args.output = str(OUTPUT_DIR / "explore.parquet")
    elif args.validate:
        dates = VALIDATE_DATES
        if args.output == str(OUTPUT_DIR / "all_dates.parquet"):
            args.output = str(OUTPUT_DIR / "validate.parquet")
    elif args.dates:
        dates = args.dates
    else:
        dates = DEFAULT_DATES

    df = collect_all_dates(
        dates=dates,
        output_path=args.output,
        incremental=args.incremental,
    )

    stats = get_stats(df)
    print_stats(stats)


def print_stats(stats: dict):
    """Print formatted stats."""
    print(f"\n{'='*60}")
    print("Dense Signal Collection Results")
    print(f"{'='*60}")
    print(f"Samples:       {stats['n_samples']:,}")
    print(f"Dates:         {stats['n_dates']}")
    print(f"Tickers:       {stats['n_tickers']}")
    print(f"Mean return:   {stats['mean_max_return']:.1f}%")
    print(f"Median return: {stats['median_max_return']:.1f}%")
    print(f"\nTier Distribution:")
    for tier, count in stats.get("tier_distribution", {}).items():
        pct = count / stats["n_samples"] * 100 if stats["n_samples"] > 0 else 0
        print(f"  {tier}: {count:,} ({pct:.1f}%)")
    print(f"\nFactor columns: {', '.join(stats.get('factor_columns', []))}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
