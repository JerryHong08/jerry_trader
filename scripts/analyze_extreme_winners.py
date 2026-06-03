"""Identify what differentiates extreme winners from ordinary signals.

For each date, find the top 1-2 tickers by max_return_pct, then analyze:
1. Factor values at entry moment (vs distribution of all signals)
2. Entry gap percentile within the day's cohort
3. Where they rank in the volume_strong_10 boolean filter vs other signals
4. Price trajectory shape (when did they start pulling away from the pack?)

Usage:
    poetry run python scripts/analyze_extreme_winners.py
    poetry run python scripts/analyze_extreme_winners.py --min-return 100 --top-n 3
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

# Add project path
sys.path.insert(0, str(Path(__file__).parent.parent / "python" / "src"))

FACTOR_COLS = [
    "rel_vol_20",
    "trade_rate",
    "quote_rate",
    "price_direction",
    "bid_ask_spread",
    "vol_accel_5_15",
    "order_imbalance",
    "gap_pct",
    "entry_gap_pct",
    "ema_20",
]


def load_all_data() -> pd.DataFrame:
    """Load and merge explore + validate parquet files."""
    explore = pd.read_parquet("data/dense_signals/explore.parquet")
    validate = pd.read_parquet("data/dense_signals/validate.parquet")
    df = pd.concat([explore, validate], ignore_index=True)
    # Compute tier for each signal
    df["tier"] = pd.cut(
        df["max_return_pct"],
        bins=[-float("inf"), 0, 10, 20, 50, 100, 200, float("inf")],
        labels=["loser", "flat", "small", "medium", "big", "huge", "extreme"],
    )
    return df


def find_extreme_winners(df: pd.DataFrame, top_n: int = 2) -> pd.DataFrame:
    """Find top-N tickers by max_return_pct for each date."""
    winners = []
    for date in sorted(df["date"].unique()):
        day_data = df[df["date"] == date]
        top = day_data.nlargest(top_n, "max_return_pct")
        top = top.copy()
        top["rank"] = range(1, len(top) + 1)
        winners.append(top)
    return pd.concat(winners, ignore_index=True)


def compare_factor_distributions(
    df: pd.DataFrame,
    winners: pd.DataFrame,
) -> dict:
    """Compare factor values of extreme winners vs all other signals."""
    results = {}
    date_strs = sorted(df["date"].unique())

    for factor in FACTOR_COLS:
        if factor not in df.columns:
            continue

        # Per-date percentile of each winner
        winner_pcts = []
        for _, row in winners.iterrows():
            date = row["date"]
            day_mask = df["date"] == date
            day_values = df.loc[day_mask, factor].dropna()
            if len(day_values) < 10:
                continue
            pct = (day_values < row[factor]).mean() * 100
            winner_pcts.append(
                {
                    "date": date,
                    "ticker": row["ticker"],
                    "rank": row["rank"],
                    "value": row[factor],
                    "percentile": pct,
                    "max_return": row["max_return_pct"],
                }
            )

        if not winner_pcts:
            continue

        wdf = pd.DataFrame(winner_pcts)
        results[factor] = {
            "winner_percentiles": wdf,
            "mean_pct": float(wdf["percentile"].mean()),
            "median_pct": float(wdf["percentile"].median()),
        }

    return results


def analyze_boolean_filter_overlap(
    df: pd.DataFrame,
    winners: pd.DataFrame,
) -> pd.DataFrame:
    """Check which boolean filters the extreme winners pass at entry."""
    filters = {
        "volume_strong_10": (df["rel_vol_20"] > 5.0) & (df["trade_rate"] > 10),
        "volume_strong": (df["rel_vol_20"] > 5.0) & (df["trade_rate"] > 5),
        "volume_entry": (df["rel_vol_20"] > 3.0) & (df["trade_rate"] > 5),
        "momentum_entry": (df["trade_rate"] > 10) & (df["rel_vol_20"] > 2.0),
        "high_conviction": (df["trade_rate"] > 50) & (df["rel_vol_20"] > 3.0),
    }

    results = []
    for _, w in winners.iterrows():
        mask = df["date"] == w["date"]
        day_df = df[mask]
        row = {
            "date": w["date"],
            "ticker": w["ticker"],
            "max_return": w["max_return_pct"],
            "rank": w["rank"],
        }
        for name, filt in filters.items():
            day_filt = filt[mask]
            total_signals = day_filt.sum()
            # Does this winner pass?
            winner_idx = day_df[day_df["ticker"] == w["ticker"]].index
            if len(winner_idx) > 0:
                row[f"{name}_pass"] = day_filt.loc[winner_idx[0]]
            else:
                row[f"{name}_pass"] = False
            row[f"{name}_day_signals"] = int(total_signals)
        results.append(row)

    return pd.DataFrame(results)


def analyze_price_trajectory(
    df: pd.DataFrame,
    winners: pd.DataFrame,
    max_return_threshold: float = 50.0,
) -> pd.DataFrame:
    """Compare entry-time characteristics of extreme winners vs others.

    Since we don't have full tick-by-tick trajectories in the parquet data,
    use available metrics: entry_gap_pct, max_return_pct, return_pct.
    """
    results = []
    for date in sorted(df["date"].unique()):
        day_data = df[df["date"] == date]
        day_winners = winners[winners["date"] == date]

        extreme_mask = day_data["max_return_pct"] > max_return_threshold
        non_extreme_mask = (day_data["max_return_pct"] > 0) & (
            day_data["max_return_pct"] <= max_return_threshold
        )

        row = {"date": date}
        row["n_extreme"] = int(extreme_mask.sum())
        row["n_non_extreme"] = int(non_extreme_mask.sum())
        row["n_total"] = len(day_data)

        # Extreme winner stats
        for suffix, mask in [
            ("extreme", extreme_mask),
            ("non_extreme", non_extreme_mask),
        ]:
            subset = day_data[mask]
            if len(subset) < 1:
                continue
            row[f"{suffix}_mean_max"] = float(subset["max_return_pct"].mean())
            row[f"{suffix}_mean_return"] = float(subset["return_pct"].mean())
            row[f"{suffix}_mean_gap"] = float(subset["entry_gap_pct"].mean())
            row[f"{suffix}_mean_relvol"] = float(subset["rel_vol_20"].mean())
            row[f"{suffix}_mean_traderate"] = float(subset["trade_rate"].mean())
            row[f"{suffix}_mean_volaccel"] = float(subset["vol_accel_5_15"].mean())
            row[f"{suffix}_mean_spread"] = float(subset["bid_ask_spread"].mean())

        results.append(row)

    return pd.DataFrame(results)


def rank_winners_vs_signals(
    df: pd.DataFrame,
    winners: pd.DataFrame,
) -> pd.DataFrame:
    """For each date, rank ALL signals by different factor combinations.
    Where do the extreme winners rank?
    """
    results = []
    for date in sorted(df["date"].unique()):
        day = df[df["date"] == date].copy()
        n_total = len(day)

        if n_total < 5:
            continue

        # Rank by individual factors
        ranks = {}
        for factor in FACTOR_COLS:
            if factor not in day.columns:
                continue
            # Higher is better for most factors (direction depends)
            day[f"{factor}_rank"] = day[factor].rank(ascending=False, pct=True)

        # Composite score: simple average of trade_rate, rel_vol, vol_accel ranks
        day["composite_rank"] = (
            day["trade_rate_rank"] * 0.4
            + day["rel_vol_20_rank"] * 0.3
            + day["vol_accel_5_15_rank"] * 0.3
        )
        day["composite_overall"] = day["composite_rank"].rank(ascending=False)

        # Where are today's extreme winners?
        day_winners = winners[winners["date"] == date]
        for _, w in day_winners.iterrows():
            w_row = day[day["ticker"] == w["ticker"]]
            if len(w_row) == 0:
                continue
            w_data = w_row.iloc[0]

            result = {
                "date": date,
                "ticker": w["ticker"],
                "max_return": w["max_return_pct"],
                "rank": w["rank"],
                # Where does this winner rank among all signals by each factor?
                "tr_pct": w_data["trade_rate_rank"],
                "rv_pct": w_data["rel_vol_20_rank"],
                "va_pct": w_data["vol_accel_5_15_rank"],
                "qr_pct": w_data.get("quote_rate_rank", np.nan),
                "gap_pct_rank": w_data.get("entry_gap_pct_rank", np.nan),
                "composite_pct": w_data["composite_rank"],
                "composite_overall_rank": int(w_data["composite_overall"]),
                "n_total": n_total,
            }
            results.append(result)

    return pd.DataFrame(results)


def summarize_findings(factor_compare, bool_overlap, trajectory, rankings):
    """Print a narrative summary of findings."""
    print("\n" + "=" * 70)
    print("EXTREME WINNER ANALYSIS — SUMMARY")
    print("=" * 70)

    # 1. Factor percentile summary
    print("\n## 1. Where do extreme winners rank by each factor?")
    print(f"   {'Factor':<22} {'Mean Pct':>8} {'Interpretation'}")
    print(f"   {'-'*22} {'-'*8} {'-'*30}")
    for factor, data in sorted(
        factor_compare.items(), key=lambda x: x[1]["mean_pct"], reverse=True
    ):
        pct = data["mean_pct"]
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        print(f"   {factor:<22} {pct:>7.1f}%  {bar}")

    # 2. Boolean filter pass rate
    print("\n## 2. Do boolean filters catch the extreme winners?")
    pass_rates = {}
    for col in bool_overlap.columns:
        if col.endswith("_pass"):
            pass_rate = bool_overlap[col].mean()
            filter_name = col.replace("_pass", "")
            pass_rates[filter_name] = pass_rate
            print(f"   {filter_name:<25}: {pass_rate:.0%} of winners pass")

    # 3. Ranking analysis
    if len(rankings) > 0:
        print("\n## 3. Where do extreme winners rank among all day's signals?")
        mean_composite_rank = rankings["composite_overall_rank"].mean()
        mean_n_total = rankings["n_total"].mean()
        print(
            f"   Average composite rank: #{mean_composite_rank:.0f} out of {mean_n_total:.0f}"
        )
        print(f"   Average percentile: {mean_composite_rank / mean_n_total * 100:.1f}%")
        print(
            f"   Best rank: #{rankings['composite_overall_rank'].min()} (top signal of the day)"
        )
        print(f"   Worst rank: #{rankings['composite_overall_rank'].max()}")

    # 4. Trajectory comparison
    print("\n## 4. Extreme (>50% max return) vs Non-extreme (0-50%) signals")
    if len(trajectory) > 0:
        ex = trajectory[trajectory["n_extreme"] > 0]
        if len(ex) > 0:
            print(f"   {'Metric':<25} {'Extreme':>10} {'NonExtreme':>10} {'Ratio':>8}")
            print(f"   {'-'*25} {'-'*10} {'-'*10} {'-'*8}")
            metrics = [
                ("mean_relvol", "mean_relvol"),
                ("mean_traderate", "mean_traderate"),
                ("mean_volaccel", "mean_volaccel"),
                ("mean_gap", "mean_gap"),
                ("mean_spread", "mean_spread"),
            ]
            for label, key_suffix in metrics:
                ex_col = f"extreme_{key_suffix}"
                ne_col = f"non_extreme_{key_suffix}"
                if ex_col in ex.columns and ne_col in ex.columns:
                    ex_mean = ex[ex_col].mean()
                    ne_mean = ex[ne_col].mean()
                    ratio = ex_mean / ne_mean if ne_mean != 0 else float("inf")
                    print(
                        f"   {label:<25} {ex_mean:>10.2f} {ne_mean:>10.2f} {ratio:>8.2f}x"
                    )

    # 5. Key insight
    print("\n## 5. Key Insight")
    print("   If extreme winners look like average signals on all existing factors,")
    print("   the differentiator is NOT in these factors — it's in something we")
    print("   haven't measured yet (tick direction, price microstructure, etc.).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--min-return",
        type=float,
        default=50.0,
        help="Minimum max_return_pct to be an 'extreme' winner",
    )
    parser.add_argument(
        "--top-n", type=int, default=2, help="Number of top winners per date"
    )
    args = parser.parse_args()

    print(f"Loading data...")
    df = load_all_data()
    print(f"Total signals: {len(df):,} across {df['date'].nunique()} dates")
    print(f"Tier distribution:\n{df['tier'].value_counts().sort_index()}")

    # Find extreme winners
    winners = find_extreme_winners(df, top_n=args.top_n)
    print(f"\nTop {args.top_n} winners per date ({len(winners)} total):")
    for _, w in winners.iterrows():
        print(
            f"  {w['date']}  {w['ticker']:<8}  max_return={w['max_return_pct']:>8.1f}%  "
            f"return={w['return_pct']:>7.1f}%  gap={w['entry_gap_pct']:>6.1f}%  "
            f"tr={w['trade_rate']:>6.1f}  rv={w['rel_vol_20']:>5.1f}  "
            f"va={w['vol_accel_5_15']:>8.0f}  spread={w['bid_ask_spread']:>5.0f}"
        )

    # Compare factor distributions
    factor_compare = compare_factor_distributions(df, winners)

    # Boolean filter overlap
    bool_overlap = analyze_boolean_filter_overlap(df, winners)

    # Price trajectory
    trajectory = analyze_price_trajectory(
        df, winners, max_return_threshold=args.min_return
    )

    # Rankings
    rankings = rank_winners_vs_signals(df, winners)

    # Summary
    summarize_findings(factor_compare, bool_overlap, trajectory, rankings)

    # Detailed per-ticker view
    print("\n" + "=" * 70)
    print("PER-TICKER BREAKDOWN")
    print("=" * 70)
    for _, w in winners.iterrows():
        print(f"\n  {w['date']} {w['ticker']} — max_return={w['max_return_pct']:.1f}%")
        for factor in FACTOR_COLS:
            if factor in factor_compare:
                wdf = factor_compare[factor]["winner_percentiles"]
                match = wdf[(wdf["date"] == w["date"]) & (wdf["ticker"] == w["ticker"])]
                if len(match) > 0:
                    pct = match.iloc[0]["percentile"]
                    val = match.iloc[0]["value"]
                    bar = "█" * max(1, int(pct / 5)) + "░" * (20 - max(1, int(pct / 5)))
                    print(
                        f"    {factor:<22}: val={val:>10.2f}  pct={pct:>5.1f}%  {bar}"
                    )

    # Boolean pass/fail per ticker
    print("\n" + "=" * 70)
    print("BOOLEAN FILTER PASS/FAIL PER WINNER")
    print("=" * 70)
    print(bool_overlap.to_string(index=False))


if __name__ == "__main__":
    main()
