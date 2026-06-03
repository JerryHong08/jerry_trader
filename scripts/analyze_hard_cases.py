"""Analyze labeled CSV → find optimal feature thresholds → output hard cases for review.

Identifies False MOMO that survive ALL individual feature cuts at 100% True recall.
These are the cases that "look like" True MOMO at decision time but aren't — the most
valuable cases for human review to discover new distinguishing patterns.

Input:  data/layer2_calibration.csv (ignition-moment features + labels)
        data/ticker_profiles_labeled.csv (entry-moment features + labels)
Output: data/hard_cases_review.csv (True MOMO + survivor False MOMO, merged features)

Usage: poetry run python scripts/analyze_hard_cases.py
"""

import numpy as np
import pandas as pd

LAYER2_CSV = "data/layer2_calibration.csv"
PROFILES_CSV = "data/ticker_profiles_labeled.csv"
OUTPUT_CSV = "data/hard_cases_review.csv"

# Pre-ignition decision-time features to scan for cuts
FEATURES = [
    "pre_btd",
    "pre_trade_count",
    "pre_dollar_vol",
    "pre_max_gap_sec",
    "pre_price_range_pct",
    "ig_delay_sec",
    "pnl_since_entry_pct",
    "entry_trade_rate",
    "entry_spread",
    "entry_large_trade_ratio",
    "entry_aggressor_ratio",
    "entry_max_trade_gap_sec",
    "entry_gap_over_120s",
    "entry_n_trades_first_5min",
    "gain_at_entry",
]


def main():
    layer2 = pd.read_csv(LAYER2_CSV)
    profiles = pd.read_csv(PROFILES_CSV)

    true_df = layer2[layer2["label"] == 1]
    false_df = layer2[layer2["label"] == 0]

    if len(true_df) == 0:
        print("ERROR: No True MOMO labeled. Label some True MOMO first.")
        return

    # ── Per-feature 100%-recall cuts ──
    survivors = set(false_df.index)
    print(
        f"{'Feature':<30s} {'True_Med':>10s} {'False_Med':>10s} {'BestCut':>10s} {'Cut%':>8s}"
    )
    print("-" * 72)

    for f in FEATURES:
        t = true_df[f].dropna()
        fv = false_df[f].dropna()
        if len(t) == 0 or len(fv) == 0:
            continue

        t_med = np.median(t)
        f_med = np.median(fv)

        if t_med > f_med:
            threshold = t.min()
            survivors &= set(fv[fv >= threshold].index)
        else:
            threshold = t.max()
            survivors &= set(fv[fv <= threshold].index)

        cut_pct = (
            (false_df.loc[list(set(false_df.index) - survivors)].shape[0])
            if len(survivors) < len(false_df)
            else 0
        )
        n_cut = len(set(false_df.index) - survivors)
        print(
            f"{f:<30s} {t_med:>10.4f} {f_med:>10.4f} {threshold:>10.4f} "
            f"{n_cut / len(false_df) * 100:>7.1f}%"
        )

    print(f"\n{'=' * 72}")
    print(
        f"False MOMO surviving ALL cuts: {len(survivors)}/{len(false_df)} "
        f"({100 * len(survivors) / len(false_df):.1f}%)"
    )

    # ── Build merged output ──
    surv_keys = set()
    for idx in survivors:
        row = layer2.loc[idx]
        surv_keys.add((str(row["date"]).strip(), str(row["ticker"]).strip()))

    true_keys = set()
    for _, row in true_df.iterrows():
        true_keys.add((str(row["date"]).strip(), str(row["ticker"]).strip()))

    profiles["_key"] = list(
        zip(profiles["date"].astype(str).str.strip(), profiles["ticker"].str.strip())
    )
    layer2["_key"] = list(
        zip(layer2["date"].astype(str).str.strip(), layer2["ticker"].str.strip())
    )

    hard_keys = surv_keys | true_keys
    hard_profiles = profiles[profiles["_key"].isin(hard_keys)].drop(columns=["_key"])
    hard_layer2 = layer2[layer2["_key"].isin(hard_keys)].drop(columns=["_key"])

    layer2_cols = [
        c for c in hard_layer2.columns if c not in ["date", "ticker", "label"]
    ]
    merged = hard_profiles.merge(
        hard_layer2[["date", "ticker"] + layer2_cols], on=["date", "ticker"], how="left"
    )

    merged["review_flag"] = merged.apply(
        lambda r: (
            "TRUE_MOMO"
            if (str(r["date"]).strip(), str(r["ticker"]).strip()) in true_keys
            else "HARD_FALSE"
        ),
        axis=1,
    )

    merged["_sort"] = merged["review_flag"].map({"TRUE_MOMO": 0, "HARD_FALSE": 1})
    merged = merged.sort_values(["_sort", "date", "ticker"]).drop(columns=["_sort"])

    merged.to_csv(OUTPUT_CSV, index=False)

    # ── Summary ──
    surv_merged = merged[merged["review_flag"] == "HARD_FALSE"]
    print(f"\nOUTPUT: {OUTPUT_CSV}")
    print(f"  TRUE_MOMO:  {(merged['review_flag'] == 'TRUE_MOMO').sum()}")
    print(f"  HARD_FALSE: {(merged['review_flag'] == 'HARD_FALSE').sum()}")
    print(f"  Columns:    {len(merged.columns)}")

    if "ig_to_peak_pct" in surv_merged.columns:
        print(
            f"\nHARD_FALSE ig_to_peak_pct: "
            f"median={surv_merged['ig_to_peak_pct'].median():.1f}%, "
            f"mean={surv_merged['ig_to_peak_pct'].mean():.1f}%"
        )

    print("\nSuggested review columns in frontend:")
    print("  pre_btd, pnl_since_entry_pct, pre_dollar_vol, entry_max_trade_gap_sec")
    print("  post_btd, ig_to_peak_pct (post-hoc hints)")
    print(
        "\nNext: load hard_cases_review.csv in the labeling module, review each HARD_FALSE,"
    )
    print(
        "      add notes about WHY it's False. Then re-run this script for updated analysis."
    )


if __name__ == "__main__":
    main()
