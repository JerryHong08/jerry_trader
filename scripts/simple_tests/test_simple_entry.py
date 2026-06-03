"""Test: can a simple entry_gap_pct filter capture extreme winners?

Compares the simplest possible entry rule against current volume_strong_10.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python" / "src"))

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


def load_data() -> pd.DataFrame:
    explore = pd.read_parquet("data/dense_signals/explore.parquet")
    validate = pd.read_parquet("data/dense_signals/validate.parquet")
    return pd.concat([explore, validate], ignore_index=True)


def evaluate(name: str, mask, df: pd.DataFrame) -> dict:
    """Evaluate a signal mask."""
    signals = df[mask]
    if len(signals) == 0:
        return {"name": name, "n": 0}

    # Per-date stats
    date_stats = {}
    for date in sorted(signals["date"].unique()):
        day = signals[signals["date"] == date]
        top = day.nlargest(1, "max_return_pct")
        date_stats[date] = {
            "n": len(day),
            "mean_max": float(day["max_return_pct"].mean()),
            "median_max": float(day["max_return_pct"].median()),
            "top_ticker": top.iloc[0]["ticker"] if len(top) > 0 else "",
            "top_max_return": (
                float(top.iloc[0]["max_return_pct"]) if len(top) > 0 else 0
            ),
            "n_extreme_50": int((day["max_return_pct"] > 50).sum()),
            "n_extreme_100": int((day["max_return_pct"] > 100).sum()),
        }

    # Overall
    overall = {
        "name": name,
        "n_total": len(signals),
        "n_dates_with_signals": len(date_stats),
        "mean_max": float(signals["max_return_pct"].mean()),
        "median_max": float(signals["max_return_pct"].median()),
        "win_rate": float((signals["max_return_pct"] > 0).mean()),
        "n_extreme_50": int((signals["max_return_pct"] > 50).sum()),
        "n_extreme_100": int((signals["max_return_pct"] > 100).sum()),
        "by_date": date_stats,
    }

    return overall


def main():
    df = load_data()
    print(f"Data: {len(df):,} samples, {df['date'].nunique()} dates\n")

    # Find ALL extreme winners for reference (>50% max_return)
    all_extreme = df[df["max_return_pct"] > 50]
    extreme_by_date = all_extreme.groupby("date").agg(
        n=("max_return_pct", "count"),
        top_ticker=("ticker", "first"),
        top_return=("max_return_pct", "max"),
        top_tr=("trade_rate", lambda x: x.iloc[0]),
        top_rv=("rel_vol_20", lambda x: x.iloc[0]),
        top_gap=("entry_gap_pct", lambda x: x.iloc[0]),
    )
    print("=== Extreme winners (>50% max_return) in raw data ===")
    print(extreme_by_date.to_string())
    print()

    # Define filters
    filters = [
        ("entry_gap > 15%", df["entry_gap_pct"] > 15.0),
        ("entry_gap > 20%", df["entry_gap_pct"] > 20.0),
        ("entry_gap > 25%", df["entry_gap_pct"] > 25.0),
        (
            "volume_strong_10 (current)",
            (df["rel_vol_20"] > 5.0) & (df["trade_rate"] > 10),
        ),
        ("volume_strong", (df["rel_vol_20"] > 5.0) & (df["trade_rate"] > 5)),
        ("momentum_entry", (df["trade_rate"] > 10) & (df["rel_vol_20"] > 2.0)),
        (
            "gap>15 OR vs10",
            (df["entry_gap_pct"] > 15.0)
            | ((df["rel_vol_20"] > 5.0) & (df["trade_rate"] > 10)),
        ),
        (
            "gap>20 OR vs10",
            (df["entry_gap_pct"] > 20.0)
            | ((df["rel_vol_20"] > 5.0) & (df["trade_rate"] > 10)),
        ),
    ]

    results = []
    for name, mask in filters:
        r = evaluate(name, mask, df)
        results.append(r)

    # Print comparison
    print(
        f"{'Filter':<25} {'N':>6} {'MeanMax':>9} {'MedMax':>8} {'WinR':>7} {'>50%':>6} {'>100%':>6}"
    )
    print("-" * 75)
    for r in results:
        print(
            f"{r['name']:<25} {r['n_total']:>6,} {r['mean_max']:>8.1f}% {r['median_max']:>7.1f}% "
            f"{r['win_rate']:>6.1%} {r['n_extreme_50']:>6} {r['n_extreme_100']:>6}"
        )

    # Per-date extreme winner capture rate
    print(f"\n{'='*75}")
    print("PER-DATE: Can the filter find at least 1 extreme winner (>50%)?")
    print(f"{'='*75}")
    dates = sorted(df["date"].unique())
    header = f"{'Date':<12}"
    for r in results:
        header += f" {r['name'][:18]:>18}"
    print(header)
    print("-" * (12 + 19 * len(results)))

    for date in dates:
        row = f"{date:<12}"
        for r in results:
            ds = r.get("by_date", {}).get(date, {})
            n50 = ds.get("n_extreme_50", 0)
            # Check if at least 1 extreme winner captured
            sym = "✓" if n50 > 0 else "✗"
            n = ds.get("n", 0)
            row += f" {sym} {n:>4}sig {n50:>2}e50"
        print(row)

    # Also show: which specific extreme winners are captured
    print(f"\n{'='*75}")
    print("EXTREME WINNER CAPTURE: Who catches each >100% winner?")
    print(f"{'='*75}")

    big = df[df["max_return_pct"] > 100].copy()
    for _, row in big.iterrows():
        print(
            f"\n  {row['date']} {row['ticker']}: max={row['max_return_pct']:.0f}%, "
            f"gap={row['entry_gap_pct']:.1f}%, tr={row['trade_rate']:.1f}, rv={row['rel_vol_20']:.1f}"
        )
        for name, mask in filters:
            idx = df[
                (df["date"] == row["date"]) & (df["ticker"] == row["ticker"])
            ].index
            if len(idx) > 0 and mask.loc[idx[0]]:
                print(f"    ✓ captured by: {name}")


if __name__ == "__main__":
    main()
