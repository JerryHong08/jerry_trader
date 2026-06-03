"""Generate Layer 2 calibration dataset — ignition-moment features with labels.

For each candidate across all 10 dates:
  1. Load trades + quotes via ExitLab
  2. Detect spread_collapse ignition (the Layer 2 decision anchor)
  3. Compute decision-time features at ignition moment (pre-ignition window stats)
  4. Compute post-ignition window features (for analysis/calibration)
  5. Compute outcome: ig_to_peak_pct
  6. Merge with human labels from ticker_profiles_labeled.csv

Output: data/layer2_calibration.csv
  - entry-moment features (from existing CSV)
  - ignition timing (delay_sec, price_at_ig, pnl_since_entry)
  - pre-ignition window (trade_count, dollar_vol, max_gap, price_range, btd)
  - post-ignition window (same stats, 5-min window after ignition)
  - outcome: ig_to_peak_pct, hit_5pct, hit_10pct, hit_20pct
  - label (True MOMO / False MOMO from human labels)

Usage: poetry run python scripts/generate_layer2_calibration.py
"""

import csv
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python" / "src"))

from jerry_trader.services.backtest.layer_lab.ignition import _detect_ignitions
from jerry_trader.services.backtest.layer_lab.lab import ExitLab

LABELED_CSV = (
    Path(__file__).resolve().parent.parent / "data" / "ticker_profiles_labeled.csv"
)
OUTPUT_CSV = Path(__file__).resolve().parent.parent / "data" / "layer2_calibration.csv"

DATES = [
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


def safe_float(v):
    try:
        return float(v) if v and v != "" else None
    except (ValueError, TypeError):
        return None


def compute_window_stats(
    trades: pd.DataFrame, window_start_ms: int, window_end_ms: int
) -> dict:
    """Compute trade-stream statistics for a time window."""
    w = trades[(trades["ts_ms"] > window_start_ms) & (trades["ts_ms"] <= window_end_ms)]
    if w.empty:
        return {
            "trade_count": 0,
            "dollar_vol": 0.0,
            "max_gap_sec": 0.0,
            "price_range_pct": 0.0,
            "btd": 0.0,
        }

    prices = w["price"].values.astype(float)
    sizes = w["size"].values.astype(float)
    tss = w["ts_ms"].values.astype(float)

    trade_count = len(w)
    dollar_vol = float((prices * sizes).sum())

    max_gap_sec = 0.0
    if len(tss) >= 2:
        gaps = np.diff(tss) / 1000.0
        max_gap_sec = float(gaps.max()) if len(gaps) > 0 else 0.0

    price_range_pct = 0.0
    if len(prices) >= 2:
        p_min, p_max = float(prices.min()), float(prices.max())
        if p_min > 0:
            price_range_pct = (p_max - p_min) / p_min * 100

    # BTD: median of (trade_price - first_price) / first_price for trades > avg size
    btd = 0.0
    if len(prices) >= 2:
        avg_size = float(sizes.mean())
        big_mask = sizes > avg_size
        if big_mask.any():
            big_prices = prices[big_mask]
            first_p = float(prices[0])
            if first_p > 0:
                btd = float(np.median((big_prices - first_p) / first_p * 100))
        else:
            first_p = float(prices[0])
            if first_p > 0:
                btd = float(np.median((prices - first_p) / first_p * 100))

    return {
        "trade_count": trade_count,
        "dollar_vol": round(dollar_vol, 0),
        "max_gap_sec": round(max_gap_sec, 1),
        "price_range_pct": round(price_range_pct, 2),
        "btd": round(btd, 2),
    }


def main():
    # ── Load labels ──
    label_map: dict[tuple[str, str], str] = {}
    with open(LABELED_CSV) as f:
        for row in csv.DictReader(f):
            key = (row["date"].strip(), row["ticker"].strip())
            label_map[key] = row.get("label", "").strip()
    print(f"Loaded {len(label_map)} labels")

    # ── Entry-moment features from existing CSV ──
    entry_features: dict[tuple[str, str], dict] = {}
    with open(LABELED_CSV) as f:
        for row in csv.DictReader(f):
            key = (row["date"].strip(), row["ticker"].strip())
            entry_features[key] = {
                "gain_at_entry": safe_float(row.get("gain_at_entry", "")),
                "trade_rate": safe_float(row.get("trade_rate", "")),
                "bid_ask_spread": safe_float(row.get("bid_ask_spread", "")),
                "large_trade_ratio": safe_float(row.get("large_trade_ratio", "")),
                "aggressor_ratio": safe_float(row.get("aggressor_ratio", "")),
                "volume_at_entry": safe_float(row.get("volume_at_entry", "")),
                "max_trade_gap_sec": safe_float(row.get("max_trade_gap_sec", "")),
                "gap_over_120s": safe_float(row.get("gap_over_120s", "")),
                "n_trades_first_5min": safe_float(row.get("n_trades_first_5min", "")),
            }

    # ── Generate ignition-moment features per date ──
    all_rows = []
    skipped_no_trades = 0
    skipped_no_entry_price = 0
    skipped_no_spread_collapse = 0

    for date in DATES:
        print(f"\n{'=' * 60}")
        print(f"Processing {date}...")

        lab = ExitLab(date)
        lab.load_candidates(top_n=999, min_gain_pct=0.0, new_entry_only=False)

        if not lab.candidates:
            print(f"  No candidates")
            continue

        et = ZoneInfo("America/New_York")
        dt_et = datetime.strptime(date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=et
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        for cand in lab.candidates:
            ticker = cand.symbol
            key = (date, ticker)
            label = label_map.get(key, "")
            if label not in ("0", "1"):
                continue  # skip unlabeled / skip

            trades = lab._load_trades(ticker)
            if trades.empty:
                skipped_no_trades += 1
                continue

            entry_price = lab.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                skipped_no_entry_price += 1
                continue

            entry_ms = cand.first_entry_ms

            # Detect ignition
            try:
                quotes = lab._load_quotes(ticker)
            except Exception:
                quotes = pd.DataFrame()
            ignitions = _detect_ignitions(
                trades, entry_ms, session_end_ms, quotes=quotes
            )

            # Use spread_collapse as the Layer 2 anchor
            ig = ignitions.get("spread_collapse")
            if ig is None or ig.get("ms") is None:
                skipped_no_spread_collapse += 1
                continue

            ig_ms = ig["ms"]
            ig_price = ig["price"]
            ig_delay_sec = (ig_ms - entry_ms) / 1000.0

            # ── Pre-ignition window: last 5 min before ignition ──
            pre_window_start = max(entry_ms, ig_ms - 300_000)
            pre = compute_window_stats(trades, pre_window_start, ig_ms)

            # ── Post-ignition window: first 5 min after ignition ──
            post_window_end = ig_ms + 300_000
            post = compute_window_stats(trades, ig_ms, post_window_end)

            # ── Outcome: ig → peak ──
            after_ig = trades[
                (trades["ts_ms"] > ig_ms) & (trades["ts_ms"] <= session_end_ms)
            ]
            if after_ig.empty:
                ig_to_peak_pct = 0.0
            else:
                peak = float(after_ig["price"].max())
                ig_to_peak_pct = (peak - ig_price) / ig_price * 100

            # ── Entry-moment features from CSV ──
            ef = entry_features.get(key, {})

            # Price movement from entry → ignition
            pnl_since_entry = (ig_price - entry_price) / entry_price * 100

            all_rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "label": int(label),
                    # Entry moment
                    "entry_ms": int(entry_ms),
                    "entry_price": round(entry_price, 4),
                    "gain_at_entry": ef.get("gain_at_entry"),
                    "entry_trade_rate": ef.get("trade_rate"),
                    "entry_spread": ef.get("bid_ask_spread"),
                    "entry_large_trade_ratio": ef.get("large_trade_ratio"),
                    "entry_aggressor_ratio": ef.get("aggressor_ratio"),
                    "entry_max_trade_gap_sec": ef.get("max_trade_gap_sec"),
                    "entry_gap_over_120s": ef.get("gap_over_120s"),
                    "entry_n_trades_first_5min": ef.get("n_trades_first_5min"),
                    # Ignition timing
                    "ig_ms": int(ig_ms),
                    "ig_price": round(ig_price, 4),
                    "ig_delay_sec": round(ig_delay_sec, 1),
                    "pnl_since_entry_pct": round(pnl_since_entry, 2),
                    # Pre-ignition window (decision-time features)
                    "pre_trade_count": pre["trade_count"],
                    "pre_dollar_vol": pre["dollar_vol"],
                    "pre_max_gap_sec": pre["max_gap_sec"],
                    "pre_price_range_pct": pre["price_range_pct"],
                    "pre_btd": pre["btd"],
                    # Post-ignition window (for analysis)
                    "post_trade_count": post["trade_count"],
                    "post_dollar_vol": post["dollar_vol"],
                    "post_max_gap_sec": post["max_gap_sec"],
                    "post_price_range_pct": post["price_range_pct"],
                    "post_btd": post["btd"],
                    # Outcome
                    "ig_to_peak_pct": round(ig_to_peak_pct, 2),
                }
            )

        print(f"  {len([r for r in all_rows if r['date'] == date])} rows for {date}")

    # ── Save ──
    df = pd.DataFrame(all_rows)
    df.to_csv(OUTPUT_CSV, index=False)

    n_true = (df["label"] == 1).sum()
    n_false = (df["label"] == 0).sum()
    total_labeled = n_true + n_false

    print(f"\n{'=' * 60}")
    print(f"OUTPUT: {OUTPUT_CSV}")
    print(f"  Total rows: {len(df)} (True={n_true}, False={n_false})")
    print(
        f"  Original labeled: {sum(1 for v in label_map.values() if v in ('0', '1'))}"
    )
    print(
        f"  Coverage: {len(df)}/{total_labeled} labeled candidates have spread_collapse"
        f" ({100*len(df)/max(total_labeled,1):.1f}%)"
    )
    print(
        f"  Skipped: {skipped_no_trades} no trades, "
        f"{skipped_no_entry_price} no entry price, "
        f"{skipped_no_spread_collapse} no spread_collapse"
    )

    # ── Quick analysis ──
    print(f"\n{'=' * 60}")
    print("QUICK ANALYSIS: Decision-time features at ignition moment")
    print(f"{'=' * 60}")

    true_df = df[df["label"] == 1]
    false_df = df[df["label"] == 0]

    decision_features = [
        "ig_delay_sec",
        "pnl_since_entry_pct",
        "pre_trade_count",
        "pre_dollar_vol",
        "pre_max_gap_sec",
        "pre_price_range_pct",
        "pre_btd",
        "entry_trade_rate",
        "entry_spread",
        "entry_max_trade_gap_sec",
        "entry_gap_over_120s",
    ]

    print(f"{'Feature':<28s} {'True_Med':>12s} {'False_Med':>12s} {'Ratio':>8s}")
    print("-" * 65)
    for f in decision_features:
        t_vals = true_df[f].dropna().values
        f_vals = false_df[f].dropna().values
        if len(t_vals) == 0 or len(f_vals) == 0:
            continue
        t_med = np.median(t_vals)
        f_med = np.median(f_vals)
        ratio = t_med / f_med if f_med != 0 else float("inf")
        print(f"{f:<28s} {t_med:>12.4f} {f_med:>12.4f} {ratio:>8.2f}")

    # Outcome comparison
    print(f"\n--- Outcome: ig_to_peak_pct ---")
    print(f"True median:  {true_df['ig_to_peak_pct'].median():.2f}%")
    print(f"False median: {false_df['ig_to_peak_pct'].median():.2f}%")
    print(f"True mean:    {true_df['ig_to_peak_pct'].mean():.2f}%")
    print(f"False mean:   {false_df['ig_to_peak_pct'].mean():.2f}%")

    # Post-ignition features (analysis only)
    print(f"\n--- Post-ignition window features (analysis only, not decision-time) ---")
    print(f"{'Feature':<25s} {'True_Med':>12s} {'False_Med':>12s} {'Ratio':>8s}")
    print("-" * 60)
    for f in [
        "post_trade_count",
        "post_dollar_vol",
        "post_max_gap_sec",
        "post_price_range_pct",
        "post_btd",
    ]:
        t_vals = true_df[f].dropna().values
        f_vals = false_df[f].dropna().values
        if len(t_vals) == 0 or len(f_vals) == 0:
            continue
        t_med = np.median(t_vals)
        f_med = np.median(f_vals)
        ratio = t_med / f_med if f_med != 0 else float("inf")
        print(f"{f:<25s} {t_med:>12.4f} {f_med:>12.4f} {ratio:>8.2f}")


if __name__ == "__main__":
    main()
