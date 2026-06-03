"""Activation-anchored pipeline -- two-anchor approach for MOMO filtration.

Part 1: Multi-date false-negative audit.
  For each candidate across all dates:
    1. Compute factor snapshot at entry moment
    2. Compute would-be tradability score at entry
    3. Flag "false negatives": MOMO candidates that would be rejected by
       entry-anchored tradability filters

Part 2: Two-anchor pipeline.
  Layer 1 (entry-anchored): permissive -- accept all PreFilter candidates.
    No spread/trade_rate/country filtering. Let them all in.
  Wait for ignition (spread_collapse OR vol_exp).
  If ignition fires within timeout -> hold to session end (Layer 2 TBD).
  If no ignition -> zombie, exit at timeout.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import polars as pl

from jerry_trader.platform.config.config import float_shares_dir
from jerry_trader.services.backtest.data_loading import _load_profiles
from jerry_trader.services.backtest.layer_lab.ignition import _detect_ignitions
from jerry_trader.services.backtest.layer_lab.lab import ExitLab


def _load_float_map() -> dict[str, float]:
    """Load float shares map from latest parquet file."""
    float_map: dict[str, float] = {}
    try:
        fs_files = sorted(
            f
            for f in os.listdir(float_shares_dir)
            if f.startswith("float_shares_") and f.endswith(".parquet")
        )
        if fs_files:
            fs_df = pl.read_parquet(os.path.join(float_shares_dir, fs_files[-1]))
            for row in fs_df.iter_rows(named=True):
                s = row["symbol"]
                if s and row["floatShares"] is not None:
                    float_map[s] = float(row["floatShares"])
    except Exception:
        pass
    return float_map


def audit_false_negatives(dates: list[str], top_n: int = 20) -> pd.DataFrame:
    """Audit all candidates across multiple dates for entry-anchored false negatives.

    Returns DataFrame with per-candidate factor values, tradability scores,
    and whether a MOMO candidate would be incorrectly rejected.
    """
    all_rows = []

    for date in dates:
        print(f"\n{'='*60}")
        print(f"Auditing {date}...")

        lab = ExitLab(date)
        lab.load_candidates(top_n=top_n)

        profiles = lab._profiles
        float_map = _load_float_map()

        et = ZoneInfo("America/New_York")
        dt_et = datetime.strptime(date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=et
        )
        session_end_ms = int(dt_et.timestamp() * 1000)

        entries = lab._snapshot_factors_at_entry()
        entry_by_ticker = {e["ticker"]: e for e in entries}

        for cand in lab.candidates:
            ticker = cand.symbol
            trades = lab._load_trades(ticker)
            if trades.empty:
                continue

            entry_price = lab.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                continue

            # Entry-to-peak
            fwd = trades[
                (trades["ts_ms"] > cand.first_entry_ms)
                & (trades["ts_ms"] <= session_end_ms)
            ]
            if not fwd.empty:
                peak = float(fwd["price"].max())
                entry_to_peak = (peak - entry_price) / entry_price * 100
            else:
                entry_to_peak = 0.0

            is_momo = entry_to_peak >= 50

            snap = entry_by_ticker.get(ticker, {})
            trade_rate = snap.get("trade_rate")
            spread = snap.get("bid_ask_spread")

            profile = profiles.get(ticker, {})
            country = profile.get("country")
            fs = float_map.get(ticker)

            # Tradability scores at entry
            score_tr = (
                min(float(trade_rate) / 15.0, 1.0)
                if trade_rate is not None and not np.isnan(trade_rate)
                else 0.0
            )
            score_sp = (
                max(0.0, 1.0 - float(spread) / 200.0)
                if spread is not None and not np.isnan(spread)
                else 0.0
            )

            if entry_price and entry_price > 0:
                z = (entry_price - 8.0) / 7.0
                score_pr = float(np.exp(-0.5 * z * z))
            else:
                score_pr = 0.0

            if fs is not None and fs > 0:
                fs_m = fs / 1_000_000
                score_fl = float(1.0 / (1.0 + np.exp((fs_m - 20.0) / 8.0)))
            else:
                score_fl = 0.0

            if country is None:
                score_ct = 0.0
            else:
                score_ct = 0.0 if country in ("CN", "HK") else 1.0

            composite = (score_tr + score_sp + score_pr + score_fl + score_ct) / 5.0

            # Rejection check
            rejection_reasons = []
            if score_tr < 0.05:
                rejection_reasons.append("trade_rate")
            if score_sp < 0.05:
                rejection_reasons.append("spread")
            if score_ct < 0.5:
                rejection_reasons.append("country(CN/HK)")

            would_reject = len(rejection_reasons) > 0
            is_false_negative = is_momo and would_reject

            all_rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "entry_ms": cand.first_entry_ms,
                    "entry_price": round(entry_price, 4),
                    "entry_to_peak_pct": round(entry_to_peak, 2),
                    "is_momo": is_momo,
                    "trade_rate": trade_rate,
                    "spread_bps": spread,
                    "score_tr": round(score_tr, 3),
                    "score_sp": round(score_sp, 3),
                    "score_composite": round(composite, 3),
                    "country": country,
                    "n_fwd_trades": snap.get("n_fwd_trades", 0),
                    "would_reject": would_reject,
                    "is_false_negative": is_false_negative,
                    "rejection_reasons": (
                        ",".join(rejection_reasons) if rejection_reasons else ""
                    ),
                }
            )

    df = pd.DataFrame(all_rows)

    # ── Summary ──
    print(f"\n{'='*60}")
    print("MULTI-DATE FALSE-NEGATIVE AUDIT")
    print(f"{'='*60}")
    print(f"Total candidates: {len(df)} across {len(dates)} dates")

    for date in dates:
        dd = df[df["date"] == date]
        momo = dd[dd["is_momo"]]
        fn = dd[dd["is_false_negative"]]
        print(
            f"  {date}: {len(dd)} candidates, {len(momo)} MOMO, {len(fn)} false-negatives"
        )

    print(f"\n── All MOMO candidates ──")
    momo_df = df[df["is_momo"]].sort_values("entry_to_peak_pct", ascending=False)
    for _, row in momo_df.iterrows():
        fn_tag = " <<< FALSE NEG" if row["is_false_negative"] else ""
        print(
            f"  {row['date']} {row['ticker']:6s}  peak={row['entry_to_peak_pct']:+.0f}%  "
            f"tr={row['trade_rate']:.1f}  spread={row['spread_bps']:.0f}bps  "
            f"composite={row['score_composite']:.3f}  reject={row['rejection_reasons']}{fn_tag}"
        )

    print(f"\n── False negatives detail ──")
    fn_df = df[df["is_false_negative"]].sort_values(
        "entry_to_peak_pct", ascending=False
    )
    if fn_df.empty:
        print("  NONE -- all MOMO pass entry-anchored filters")
    else:
        for _, row in fn_df.iterrows():
            print(
                f"  {row['date']} {row['ticker']:6s}  peak={row['entry_to_peak_pct']:+.0f}%  "
                f"reject={row['rejection_reasons']}  "
                f"tr={row['trade_rate']:.1f}  sp={row['spread_bps']:.0f}bps"
            )

    total_momo = df["is_momo"].sum()
    total_fn = df["is_false_negative"].sum()
    print(f"\n── Summary ──")
    print(f"  Total MOMO: {total_momo}")
    print(f"  False negatives (would be rejected): {total_fn}")
    if total_momo > 0:
        print(f"  FN rate: {total_fn}/{total_momo} ({total_fn/total_momo*100:.0f}%)")

    return df


@dataclass
class AnchorResult:
    """Outcome for a single candidate in the two-anchor pipeline."""

    date: str
    ticker: str
    entry_ms: int
    entry_price: float
    gain_at_entry: float
    # Ignition
    ignited: bool
    ignition_method: str = ""
    ignition_ms: int = 0
    ignition_price: float = 0.0
    ignition_delay_sec: float = 0.0
    # Zombie exit (if not ignited)
    zombie_exit_price: float = 0.0
    zombie_exit_ms: int = 0
    # Outcome
    entry_to_peak_pct: float = 0.0
    is_momo: bool = False
    final_pnl_pct: float = 0.0
    exit_reason: str = ""
    # Ignition-moment features (computed from ignition → ignition+5min window)
    ig_trade_count_5min: int = 0
    ig_dollar_vol_5min: float = 0.0
    ig_price_range_pct: float = 0.0
    ig_btd: float = 0.0
    ig_max_gap_sec: float = 0.0
    ig_to_peak_pct: float = 0.0


def _nearest_trade_price(trades: pd.DataFrame, target_ms: int) -> float | None:
    """Last trade price at or before target_ms. None if no trade exists."""
    before = trades[trades["ts_ms"] <= target_ms]
    if not before.empty:
        return float(before["price"].iloc[-1])
    return None


def run_two_anchor(
    dates: list[str], top_n: int = 20, ignition_timeout_min: int = 30
) -> pd.DataFrame:
    """Run the two-anchor pipeline across multiple dates.

    Layer 1 (entry-anchored): accept ALL PreFilter candidates.
    Wait for ignition (spread_collapse OR vol_exp).
    If ignition fires within timeout -> hold to session end.
    If no ignition -> zombie, exit at timeout.

    Returns DataFrame with one row per candidate.
    """
    results: list[AnchorResult] = []

    for date in dates:
        print(f"\n{'='*60}")
        print(f"Two-Anchor Pipeline: {date}")
        print(f"{'='*60}")

        lab = ExitLab(date)
        lab.load_candidates(top_n=top_n)

        et = ZoneInfo("America/New_York")
        dt_et = datetime.strptime(date, "%Y-%m-%d").replace(
            hour=9, minute=30, tzinfo=et
        )
        session_end_ms = int(dt_et.timestamp() * 1000)
        timeout_ms = ignition_timeout_min * 60 * 1000

        for cand in lab.candidates:
            ticker = cand.symbol
            trades = lab._load_trades(ticker)
            if trades.empty:
                continue

            entry_price = lab.find_entry_price(trades, cand.first_entry_ms)
            if entry_price is None:
                continue

            entry_ms = cand.first_entry_ms
            ignition_deadline_ms = entry_ms + timeout_ms

            # --- Detect ignition ---
            quotes = lab._load_quotes(ticker)
            ignitions = _detect_ignitions(
                trades, entry_ms, session_end_ms, quotes=quotes
            )

            ignited = False
            ignition_method = ""
            ig_ms = 0
            ig_price = 0.0
            ig_delay = 0.0

            for method in ["spread_collapse", "vol_exp"]:
                ig = ignitions.get(method)
                if ig and ig.get("ms") and ig["ms"] <= ignition_deadline_ms:
                    ignited = True
                    ignition_method = method
                    ig_ms = ig["ms"]
                    ig_price = ig["price"]
                    ig_delay = (ig_ms - entry_ms) / 1000.0
                    break

            # --- Compute outcome ---
            fwd = trades[
                (trades["ts_ms"] > entry_ms) & (trades["ts_ms"] <= session_end_ms)
            ]
            if not fwd.empty:
                entry_to_peak = (
                    (float(fwd["price"].max()) - entry_price) / entry_price * 100
                )
            else:
                entry_to_peak = 0.0

            is_momo = entry_to_peak >= 50

            if ignited:
                # Ignition-moment features: first 5 min window after ignition
                ig_window_end = ig_ms + 300_000
                ig_window = trades[
                    (trades["ts_ms"] > ig_ms) & (trades["ts_ms"] <= ig_window_end)
                ]
                if not ig_window.empty:
                    ig_prices = ig_window["price"].values.astype(float)
                    ig_sizes = ig_window["size"].values.astype(float)
                    ig_trade_count_5min = len(ig_window)
                    ig_dollar_vol_5min = float((ig_prices * ig_sizes).sum())
                    ig_price_range_pct = (
                        (float(ig_prices.max()) - float(ig_prices.min()))
                        / ig_price
                        * 100
                    )
                    # BTD: median of (trade_price - ig_price) / ig_price for trades > avg size
                    avg_size = float(ig_sizes.mean())
                    big_trades = ig_prices[ig_sizes > avg_size]
                    if len(big_trades) > 0:
                        ig_btd = float(
                            np.median((big_trades - ig_price) / ig_price * 100)
                        )
                    else:
                        ig_btd = float(
                            np.median((ig_prices - ig_price) / ig_price * 100)
                        )
                    # Max gap
                    ig_ts = ig_window["ts_ms"].values.astype(float)
                    if len(ig_ts) >= 2:
                        ig_max_gap_sec = float(np.max(np.diff(ig_ts))) / 1000.0
                    else:
                        ig_max_gap_sec = (
                            float(300 - ig_delay) if ig_delay < 300 else 0.0
                        )
                else:
                    ig_trade_count_5min = 0
                    ig_dollar_vol_5min = 0.0
                    ig_price_range_pct = 0.0
                    ig_btd = 0.0
                    ig_max_gap_sec = 300.0

                # Hold from ignition to session end
                after_ig = trades[
                    (trades["ts_ms"] > ig_ms) & (trades["ts_ms"] <= session_end_ms)
                ]
                if after_ig.empty:
                    final_pnl = 0.0
                    ig_to_peak = 0.0
                else:
                    after_prices = after_ig["price"].values.astype(float)
                    final_pnl = (float(after_prices[-1]) - ig_price) / ig_price * 100
                    ig_to_peak = (float(after_prices.max()) - ig_price) / ig_price * 100
                exit_reason = f"ignited_{ignition_method}"
                zombie_exit_price = 0.0
                zombie_exit_ms = 0
            else:
                ig_trade_count_5min = 0
                ig_dollar_vol_5min = 0.0
                ig_price_range_pct = 0.0
                ig_btd = 0.0
                ig_max_gap_sec = 0.0
                ig_to_peak = 0.0
                # Zombie: exit at last trade before deadline
                exit_price = _nearest_trade_price(trades, ignition_deadline_ms)
                if exit_price is None or exit_price <= 0:
                    exit_price = entry_price
                zombie_exit_price = exit_price
                zombie_exit_ms = ignition_deadline_ms
                final_pnl = (exit_price - entry_price) / entry_price * 100
                exit_reason = "zombie_timeout"
                ig_delay = 0.0
                ig_price = 0.0

            results.append(
                AnchorResult(
                    date=date,
                    ticker=ticker,
                    entry_ms=entry_ms,
                    entry_price=round(entry_price, 4),
                    gain_at_entry=round(cand.gain_at_entry, 2),
                    ignited=ignited,
                    ignition_method=ignition_method,
                    ignition_ms=ig_ms,
                    ignition_price=round(ig_price, 4) if ig_price else 0.0,
                    ignition_delay_sec=round(ig_delay, 1),
                    zombie_exit_price=round(zombie_exit_price, 4),
                    zombie_exit_ms=zombie_exit_ms,
                    entry_to_peak_pct=round(entry_to_peak, 2),
                    is_momo=is_momo,
                    final_pnl_pct=round(final_pnl, 4),
                    exit_reason=exit_reason,
                    ig_trade_count_5min=ig_trade_count_5min,
                    ig_dollar_vol_5min=round(ig_dollar_vol_5min, 0),
                    ig_price_range_pct=round(ig_price_range_pct, 2),
                    ig_btd=round(ig_btd, 2),
                    ig_max_gap_sec=round(ig_max_gap_sec, 1),
                    ig_to_peak_pct=round(ig_to_peak, 2),
                )
            )

    df = pd.DataFrame([vars(r) for r in results])

    # ── Summary ──
    print(f"\n{'='*60}")
    print("TWO-ANCHOR PIPELINE SUMMARY")
    print(f"{'='*60}")
    print(f"Dates: {dates}")
    print(f"Ignition timeout: {ignition_timeout_min} min")
    print(f"Total candidates: {len(df)}")

    ignited_df = df[df["ignited"]]
    zombie_df = df[~df["ignited"]]
    momo_df = df[df["is_momo"]]

    n_ignited = len(ignited_df)
    n_zombie = len(zombie_df)
    n_momo = len(momo_df)
    momo_ignited = ignited_df["is_momo"].sum()
    momo_zombie = zombie_df["is_momo"].sum()

    print(f"\n── Ignition ──")
    print(f"  Ignited:  {n_ignited}/{len(df)} ({n_ignited/max(len(df),1)*100:.0f}%)")
    print(f"  Zombies:  {n_zombie}/{len(df)} ({n_zombie/max(len(df),1)*100:.0f}%)")
    if n_ignited > 0:
        med_delay = ignited_df["ignition_delay_sec"].median()
        print(f"  Median ignition delay: {med_delay:.0f}s")
        for m in ["spread_collapse", "vol_exp"]:
            n_m = (ignited_df["ignition_method"] == m).sum()
            print(f"  {m}: {n_m}")

    print(f"\n── MOMO Retention ──")
    print(f"  Total MOMO: {n_momo}")
    print(f"  MOMO ignited: {momo_ignited}/{n_momo}" if n_momo > 0 else "  No MOMO")
    print(f"  MOMO zombies:  {momo_zombie}/{n_momo}" if n_momo > 0 else "")
    if n_momo > 0:
        print(f"  MOMO capture rate: {momo_ignited/max(n_momo,1)*100:.0f}%")

    print(f"\n── P&L ──")
    print(f"  All candidates mean PnL:     {df['final_pnl_pct'].mean():+.2f}%")
    print(
        f"  Ignited mean PnL:            {ignited_df['final_pnl_pct'].mean():+.2f}%"
        if n_ignited > 0
        else "  N/A"
    )
    print(
        f"  Zombie mean PnL:             {zombie_df['final_pnl_pct'].mean():+.2f}%"
        if n_zombie > 0
        else "  N/A"
    )
    print(
        f"  MOMO mean PnL:               {momo_df['final_pnl_pct'].mean():+.2f}%"
        if n_momo > 0
        else "  N/A"
    )

    if n_zombie > 0:
        zombie_pnl_sum = zombie_df["final_pnl_pct"].sum()
        print(f"  Zombie total PnL (avoidable?): {zombie_pnl_sum:+.2f}%")

    print(f"\n── MOMO Zombies (lost) ──")
    momo_zombie_df = zombie_df[zombie_df["is_momo"]].sort_values(
        "entry_to_peak_pct", ascending=False
    )
    if momo_zombie_df.empty:
        print("  NONE -- all MOMO ignited within timeout")
    else:
        for _, row in momo_zombie_df.iterrows():
            print(
                f"  {row['date']} {row['ticker']:6s}  peak={row['entry_to_peak_pct']:+.0f}%  "
                f"gain_at_entry={row['gain_at_entry']:+.1f}%"
            )
        print(
            f"  => {len(momo_zombie_df)} MOMO missed. "
            f"Consider extending timeout or adding ignition methods."
        )

    # ── Ignition-moment features: MOMO vs noise ──
    ig_momo = ignited_df[ignited_df["is_momo"]]
    ig_noise = ignited_df[~ignited_df["is_momo"]]

    features = [
        ("ig_trade_count_5min", "Trade count (5min)"),
        ("ig_dollar_vol_5min", "Dollar vol (5min)"),
        ("ig_price_range_pct", "Price range %"),
        ("ig_btd", "Big-trade delta %"),
        ("ig_max_gap_sec", "Max trade gap (s)"),
        ("ignition_delay_sec", "Ignition delay (s)"),
        ("ig_to_peak_pct", "Ig→peak %"),
    ]

    print(f"\n── Ignition-Moment Features: MOMO vs Noise ──")
    if len(ig_momo) == 0 or len(ig_noise) == 0:
        print("  Need both MOMO and noise to compare.")
    else:
        print(
            f"  {'Feature':<25s} {'MOMO (n=' + str(len(ig_momo)) + ')':>20s}  "
            f"{'Noise (n=' + str(len(ig_noise)) + ')':>20s}  {'Diff':>8s}"
        )
        print(f"  {'-'*25} {'-'*20}  {'-'*20}  {'-'*8}")
        for col, label in features:
            if col not in ignited_df.columns:
                continue
            m_val = ig_momo[col].median()
            n_val = ig_noise[col].median()
            m_str = (
                f"{m_val:.1f}"
                if not isinstance(m_val, (int, float)) or not np.isnan(m_val)
                else "N/A"
            )
            if isinstance(m_val, (int, float)) and not np.isnan(m_val):
                m_str = (
                    f"{m_val:,.0f}"
                    if col in ("ig_dollar_vol_5min",)
                    else f"{m_val:.1f}"
                )
            if isinstance(n_val, (int, float)) and not np.isnan(n_val):
                n_str = (
                    f"{n_val:,.0f}"
                    if col in ("ig_dollar_vol_5min",)
                    else f"{n_val:.1f}"
                )
            else:
                n_str = "N/A"
            if (
                isinstance(m_val, (int, float))
                and isinstance(n_val, (int, float))
                and not np.isnan(m_val)
                and not np.isnan(n_val)
                and n_val != 0
            ):
                diff = ((m_val - n_val) / abs(n_val)) * 100
                diff_str = f"{diff:+.0f}%"
            else:
                diff_str = ""
            print(f"  {label:<25s} {m_str:>20s}  {n_str:>20s}  {diff_str:>8s}")

        # Per-date breakdown
        print(f"\n  ── Per-Date MOMO Capture ──")
        for date in sorted(ignited_df["date"].unique()):
            dd = df[df["date"] == date]
            dd_momo = dd[dd["is_momo"]]
            dd_ig = dd[dd["ignited"]]
            dd_ig_momo = dd_ig[dd_ig["is_momo"]]
            print(
                f"  {date}: {len(dd_ig_momo)}/{len(dd_momo)} MOMO ignited"
                f"  ({len(dd_ig)}/{len(dd)} ignited)"
            )

    return df


if __name__ == "__main__":
    run_two_anchor(
        ["2026-03-02", "2026-03-04", "2026-03-11", "2026-03-12", "2026-03-13"],
        top_n=20,
        ignition_timeout_min=30,
    )
