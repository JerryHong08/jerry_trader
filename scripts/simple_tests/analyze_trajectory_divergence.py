"""Analyze when extreme winners and losers diverge after entry.

Direct ClickHouse query for trade trajectories.
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "python" / "src"))

from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("trajectory", log_to_file=True)


def load_trades(
    ch, tickers: list[str], date: str
) -> dict[str, list[tuple[int, float]]]:
    """Load all trades for given tickers on a date from ClickHouse."""
    if not tickers:
        return {}

    ticker_list = "', '".join(tickers)
    query = f"""
        SELECT ticker, sip_timestamp, price
        FROM trades
        WHERE ticker IN ('{ticker_list}')
          AND date = '{date}'
        ORDER BY ticker, sip_timestamp
    """
    result = ch.query(query)
    trades: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for row in result.result_rows:
        # ClickHouse sip_timestamp is nanoseconds; convert to milliseconds
        trades[row[0]].append((int(int(row[1]) / 1_000_000), float(row[2])))
    return dict(trades)


def compute_trajectory(
    trades: list[tuple[int, float]],
    entry_ms: int,
    entry_price: float,
    window_minutes: int = 30,
    sample_interval_s: int = 10,
) -> dict[int, float]:
    """Compute normalized price trajectory after entry."""
    traj = {}
    window_ms = window_minutes * 60 * 1000

    # Get last price before each sample point
    sorted_trades = sorted(trades, key=lambda x: x[0])
    idx = 0
    for sec in range(0, window_minutes * 60, sample_interval_s):
        target_ms = entry_ms + sec * 1000
        # Advance to last trade <= target_ms
        while idx < len(sorted_trades) and sorted_trades[idx][0] <= target_ms:
            idx += 1
        if idx > 0:
            _, price = sorted_trades[idx - 1]
            if price > 0:
                pct = (price - entry_price) / entry_price * 100
                traj[sec] = pct

    return traj


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dates",
        type=str,
        default="2026-03-04,2026-03-06,2026-03-09,2026-03-10,2026-03-13",
    )
    parser.add_argument("--window", type=int, default=30)
    parser.add_argument("--max-tickers", type=int, default=5)
    args = parser.parse_args()

    dates = [d.strip() for d in args.dates.split(",")]

    explore = pd.read_parquet("data/dense_signals/explore.parquet")
    validate = pd.read_parquet("data/dense_signals/validate.parquet")
    all_signals = pd.concat([explore, validate], ignore_index=True)

    ch = get_clickhouse_client()

    all_trajectories: dict[str, list[dict]] = defaultdict(list)

    for date in dates:
        print(f"\n{'='*60}")
        print(f"Date: {date}")
        print(f"{'='*60}")

        day = all_signals[all_signals["date"] == date]
        if len(day) == 0:
            continue

        # Find tickers per category
        extreme = day[day["max_return_pct"] > 50].nlargest(
            args.max_tickers, "max_return_pct"
        )
        losers = day[day["max_return_pct"] < 0].nsmallest(
            args.max_tickers, "max_return_pct"
        )
        # Middle: random sample of non-extreme, non-loser tickers
        middle_pool = day[(day["max_return_pct"] >= 0) & (day["max_return_pct"] <= 50)]
        middle = middle_pool.sample(
            min(args.max_tickers, len(middle_pool)), random_state=42
        )

        tickers_info = {}
        for label, df_sub in [
            ("extreme_winner", extreme),
            ("loser", losers),
            ("middle", middle),
        ]:
            for _, row in df_sub.iterrows():
                key = row["ticker"]
                if key not in tickers_info:
                    tickers_info[key] = (
                        int(row["entry_time_ms"]),
                        float(row["entry_price"]),
                        float(row["max_return_pct"]),
                        label,
                    )

        if not tickers_info:
            continue

        # Load trade data
        symbols = list(tickers_info.keys())
        print(f"  Loading trades for {len(symbols)} tickers...")
        trades_map = load_trades(ch, symbols, date)

        for symbol, (entry_ms, entry_price, max_ret, label) in tickers_info.items():
            if symbol not in trades_map:
                continue
            traj = compute_trajectory(
                trades_map[symbol],
                entry_ms,
                entry_price,
                window_minutes=args.window,
            )
            if len(traj) >= 10:
                all_trajectories[label].append(
                    {
                        "symbol": symbol,
                        "entry_price": entry_price,
                        "max_return": max_ret,
                        "trajectory": traj,
                        "date": date,
                    }
                )

        for label in ["extreme_winner", "loser", "middle"]:
            n = sum(1 for t in tickers_info.values() if t[3] == label)
            has_data = sum(1 for t in all_trajectories[label] if t["date"] == date)
            print(f"  {label}: {n} requested, {has_data} loaded")

    # ── Aggregate ──
    print(f"\n{'='*70}")
    print(
        f"AGGREGATE TRAJECTORY ({len(dates)} dates, {args.max_tickers} tickers/category/date)"
    )
    print(f"{'='*70}")

    for category in ["extreme_winner", "loser", "middle"]:
        trajs = all_trajectories[category]
        if not trajs:
            print(f"\n  {category}: no data")
            continue

        time_buckets: dict[int, list[float]] = defaultdict(list)
        for t in trajs:
            for sec, pct in t["trajectory"].items():
                time_buckets[sec].append(pct)

        print(f"\n  {category} (n={len(trajs)}):")
        print(
            f"  {'Sec':>5}  {'Min':>7}  {'P10':>7}  {'P25':>7}  {'P50':>7}  {'P75':>7}  {'P90':>7}  {'Max':>7}"
        )
        print(
            f"  {'-'*5}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*7}"
        )

        for sec in sorted(time_buckets.keys()):
            vals = time_buckets[sec]
            if len(vals) < 3:
                continue
            p10, p25, p50, p75, p90 = np.percentile(vals, [10, 25, 50, 75, 90])
            print(
                f"  {sec:>4}s  {min(vals):>+6.1f}%  {p10:>+6.1f}%  {p25:>+6.1f}%  "
                f"{p50:>+6.1f}%  {p75:>+6.1f}%  {p90:>+6.1f}%  {max(vals):>+6.1f}%"
            )

    # ── Divergence timeline ──
    print(f"\n{'='*70}")
    print("DIVERGENCE TIMELINE")
    print(f"{'='*70}")

    extreme_trajs = all_trajectories.get("extreme_winner", [])
    loser_trajs = all_trajectories.get("loser", [])

    if extreme_trajs and loser_trajs:
        print(
            f"\n  {'Sec':>5}  {'W P50':>8}  {'L P50':>8}  {'Δ':>7}  {'W%>0':>7}  {'L%>0':>7}"
        )
        print(f"  {'-'*5}  {'-'*8}  {'-'*8}  {'-'*7}  {'-'*7}  {'-'*7}")

        for sec in [0, 10, 30, 60, 120, 300, 600, 900, 1200, 1800]:
            e_vals, l_vals = [], []
            for t in extreme_trajs:
                if sec in t["trajectory"]:
                    e_vals.append(t["trajectory"][sec])
            for t in loser_trajs:
                if sec in t["trajectory"]:
                    l_vals.append(t["trajectory"][sec])

            if len(e_vals) < 3 or len(l_vals) < 3:
                continue

            e_p50 = np.median(e_vals)
            l_p50 = np.median(l_vals)
            delta = e_p50 - l_p50
            e_above = sum(1 for v in e_vals if v > 0) / len(e_vals) * 100
            l_above = sum(1 for v in l_vals if v > 0) / len(l_vals) * 100

            marker = " ◄◄" if delta > 10 else " ◄" if delta > 5 else ""
            print(
                f"  {sec:>4}s  {e_p50:>+7.1f}%  {l_p50:>+7.1f}%  {delta:>+6.1f}%  "
                f"{e_above:>6.0f}%  {l_above:>6.0f}%{marker}"
            )

    # ── Individual ──
    print(f"\n{'='*70}")
    print("INDIVIDUAL TRAJECTORIES (first 10 min key points)")
    print(f"{'='*70}")

    for category in ["extreme_winner", "loser"]:
        print(f"\n  --- {category} ---")
        for t in all_trajectories[category]:
            parts = []
            for sec in [0, 30, 60, 120, 300, 600]:
                if sec in t["trajectory"]:
                    parts.append(f"{sec}s:{t['trajectory'][sec]:+.1f}%")
            print(
                f"  {t['date']} {t['symbol']:<6} max={t['max_return']:>7.1f}% | {' '.join(parts)}"
            )


if __name__ == "__main__":
    main()
