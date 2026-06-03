"""Multi-date exit strategy test — fast single-pass per date.

Usage:
    poetry run python scripts/multi_date_exit_test.py
    poetry run python scripts/multi_date_exit_test.py --dates 2026-03-04,2026-03-13
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python" / "src"))

from jerry_trader.services.backtest.exit_strategy import (
    ExitStrategyConfig,
    apply_exit_strategy,
)
from jerry_trader.services.backtest.pipeline import BacktestPipeline
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("multi_exit_test", log_to_file=True)

# Top exit variants to test
EXITS = [
    (
        "tp15_sl20",
        ExitStrategyConfig(
            name="tp15_sl20", partial_exits=[(15.0, 1.0)], stop_loss_pct=20.0
        ),
    ),
    (
        "tp10_sl15",
        ExitStrategyConfig(
            name="tp10_sl15", partial_exits=[(10.0, 1.0)], stop_loss_pct=15.0
        ),
    ),
    (
        "partial3_sl15",
        ExitStrategyConfig(
            name="partial3_sl15",
            partial_exits=[(3.0, 0.30), (10.0, 0.70)],
            stop_loss_pct=15.0,
        ),
    ),
    (
        "trail10_sl15",
        ExitStrategyConfig(
            name="trail10_sl15",
            partial_exits=[],
            stop_loss_pct=15.0,
            trailing_stop_trigger_pct=10.0,
            trailing_stop_pct=5.0,
        ),
    ),
    (
        "trail5_sl10",
        ExitStrategyConfig(
            name="trail5_sl10",
            partial_exits=[],
            stop_loss_pct=10.0,
            trailing_stop_trigger_pct=5.0,
            trailing_stop_pct=3.0,
        ),
    ),
    (
        "no_exit_hold",
        ExitStrategyConfig(name="no_exit_hold", partial_exits=[], stop_loss_pct=100.0),
    ),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dates",
        type=str,
        default="2026-03-04,2026-03-06,2026-03-10,2026-03-12,2026-03-13",
    )
    args = parser.parse_args()
    dates = [d.strip() for d in args.dates.split(",")]

    # Aggregate results: exit_name -> [returns]
    agg: dict[str, list[float]] = defaultdict(list)
    date_stats: dict[str, dict] = {}

    print(f"Testing {len(EXITS)} exit strategies across {len(dates)} dates\n")

    for date in dates:
        date_stats[date] = {}
        print(f"  {date}: ", end="", flush=True)

        for exit_name, exit_cfg in EXITS:
            t0 = time.time()
            pipeline = BacktestPipeline(mode="streaming", exit_strategy=exit_cfg)
            result = pipeline.run(date=date)

            returns = [s.return_pct for s in result.signals]
            agg[exit_name].extend(returns)

            n = result.total_signals
            wr = result.win_rate
            ev = result.avg_return
            date_stats[date][exit_name] = (n, ev)
            elapsed = time.time() - t0
            print(f"{exit_name}:{n}sig/{wr:.0f}%/{ev:+.1f}% ", end="", flush=True)

        print()

    # Aggregate summary
    print(f"\n{'='*80}")
    print(f"  AGGREGATE RESULTS ({len(dates)} dates)")
    print(f"{'='*80}")
    print(f"  {'Exit':<20} {'N':>5} {'Win%':>7} {'EV%':>8} {'Median%':>8}")
    print(f"  {'-'*20} {'-'*5} {'-'*7} {'-'*8} {'-'*8}")

    best_name, best_ev = "", float("-inf")
    baseline_ev = 0

    for exit_name, _ in EXITS:
        returns = agg[exit_name]
        n = len(returns)
        if n == 0:
            continue
        wins = sum(1 for r in returns if r > 0)
        wr = wins / n * 100
        ev = statistics.mean(returns)
        med = statistics.median(returns)

        marker = ""
        if ev > best_ev:
            best_ev = ev
            best_name = exit_name
            marker = " ★"

        if exit_name == "tp15_sl20":
            baseline_ev = ev

        print(f"  {exit_name+marker:<20} {n:>5} {wr:>6.1f}% {ev:>+7.2f}% {med:>+7.2f}%")

    print(f"\n  BEST: {best_name} ({best_ev:+.2f}% EV)")
    print(f"  vs tp15_sl20: {best_ev - baseline_ev:+.2f}% EV delta")

    # Per-date breakdown
    print(f"\n  Per-date EV breakdown:")
    header = f"  {'Date':<12}"
    for exit_name, _ in EXITS:
        header += f" {exit_name:>15}"
    print(header)
    for date in dates:
        row = f"  {date:<12}"
        for exit_name, _ in EXITS:
            stats = date_stats[date].get(exit_name, (0, 0.0))
            row += f" {stats[0]:>3}sig{stats[1]:>+8.1f}%"
        print(row)


if __name__ == "__main__":
    main()
