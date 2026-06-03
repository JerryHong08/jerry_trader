#!/usr/bin/env python3
"""Big Winner Coverage Analysis.

Goal: Understand what big winners exist each day and whether our strategy captures them.

Analysis:
1. Daily winner distribution by gain tiers (50%+, 20-50%, 10-20%)
2. Strategy coverage rate - what did we capture vs miss
3. Diagnosis of missed winners (entry filter too strict? wrong timing?)

Usage:
    poetry run python -m jerry_trader.scripts.big_winner_coverage_analysis --date 2026-03-13
    poetry run python -m jerry_trader.scripts.big_winner_coverage_analysis --date-range 2026-03-11 2026-03-13
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytz

from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.event.pipeline import BacktestPipeline
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("big_winner_coverage", log_to_file=True)


# Winner tier definitions
WINNER_TIERS = {
    "super": {"name": "Super Winner", "min_gain": 100, "color": "\033[95m"},  # Magenta
    "big": {
        "name": "Big Winner",
        "min_gain": 50,
        "max_gain": 100,
        "color": "\033[91m",
    },  # Red
    "medium": {
        "name": "Medium Winner",
        "min_gain": 20,
        "max_gain": 50,
        "color": "\033[93m",
    },  # Yellow
    "small": {
        "name": "Small Winner",
        "min_gain": 10,
        "max_gain": 20,
        "color": "\033[92m",
    },  # Green
    "none": {"name": "No Gain", "max_gain": 10, "color": "\033[90m"},  # Gray
}


@dataclass
class TickerOutcome:
    """Final outcome for a ticker in pre-market session."""

    symbol: str
    date: str

    # Price trajectory
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    max_gain_pct: float

    # Volume
    total_volume: int
    total_trades: int

    # Timing
    first_trade_ms: int
    last_trade_ms: int
    time_to_high_ms: int
    duration_minutes: float

    # Gap at entry (from candidate)
    gap_pct: float = 0.0

    # Tier classification
    tier: str = "none"

    # Strategy capture
    captured_by_strategy: bool = False
    entry_time_ms: int | None = None
    entry_price: float | None = None
    entry_gain_pct: float | None = None

    # If captured, what was the result?
    strategy_return_pct: float | None = None

    # Diagnosis if missed
    miss_reason: str | None = (
        None  # "gap_filter", "volume_filter", "timing", "not_in_candidates"
    )


@dataclass
class DailyCoverageReport:
    """Coverage analysis for a single date."""

    date: str
    total_candidates: int = 0

    # Winner distribution
    winners_by_tier: dict[str, list[TickerOutcome]] = field(default_factory=dict)

    # Coverage stats
    total_winners: int = 0
    captured_winners: int = 0
    coverage_rate: float = 0.0

    # Strategy signals
    total_signals: int = 0
    signals_that_were_winners: int = 0

    # Diagnosis
    missed_by_filter: dict[str, int] = field(default_factory=dict)


def classify_tier(gain_pct: float) -> str:
    """Classify ticker into winner tier."""
    for tier_key, tier_def in WINNER_TIERS.items():
        min_gain = tier_def.get("min_gain", 0)
        max_gain = tier_def.get("max_gain", float("inf"))
        if min_gain <= gain_pct < max_gain:
            return tier_key
    return "none"


def analyze_ticker_outcome(
    symbol: str,
    date: str,
    ticker_data: Any,
    session_start_ms: int,
    session_end_ms: int,
    gap_pct: float = 0.0,
) -> TickerOutcome | None:
    """Analyze final outcome for a ticker."""
    if not ticker_data.trades:
        return None

    # Filter to pre-market trades
    premarket_trades = [
        (ts, price, vol)
        for ts, price, vol in ticker_data.trades
        if session_start_ms <= ts < session_end_ms
    ]

    if len(premarket_trades) < 10:
        return None

    timestamps = [t[0] for t in premarket_trades]
    prices = [t[1] for t in premarket_trades]
    volumes = [t[2] for t in premarket_trades]

    open_price = prices[0]
    high_price = max(prices)
    low_price = min(prices)
    close_price = prices[-1]

    max_gain_pct = (high_price - open_price) / open_price * 100
    total_volume = sum(volumes)
    total_trades = len(premarket_trades)

    first_trade_ms = timestamps[0]
    last_trade_ms = timestamps[-1]
    duration_minutes = (last_trade_ms - first_trade_ms) / 60000

    high_idx = prices.index(high_price)
    time_to_high_ms = timestamps[high_idx] - first_trade_ms

    tier = classify_tier(max_gain_pct)

    return TickerOutcome(
        symbol=symbol,
        date=date,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        max_gain_pct=max_gain_pct,
        total_volume=total_volume,
        total_trades=total_trades,
        first_trade_ms=first_trade_ms,
        last_trade_ms=last_trade_ms,
        time_to_high_ms=time_to_high_ms,
        duration_minutes=duration_minutes,
        gap_pct=gap_pct,
        tier=tier,
    )


def run_strategy_and_match(
    date: str,
    outcomes: list[TickerOutcome],
) -> tuple[list[dict], dict[str, TickerOutcome]]:
    """Run backtest strategy and match signals to outcomes.

    Returns:
        (signals, outcomes_by_symbol)
    """
    # Run backtest with streaming mode
    pipeline = BacktestPipeline(
        experiment_id=f"coverage_{date}",
        mode="streaming",
    )
    result = pipeline.run(date=date, events=["gap_up_watch", "momentum_entry"])

    # Build outcome lookup
    outcomes_by_symbol = {o.symbol: o for o in outcomes}

    # Match signals to outcomes
    for signal in result.signals:
        symbol = signal.ticker
        if symbol in outcomes_by_symbol:
            outcome = outcomes_by_symbol[symbol]
            outcome.captured_by_strategy = True
            outcome.entry_time_ms = int(signal.entry_time.timestamp() * 1000)
            outcome.entry_price = signal.entry_price
            outcome.entry_gain_pct = (
                (signal.entry_price - outcome.open_price) / outcome.open_price * 100
            )
            outcome.strategy_return_pct = signal.return_pct

    return result.signals, outcomes_by_symbol


def diagnose_missed_winner(outcome: TickerOutcome, candidates: list[Any]) -> str:
    """Diagnose why a winner was missed."""
    # Check if it was in candidates at all
    was_candidate = any(c.symbol == outcome.symbol for c in candidates)
    if not was_candidate:
        return "not_in_candidates"

    # Check gap filter (entry_gap_pct > 4% required)
    if outcome.gap_pct < 4.0:
        return "gap_filter"

    # Check volume (need enough activity)
    if outcome.total_trades < 100:
        return "low_volume"

    # Check timing (did we have enough time to capture?)
    if outcome.time_to_high_ms < 5 * 60 * 1000:  # Less than 5 min to peak
        return "too_fast"

    # Check if captured but not a winner (wrong timing)
    if outcome.captured_by_strategy:
        return "captured_not_winner"

    # Default: timing issue (conditions never met)
    return "timing"


def analyze_missed_winners_details(
    outcomes: list[TickerOutcome],
    candidates: list[Any],
    ticker_data_map: dict,
    session_start_ms: int,
) -> dict:
    """Analyze detailed factors for missed winners to understand why ENTRY didn't trigger."""
    from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
    from jerry_trader.services.backtest.config import BacktestConfig

    missed_timing = [
        o
        for o in outcomes
        if o.tier in ["super", "big", "medium"] and o.miss_reason == "timing"
    ]

    if not missed_timing:
        return {}

    details = {}
    config = BacktestConfig()

    for outcome in missed_timing[:5]:  # Analyze top 5 missed winners
        symbol = outcome.symbol
        if symbol not in ticker_data_map:
            continue

        ticker_data = ticker_data_map[symbol]
        if not ticker_data.trades:
            continue

        # Compute factors using batch engine
        adapter = FactorEngineBatchAdapter()
        factor_ts = adapter.compute(ticker_data, config)

        if not factor_ts:
            continue

        # Find factors at different time points
        # Get the time when this ticker first entered top 20
        candidate = next((c for c in candidates if c.symbol == symbol), None)
        entry_time_ms = candidate.first_entry_ms if candidate else session_start_ms

        # Find factor values at entry time + 5min, 10min, 15min
        time_points = {}
        for delta_min in [5, 10, 15, 20]:
            target_ms = entry_time_ms + delta_min * 60 * 1000
            closest_ts = min(
                (ts for ts in factor_ts.keys() if ts >= target_ms),
                default=None,
                key=lambda ts: abs(ts - target_ms),
            )
            if closest_ts:
                time_points[f"{delta_min}min"] = factor_ts[closest_ts]

        details[symbol] = {
            "max_gain": outcome.max_gain_pct,
            "gap": outcome.gap_pct,
            "total_trades": outcome.total_trades,
            "time_to_high_min": outcome.time_to_high_ms / 60000,
            "factors_at_times": time_points,
        }

    return details


def analyze_date(date: str) -> DailyCoverageReport:
    """Analyze big winner coverage for a single date."""
    logger.info(f"Analyzing {date}...")

    ch = get_clickhouse_client()

    # Get candidates
    pre_filter = PreFilter(ch_client=ch)
    candidates = pre_filter.find(date, PreFilterConfig())

    if not candidates:
        logger.warning(f"No candidates for {date}")
        return DailyCoverageReport(date=date)

    # Load trade data
    config = BacktestConfig(date=date)
    data_loader = DataLoader(ch_client=ch)
    ticker_data_map = data_loader.load(candidates, config)

    # Session window
    ny_tz = pytz.timezone("America/New_York")
    session_start_ms = int(
        ny_tz.localize(datetime.strptime(f"{date} 04:00", "%Y-%m-%d %H:%M")).timestamp()
        * 1000
    )
    session_end_ms = int(
        ny_tz.localize(datetime.strptime(f"{date} 09:30", "%Y-%m-%d %H:%M")).timestamp()
        * 1000
    )

    # Analyze outcomes
    outcomes = []
    for symbol, ticker_data in ticker_data_map.items():
        candidate = next((c for c in candidates if c.symbol == symbol), None)
        gap_pct = candidate.gain_at_entry if candidate else 0

        outcome = analyze_ticker_outcome(
            symbol, date, ticker_data, session_start_ms, session_end_ms, gap_pct
        )
        if outcome:
            outcomes.append(outcome)

    logger.info(f"Analyzed {len(outcomes)} tickers")

    # Run strategy and match
    signals, outcomes_by_symbol = run_strategy_and_match(date, outcomes)

    # Diagnose missed winners
    for outcome in outcomes:
        if (
            outcome.tier in ["super", "big", "medium"]
            and not outcome.captured_by_strategy
        ):
            outcome.miss_reason = diagnose_missed_winner(outcome, candidates)

    # Build report
    report = DailyCoverageReport(
        date=date,
        total_candidates=len(outcomes),
    )

    # Group by tier
    for outcome in outcomes:
        if outcome.tier not in report.winners_by_tier:
            report.winners_by_tier[outcome.tier] = []
        report.winners_by_tier[outcome.tier].append(outcome)

    # Count winners (medium+)
    report.total_winners = sum(
        len(report.winners_by_tier.get(tier, [])) for tier in ["super", "big", "medium"]
    )

    # Count captured winners
    report.captured_winners = sum(
        1
        for o in outcomes
        if o.tier in ["super", "big", "medium"] and o.captured_by_strategy
    )

    report.coverage_rate = (
        report.captured_winners / report.total_winners * 100
        if report.total_winners > 0
        else 0
    )

    # Strategy stats
    report.total_signals = len(signals)
    report.signals_that_were_winners = sum(
        1
        for o in outcomes
        if o.captured_by_strategy and o.tier in ["super", "big", "medium"]
    )

    # Miss diagnosis
    for outcome in outcomes:
        if outcome.tier in ["super", "big", "medium"] and outcome.miss_reason:
            reason = outcome.miss_reason
            report.missed_by_filter[reason] = report.missed_by_filter.get(reason, 0) + 1

    return report


def format_report(report: DailyCoverageReport) -> str:
    """Format coverage report."""
    lines = [
        f"\n{'=' * 80}",
        f"Big Winner Coverage Analysis: {report.date}",
        f"{'=' * 80}\n",
        f"Total Candidates: {report.total_candidates}",
        f"Total Signals: {report.total_signals}",
        "",
        "## Winner Distribution by Tier",
        "",
    ]

    for tier_key in ["super", "big", "medium", "small"]:
        tier_def = WINNER_TIERS[tier_key]
        winners = report.winners_by_tier.get(tier_key, [])
        color = tier_def["color"]
        reset = "\033[0m"

        if winners:
            lines.append(
                f"{color}{tier_def['name']} ({tier_def.get('min_gain', 0)}%+): {len(winners)}{reset}"
            )
            for w in sorted(winners, key=lambda x: x.max_gain_pct, reverse=True):
                captured = (
                    "✓ CAPTURED"
                    if w.captured_by_strategy
                    else f"✗ MISSED ({w.miss_reason})"
                )
                lines.append(
                    f"  - {w.symbol}: {w.max_gain_pct:.1f}% gain, {w.gap_pct:.1f}% gap, "
                    f"{w.total_trades} trades → {captured}"
                )

    lines.extend(
        [
            "",
            "## Coverage Summary",
            "",
            f"Total Winners (Medium+): {report.total_winners}",
            f"Captured by Strategy: {report.captured_winners}",
            f"Coverage Rate: {report.coverage_rate:.1f}%",
            "",
            "## Miss Diagnosis",
            "",
        ]
    )

    if report.missed_by_filter:
        for reason, count in sorted(
            report.missed_by_filter.items(), key=lambda x: -x[1]
        ):
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("No missed winners!")

    lines.extend(
        [
            "",
            "## Strategy Performance on Winners",
            "",
            f"Signals that were winners: {report.signals_that_were_winners}/{report.total_signals}",
        ]
    )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Big Winner Coverage Analysis")
    parser.add_argument("--date", help="Single date to analyze (YYYY-MM-DD)")
    parser.add_argument("--date-range", nargs=2, help="Date range (start end)")
    parser.add_argument("--output", help="Output file for report")

    args = parser.parse_args()

    dates = []
    if args.date:
        dates = [args.date]
    elif args.date_range:
        start = datetime.strptime(args.date_range[0], "%Y-%m-%d")
        end = datetime.strptime(args.date_range[1], "%Y-%m-%d")
        # For now, just use the two dates
        dates = [args.date_range[0], args.date_range[1]]
    else:
        # Default: analyze recent dates
        dates = ["2026-03-11", "2026-03-12", "2026-03-13"]

    all_reports = []
    for date in dates:
        report = analyze_date(date)
        all_reports.append(report)
        print(format_report(report))

    # Aggregate summary
    if len(all_reports) > 1:
        print("\n" + "=" * 80)
        print("AGGREGATE SUMMARY")
        print("=" * 80 + "\n")

        total_winners = sum(r.total_winners for r in all_reports)
        total_captured = sum(r.captured_winners for r in all_reports)
        total_signals = sum(r.total_signals for r in all_reports)

        print(f"Total Winners (Medium+): {total_winners}")
        print(f"Total Captured: {total_captured}")
        print(
            f"Overall Coverage: {total_captured / total_winners * 100:.1f}%"
            if total_winners > 0
            else "N/A"
        )
        print(f"Total Signals: {total_signals}")

        # Aggregate miss diagnosis
        all_misses = {}
        for r in all_reports:
            for reason, count in r.missed_by_filter.items():
                all_misses[reason] = all_misses.get(reason, 0) + count

        if all_misses:
            print("\nMiss Diagnosis (aggregate):")
            for reason, count in sorted(all_misses.items(), key=lambda x: -x[1]):
                print(f"  - {reason}: {count}")

    # Save to file if requested
    if args.output:
        output_path = Path(args.output)
        output_path.write_text("\n".join(format_report(r) for r in all_reports))
        logger.info(f"Report saved to {output_path}")


if __name__ == "__main__":
    main()
