"""Big Winner Trajectory Analysis - Tier-separated trajectory pattern analysis.

Analyzes price and volume trajectories for each tier to identify:
1. When Super Winners start showing momentum
2. Volume trajectory patterns by tier
3. Medium vs Big Winner divergence points

Usage:
    poetry run python -m jerry_trader.scripts.big_winner_trajectory_analysis
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytz

from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("big_winner_trajectory", log_to_file=True)

# Tier definitions (same as stratified analysis)
TIER_DEFINITIONS = {
    1: {"name": "Super Winner", "min_gain": 100, "min_volume": 500_000},
    2: {"name": "Big Winner", "min_gain": 60, "max_gain": 100, "min_volume": 1_000_000},
    3: {
        "name": "Medium Winner",
        "min_gain": 40,
        "max_gain": 60,
        "min_volume": 1_000_000,
    },
    4: {"name": "Small Winner", "min_gain": 20, "max_gain": 40, "min_volume": 500_000},
    5: {"name": "Failed", "max_gain": 20},
}


@dataclass
class TrajectoryPoint:
    """A single point in a trajectory."""

    minute: float
    price: float
    volume: int
    cumulative_volume: int
    gain_pct: float


@dataclass
class TickerTrajectory:
    """Full trajectory for a ticker."""

    symbol: str
    date: str
    tier: int
    tier_name: str
    final_max_gain: float
    final_volume: int

    # Trajectory data
    price_trajectory: list[TrajectoryPoint]
    volume_trajectory: list[TrajectoryPoint]

    # Key metrics
    time_to_10pct: float = 0.0
    time_to_25pct: float = 0.0
    time_to_50pct: float = 0.0
    time_to_peak: float = 0.0
    volume_peak_time: float = 0.0


def classify_tier(gain: float, volume: int) -> tuple[int, str]:
    """Classify ticker into tier."""
    for tier, definition in TIER_DEFINITIONS.items():
        min_gain = definition.get("min_gain", 0)
        max_gain = definition.get("max_gain", float("inf"))
        min_volume = definition.get("min_volume", 0)

        if gain >= min_gain and gain < max_gain and volume >= min_volume:
            return tier, definition["name"]

    return 5, "Failed"


def extract_trajectory(
    symbol: str,
    date: str,
    ticker_data: Any,
    session_start_ms: int,
    session_end_ms: int,
) -> TickerTrajectory | None:
    """Extract full trajectory for a ticker."""
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

    # Basic stats
    open_price = prices[0]
    high_price = max(prices)
    final_max_gain = (high_price - open_price) / open_price * 100
    final_volume = sum(volumes)

    # Classify tier
    tier, tier_name = classify_tier(final_max_gain, final_volume)

    # Only analyze tiers 1-3 (Super, Big, Medium Winners)
    if tier > 3:
        return None

    first_trade_ms = timestamps[0]

    # Build trajectory at 5-minute intervals
    price_trajectory = []
    volume_trajectory = []

    cumulative_volume = 0
    interval_ms = 5 * 60 * 1000  # 5 minutes

    for interval in range(0, 70):  # 0 to 345 minutes (5.75 hours)
        cutoff_ms = first_trade_ms + interval * interval_ms

        # Get all trades up to this point
        window_trades = [(ts, p, v) for ts, p, v in premarket_trades if ts <= cutoff_ms]

        if not window_trades:
            continue

        window_prices = [p for _, p, _ in window_trades]
        window_volumes = [v for _, _, v in window_trades]

        current_price = window_prices[-1]
        current_volume = sum(window_volumes)
        cumulative_volume = current_volume

        high_so_far = max(window_prices)
        gain_pct = (high_so_far - open_price) / open_price * 100

        minute = interval * 5

        point = TrajectoryPoint(
            minute=minute,
            price=current_price,
            volume=current_volume,
            cumulative_volume=cumulative_volume,
            gain_pct=gain_pct,
        )

        price_trajectory.append(point)
        volume_trajectory.append(point)

    # Calculate key timing metrics
    def find_time_to_threshold(threshold_pct: float) -> float:
        for point in price_trajectory:
            if point.gain_pct >= threshold_pct:
                return point.minute
        return 0.0

    time_to_10pct = find_time_to_threshold(10)
    time_to_25pct = find_time_to_threshold(25)
    time_to_50pct = find_time_to_threshold(50)

    # Time to peak
    peak_idx = prices.index(high_price)
    time_to_peak = (timestamps[peak_idx] - first_trade_ms) / 60000

    # Volume peak time
    # Find when cumulative volume growth rate is highest
    if len(volume_trajectory) > 1:
        max_growth_rate = 0
        volume_peak_time = 0
        for i in range(1, len(volume_trajectory)):
            growth_rate = (
                volume_trajectory[i].cumulative_volume
                - volume_trajectory[i - 1].cumulative_volume
            )
            if growth_rate > max_growth_rate:
                max_growth_rate = growth_rate
                volume_peak_time = volume_trajectory[i].minute
    else:
        volume_peak_time = 0

    return TickerTrajectory(
        symbol=symbol,
        date=date,
        tier=tier,
        tier_name=tier_name,
        final_max_gain=final_max_gain,
        final_volume=final_volume,
        price_trajectory=price_trajectory,
        volume_trajectory=volume_trajectory,
        time_to_10pct=time_to_10pct,
        time_to_25pct=time_to_25pct,
        time_to_50pct=time_to_50pct,
        time_to_peak=time_to_peak,
        volume_peak_time=volume_peak_time,
    )


def analyze_date(date: str) -> list[TickerTrajectory]:
    """Analyze all tickers for a single date."""
    ch = get_clickhouse_client()

    pre_filter = PreFilter(ch_client=ch)
    candidates = pre_filter.find(date, PreFilterConfig())

    if not candidates:
        return []

    config = BacktestConfig(date=date)
    data_loader = DataLoader(ch_client=ch)
    ticker_data_map = data_loader.load(candidates, config)

    ny_tz = pytz.timezone("America/New_York")
    session_start_ms = int(
        ny_tz.localize(datetime.strptime(f"{date} 04:00", "%Y-%m-%d %H:%M")).timestamp()
        * 1000
    )
    session_end_ms = int(
        ny_tz.localize(datetime.strptime(f"{date} 09:30", "%Y-%m-%d %H:%M")).timestamp()
        * 1000
    )

    trajectories = []
    for symbol, ticker_data in ticker_data_map.items():
        traj = extract_trajectory(
            symbol, date, ticker_data, session_start_ms, session_end_ms
        )
        if traj:
            trajectories.append(traj)

    return trajectories


def compute_tier_trajectory_stats(trajectories: list[TickerTrajectory]) -> dict:
    """Compute trajectory statistics for each tier."""
    tier_stats = {}

    for tier in [1, 2, 3]:
        tier_trajs = [t for t in trajectories if t.tier == tier]

        if not tier_trajs:
            continue

        # Time metrics
        times_to_10pct = [t.time_to_10pct for t in tier_trajs if t.time_to_10pct > 0]
        times_to_25pct = [t.time_to_25pct for t in tier_trajs if t.time_to_25pct > 0]
        times_to_50pct = [t.time_to_50pct for t in tier_trajs if t.time_to_50pct > 0]
        times_to_peak = [t.time_to_peak for t in tier_trajs]
        volume_peak_times = [t.volume_peak_time for t in tier_trajs]

        tier_stats[tier] = {
            "name": TIER_DEFINITIONS[tier]["name"],
            "count": len(tier_trajs),
            "time_to_10pct": {
                "mean": np.mean(times_to_10pct) if times_to_10pct else 0,
                "median": np.median(times_to_10pct) if times_to_10pct else 0,
                "count": len(times_to_10pct),
            },
            "time_to_25pct": {
                "mean": np.mean(times_to_25pct) if times_to_25pct else 0,
                "median": np.median(times_to_25pct) if times_to_25pct else 0,
                "count": len(times_to_25pct),
            },
            "time_to_50pct": {
                "mean": np.mean(times_to_50pct) if times_to_50pct else 0,
                "median": np.median(times_to_50pct) if times_to_50pct else 0,
                "count": len(times_to_50pct),
            },
            "time_to_peak": {
                "mean": np.mean(times_to_peak) if times_to_peak else 0,
                "median": np.median(times_to_peak) if times_to_peak else 0,
            },
            "volume_peak_time": {
                "mean": np.mean(volume_peak_times) if volume_peak_times else 0,
                "median": np.median(volume_peak_times) if volume_peak_times else 0,
            },
        }

    return tier_stats


def compute_average_trajectories(trajectories: list[TickerTrajectory]) -> dict:
    """Compute average gain and volume trajectories for each tier."""
    tier_trajectories = {}

    for tier in [1, 2, 3]:
        tier_trajs = [t for t in trajectories if t.tier == tier]

        if not tier_trajs:
            continue

        # Aggregate by minute
        minute_gains = {}
        minute_volumes = {}

        for traj in tier_trajs:
            for point in traj.price_trajectory:
                minute = point.minute
                if minute not in minute_gains:
                    minute_gains[minute] = []
                    minute_volumes[minute] = []

                minute_gains[minute].append(point.gain_pct)
                minute_volumes[minute].append(point.cumulative_volume)

        # Compute averages
        avg_gain_trajectory = []
        avg_volume_trajectory = []

        for minute in sorted(minute_gains.keys()):
            avg_gain_trajectory.append(
                {
                    "minute": minute,
                    "avg_gain": np.mean(minute_gains[minute]),
                    "median_gain": np.median(minute_gains[minute]),
                }
            )
            avg_volume_trajectory.append(
                {
                    "minute": minute,
                    "avg_volume": np.mean(minute_volumes[minute]),
                    "median_volume": np.median(minute_volumes[minute]),
                }
            )

        tier_trajectories[tier] = {
            "name": TIER_DEFINITIONS[tier]["name"],
            "gain_trajectory": avg_gain_trajectory,
            "volume_trajectory": avg_volume_trajectory,
        }

    return tier_trajectories


def format_trajectory_report(
    trajectories: list[TickerTrajectory],
    tier_stats: dict,
    tier_trajectories: dict,
) -> str:
    """Format trajectory analysis report."""
    lines = [
        "# Big Winner Trajectory Analysis Report",
        "",
        "## Overview",
        f"- Total trajectories analyzed: {len(trajectories)}",
        f"- Super Winners (Tier 1): {len([t for t in trajectories if t.tier == 1])}",
        f"- Big Winners (Tier 2): {len([t for t in trajectories if t.tier == 2])}",
        f"- Medium Winners (Tier 3): {len([t for t in trajectories if t.tier == 3])}",
        "",
        "---",
        "",
        "## Momentum Onset Analysis",
        "",
        "### Time to Gain Thresholds (minutes)",
        "",
        "| Tier | Time to 10% | Time to 25% | Time to 50% | Time to Peak |",
        "|------|--------------|-------------|-------------|--------------|",
    ]

    for tier in [1, 2, 3]:
        if tier in tier_stats:
            stats = tier_stats[tier]
            lines.append(
                f"| {tier} ({stats['name'][:6]}) | "
                f"{stats['time_to_10pct']['median']:.1f} | "
                f"{stats['time_to_25pct']['median']:.1f} | "
                f"{stats['time_to_50pct']['median']:.1f} | "
                f"{stats['time_to_peak']['median']:.1f} |"
            )

    lines.extend(
        [
            "",
            "**Key Insights**:",
            "",
        ]
    )

    if 1 in tier_stats and 3 in tier_stats:
        super_time = tier_stats[1]["time_to_10pct"]["median"]
        medium_time = tier_stats[3]["time_to_10pct"]["median"]

        if super_time > 0 and medium_time > 0:
            if super_time > medium_time:
                lines.append(
                    f"- **Super Winners take LONGER to reach 10%** ({super_time:.1f} min) "
                    f"than Medium Winners ({medium_time:.1f} min)"
                )
                lines.append(
                    "- This confirms: Super Winners are 'slow burners' that build momentum gradually"
                )
            else:
                lines.append(
                    f"- Super Winners reach 10% faster ({super_time:.1f} min) "
                    f"than Medium Winners ({medium_time:.1f} min)"
                )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Volume Peak Analysis",
            "",
            "| Tier | Volume Peak Time (min) | Interpretation |",
            "|------|------------------------|----------------|",
        ]
    )

    for tier in [1, 2, 3]:
        if tier in tier_stats:
            stats = tier_stats[tier]
            peak_time = stats["volume_peak_time"]["median"]

            if peak_time < 30:
                interpretation = "Early spike"
            elif peak_time < 120:
                interpretation = "Mid-session peak"
            else:
                interpretation = "Late surge"

            lines.append(
                f"| {tier} ({stats['name'][:6]}) | {peak_time:.1f} | {interpretation} |"
            )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Average Gain Trajectory by Tier",
            "",
            "| Minute | Super Winner | Big Winner | Medium Winner |",
            "|--------|--------------|------------|---------------|",
        ]
    )

    # Show key time points
    key_minutes = [5, 10, 15, 30, 60, 120, 180, 240, 300]

    for minute in key_minutes:
        row = [f"| {minute} |"]

        for tier in [1, 2, 3]:
            if tier in tier_trajectories:
                traj = tier_trajectories[tier]["gain_trajectory"]
                # Find the closest minute
                point = next((p for p in traj if p["minute"] >= minute), None)
                if point:
                    row.append(f" {point['median_gain']:.1f}% |")
                else:
                    row.append(" - |")
            else:
                row.append(" - |")

        lines.append("".join(row))

    lines.extend(
        [
            "",
            "---",
            "",
            "## Medium vs Big Winner Comparison",
            "",
        ]
    )

    if 2 in tier_stats and 3 in tier_stats:
        big_stats = tier_stats[2]
        med_stats = tier_stats[3]

        lines.extend(
            [
                "### Why do Medium Winners have higher early gains but lower final gains?",
                "",
                "**Timing Comparison**:",
                f"- Medium Winners reach 10% at: {med_stats['time_to_10pct']['median']:.1f} min",
                f"- Big Winners reach 10% at: {big_stats['time_to_10pct']['median']:.1f} min",
                "",
                "**Peak Timing**:",
                f"- Medium Winners peak at: {med_stats['time_to_peak']['median']:.1f} min",
                f"- Big Winners peak at: {big_stats['time_to_peak']['median']:.1f} min",
                "",
            ]
        )

        if med_stats["time_to_peak"]["median"] < big_stats["time_to_peak"]["median"]:
            lines.extend(
                [
                    "**Hypothesis**: Medium Winners peak early and fade, while Big Winners sustain momentum longer.",
                    "",
                    "**Implication**: Early high gains may be a warning sign, not a positive signal.",
                ]
            )
        else:
            lines.extend(
                [
                    "**Observation**: Both tiers peak at similar times, suggesting the difference is in magnitude, not timing.",
                ]
            )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Key Findings",
            "",
        ]
    )

    findings = []

    if 1 in tier_stats:
        super_time_10 = tier_stats[1]["time_to_10pct"]["median"]
        if super_time_10 > 60:
            findings.append(
                f"1. **Super Winners are slow burners**: They take {super_time_10:.0f}+ minutes to reach 10% gain"
            )

    if 1 in tier_stats and 3 in tier_stats:
        super_peak = tier_stats[1]["volume_peak_time"]["median"]
        med_peak = tier_stats[3]["volume_peak_time"]["median"]

        if super_peak > med_peak:
            findings.append(
                f"2. **Volume peaks later for Super Winners**: {super_peak:.0f} min vs {med_peak:.0f} min for Medium"
            )

    if 2 in tier_stats and 3 in tier_stats:
        big_peak_time = tier_stats[2]["time_to_peak"]["median"]
        med_peak_time = tier_stats[3]["time_to_peak"]["median"]

        if big_peak_time > med_peak_time:
            findings.append(
                f"3. **Big Winners sustain momentum longer**: Peak at {big_peak_time:.0f} min vs {med_peak_time:.0f} min"
            )

    lines.extend(findings)

    lines.extend(
        [
            "",
            "---",
            "",
            "## Strategy Implications",
            "",
            "### Entry Timing",
            "",
            "Based on trajectory analysis:",
            "",
            "1. **Don't chase early high gains** - Medium Winners show high early gains but fade",
            "2. **Wait for sustained momentum** - Super Winners build gradually",
            "3. **Monitor volume trajectory** - Late volume surge is positive signal",
            "",
            "### Recommended Entry Windows",
            "",
            "- **Early Entry (5-15 min)**: Only for tickers with exceptional volume (>500k in 5min)",
            "- **Mid Entry (30-60 min)**: Wait for momentum confirmation",
            "- **Late Entry (60-120 min)**: For slow burners showing sustained volume",
        ]
    )

    return "\n".join(lines)


def main():
    """Run trajectory analysis."""
    # Get available dates
    ch = get_clickhouse_client()
    query = """
        SELECT DISTINCT date
        FROM jerry_trader.trades
        WHERE date >= '2026-03-01'
        ORDER BY date
    """
    result = ch.query(query)
    dates = [row[0] for row in result.result_rows]

    logger.info(f"Found {len(dates)} dates to analyze")
    print(
        f"\nAnalyzing {len(dates)} dates: {', '.join(dates[:5])}{'...' if len(dates) > 5 else ''}\n"
    )

    all_trajectories = []

    for i, date in enumerate(dates):
        logger.info(f"Analyzing {date} ({i+1}/{len(dates)})...")
        try:
            trajectories = analyze_date(date)
            all_trajectories.extend(trajectories)
            logger.info(f"  Found {len(trajectories)} tier 1-3 trajectories")
        except Exception as e:
            logger.error(f"  Error analyzing {date}: {e}")
            continue

    # Compute statistics
    tier_stats = compute_tier_trajectory_stats(all_trajectories)
    tier_trajectories = compute_average_trajectories(all_trajectories)

    # Generate report
    report = format_trajectory_report(all_trajectories, tier_stats, tier_trajectories)
    print("\n" + report)

    # Save report
    output_path = Path("reports/big_winner_trajectory_analysis.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report)
    logger.info(f"\nReport saved to {output_path}")

    # Save trajectory data for visualization
    data_output = {
        "tier_stats": tier_stats,
        "tier_trajectories": tier_trajectories,
    }

    import json

    json_path = Path("reports/big_winner_trajectory_data.json")
    with open(json_path, "w") as f:
        json.dump(data_output, f, indent=2, default=str)
    logger.info(f"Data saved to {json_path}")


if __name__ == "__main__":
    main()
