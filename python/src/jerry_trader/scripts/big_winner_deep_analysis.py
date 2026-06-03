"""Deep Analysis of Big Winners - False Positives and Slow-Burners.

Extended analysis:
1. Analyze more dates (full available range)
2. Calculate false positive rate for early signals
3. Analyze "slow-burner" big winners that don't show early signals
4. Find optimal entry criteria with confidence levels

Usage:
    poetry run python -m jerry_trader.scripts.big_winner_deep_analysis
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pytz

from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("big_winner_deep_analysis", log_to_file=True)


@dataclass
class EarlyTrajectory:
    """Early trajectory analysis for a ticker."""

    symbol: str
    date: str

    # Final outcome
    final_max_gain: float
    final_volume: int
    is_big_winner: bool  # Final max gain > 40% and volume > 1M

    # Early indicators (first N minutes)
    gain_5min: float = 0.0
    gain_10min: float = 0.0
    gain_15min: float = 0.0

    volume_5min: int = 0
    volume_10min: int = 0
    volume_15min: int = 0

    trades_5min: int = 0
    trades_10min: int = 0
    trades_15min: int = 0

    trade_rate_5min: float = 0.0
    trade_rate_10min: float = 0.0
    trade_rate_15min: float = 0.0

    # Price action patterns
    price_acceleration_5min: float = 0.0
    price_acceleration_10min: float = 0.0

    # Gap at open
    gap_pct: float = 0.0

    # Relative metrics
    rel_volume_5min: float = 0.0
    rel_trade_rate_5min: float = 0.0

    # Classification
    is_early_signal: bool = False  # Met early signal criteria
    signal_type: str = ""  # "early_explosive", "slow_burner", "false_positive"


def analyze_early_trajectory(
    symbol: str,
    date: str,
    ticker_data: Any,
    session_start_ms: int,
    session_end_ms: int,
    gap_pct: float,
) -> EarlyTrajectory | None:
    """Analyze early trajectory for a single ticker."""
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
    final_max_gain = (high_price - open_price) / open_price * 100
    final_volume = sum(volumes)

    is_big_winner = final_max_gain > 40 and final_volume > 1_000_000

    first_trade_ms = timestamps[0]
    min_5_ms = 5 * 60 * 1000
    min_10_ms = 10 * 60 * 1000
    min_15_ms = 15 * 60 * 1000

    def get_stats_at_time(window_ms: int) -> tuple[float, int, int]:
        cutoff_ms = first_trade_ms + window_ms
        window_prices = [p for ts, p, _ in premarket_trades if ts <= cutoff_ms]
        window_volumes = [v for ts, _, v in premarket_trades if ts <= cutoff_ms]

        if not window_prices:
            return 0.0, 0, 0

        window_high = max(window_prices)
        window_gain = (window_high - open_price) / open_price * 100
        window_volume = sum(window_volumes)
        window_trades = len(window_prices)

        return window_gain, window_volume, window_trades

    gain_5min, volume_5min, trades_5min = get_stats_at_time(min_5_ms)
    gain_10min, volume_10min, trades_10min = get_stats_at_time(min_10_ms)
    gain_15min, volume_15min, trades_15min = get_stats_at_time(min_15_ms)

    trade_rate_5min = trades_5min / 5 if trades_5min > 0 else 0
    trade_rate_10min = trades_10min / 10 if trades_10min > 0 else 0
    trade_rate_15min = trades_15min / 15 if trades_15min > 0 else 0

    price_acceleration_5min = gain_5min / 5 if gain_5min > 0 else 0
    price_acceleration_10min = gain_10min / 10 if gain_10min > 0 else 0

    return EarlyTrajectory(
        symbol=symbol,
        date=date,
        final_max_gain=final_max_gain,
        final_volume=final_volume,
        is_big_winner=is_big_winner,
        gain_5min=gain_5min,
        gain_10min=gain_10min,
        gain_15min=gain_15min,
        volume_5min=volume_5min,
        volume_10min=volume_10min,
        volume_15min=volume_15min,
        trades_5min=trades_5min,
        trades_10min=trades_10min,
        trades_15min=trades_15min,
        trade_rate_5min=trade_rate_5min,
        trade_rate_10min=trade_rate_10min,
        trade_rate_15min=trade_rate_15min,
        price_acceleration_5min=price_acceleration_5min,
        price_acceleration_10min=price_acceleration_10min,
        gap_pct=gap_pct,
    )


def get_available_dates() -> list[str]:
    """Get list of available dates from ClickHouse."""
    ch = get_clickhouse_client()

    # Query distinct dates from trades table (date is a String column)
    query = """
        SELECT DISTINCT date
        FROM jerry_trader.trades
        WHERE date >= '2026-03-01'
        ORDER BY date
    """

    result = ch.query(query)
    dates = [row[0] for row in result.result_rows]

    return dates


def analyze_date(date: str) -> list[EarlyTrajectory]:
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
        candidate = next((c for c in candidates if c.symbol == symbol), None)
        gap_pct = candidate.gain_at_entry if candidate else 0

        traj = analyze_early_trajectory(
            symbol, date, ticker_data, session_start_ms, session_end_ms, gap_pct
        )
        if traj:
            trajectories.append(traj)

    # Compute relative metrics
    if trajectories:
        median_volume_5min = np.median([t.volume_5min for t in trajectories])
        median_trade_rate_5min = np.median([t.trade_rate_5min for t in trajectories])

        for t in trajectories:
            t.rel_volume_5min = (
                t.volume_5min / median_volume_5min if median_volume_5min > 0 else 0
            )
            t.rel_trade_rate_5min = (
                t.trade_rate_5min / median_trade_rate_5min
                if median_trade_rate_5min > 0
                else 0
            )

    return trajectories


def classify_trajectories(trajectories: list[EarlyTrajectory]) -> None:
    """Classify trajectories based on early signals and outcomes.

    Early signal criteria (must meet ALL):
    - volume_5min > 100,000 OR rel_volume_5min > 10
    - trade_rate_5min > 20
    - gain_5min > 5%

    Classifications:
    - early_explosive: Met early signals AND became big winner
    - slow_burner: Did NOT meet early signals BUT became big winner
    - false_positive: Met early signals BUT did NOT become big winner
    - other: Did not meet signals, did not become big winner
    """
    for t in trajectories:
        # Check early signal criteria
        volume_signal = t.volume_5min > 100_000 or t.rel_volume_5min > 10
        rate_signal = t.trade_rate_5min > 20
        gain_signal = t.gain_5min > 5

        t.is_early_signal = volume_signal and rate_signal and gain_signal

        if t.is_early_signal and t.is_big_winner:
            t.signal_type = "early_explosive"
        elif not t.is_early_signal and t.is_big_winner:
            t.signal_type = "slow_burner"
        elif t.is_early_signal and not t.is_big_winner:
            t.signal_type = "false_positive"
        else:
            t.signal_type = "other"


def format_deep_analysis_report(trajectories: list[EarlyTrajectory]) -> str:
    """Format comprehensive analysis report."""
    # Classify all trajectories
    classify_trajectories(trajectories)

    # Count by category
    early_explosive = [t for t in trajectories if t.signal_type == "early_explosive"]
    slow_burners = [t for t in trajectories if t.signal_type == "slow_burner"]
    false_positives = [t for t in trajectories if t.signal_type == "false_positive"]
    others = [t for t in trajectories if t.signal_type == "other"]
    big_winners = [t for t in trajectories if t.is_big_winner]

    lines = [
        "# Big Winner Deep Analysis Report",
        "",
        "## Overview",
        f"- Total tickers analyzed: {len(trajectories)}",
        f"- Big winners (gain>40%, volume>1M): {len(big_winners)}",
        f"- Dates analyzed: {len(set(t.date for t in trajectories))}",
        "",
        "## Classification Summary",
        "",
        "| Category | Count | Description |",
        "|----------|-------|-------------|",
        f"| Early Explosive | {len(early_explosive)} | Met early signals AND became big winner |",
        f"| Slow Burner | {len(slow_burners)} | Did NOT meet early signals BUT became big winner |",
        f"| False Positive | {len(false_positives)} | Met early signals BUT did NOT become big winner |",
        f"| Other | {len(others)} | No signals, not a big winner |",
        "",
        "## Signal Effectiveness",
        "",
    ]

    # Calculate effectiveness metrics
    total_signals = len(early_explosive) + len(false_positives)
    if total_signals > 0:
        precision = len(early_explosive) / total_signals * 100
        lines.extend(
            [
                f"- **Precision**: {precision:.1f}% ({len(early_explosive)}/{total_signals} signals led to big winners)",
            ]
        )

    if len(big_winners) > 0:
        recall = len(early_explosive) / len(big_winners) * 100
        lines.extend(
            [
                f"- **Recall**: {recall:.1f}% ({len(early_explosive)}/{len(big_winners)} big winners caught early)",
            ]
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Early Explosive Big Winners",
            "",
            "These big winners showed strong early signals and were catchable:",
            "",
            "| Symbol | Date | Final Gain | 5min Gain | 5min Vol | 5min Rate | Rel Vol |",
            "|--------|------|------------|-----------|----------|-----------|---------|",
        ]
    )

    for t in sorted(early_explosive, key=lambda x: x.final_max_gain, reverse=True):
        lines.append(
            f"| {t.symbol} | {t.date} | {t.final_max_gain:.1f}% | {t.gain_5min:.1f}% | "
            f"{t.volume_5min:,} | {t.trade_rate_5min:.1f} | {t.rel_volume_5min:.1f}x |"
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Slow Burner Big Winners",
            "",
            "These big winners did NOT show early signals - they developed slowly:",
            "",
            "| Symbol | Date | Final Gain | 5min Gain | 5min Vol | 5min Rate | Gap |",
            "|--------|------|------------|-----------|----------|-----------|-----|",
        ]
    )

    for t in sorted(slow_burners, key=lambda x: x.final_max_gain, reverse=True):
        lines.append(
            f"| {t.symbol} | {t.date} | {t.final_max_gain:.1f}% | {t.gain_5min:.1f}% | "
            f"{t.volume_5min:,} | {t.trade_rate_5min:.1f} | {t.gap_pct:.1f}% |"
        )

    if slow_burners:
        # Analyze slow burner characteristics
        avg_gain = np.mean([t.final_max_gain for t in slow_burners])
        avg_gap = np.mean([t.gap_pct for t in slow_burners])
        avg_15min_gain = np.mean([t.gain_15min for t in slow_burners])

        lines.extend(
            [
                "",
                "### Slow Burner Analysis",
                f"- Average final gain: {avg_gain:.1f}%",
                f"- Average gap at open: {avg_gap:.1f}%",
                f"- Average 15min gain: {avg_15min_gain:.1f}%",
                "",
                "**Pattern**: These stocks often have moderate gaps and build momentum gradually.",
                "They may require a different entry strategy (wait for 15-30 min confirmation).",
            ]
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## False Positives",
            "",
            "These showed early signals but did NOT become big winners:",
            "",
            "| Symbol | Date | Final Gain | 5min Gain | 5min Vol | 5min Rate | Outcome |",
            "|--------|------|------------|-----------|----------|-----------|---------|",
        ]
    )

    for t in sorted(false_positives, key=lambda x: x.final_max_gain, reverse=True)[:20]:
        outcome = f"{t.final_max_gain:.1f}% gain"
        lines.append(
            f"| {t.symbol} | {t.date} | {t.final_max_gain:.1f}% | {t.gain_5min:.1f}% | "
            f"{t.volume_5min:,} | {t.trade_rate_5min:.1f} | {outcome} |"
        )

    if len(false_positives) > 20:
        lines.append(f"| ... and {len(false_positives) - 20} more | | | | | | |")

    if false_positives:
        lines.extend(
            [
                "",
                "### False Positive Analysis",
                f"- Total false positives: {len(false_positives)}",
                f"- Average final gain: {np.mean([t.final_max_gain for t in false_positives]):.1f}%",
                f"- Median final gain: {np.median([t.final_max_gain for t in false_positives]):.1f}%",
                "",
                "**Note**: Some 'false positives' still had decent gains (20-40%), just not 'big winner' level.",
            ]
        )

    # Summary statistics
    lines.extend(
        [
            "",
            "---",
            "",
            "## Summary Statistics",
            "",
            "### By Date",
            "",
            "| Date | Tickers | Big Winners | Early Explosive | Slow Burners |",
            "|------|---------|-------------|-----------------|--------------|",
        ]
    )

    dates = sorted(set(t.date for t in trajectories))
    for date in dates:
        date_trajs = [t for t in trajectories if t.date == date]
        date_winners = [t for t in date_trajs if t.is_big_winner]
        date_early = [t for t in date_trajs if t.signal_type == "early_explosive"]
        date_slow = [t for t in date_trajs if t.signal_type == "slow_burner"]

        lines.append(
            f"| {date} | {len(date_trajs)} | {len(date_winners)} | {len(date_early)} | {len(date_slow)} |"
        )

    # Key insights
    lines.extend(
        [
            "",
            "---",
            "",
            "## Key Insights",
            "",
            "### 1. Early Signal Criteria",
            "",
            "Based on this analysis, the following criteria identify big winners early:",
            "",
            "- **Volume in first 5 min > 100,000** OR **relative volume > 10x median**",
            "- **Trade rate > 20 trades/min** in first 5 min",
            "- **Price gain > 5%** in first 5 min",
            "",
            (
                f"**Precision**: {precision:.1f}% of signals lead to big winners"
                if total_signals > 0
                else ""
            ),
            (
                f"**Recall**: {recall:.1f}% of big winners are caught early"
                if len(big_winners) > 0
                else ""
            ),
            "",
            "### 2. Strategy Implications",
            "",
        ]
    )

    if len(slow_burners) > 0:
        lines.extend(
            [
                f"- **{len(slow_burners)} big winners are 'slow burners'** - they don't show early signals",
                "- Need a secondary strategy for these (e.g., monitor throughout session)",
                "- Consider re-evaluating at 15min, 30min marks",
            ]
        )

    if len(false_positives) > 0:
        lines.extend(
            [
                f"- **{len(false_positives)} false positives** - signals that didn't lead to big winners",
                "- Consider adding more filters (e.g., catalyst news, sector momentum)",
            ]
        )

    return "\n".join(lines)


def main():
    """Run deep analysis."""
    # Get available dates
    logger.info("Fetching available dates...")
    dates = get_available_dates()

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
            logger.info(f"  Found {len(trajectories)} tickers")
        except Exception as e:
            logger.error(f"  Error analyzing {date}: {e}")
            continue

    # Generate report
    report = format_deep_analysis_report(all_trajectories)
    print("\n" + report)

    # Save report
    output_path = Path("reports/big_winner_deep_analysis.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report)
    logger.info(f"\nReport saved to {output_path}")


if __name__ == "__main__":
    main()
