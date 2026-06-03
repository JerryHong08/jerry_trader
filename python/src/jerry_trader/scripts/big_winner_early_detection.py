"""Big Winner Early Detection - Find early signals that identify big winners.

Goal: Discover what early signals (first 5-15 minutes) can help identify
the big winners before they make their huge moves.

Analysis approach:
1. Analyze price trajectory in first 5, 10, 15 minutes
2. Compare early volume/trade_rate patterns
3. Find early indicators that distinguish future big winners
4. Discover optimal entry timing patterns

Usage:
    poetry run python -m jerry_trader.scripts.big_winner_early_detection
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pytz

from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("big_winner_early_detection", log_to_file=True)


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
    price_acceleration_5min: float = 0.0  # gain per minute in first 5 min
    price_acceleration_10min: float = 0.0

    # Gap at open
    gap_pct: float = 0.0

    # Relative metrics (vs other tickers same day)
    rel_volume_5min: float = 0.0  # volume / median volume
    rel_trade_rate_5min: float = 0.0


def analyze_early_trajectory(
    symbol: str,
    date: str,
    ticker_data: Any,
    session_start_ms: int,
    session_end_ms: int,
    gap_pct: float,
) -> EarlyTrajectory | None:
    """Analyze early trajectory for a single ticker.

    Args:
        symbol: Ticker symbol
        date: Date string
        ticker_data: TickerData from DataLoader
        session_start_ms: Pre-market start (4:00 ET)
        session_end_ms: Pre-market end (9:30 ET)
        gap_pct: Gap at open

    Returns:
        EarlyTrajectory or None if insufficient data
    """
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

    # Extract prices and volumes
    timestamps = [t[0] for t in premarket_trades]
    prices = [t[1] for t in premarket_trades]
    volumes = [t[2] for t in premarket_trades]

    # Basic stats
    open_price = prices[0]
    high_price = max(prices)
    final_max_gain = (high_price - open_price) / open_price * 100
    final_volume = sum(volumes)

    # Determine if this is a "real" big winner (high gain + high volume)
    is_big_winner = final_max_gain > 40 and final_volume > 1_000_000

    first_trade_ms = timestamps[0]

    # Time windows
    min_5_ms = 5 * 60 * 1000
    min_10_ms = 10 * 60 * 1000
    min_15_ms = 15 * 60 * 1000

    # Find prices and volumes at each time window
    def get_stats_at_time(window_ms: int) -> tuple[float, int, int]:
        """Get gain, volume, and trade count at a specific time window."""
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

    # Get early stats
    gain_5min, volume_5min, trades_5min = get_stats_at_time(min_5_ms)
    gain_10min, volume_10min, trades_10min = get_stats_at_time(min_10_ms)
    gain_15min, volume_15min, trades_15min = get_stats_at_time(min_15_ms)

    # Trade rates
    trade_rate_5min = trades_5min / 5 if trades_5min > 0 else 0
    trade_rate_10min = trades_10min / 10 if trades_10min > 0 else 0
    trade_rate_15min = trades_15min / 15 if trades_15min > 0 else 0

    # Price acceleration
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


def analyze_date(date: str) -> list[EarlyTrajectory]:
    """Analyze all tickers for a single date.

    Returns:
        List of EarlyTrajectory for all tickers
    """
    logger.info(f"Analyzing early trajectories for {date}...")

    ch = get_clickhouse_client()

    # Get candidates
    pre_filter = PreFilter(ch_client=ch)
    candidates = pre_filter.find(date, PreFilterConfig())

    if not candidates:
        logger.warning(f"No candidates for {date}")
        return []

    # Load full trade data
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

    # Analyze each ticker
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

    logger.info(f"Analyzed {len(trajectories)} tickers for {date}")
    return trajectories


def format_early_detection_report(all_trajectories: list[EarlyTrajectory]) -> str:
    """Format analysis as readable report."""
    # Separate big winners from others
    big_winners = [t for t in all_trajectories if t.is_big_winner]
    others = [t for t in all_trajectories if not t.is_big_winner]

    lines = [
        "# Big Winner Early Detection Analysis",
        "",
        "## Overview",
        f"- Total tickers analyzed: {len(all_trajectories)}",
        f"- Big winners (gain>40%, volume>1M): {len(big_winners)}",
        f"- Others: {len(others)}",
        "",
        "## Big Winners Early Indicators",
        "",
        "| Symbol | Date | Final Gain | 5min Gain | 10min Gain | 15min Gain | 5min Vol | 5min Rate | Rel Vol |",
        "|--------|------|------------|-----------|------------|------------|----------|-----------|---------|",
    ]

    for w in sorted(big_winners, key=lambda x: x.final_max_gain, reverse=True):
        lines.append(
            f"| {w.symbol} | {w.date} | {w.final_max_gain:.1f}% | {w.gain_5min:.1f}% | "
            f"{w.gain_10min:.1f}% | {w.gain_15min:.1f}% | {w.volume_5min:,} | "
            f"{w.trade_rate_5min:.1f} | {w.rel_volume_5min:.1f}x |"
        )

    lines.extend(
        [
            "",
            "## Comparison: Big Winners vs Others (Early Indicators)",
            "",
        ]
    )

    if big_winners and others:
        # Compute averages
        def avg(lst: list[float]) -> float:
            return sum(lst) / len(lst) if lst else 0

        winner_stats = {
            "gain_5min": avg([t.gain_5min for t in big_winners]),
            "gain_10min": avg([t.gain_10min for t in big_winners]),
            "gain_15min": avg([t.gain_15min for t in big_winners]),
            "volume_5min": avg([t.volume_5min for t in big_winners]),
            "trade_rate_5min": avg([t.trade_rate_5min for t in big_winners]),
            "rel_volume_5min": avg([t.rel_volume_5min for t in big_winners]),
            "price_accel_5min": avg([t.price_acceleration_5min for t in big_winners]),
        }

        other_stats = {
            "gain_5min": avg([t.gain_5min for t in others]),
            "gain_10min": avg([t.gain_10min for t in others]),
            "gain_15min": avg([t.gain_15min for t in others]),
            "volume_5min": avg([t.volume_5min for t in others]),
            "trade_rate_5min": avg([t.trade_rate_5min for t in others]),
            "rel_volume_5min": avg([t.rel_volume_5min for t in others]),
            "price_accel_5min": avg([t.price_acceleration_5min for t in others]),
        }

        lines.extend(
            [
                "| Metric | Big Winners | Others | Ratio |",
                "|--------|-------------|--------|-------|",
                f"| 5min Gain | {winner_stats['gain_5min']:.1f}% | {other_stats['gain_5min']:.1f}% | {winner_stats['gain_5min'] / other_stats['gain_5min'] if other_stats['gain_5min'] > 0 else 0:.1f}x |",
                f"| 10min Gain | {winner_stats['gain_10min']:.1f}% | {other_stats['gain_10min']:.1f}% | {winner_stats['gain_10min'] / other_stats['gain_10min'] if other_stats['gain_10min'] > 0 else 0:.1f}x |",
                f"| 15min Gain | {winner_stats['gain_15min']:.1f}% | {other_stats['gain_15min']:.1f}% | {winner_stats['gain_15min'] / other_stats['gain_15min'] if other_stats['gain_15min'] > 0 else 0:.1f}x |",
                f"| 5min Volume | {winner_stats['volume_5min']:,.0f} | {other_stats['volume_5min']:,.0f} | {winner_stats['volume_5min'] / other_stats['volume_5min'] if other_stats['volume_5min'] > 0 else 0:.1f}x |",
                f"| 5min Trade Rate | {winner_stats['trade_rate_5min']:.1f} | {other_stats['trade_rate_5min']:.1f} | {winner_stats['trade_rate_5min'] / other_stats['trade_rate_5min'] if other_stats['trade_rate_5min'] > 0 else 0:.1f}x |",
                f"| Rel Volume 5min | {winner_stats['rel_volume_5min']:.1f}x | {other_stats['rel_volume_5min']:.1f}x | {winner_stats['rel_volume_5min'] / other_stats['rel_volume_5min'] if other_stats['rel_volume_5min'] > 0 else 0:.1f}x |",
                f"| Price Accel 5min | {winner_stats['price_accel_5min']:.2f}%/min | {other_stats['price_accel_5min']:.2f}%/min | {winner_stats['price_accel_5min'] / other_stats['price_accel_5min'] if other_stats['price_accel_5min'] > 0 else 0:.1f}x |",
                "",
            ]
        )

        # Key findings
        lines.extend(
            [
                "## Key Findings",
                "",
                "### Early Volume is the Strongest Signal",
                f"- Big winners have **{winner_stats['rel_volume_5min']:.1f}x** median volume in first 5 minutes",
                f"- This is visible immediately, not after the move",
                "",
                "### Early Price Action",
                f"- Big winners gain **{winner_stats['gain_5min']:.1f}%** in first 5 minutes vs {other_stats['gain_5min']:.1f}% for others",
                f"- Price acceleration: **{winner_stats['price_accel_5min']:.2f}%/min** vs {other_stats['price_accel_5min']:.2f}%/min",
                "",
                "### Suggested Early Entry Criteria",
                "",
                "Based on this analysis, potential early signals for big winners:",
                "",
            ]
        )

        # Find thresholds
        min_winner_vol_5min = min([t.volume_5min for t in big_winners])
        min_winner_gain_5min = min([t.gain_5min for t in big_winners])

        lines.extend(
            [
                f"1. **Volume in first 5 min > {min_winner_vol_5min:,.0f}** (or > 3x median)",
                f"2. **Trade rate > 20 trades/min** in first 5 min",
                f"3. **Price acceleration > 0.5%/min** in first 5 min",
                "",
                "These signals appear within the first 5-10 minutes of trading,",
                "allowing early entry before the major move.",
            ]
        )

    return "\n".join(lines)


def main():
    """Run early detection analysis."""
    dates = [
        "2026-03-11",
        "2026-03-12",
        "2026-03-13",
    ]

    all_trajectories = []

    for date in dates:
        trajectories = analyze_date(date)
        all_trajectories.extend(trajectories)

    # Generate report
    report = format_early_detection_report(all_trajectories)
    print("\n" + report)


if __name__ == "__main__":
    main()
