"""Find Big Winners - Identify tickers with exceptional pre-market gains.

Goal: Find the 1-2 big winners each day (60%+ gains) by discovering patterns
rather than hardcoding thresholds.

Analysis approach:
1. Scan each ticker's pre-market price trajectory
2. Compute max gain and volume characteristics
3. Identify what distinguishes big winners from other signals
4. Find patterns that precede 60%+ moves

Usage:
    poetry run python -m jerry_trader.scripts.find_big_winners
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import pytz

from jerry_trader.domain.session import SessionPhase, get_session_phase_from_epoch_ms
from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.batch_engine import FactorEngineBatchAdapter
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("find_big_winners", log_to_file=True)


@dataclass
class TickerTrajectory:
    """A ticker's pre-market price and volume trajectory."""

    symbol: str
    date: str

    # Price trajectory
    open_price: float
    high_price: float
    low_price: float
    close_price: float  # Last pre-market price
    max_gain_pct: float  # (high - open) / open * 100

    # Volume characteristics
    total_volume: int
    total_trades: int
    avg_trade_size: float

    # Timing
    first_trade_ms: int
    last_trade_ms: int
    time_to_high_ms: int  # When did it reach max gain
    duration_minutes: float

    # Gap at open
    gap_pct: float

    # Volume profile (relative to normal)
    rel_vol_estimate: float = 0.0

    # Trade rate
    trade_rate: float = 0.0  # trades per minute

    # Price acceleration (how fast it moved)
    price_acceleration: float = 0.0  # gain per minute at peak

    # Is this a big winner?
    is_big_winner: bool = False


@dataclass
class BigWinnerAnalysis:
    """Analysis results for big winners."""

    date: str
    total_tickers: int = 0
    big_winners: list[TickerTrajectory] = field(default_factory=list)

    # Characteristics of big winners vs others
    winner_avg_gain: float = 0.0
    winner_avg_volume: float = 0.0
    winner_avg_trade_rate: float = 0.0
    winner_avg_rel_vol: float = 0.0

    # Thresholds discovered
    discovered_thresholds: dict[str, float] = field(default_factory=dict)


def analyze_ticker_trajectory(
    symbol: str,
    date: str,
    ticker_data: Any,
    session_start_ms: int,
    session_end_ms: int,
) -> TickerTrajectory | None:
    """Analyze a single ticker's pre-market trajectory.

    Args:
        symbol: Ticker symbol
        date: Date string
        ticker_data: TickerData from DataLoader
        session_start_ms: Pre-market start (4:00 ET)
        session_end_ms: Pre-market end (9:30 ET)

    Returns:
        TickerTrajectory or None if insufficient data
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
        return None  # Need at least 10 trades

    # Extract prices and volumes
    timestamps = [t[0] for t in premarket_trades]
    prices = [t[1] for t in premarket_trades]
    volumes = [t[2] for t in premarket_trades]

    # Basic stats
    open_price = prices[0]
    high_price = max(prices)
    low_price = min(prices)
    close_price = prices[-1]

    max_gain_pct = (high_price - open_price) / open_price * 100
    total_volume = sum(volumes)
    total_trades = len(premarket_trades)
    avg_trade_size = total_volume / total_trades if total_trades > 0 else 0

    first_trade_ms = timestamps[0]
    last_trade_ms = timestamps[-1]
    duration_minutes = (last_trade_ms - first_trade_ms) / 60000

    # Find when max gain occurred
    high_idx = prices.index(high_price)
    time_to_high_ms = timestamps[high_idx] - first_trade_ms

    # Trade rate (trades per minute)
    trade_rate = total_trades / duration_minutes if duration_minutes > 0 else 0

    # Price acceleration (gain per minute at peak)
    if time_to_high_ms > 0:
        price_acceleration = max_gain_pct / (time_to_high_ms / 60000)
    else:
        price_acceleration = 0

    # Compute relative volume estimate
    # We'll compute this properly with factor engine
    rel_vol_estimate = 0.0

    # Get gap at open (from candidate if available)
    gap_pct = 0.0

    return TickerTrajectory(
        symbol=symbol,
        date=date,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        max_gain_pct=max_gain_pct,
        total_volume=total_volume,
        total_trades=total_trades,
        avg_trade_size=avg_trade_size,
        first_trade_ms=first_trade_ms,
        last_trade_ms=last_trade_ms,
        time_to_high_ms=time_to_high_ms,
        duration_minutes=duration_minutes,
        gap_pct=gap_pct,
        rel_vol_estimate=rel_vol_estimate,
        trade_rate=trade_rate,
        price_acceleration=price_acceleration,
    )


def find_big_winners_for_date(date: str) -> BigWinnerAnalysis:
    """Find big winners for a single date.

    Args:
        date: Date string YYYY-MM-DD

    Returns:
        BigWinnerAnalysis with all trajectories and winners
    """
    logger.info(f"Analyzing {date} for big winners...")

    ch = get_clickhouse_client()

    # Get candidates (top 20 gainers at open)
    pre_filter = PreFilter(ch_client=ch)
    candidates = pre_filter.find(date, PreFilterConfig())

    if not candidates:
        logger.warning(f"No candidates for {date}")
        return BigWinnerAnalysis(date=date)

    # Load full trade data
    config = BacktestConfig(date=date)
    data_loader = DataLoader(ch_client=ch)
    ticker_data_map = data_loader.load(candidates, config)

    # Session window (4:00 - 9:30 ET)
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
        traj = analyze_ticker_trajectory(
            symbol, date, ticker_data, session_start_ms, session_end_ms
        )
        if traj:
            # Add gap from candidate
            candidate = next((c for c in candidates if c.symbol == symbol), None)
            if candidate:
                traj.gap_pct = candidate.gain_at_entry
            trajectories.append(traj)

    logger.info(f"Analyzed {len(trajectories)} tickers for {date}")

    # Sort by max gain
    trajectories.sort(key=lambda t: t.max_gain_pct, reverse=True)

    # Discover patterns - what distinguishes top performers?
    # We'll use percentile-based discovery instead of hardcoding thresholds

    if not trajectories:
        return BigWinnerAnalysis(date=date)

    gains = [t.max_gain_pct for t in trajectories]
    volumes = [t.total_volume for t in trajectories]
    trade_rates = [t.trade_rate for t in trajectories]

    # Find "big winners" - top performers by gain
    # Use a dynamic threshold: top 10% or top 3, whichever is larger
    top_n = max(3, int(len(trajectories) * 0.1))
    big_winners = trajectories[:top_n]

    # Mark them
    for w in big_winners:
        w.is_big_winner = True

    # Analyze characteristics
    analysis = BigWinnerAnalysis(
        date=date,
        total_tickers=len(trajectories),
        big_winners=big_winners,
    )

    if big_winners:
        analysis.winner_avg_gain = np.mean([w.max_gain_pct for w in big_winners])
        analysis.winner_avg_volume = np.mean([w.total_volume for w in big_winners])
        analysis.winner_avg_trade_rate = np.mean([w.trade_rate for w in big_winners])

        # Discover thresholds that separate winners from others
        non_winners = trajectories[top_n:]
        if non_winners:
            non_winner_avg_gain = np.mean([t.max_gain_pct for t in non_winners])
            non_winner_avg_volume = np.mean([t.total_volume for t in non_winners])
            non_winner_avg_trade_rate = np.mean([t.trade_rate for t in non_winners])

            # Thresholds that could identify winners
            analysis.discovered_thresholds = {
                "min_gain_winner": min(w.max_gain_pct for w in big_winners),
                "avg_gain_winner": analysis.winner_avg_gain,
                "avg_gain_non_winner": non_winner_avg_gain,
                "volume_ratio": (
                    analysis.winner_avg_volume / non_winner_avg_volume
                    if non_winner_avg_volume > 0
                    else 0
                ),
                "trade_rate_ratio": (
                    analysis.winner_avg_trade_rate / non_winner_avg_trade_rate
                    if non_winner_avg_trade_rate > 0
                    else 0
                ),
            }

    return analysis


def format_big_winner_report(analysis: BigWinnerAnalysis) -> str:
    """Format analysis as readable report."""
    lines = [
        f"# Big Winners Analysis: {analysis.date}",
        "",
        f"## Overview",
        f"- Total tickers analyzed: {analysis.total_tickers}",
        f"- Big winners identified: {len(analysis.big_winners)}",
        "",
        "## Big Winners Detail",
    ]

    for i, winner in enumerate(analysis.big_winners, 1):
        lines.extend(
            [
                f"### {i}. {winner.symbol}",
                f"- Max gain: {winner.max_gain_pct:.1f}%",
                f"- Gap at open: {winner.gap_pct:.1f}%",
                f"- Total volume: {winner.total_volume:,}",
                f"- Total trades: {winner.total_trades:,}",
                f"- Trade rate: {winner.trade_rate:.1f} trades/min",
                f"- Duration: {winner.duration_minutes:.1f} min",
                f"- Time to high: {winner.time_to_high_ms / 60000:.1f} min",
                f"- Price acceleration: {winner.price_acceleration:.2f}%/min",
                "",
            ]
        )

    if analysis.discovered_thresholds:
        lines.extend(
            [
                "## Discovered Patterns",
                f"- Min gain for winners: {analysis.discovered_thresholds['min_gain_winner']:.1f}%",
                f"- Avg gain (winners): {analysis.discovered_thresholds['avg_gain_winner']:.1f}%",
                f"- Avg gain (others): {analysis.discovered_thresholds['avg_gain_non_winner']:.1f}%",
                f"- Volume ratio (winner/other): {analysis.discovered_thresholds['volume_ratio']:.2f}x",
                f"- Trade rate ratio (winner/other): {analysis.discovered_thresholds['trade_rate_ratio']:.2f}x",
            ]
        )

    return "\n".join(lines)


def main():
    """Run big winner analysis."""
    dates = [
        "2026-03-11",
        "2026-03-12",
        "2026-03-13",
    ]

    all_analyses = []

    for date in dates:
        analysis = find_big_winners_for_date(date)
        all_analyses.append(analysis)

        report = format_big_winner_report(analysis)
        print("\n" + report)

    # Aggregate analysis across all dates
    print("\n" + "=" * 80)
    print("\n# Aggregate Analysis Across All Dates\n")

    all_winners = [w for a in all_analyses for w in a.big_winners]

    if all_winners:
        print(f"Total big winners found: {len(all_winners)}\n")

        print("## All Big Winners Summary\n")
        print("| Date | Symbol | Max Gain | Gap | Volume | Trade Rate | Acceleration |")
        print("|------|--------|----------|-----|--------|------------|--------------|")
        for w in all_winners:
            print(
                f"| {w.date} | {w.symbol} | {w.max_gain_pct:.1f}% | {w.gap_pct:.1f}% | "
                f"{w.total_volume:,} | {w.trade_rate:.1f} | {w.price_acceleration:.2f} |"
            )

        # Aggregate statistics
        print("\n## Aggregate Statistics\n")
        print(
            f"- Average max gain: {np.mean([w.max_gain_pct for w in all_winners]):.1f}%"
        )
        print(f"- Average gap: {np.mean([w.gap_pct for w in all_winners]):.1f}%")
        print(
            f"- Average volume: {np.mean([w.total_volume for w in all_winners]):,.0f}"
        )
        print(
            f"- Average trade rate: {np.mean([w.trade_rate for w in all_winners]):.1f} trades/min"
        )
        print(
            f"- Average acceleration: {np.mean([w.price_acceleration for w in all_winners]):.2f}%/min"
        )


if __name__ == "__main__":
    main()
