"""Big Winner Stratified Analysis - Tier-based feature comparison.

Analyzes big winners by gain tiers to identify distinguishing features.

Usage:
    poetry run python -m jerry_trader.scripts.big_winner_stratified_analysis
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytz
from scipy import stats

from jerry_trader.platform.storage.clickhouse import get_clickhouse_client
from jerry_trader.services.backtest.config import BacktestConfig, PreFilterConfig
from jerry_trader.services.backtest.data_loader import DataLoader
from jerry_trader.services.backtest.pre_filter import PreFilter
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("big_winner_stratified", log_to_file=True)


# Tier definitions
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
class TickerFeatures:
    """Features for a single ticker."""

    symbol: str
    date: str

    # Final outcome
    final_max_gain: float
    final_volume: int
    tier: int
    tier_name: str

    # Price features
    gain_5min: float = 0.0
    gain_10min: float = 0.0
    gain_15min: float = 0.0
    gain_30min: float = 0.0
    gain_60min: float = 0.0

    price_acceleration_5min: float = 0.0
    price_acceleration_10min: float = 0.0
    price_volatility_5min: float = 0.0

    # Volume features
    volume_5min: int = 0
    volume_10min: int = 0
    volume_15min: int = 0
    volume_30min: int = 0

    trade_count_5min: int = 0
    trade_count_10min: int = 0
    trade_count_15min: int = 0

    trade_rate_5min: float = 0.0
    trade_rate_10min: float = 0.0
    avg_trade_size_5min: float = 0.0

    # Relative features
    rel_volume_5min: float = 0.0
    rel_gain_5min: float = 0.0

    # Time features
    time_to_10pct: float = 0.0  # minutes
    time_to_25pct: float = 0.0
    time_to_50pct: float = 0.0

    # Gap
    gap_pct: float = 0.0

    # Duration
    duration_minutes: float = 0.0
    time_to_peak: float = 0.0


def classify_tier(gain: float, volume: int) -> tuple[int, str]:
    """Classify ticker into tier based on gain and volume."""
    for tier, definition in TIER_DEFINITIONS.items():
        min_gain = definition.get("min_gain", 0)
        max_gain = definition.get("max_gain", float("inf"))
        min_volume = definition.get("min_volume", 0)

        if gain >= min_gain and gain < max_gain and volume >= min_volume:
            return tier, definition["name"]

    return 5, "Failed"


def extract_features(
    symbol: str,
    date: str,
    ticker_data: Any,
    session_start_ms: int,
    session_end_ms: int,
    gap_pct: float,
    median_volume_5min: float,
    median_gain_5min: float,
) -> TickerFeatures | None:
    """Extract comprehensive features for a ticker."""
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

    first_trade_ms = timestamps[0]
    duration_minutes = (timestamps[-1] - first_trade_ms) / 60000

    # Time windows
    windows = {
        "5min": 5 * 60 * 1000,
        "10min": 10 * 60 * 1000,
        "15min": 15 * 60 * 1000,
        "30min": 30 * 60 * 1000,
        "60min": 60 * 60 * 1000,
    }

    def get_window_stats(window_ms: int) -> dict:
        cutoff_ms = first_trade_ms + window_ms
        window_trades = [(ts, p, v) for ts, p, v in premarket_trades if ts <= cutoff_ms]

        if not window_trades:
            return {"gain": 0, "volume": 0, "trades": 0, "high": open_price}

        window_prices = [p for _, p, _ in window_trades]
        window_volumes = [v for _, _, v in window_trades]

        window_high = max(window_prices)
        window_gain = (window_high - open_price) / open_price * 100
        window_volume = sum(window_volumes)
        window_trades_count = len(window_trades)

        return {
            "gain": window_gain,
            "volume": window_volume,
            "trades": window_trades_count,
            "high": window_high,
        }

    # Get stats for each window
    stats_5min = get_window_stats(windows["5min"])
    stats_10min = get_window_stats(windows["10min"])
    stats_15min = get_window_stats(windows["15min"])
    stats_30min = get_window_stats(windows["30min"])
    stats_60min = get_window_stats(windows["60min"])

    # Price acceleration
    price_acceleration_5min = stats_5min["gain"] / 5 if stats_5min["gain"] > 0 else 0
    price_acceleration_10min = (
        stats_10min["gain"] / 10 if stats_10min["gain"] > 0 else 0
    )

    # Price volatility (std of returns in 5min)
    if stats_5min["trades"] > 1:
        window_5min_prices = [
            p for ts, p, _ in premarket_trades if ts <= first_trade_ms + windows["5min"]
        ]
        if len(window_5min_prices) > 1:
            returns = [
                (p2 - p1) / p1
                for p1, p2 in zip(window_5min_prices[:-1], window_5min_prices[1:])
            ]
            price_volatility_5min = float(np.std(returns)) * 100 if returns else 0
        else:
            price_volatility_5min = 0
    else:
        price_volatility_5min = 0

    # Trade rate
    trade_rate_5min = stats_5min["trades"] / 5
    trade_rate_10min = stats_10min["trades"] / 10

    # Average trade size
    avg_trade_size_5min = (
        stats_5min["volume"] / stats_5min["trades"] if stats_5min["trades"] > 0 else 0
    )

    # Relative features
    rel_volume_5min = (
        stats_5min["volume"] / median_volume_5min if median_volume_5min > 0 else 0
    )
    rel_gain_5min = stats_5min["gain"] / median_gain_5min if median_gain_5min > 0 else 0

    # Time to thresholds
    def find_time_to_threshold(threshold_pct: float) -> float:
        """Find time (in minutes) to reach threshold gain."""
        for ts, price, _ in premarket_trades:
            gain = (price - open_price) / open_price * 100
            if gain >= threshold_pct:
                return (ts - first_trade_ms) / 60000
        return 0.0  # Never reached

    time_to_10pct = find_time_to_threshold(10)
    time_to_25pct = find_time_to_threshold(25)
    time_to_50pct = find_time_to_threshold(50)

    # Time to peak
    peak_idx = prices.index(high_price)
    time_to_peak = (timestamps[peak_idx] - first_trade_ms) / 60000

    return TickerFeatures(
        symbol=symbol,
        date=date,
        final_max_gain=final_max_gain,
        final_volume=final_volume,
        tier=tier,
        tier_name=tier_name,
        gain_5min=stats_5min["gain"],
        gain_10min=stats_10min["gain"],
        gain_15min=stats_15min["gain"],
        gain_30min=stats_30min["gain"],
        gain_60min=stats_60min["gain"],
        price_acceleration_5min=price_acceleration_5min,
        price_acceleration_10min=price_acceleration_10min,
        price_volatility_5min=price_volatility_5min,
        volume_5min=stats_5min["volume"],
        volume_10min=stats_10min["volume"],
        volume_15min=stats_15min["volume"],
        volume_30min=stats_30min["volume"],
        trade_count_5min=stats_5min["trades"],
        trade_count_10min=stats_10min["trades"],
        trade_count_15min=stats_15min["trades"],
        trade_rate_5min=trade_rate_5min,
        trade_rate_10min=trade_rate_10min,
        avg_trade_size_5min=avg_trade_size_5min,
        rel_volume_5min=rel_volume_5min,
        rel_gain_5min=rel_gain_5min,
        time_to_10pct=time_to_10pct,
        time_to_25pct=time_to_25pct,
        time_to_50pct=time_to_50pct,
        gap_pct=gap_pct,
        duration_minutes=duration_minutes,
        time_to_peak=time_to_peak,
    )


def analyze_date(date: str) -> list[TickerFeatures]:
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

    # First pass: compute medians
    first_pass_features = []
    for symbol, ticker_data in ticker_data_map.items():
        if ticker_data.trades:
            premarket_trades = [
                (ts, price, vol)
                for ts, price, vol in ticker_data.trades
                if session_start_ms <= ts < session_end_ms
            ]
            if len(premarket_trades) >= 10:
                first_trade_ms = premarket_trades[0][0]
                cutoff_ms = first_trade_ms + 5 * 60 * 1000
                window_trades = [
                    (ts, p, v) for ts, p, v in premarket_trades if ts <= cutoff_ms
                ]
                if window_trades:
                    open_price = window_trades[0][1]
                    window_high = max(p for _, p, _ in window_trades)
                    window_gain = (window_high - open_price) / open_price * 100
                    window_volume = sum(v for _, _, v in window_trades)
                    first_pass_features.append((window_gain, window_volume))

    if not first_pass_features:
        return []

    median_gain_5min = np.median([f[0] for f in first_pass_features])
    median_volume_5min = np.median([f[1] for f in first_pass_features])

    # Second pass: extract full features
    features = []
    for symbol, ticker_data in ticker_data_map.items():
        candidate = next((c for c in candidates if c.symbol == symbol), None)
        gap_pct = candidate.gain_at_entry if candidate else 0

        feat = extract_features(
            symbol,
            date,
            ticker_data,
            session_start_ms,
            session_end_ms,
            gap_pct,
            median_volume_5min,
            median_gain_5min,
        )
        if feat:
            features.append(feat)

    return features


def compute_tier_statistics(
    features: list[TickerFeatures],
) -> dict[int, dict[str, Any]]:
    """Compute statistics for each tier."""
    tier_data = {}
    for tier in range(1, 6):
        tier_features = [f for f in features if f.tier == tier]
        if not tier_features:
            continue

        # Convert to dict for easier analysis
        df = pd.DataFrame(
            [
                {
                    "gain_5min": f.gain_5min,
                    "gain_10min": f.gain_10min,
                    "gain_15min": f.gain_15min,
                    "volume_5min": f.volume_5min,
                    "trade_rate_5min": f.trade_rate_5min,
                    "rel_volume_5min": f.rel_volume_5min,
                    "price_acceleration_5min": f.price_acceleration_5min,
                    "gap_pct": f.gap_pct,
                    "time_to_10pct": f.time_to_10pct,
                    "time_to_25pct": f.time_to_25pct,
                    "time_to_peak": f.time_to_peak,
                    "final_max_gain": f.final_max_gain,
                }
                for f in tier_features
            ]
        )

        tier_data[tier] = {
            "count": len(tier_features),
            "name": TIER_DEFINITIONS[tier]["name"],
            "stats": {
                col: {
                    "mean": float(df[col].mean()),
                    "median": float(df[col].median()),
                    "std": float(df[col].std()),
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                }
                for col in df.columns
            },
        }

    return tier_data


def statistical_comparison(tier_data: dict[int, dict], feature: str) -> dict:
    """Compare feature across tiers using statistical tests."""
    results = {}

    # Get data for each tier
    tier_values = {}
    for tier, data in tier_data.items():
        if feature in data["stats"]:
            # We need raw values, not just stats
            # For now, use mean as approximation
            tier_values[tier] = data["stats"][feature]["mean"]

    # ANOVA-like comparison (using means for now)
    if len(tier_values) >= 2:
        # Compare Tier 1 vs Tier 5 (Super vs Failed)
        if 1 in tier_values and 5 in tier_values:
            results["super_vs_failed"] = {
                "super_mean": tier_values[1],
                "failed_mean": tier_values[5],
                "ratio": tier_values[1] / tier_values[5] if tier_values[5] != 0 else 0,
            }

        # Compare Tier 1 vs Tier 2 (Super vs Big)
        if 1 in tier_values and 2 in tier_values:
            results["super_vs_big"] = {
                "super_mean": tier_values[1],
                "big_mean": tier_values[2],
                "ratio": tier_values[1] / tier_values[2] if tier_values[2] != 0 else 0,
            }

        # Compare Tier 2 vs Tier 3 (Big vs Medium)
        if 2 in tier_values and 3 in tier_values:
            results["big_vs_medium"] = {
                "big_mean": tier_values[2],
                "medium_mean": tier_values[3],
                "ratio": tier_values[2] / tier_values[3] if tier_values[3] != 0 else 0,
            }

    return results


def format_report(features: list[TickerFeatures], tier_data: dict) -> str:
    """Format comprehensive analysis report."""
    lines = [
        "# Big Winner Stratified Analysis Report",
        "",
        "## Tier Distribution",
        "",
        "| Tier | Name | Count | Gain Range | Volume Range |",
        "|------|------|-------|------------|--------------|",
    ]

    for tier in range(1, 6):
        if tier in tier_data:
            data = tier_data[tier]
            defn = TIER_DEFINITIONS[tier]
            gain_range = f"{defn.get('min_gain', 0)}-{defn.get('max_gain', '∞')}%"
            vol_range = f">{defn.get('min_volume', 0):,}"
            lines.append(
                f"| {tier} | {data['name']} | {data['count']} | {gain_range} | {vol_range} |"
            )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Feature Comparison by Tier",
            "",
        ]
    )

    # Key features to analyze
    key_features = [
        ("gain_5min", "Gain at 5min (%)"),
        ("gain_10min", "Gain at 10min (%)"),
        ("gain_15min", "Gain at 15min (%)"),
        ("volume_5min", "Volume at 5min"),
        ("trade_rate_5min", "Trade Rate (trades/min)"),
        ("rel_volume_5min", "Relative Volume"),
        ("price_acceleration_5min", "Price Acceleration (%/min)"),
        ("gap_pct", "Gap at Open (%)"),
        ("time_to_10pct", "Time to 10% gain (min)"),
        ("time_to_peak", "Time to Peak (min)"),
    ]

    for feature_key, feature_name in key_features:
        lines.extend(
            [
                f"### {feature_name}",
                "",
                "| Tier | Mean | Median | Std | Min | Max |",
                "|------|------|--------|-----|-----|-----|",
            ]
        )

        for tier in range(1, 6):
            if tier in tier_data and feature_key in tier_data[tier]["stats"]:
                stats = tier_data[tier]["stats"][feature_key]
                lines.append(
                    f"| {tier} | {stats['mean']:.2f} | {stats['median']:.2f} | "
                    f"{stats['std']:.2f} | {stats['min']:.2f} | {stats['max']:.2f} |"
                )

        # Add comparison
        comparison = statistical_comparison(tier_data, feature_key)
        if comparison:
            lines.extend(
                [
                    "",
                    "**Key Comparisons**:",
                ]
            )
            for comp_name, comp_data in comparison.items():
                lines.append(f"- {comp_name}: ratio = {comp_data['ratio']:.2f}x")

        lines.append("")

    # Tier-specific insights
    lines.extend(
        [
            "",
            "---",
            "",
            "## Tier-Specific Insights",
            "",
        ]
    )

    if 1 in tier_data:
        lines.extend(
            [
                "### Super Winners (Tier 1)",
                f"- Count: {tier_data[1]['count']}",
                f"- Average 5min gain: {tier_data[1]['stats']['gain_5min']['mean']:.1f}%",
                f"- Average trade rate: {tier_data[1]['stats']['trade_rate_5min']['mean']:.1f} trades/min",
                f"- Average time to peak: {tier_data[1]['stats']['time_to_peak']['mean']:.1f} min",
                "",
            ]
        )

    if 2 in tier_data:
        lines.extend(
            [
                "### Big Winners (Tier 2)",
                f"- Count: {tier_data[2]['count']}",
                f"- Average 5min gain: {tier_data[2]['stats']['gain_5min']['mean']:.1f}%",
                f"- Average trade rate: {tier_data[2]['stats']['trade_rate_5min']['mean']:.1f} trades/min",
                "",
            ]
        )

    if 3 in tier_data:
        lines.extend(
            [
                "### Medium Winners (Tier 3)",
                f"- Count: {tier_data[3]['count']}",
                f"- Average 5min gain: {tier_data[3]['stats']['gain_5min']['mean']:.1f}%",
                "",
            ]
        )

    # Summary
    lines.extend(
        [
            "",
            "---",
            "",
            "## Summary",
            "",
            "### Key Findings",
            "",
        ]
    )

    # Find distinguishing features
    distinguishing_features = []
    for feature_key, feature_name in key_features:
        comparison = statistical_comparison(tier_data, feature_key)
        if comparison and "super_vs_failed" in comparison:
            ratio = comparison["super_vs_failed"]["ratio"]
            if ratio > 2 or ratio < 0.5:  # Significant difference
                distinguishing_features.append((feature_name, ratio))

    if distinguishing_features:
        lines.append("**Most Distinguishing Features (Super vs Failed)**:")
        for name, ratio in sorted(
            distinguishing_features, key=lambda x: abs(x[1]), reverse=True
        ):
            lines.append(f"- {name}: {ratio:.2f}x difference")

    return "\n".join(lines)


def main():
    """Run stratified analysis."""
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

    all_features = []

    for i, date in enumerate(dates):
        logger.info(f"Analyzing {date} ({i+1}/{len(dates)})...")
        try:
            features = analyze_date(date)
            all_features.extend(features)
            logger.info(f"  Found {len(features)} tickers")
        except Exception as e:
            logger.error(f"  Error analyzing {date}: {e}")
            continue

    # Compute tier statistics
    tier_data = compute_tier_statistics(all_features)

    # Generate report
    report = format_report(all_features, tier_data)
    print("\n" + report)

    # Save report
    output_path = Path("reports/big_winner_stratified_analysis.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report)
    logger.info(f"\nReport saved to {output_path}")

    # Also save raw data as CSV
    df = pd.DataFrame(
        [
            {
                "symbol": f.symbol,
                "date": f.date,
                "tier": f.tier,
                "tier_name": f.tier_name,
                "final_max_gain": f.final_max_gain,
                "final_volume": f.final_volume,
                "gain_5min": f.gain_5min,
                "gain_10min": f.gain_10min,
                "gain_15min": f.gain_15min,
                "volume_5min": f.volume_5min,
                "trade_rate_5min": f.trade_rate_5min,
                "rel_volume_5min": f.rel_volume_5min,
                "price_acceleration_5min": f.price_acceleration_5min,
                "gap_pct": f.gap_pct,
                "time_to_10pct": f.time_to_10pct,
                "time_to_25pct": f.time_to_25pct,
                "time_to_peak": f.time_to_peak,
            }
            for f in all_features
        ]
    )

    csv_path = Path("reports/big_winner_stratified_data.csv")
    df.to_csv(csv_path, index=False)
    logger.info(f"Data saved to {csv_path}")


if __name__ == "__main__":
    main()
