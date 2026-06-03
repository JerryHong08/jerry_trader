"""Training Data Collector - Collects factor-return pairs for ML training.

Collects training samples from:
1. signal_events table (real-time triggers)
2. backtest results (historical signals)
3. Live SignalEngine triggers (future)

Each sample contains:
- Factors at trigger time
- Actual return (computed via ReturnFiller)
- Metadata (event name, ticker, timestamp)

Usage:
    # Collect from signal_events
    collector = TrainingDataCollector()
    samples = collector.collect_from_signal_events()

    # Collect from backtest
    samples = collector.collect_from_backtest("2026-03-13")

    # Export for training
    collector.export(samples, "data/training_samples.parquet")
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = setup_logger("training_data", log_to_file=True)

# All factors that should be collected
ALL_FACTORS = [
    "relative_volume",
    "trade_rate",
    "price_direction",
    "gap_percent",
    "bid_ask_spread",
    "entry_gap_pct",
    "order_imbalance",
    "quote_rate",
    "vol_accel_5_15",
    "ema_20",
]

# Return horizons to collect
RETURN_HORIZONS = ["return_1m", "return_5m", "return_15m"]


@dataclass
class TrainingSample:
    """Single training sample with factors and return."""

    # Identifier
    sample_id: str
    ticker: str
    trigger_time_ns: int
    event_name: str

    # Features
    factors: dict[str, float]

    # Target
    return_pct: float  # Exit price return (30min later)
    max_return_pct: float = 0.0  # Max gain during hold period (for ML training)
    return_1m: Optional[float] = None
    return_15m: Optional[float] = None

    # Metadata
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    max_price: Optional[float] = None
    trigger_price: Optional[float] = None
    session_id: Optional[str] = None
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        """Convert to flat dict for DataFrame."""
        return {
            "sample_id": self.sample_id,
            "ticker": self.ticker,
            "trigger_time_ns": self.trigger_time_ns,
            "event_name": self.event_name,
            **self.factors,
            "return_pct": self.return_pct,
            "max_return_pct": self.max_return_pct,
            "return_1m": self.return_1m,
            "return_15m": self.return_15m,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "max_price": self.max_price,
            "trigger_price": self.trigger_price,
            "session_id": self.session_id,
            "collected_at": self.collected_at,
        }


class TrainingDataCollector:
    """Collects training samples from various sources.

    Sources:
    1. signal_events table - real-time triggers with computed returns
    2. backtest_results - historical backtest signals
    3. Live triggers - via callback from SignalEngine

    Example:
        collector = TrainingDataCollector()

        # Collect from signal_events
        samples = collector.collect_from_signal_events(
            min_return_horizon="return_5m"
        )

        # Collect from backtest
        samples = collector.collect_from_backtest(
            dates=["2026-03-11", "2026-03-12", "2026-03-13"]
        )

        # Export
        collector.export(samples, "data/training/v1.parquet")
    """

    def __init__(self, clickhouse_config: Optional[dict] = None):
        """Initialize collector.

        Args:
            clickhouse_config: ClickHouse connection config
        """
        self._ch_config = clickhouse_config
        self._ch_client = None

    def _get_client(self):
        """Get ClickHouse client."""
        if self._ch_client is None:
            from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

            self._ch_client = get_clickhouse_client(self._ch_config)
        return self._ch_client

    def collect_from_signal_events(
        self,
        event_names: Optional[list[str]] = None,
        min_return_horizon: str = "return_5m",
        limit: int = 10000,
    ) -> list[TrainingSample]:
        """Collect training samples from signal_events table.

        Args:
            event_names: Filter to specific events (None = all)
            min_return_horizon: Minimum return horizon required
            limit: Max samples to collect

        Returns:
            List of TrainingSample
        """
        ch = self._get_client()
        if not ch:
            logger.error("No ClickHouse client")
            return []

        # Build query
        where_clauses = [f"{min_return_horizon} IS NOT NULL"]
        if event_names:
            escaped = [f"'{e}'" for e in event_names]
            where_clauses.append(f"rule_id IN ({', '.join(escaped)})")

        query = f"""
            SELECT
                id, ticker, trigger_time, rule_id, trigger_price,
                factors, return_1m, return_5m, return_15m, session
            FROM signal_events FINAL
            WHERE {' AND '.join(where_clauses)}
            ORDER BY trigger_time DESC
            LIMIT {limit}
        """

        try:
            result = ch.query(query)
            samples = []

            for row in result.result_rows:
                sample = self._parse_signal_event_row(row, min_return_horizon)
                if sample:
                    samples.append(sample)

            logger.info(f"Collected {len(samples)} samples from signal_events")
            return samples

        except Exception as e:
            logger.error(f"Failed to collect from signal_events: {e}")
            return []

    def collect_from_backtest(
        self,
        dates: Optional[list[str]] = None,
        min_return_horizon: str = "return_5m",
    ) -> list[TrainingSample]:
        """Collect training samples from backtest results.

        Uses the backtest explorer to collect signals with full factor set.

        Args:
            dates: List of dates to collect (None = all available)
            min_return_horizon: Which return column to use as target

        Returns:
            List of TrainingSample
        """
        from jerry_trader.services.backtest.dense_signal_collector import (
            collect_signals,
        )

        samples = []

        # Default to all available dates
        if dates is None:
            dates = [
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

        for date in dates:
            try:
                df = collect_signals(date)
                if df.empty:
                    continue

                # Filter valid signals
                valid = df[df["relative_volume"] > 0].dropna(subset=["return_pct"])

                for _, row in valid.iterrows():
                    factors = {f: row.get(f, 0.0) for f in ALL_FACTORS}

                    sample = TrainingSample(
                        sample_id=f"bt_{date}_{row.name}",
                        ticker=row.get("ticker", "UNKNOWN"),
                        trigger_time_ns=int(
                            row.get("entry_time_ms", 0) * 1_000_000
                        ),  # ms to ns
                        event_name=row.get("event_name", "backtest"),
                        factors=factors,
                        return_pct=float(row.get("return_pct", 0)),  # Exit price return
                        max_return_pct=float(
                            row.get("max_return_pct", 0)
                        ),  # Max gain during hold
                        return_1m=(
                            float(row.get("return_1m", 0))
                            if "return_1m" in row
                            else None
                        ),
                        return_15m=(
                            float(row.get("return_15m", 0))
                            if "return_15m" in row
                            else None
                        ),
                        entry_price=row.get("entry_price"),
                        exit_price=row.get("exit_price"),
                        max_price=row.get("max_price"),
                        trigger_price=row.get("trigger_price"),
                    )
                    samples.append(sample)

                logger.info(f"Collected {len(valid)} samples from {date}")

            except Exception as e:
                logger.warning(f"Failed to collect from {date}: {e}")
                continue

        logger.info(f"Total collected: {len(samples)} samples from backtest")
        return samples

    def _parse_signal_event_row(
        self,
        row: tuple,
        min_return_horizon: str,
    ) -> Optional[TrainingSample]:
        """Parse a signal_events row into TrainingSample."""
        try:
            (
                sample_id,
                ticker,
                trigger_time,
                rule_id,
                trigger_price,
                factors_json,
                return_1m,
                return_5m,
                return_15m,
                session,
            ) = row

            # Parse factors JSON
            factors = json.loads(factors_json) if factors_json else {}

            # Get primary return
            return_map = {
                "return_1m": return_1m,
                "return_5m": return_5m,
                "return_15m": return_15m,
            }
            primary_return = return_map.get(min_return_horizon)
            if primary_return is None:
                return None

            return TrainingSample(
                sample_id=str(sample_id),
                ticker=str(ticker),
                trigger_time_ns=int(trigger_time),
                event_name=str(rule_id),
                factors=factors,
                return_pct=float(primary_return),
                return_1m=float(return_1m) if return_1m else None,
                return_15m=float(return_15m) if return_15m else None,
                trigger_price=float(trigger_price) if trigger_price else None,
                session_id=str(session) if session else None,
            )

        except Exception as e:
            logger.warning(f"Failed to parse row: {e}")
            return None

    def export(
        self,
        samples: list[TrainingSample],
        output_path: str | Path,
        format: str = "parquet",
    ) -> int:
        """Export samples to file.

        Args:
            samples: List of TrainingSample
            output_path: Output file path
            format: Output format (parquet, csv)

        Returns:
            Number of samples exported
        """
        if not samples:
            logger.warning("No samples to export")
            return 0

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to DataFrame
        df = pd.DataFrame([s.to_dict() for s in samples])

        # Export
        if format == "parquet":
            df.to_parquet(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)

        logger.info(f"Exported {len(samples)} samples to {output_path}")
        return len(samples)

    def get_stats(self, samples: list[TrainingSample]) -> dict:
        """Compute statistics for training samples.

        Args:
            samples: List of TrainingSample

        Returns:
            Dict with statistics
        """
        if not samples:
            return {"count": 0}

        df = pd.DataFrame([s.to_dict() for s in samples])

        stats = {
            "count": len(samples),
            "tickers": df["ticker"].nunique(),
            "events": df["event_name"].unique().tolist(),
            "date_range": {
                "min": df["trigger_time_ns"].min(),
                "max": df["trigger_time_ns"].max(),
            },
            "return_stats": {
                "mean": df["return_pct"].mean(),
                "std": df["return_pct"].std(),
                "min": df["return_pct"].min(),
                "max": df["return_pct"].max(),
                "positive_ratio": (df["return_pct"] > 0).mean(),
            },
            "factor_availability": {},
        }

        # Factor availability
        for factor in ALL_FACTORS:
            if factor in df.columns:
                non_null = df[factor].notna().sum()
                stats["factor_availability"][factor] = {
                    "available": non_null,
                    "ratio": non_null / len(samples),
                }

        return stats


def format_stats_report(stats: dict) -> str:
    """Format stats as readable report."""
    lines = [
        "# Training Data Statistics",
        "",
        f"- Total samples: {stats['count']}",
        f"- Unique tickers: {stats['tickers']}",
        f"- Events: {', '.join(stats['events'])}",
        "",
        "## Return Statistics",
        f"- Mean: {stats['return_stats']['mean']:.2%}",
        f"- Std: {stats['return_stats']['std']:.2%}",
        f"- Min: {stats['return_stats']['min']:.2%}",
        f"- Max: {stats['return_stats']['max']:.2%}",
        f"- Positive ratio: {stats['return_stats']['positive_ratio']:.1%}",
        "",
        "## Factor Availability",
    ]

    for factor, info in stats.get("factor_availability", {}).items():
        lines.append(
            f"- {factor}: {info['ratio']:.1%} ({info['available']}/{stats['count']})"
        )

    return "\n".join(lines)


__all__ = [
    "TrainingDataCollector",
    "TrainingSample",
    "ALL_FACTORS",
    "format_stats_report",
]
