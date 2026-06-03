"""CLI for Training Data Collection.

Usage:
    # Collect from signal_events
    poetry run python -m jerry_trader.services.training_data.cli collect --source signal_events

    # Collect from backtest
    poetry run python -m jerry_trader.services.training_data.cli collect --source backtest --dates 2026-03-11 2026-03-12

    # Export with train/test split
    poetry run python -m jerry_trader.services.training_data.cli export --input data/training_samples.parquet --output-dir data/training/v1

    # Show statistics
    poetry run python -m jerry_trader.services.training_data.cli stats --input data/training_samples.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from jerry_trader.services.training_data.collector import (
    TrainingDataCollector,
    format_stats_report,
)
from jerry_trader.services.training_data.exporter import TrainingDataExporter
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.paths import PROJECT_ROOT

logger = setup_logger("training_data_cli", log_to_file=True)


def cmd_collect(args):
    """Collect training data from specified source."""
    collector = TrainingDataCollector()

    if args.source == "signal_events":
        samples = collector.collect_from_signal_events(
            event_names=args.events.split(",") if args.events else None,
            limit=args.limit,
        )
    elif args.source == "backtest":
        dates = args.dates if args.dates else None
        samples = collector.collect_from_backtest(dates=dates)
    else:
        print(f"Unknown source: {args.source}")
        return 1

    if not samples:
        print("No samples collected")
        return 1

    # Export
    output_path = (
        Path(args.output)
        if args.output
        else PROJECT_ROOT / "data" / "training_samples.parquet"
    )
    count = collector.export(samples, output_path)

    print(f"Collected and exported {count} samples to {output_path}")

    # Show stats
    stats = collector.get_stats(samples)
    print("\n" + format_stats_report(stats))

    return 0


def cmd_export(args):
    """Export training data with train/test split."""
    import pandas as pd

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    # Load samples
    df = (
        pd.read_parquet(input_path)
        if input_path.suffix == ".parquet"
        else pd.read_csv(input_path)
    )

    # Convert to TrainingSample objects
    from jerry_trader.services.training_data.collector import TrainingSample

    samples = []
    for _, row in df.iterrows():
        factors = {
            f: row.get(f, 0.0)
            for f in [
                "relative_volume",
                "trade_rate",
                "price_direction",
                "gap_percent",
                "bid_ask_spread",
                "entry_gap_pct",
                "order_imbalance",
                "quote_rate",
            ]
        }
        samples.append(
            TrainingSample(
                sample_id=str(row.get("sample_id", "")),
                ticker=str(row.get("ticker", "")),
                trigger_time_ns=int(row.get("trigger_time_ns", 0)),
                event_name=str(row.get("event_name", "")),
                factors=factors,
                return_pct=float(row.get("return_pct", 0)),
            )
        )

    # Export with split
    exporter = TrainingDataExporter()
    output_dir = Path(args.output_dir)

    test_dates = args.test_dates.split(",") if args.test_dates else None

    result = exporter.export_with_split(
        samples,
        output_dir=output_dir,
        test_dates=test_dates,
        test_ratio=args.test_ratio,
    )

    print(f"Exported train: {result['train']}, test: {result['test']} to {output_dir}")
    return 0


def cmd_stats(args):
    """Show statistics for training data."""
    import pandas as pd

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    df = (
        pd.read_parquet(input_path)
        if input_path.suffix == ".parquet"
        else pd.read_csv(input_path)
    )

    stats = {
        "count": len(df),
        "tickers": df["ticker"].nunique() if "ticker" in df.columns else 0,
        "events": (
            df["event_name"].unique().tolist() if "event_name" in df.columns else []
        ),
        "return_stats": {
            "mean": df["return_pct"].mean() if "return_pct" in df.columns else 0,
            "std": df["return_pct"].std() if "return_pct" in df.columns else 0,
            "min": df["return_pct"].min() if "return_pct" in df.columns else 0,
            "max": df["return_pct"].max() if "return_pct" in df.columns else 0,
            "positive_ratio": (
                (df["return_pct"] > 0).mean() if "return_pct" in df.columns else 0
            ),
        },
        "factor_availability": {},
    }

    # Factor availability
    factors = [
        "relative_volume",
        "trade_rate",
        "price_direction",
        "gap_percent",
        "bid_ask_spread",
        "entry_gap_pct",
        "order_imbalance",
        "quote_rate",
    ]
    for factor in factors:
        if factor in df.columns:
            non_null = df[factor].notna().sum()
            stats["factor_availability"][factor] = {
                "available": non_null,
                "ratio": non_null / len(df),
            }

    print(format_stats_report(stats))
    return 0


def main():
    parser = argparse.ArgumentParser(description="Training Data CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # collect command
    collect_parser = subparsers.add_parser("collect", help="Collect training data")
    collect_parser.add_argument(
        "--source",
        choices=["signal_events", "backtest"],
        default="backtest",
        help="Data source",
    )
    collect_parser.add_argument(
        "--dates", nargs="+", help="Dates to collect (for backtest)"
    )
    collect_parser.add_argument("--events", help="Event names filter (comma-separated)")
    collect_parser.add_argument("--limit", type=int, default=10000, help="Max samples")
    collect_parser.add_argument("--output", "-o", help="Output file path")

    # export command
    export_parser = subparsers.add_parser("export", help="Export with train/test split")
    export_parser.add_argument("--input", "-i", required=True, help="Input file")
    export_parser.add_argument(
        "--output-dir", "-o", required=True, help="Output directory"
    )
    export_parser.add_argument("--test-dates", help="Test dates (comma-separated)")
    export_parser.add_argument(
        "--test-ratio", type=float, default=0.3, help="Test ratio"
    )

    # stats command
    stats_parser = subparsers.add_parser("stats", help="Show statistics")
    stats_parser.add_argument("--input", "-i", required=True, help="Input file")

    args = parser.parse_args()

    if args.command == "collect":
        return cmd_collect(args)
    elif args.command == "export":
        return cmd_export(args)
    elif args.command == "stats":
        return cmd_stats(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
