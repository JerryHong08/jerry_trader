"""CLI entry point for backtest.

Usage:
    # Single date
    poetry run python -m jerry_trader.services.backtest.cli --date 2026-03-13

    # With custom rules dir
    poetry run python -m jerry_trader.services.backtest.cli \
        --date 2026-03-13 --rules config/rules/

    # Date range
    poetry run python -m jerry_trader.services.backtest.cli \
        --date-range 2026-03-01 2026-03-10

    # Console only (no ClickHouse write)
    poetry run python -m jerry_trader.services.backtest.cli \
        --date 2026-03-13 --no-ch

    # With custom slippage and gain threshold
    poetry run python -m jerry_trader.services.backtest.cli \
        --date 2026-03-13 --slippage 0.002 --gain-threshold 3.0
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

from jerry_trader.services.backtest.config import BacktestConfig
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.cli", log_to_file=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Jerry Trader — Batch Backtest Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Date selection (mutually exclusive)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--date",
        type=str,
        help="Backtest date in YYYY-MM-DD format",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range (inclusive) in YYYY-MM-DD format",
    )

    # Rules
    parser.add_argument(
        "--rules",
        type=str,
        default="config/rules/",
        help="Directory containing DSL rule YAML files (default: config/rules/)",
    )

    # Filtering
    parser.add_argument(
        "--gain-threshold",
        type=float,
        default=2.0,
        help="Minimum gain %% for pre-filter (default: 2.0)",
    )

    # Slippage
    parser.add_argument(
        "--slippage",
        type=float,
        default=0.001,
        help="Slippage buffer as decimal (default: 0.001 = 0.1%%)",
    )

    # Output
    parser.add_argument(
        "--no-ch",
        action="store_true",
        help="Skip ClickHouse persistence (console output only)",
    )
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Skip console output",
    )

    # Data paths
    parser.add_argument(
        "--trades-dir",
        type=str,
        default="",
        help="Directory containing trades Parquet files",
    )
    parser.add_argument(
        "--quotes-dir",
        type=str,
        default="",
        help="Directory containing quotes Parquet files",
    )

    return parser.parse_args(argv)


def _get_clickhouse_config() -> dict | None:
    """Load ClickHouse config from environment."""
    import os

    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    database = os.getenv("CLICKHOUSE_DATABASE", "jerry_trader")
    password_env = os.getenv("CLICKHOUSE_PASSWORD_ENV", "CLICKHOUSE_PASSWORD")

    password = os.getenv(password_env, "")
    if not password:
        logger.warning("No ClickHouse password configured")
        return None

    return {
        "host": host,
        "port": port,
        "user": user,
        "database": database,
        "password_env": password_env,
    }


def _get_ch_client(config: dict | None):
    """Create ClickHouse client from config."""
    if not config:
        return None

    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    return get_clickhouse_client(config)


def run_single_date(
    date: str,
    args: argparse.Namespace,
) -> None:
    """Run backtest for a single date."""
    from jerry_trader.services.backtest.runner import BacktestRunner

    ch_config = None if args.no_ch else _get_clickhouse_config()
    ch_client = _get_ch_client(ch_config)

    config = BacktestConfig(
        date=date,
        rules_dir=args.rules,
        gain_threshold=args.gain_threshold,
        slippage_buffer=args.slippage,
        output_clickhouse=not args.no_ch and ch_client is not None,
        output_console=not args.no_console,
        clickhouse_config=ch_config,
        trades_dir=args.trades_dir,
        quotes_dir=args.quotes_dir,
    )

    runner = BacktestRunner(config, ch_client=ch_client)
    result = runner.run()

    if result.total_signals == 0:
        logger.info(f"No signals triggered for {date}")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.date:
        run_single_date(args.date, args)
    elif args.date_range:
        start = datetime.strptime(args.date_range[0], "%Y-%m-%d")
        end = datetime.strptime(args.date_range[1], "%Y-%m-%d")

        current = start
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            logger.info(f"=== Running backtest for {date_str} ===")
            try:
                run_single_date(date_str, args)
            except Exception as e:
                logger.error(f"Backtest failed for {date_str}: {e}")
            current += timedelta(days=1)


if __name__ == "__main__":
    main()
