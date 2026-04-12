"""CLI entry point for backtest.

Usage:
    # Single date backtest
    poetry run python -m jerry_trader.services.backtest.cli --date 2026-03-13

    # With custom rules dir
    poetry run python -m jerry_trader.services.backtest.cli \
        --date 2026-03-13 --rules config/rules/

    # Date range
    poetry run python -m jerry_trader.services.backtest.cli \
        --date-range 2026-03-01 2026-03-10

    # Dry-run (candidates only, no factor/signal)
    poetry run python -m jerry_trader.services.backtest.cli \
        --date 2026-03-13 --candidates-only

    # With custom slippage and gain threshold
    poetry run python -m jerry_trader.services.backtest.cli \
        --date 2026-03-13 --slippage 0.002 --gain-threshold 3.0

    # Strategy mining (parameter sweep)
    poetry run python -m jerry_trader.services.backtest.cli mining --date 2026-03-13

    # Mining with custom thresholds
    poetry run python -m jerry_trader.services.backtest.cli mining \
        --date 2026-03-13 --min-rate 50 --max-rate 300 --step 25
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from jerry_trader.services.backtest.config import BacktestConfig
from jerry_trader.shared.logging.logger import setup_logger

if TYPE_CHECKING:
    from jerry_trader.domain.backtest.types import BacktestResult

logger = setup_logger("backtest.cli", log_to_file=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Jerry Trader — Batch Backtest Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Subcommands: backtest (default) or mining
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # === Backtest subcommand (default behavior) ===
    backtest_parser = subparsers.add_parser("backtest", help="Run backtest pipeline")
    _add_backtest_args(backtest_parser)

    # === Mining subcommand ===
    mining_parser = subparsers.add_parser("mining", help="Strategy parameter mining")
    _add_mining_args(mining_parser)

    # Legacy: support direct args without subcommand (backtest is default)
    # If no subcommand specified, parse as backtest
    if argv is None:
        argv = sys.argv[1:]

    if not argv or argv[0] not in ["backtest", "mining"]:
        # Legacy mode: treat as backtest args
        legacy_parser = argparse.ArgumentParser(
            description="Jerry Trader — Batch Backtest Pipeline (legacy mode)",
        )
        _add_backtest_args(legacy_parser)
        return legacy_parser.parse_args(argv)

    return parser.parse_args(argv)


def _add_backtest_args(parser: argparse.ArgumentParser) -> None:
    """Add backtest-specific arguments to parser."""

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
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Rank threshold for candidate selection (default: 20)",
    )
    parser.add_argument(
        "--all-top-n",
        action="store_true",
        help="Include stocks already in top N at session start (default: only new entries)",
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
        "--no-console",
        action="store_true",
        help="Skip console output",
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Show factor values at trigger time for each signal",
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

    # Dry-run
    parser.add_argument(
        "--candidates-only",
        action="store_true",
        help="Dry-run: show pre-filter candidates without running factor/signal pipeline",
    )

    # Experiment logging
    parser.add_argument(
        "--record-experiment",
        action="store_true",
        help="Record this backtest as a structured experiment log",
    )
    parser.add_argument(
        "--hypothesis",
        type=str,
        default="",
        help="Hypothesis statement for experiment log (required with --record-experiment)",
    )
    parser.add_argument(
        "--experiment-type",
        type=str,
        choices=[
            "parameter_optimization",
            "factor_test",
            "rule_combination",
            "data_validation",
        ],
        default="parameter_optimization",
        help="Type of experiment (default: parameter_optimization)",
    )

    # Parallel execution
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run dates in parallel (only for date range)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4, max: number of dates)",
    )


def _add_mining_args(parser: argparse.ArgumentParser) -> None:
    """Add mining-specific arguments to parser."""

    # Date (single only for now)
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Backtest date in YYYY-MM-DD format",
    )

    # Threshold range
    parser.add_argument(
        "--min-rate",
        type=float,
        default=100.0,
        help="Minimum trade_rate threshold (default: 100)",
    )
    parser.add_argument(
        "--max-rate",
        type=float,
        default=400.0,
        help="Maximum trade_rate threshold (default: 400)",
    )
    parser.add_argument(
        "--step",
        type=float,
        default=50.0,
        help="Step size for threshold sweep (default: 50)",
    )

    # Filtering
    parser.add_argument(
        "--gain-threshold",
        type=float,
        default=2.0,
        help="Minimum gain %% for pre-filter (default: 2.0)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Rank threshold for candidate selection (default: 20)",
    )

    # Evaluation criteria
    parser.add_argument(
        "--min-signals",
        type=int,
        default=3,
        help="Minimum signals to consider strategy valid (default: 3)",
    )

    # Experiment recording
    parser.add_argument(
        "--record-experiment",
        action="store_true",
        default=True,
        help="Record results to experiments/ directory (default: True)",
    )
    parser.add_argument(
        "--no-record",
        action="store_true",
        help="Skip experiment recording (just print results)",
    )


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
    ch_client=None,
    ch_config: dict | None = None,
) -> "BacktestResult":
    """Run backtest for a single date."""
    from jerry_trader.services.backtest.config import PreFilterConfig
    from jerry_trader.services.backtest.runner import BacktestRunner

    ch_config = _get_clickhouse_config()
    ch_client = _get_ch_client(ch_config)

    if ch_client is None:
        logger.error("No ClickHouse client — cannot run backtest")
        print("ERROR: ClickHouse connection required. Set CLICKHOUSE_PASSWORD env var.")
        return

    pre_filter = PreFilterConfig(
        top_n=args.top_n,
        min_gain_pct=args.gain_threshold,
        new_entry_only=not args.all_top_n,  # --all-top-n disables new_entry_only
    )

    config = BacktestConfig(
        date=date,
        rules_dir=args.rules,
        gain_threshold=args.gain_threshold,
        slippage_buffer=args.slippage,
        output_clickhouse=ch_client is not None,
        output_console=not args.no_console,
        clickhouse_config=ch_config,
        trades_dir=args.trades_dir,
        quotes_dir=args.quotes_dir,
        candidates_only=args.candidates_only,
        detailed=args.detailed,
        pre_filter=pre_filter,
    )

    runner = BacktestRunner(config, ch_client=ch_client)
    result = runner.run()

    # Skip "no signals" message in dry-run mode (candidates-only)
    if result.total_signals == 0 and not config.candidates_only:
        logger.info(f"No signals triggered for {date}")

    # Record experiment if flag is set
    if args.record_experiment and not config.candidates_only:
        _record_experiment(date, args, result)

    return result


def _record_experiment(
    date: str,
    args: argparse.Namespace,
    result: "BacktestResult",
) -> str:
    """Record backtest result in simplified experiment log.

    Returns:
        experiment_id
    """
    from jerry_trader.services.backtest.experiment_logger import ExperimentLogger

    logger_exp = ExperimentLogger()

    # Build hypothesis
    hypothesis = args.hypothesis or f"Backtest on {date}"

    # Build CLI command for reproducibility
    cli_parts = [
        "poetry run python -m jerry_trader.services.backtest.cli",
        f"--date {date}",
        f"--gain-threshold {args.gain_threshold}",
        f"--top-n {args.top_n}",
    ]
    if args.detailed:
        cli_parts.append("--detailed")
    if args.all_top_n:
        cli_parts.append("--all-top-n")
    cli_command = " ".join(cli_parts)

    # Generate lessons from result
    win_rate_10m = result.win_rate.get("10m", 0) * 100
    lessons = [
        f"{result.total_signals} signals, {win_rate_10m:.0f}% win rate on {date}",
        "Single date may be outlier — need multi-date validation",
    ]

    # Add ticker-specific lessons if available
    if hasattr(result, "signals") and result.signals:
        # Find best and worst tickers
        ticker_stats = {}
        for sig in result.signals:
            ticker = sig.symbol
            if ticker not in ticker_stats:
                ticker_stats[ticker] = {"signals": 0, "wins": 0}
            ticker_stats[ticker]["signals"] += 1
            if sig.returns and sig.returns.get("10m", 0) > 0:
                ticker_stats[ticker]["wins"] += 1

        # Find interesting patterns
        for ticker, stats in ticker_stats.items():
            if stats["signals"] >= 2:
                win_pct = stats["wins"] / stats["signals"] * 100
                if win_pct >= 50:
                    lessons.append(f"{ticker} {win_pct:.0f}% win — success pattern")
                elif win_pct == 0:
                    lessons.append(f"{ticker} 0% win — failure pattern")

    # Record experiment (simplified)
    exp_id = logger_exp.record_experiment(
        hypothesis=hypothesis,
        date=date,
        cli_command=cli_command,
        result=result,
        lessons=lessons,
    )

    print(f"\nExperiment recorded: {exp_id}")
    print(f"  Location: experiments/{logger_exp.daily_dir.name}/")

    return exp_id


def _run_single_date_worker(
    date: str,
    args_dict: dict,
    ch_config: dict | None,
) -> tuple[str, "BacktestResult"]:
    """Worker function for parallel execution.

    Creates its own CH client (can't share across processes).
    Returns (date, result) tuple for aggregation.
    """
    import argparse

    # Reconstruct args from dict
    args = argparse.Namespace(**args_dict)

    ch_client = _get_ch_client(ch_config) if ch_config else None
    result = run_single_date(date, args, ch_client=ch_client, ch_config=ch_config)
    return (date, result)


def run_mining(args: argparse.Namespace) -> None:
    """Run strategy mining for a single date.

    Results are printed to console and optionally recorded to experiments/
    """
    from datetime import datetime

    from jerry_trader.services.backtest.mining import MiningConfig, StrategyMiner

    config = MiningConfig(
        trade_rate_range=(args.min_rate, args.max_rate, args.step),
        gain_threshold=args.gain_threshold,
        top_n=args.top_n,
        min_signals=args.min_signals,
    )

    miner = StrategyMiner(config)
    record = args.record_experiment and not args.no_record
    results = miner.mine(args.date, record_experiment=record)

    # Print summary to console
    report = miner.report(results)
    print(report)

    if record and results:
        print(
            f"\nResults recorded to experiments/{datetime.now().strftime('%Y-%m-%d')}/"
        )
        print("See knowledge.yaml for accumulated lessons")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Dispatch to appropriate subcommand
    if hasattr(args, "command") and args.command == "mining":
        run_mining(args)
        return

    # Backtest (default or explicit)
    if args.date:
        run_single_date(args.date, args)
    elif args.date_range:
        start = datetime.strptime(args.date_range[0], "%Y-%m-%d")
        end = datetime.strptime(args.date_range[1], "%Y-%m-%d")

        # CH connection required for date range
        ch_config = _get_clickhouse_config()

        dates = []
        current = start
        while current <= end:
            # Skip weekends
            if current.weekday() < 5:
                dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        logger.info(
            f"=== Date range backtest: {dates[0]} to {dates[-1]} ({len(dates)} trading days) ==="
        )

        all_results = []

        # Parallel execution
        if args.parallel and len(dates) > 1:
            from concurrent.futures import ProcessPoolExecutor, as_completed

            # Convert args to dict (can't pickle Namespace)
            args_dict = {
                "rules": args.rules,
                "gain_threshold": args.gain_threshold,
                "top_n": args.top_n,
                "all_top_n": args.all_top_n,
                "slippage": args.slippage,
                "no_console": args.no_console,
                "trades_dir": args.trades_dir,
                "quotes_dir": args.quotes_dir,
                "candidates_only": args.candidates_only,
                "detailed": args.detailed,
            }

            workers = args.workers or min(4, len(dates))
            logger.info(f"  Running with {workers} parallel workers...")

            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        _run_single_date_worker, date, args_dict, ch_config
                    ): date
                    for date in dates
                }
                for future in as_completed(futures):
                    date = futures[future]
                    try:
                        date_str, result = future.result()
                        all_results.append((date_str, result))
                        logger.info(f"  ✓ {date_str}: {result.total_signals} signals")
                    except Exception as e:
                        logger.error(f"Backtest failed for {date}: {e}")

        # Serial execution (default)
        else:
            ch_client = _get_ch_client(ch_config)
            for i, date_str in enumerate(dates):
                logger.info(
                    f"=== [{i+1}/{len(dates)}] Running backtest for {date_str} ==="
                )
                try:
                    result = run_single_date(
                        date_str, args, ch_client=ch_client, ch_config=ch_config
                    )
                    all_results.append((date_str, result))
                except Exception as e:
                    logger.error(f"Backtest failed for {date_str}: {e}")

        # Cross-date summary (sort by date)
        all_results.sort(key=lambda x: x[0])
        if len(all_results) > 1:
            total_signals = sum(r.total_signals for _, r in all_results)
            dates_with_signals = sum(1 for _, r in all_results if r.total_signals > 0)
            print(f"\n{'='*70}")
            print(f"  DATE RANGE SUMMARY: {dates[0]} — {dates[-1]} ({len(dates)} days)")
            print(f"{'='*70}")
            print(f"  Dates with signals: {dates_with_signals}/{len(all_results)}")
            print(f"  Total signals:      {total_signals}")
            print()
            print(
                f"  {'Date':12s} {'Signals':>8s} {'WinRate':>10s} {'AvgRet':>10s} {'PF':>8s}"
            )
            print(f"  {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*8}")
            for date_str, r in all_results:
                wr = f"{list(r.win_rate.values())[0]*100:.0f}%" if r.win_rate else "—"
                ar = f"{list(r.avg_return.values())[0]:.2f}%" if r.avg_return else "—"
                pf = f"{r.profit_factor:.2f}" if r.profit_factor else "—"
                print(
                    f"  {date_str:12s} {r.total_signals:>8d} {wr:>10s} {ar:>10s} {pf:>8s}"
                )
            print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
