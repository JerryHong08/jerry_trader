"""CLI for backtest data operations.

Usage:
    # Check data readiness
    poetry run python -m jerry_trader.services.backtest.data.cli check --date 2026-03-13

    # Download from Polygon
    poetry run python -m jerry_trader.services.backtest.data.cli download --date 2026-03-13

    # Convert CSV.gz to Parquet
    poetry run python -m jerry_trader.services.backtest.data.cli convert --date 2026-03-13

    # Build snapshot in ClickHouse
    poetry run python -m jerry_trader.services.backtest.data.cli build-snapshot --date 2026-03-13

    # Full prepare pipeline (download + convert + check + build-snapshot)
    poetry run python -m jerry_trader.services.backtest.data.cli prepare --date 2026-03-13
"""

from __future__ import annotations

import argparse
import sys

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.data.cli", log_to_file=True)


def _get_ch_client(database: str = "jerry_trader"):
    """Get ClickHouse client from environment."""
    import os

    from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

    config = {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "database": database,
        "password_env": os.getenv("CLICKHOUSE_PASSWORD_ENV", "CLICKHOUSE_PASSWORD"),
    }
    return get_clickhouse_client(config)


def cmd_check(args):
    """Check data readiness for a date or date range."""
    from jerry_trader.services.backtest.data.checker import (
        check_date,
        check_date_range,
        print_check_result,
        print_date_range_summary,
    )

    ch_client = _get_ch_client() if not args.no_ch else None

    if args.end_date:
        # Date range check
        results = check_date_range(args.date, args.end_date, ch_client=ch_client)
        print_date_range_summary(results)
    else:
        # Single date check
        result = check_date(args.date, ch_client=ch_client)
        print_check_result(result)
    sys.exit(0)


def cmd_download(args):
    """Download data from Polygon."""
    from jerry_trader.services.backtest.data.downloader import download_date_range

    data_types = args.types.split(",") if args.types else None
    results = download_date_range(
        start_date=args.date,
        end_date=args.end_date or args.date,
        data_types=data_types,
        max_workers=args.workers,
    )
    print(f"\nDownloaded {len(results)} files")


def cmd_convert(args):
    """Convert CSV.gz to Parquet."""
    from jerry_trader.services.backtest.data.converter import convert_date_range

    data_types = args.types.split(",") if args.types else None
    results = convert_date_range(
        start_date=args.date,
        end_date=args.end_date or args.date,
        data_types=data_types,
    )
    print(f"\nConverted {len(results)} files")


def cmd_build_snapshot(args):
    """Build market_snapshot from trades Parquet."""
    from jerry_trader.platform.config.session import make_session_id
    from jerry_trader.services.backtest.data.snapshot_builder import build_and_insert

    ch_client = _get_ch_client()
    if ch_client is None:
        print(
            "ERROR: ClickHouse client not available. Set CLICKHOUSE_PASSWORD env var."
        )
        sys.exit(1)

    mode = args.mode
    session_id = args.session_id
    if not session_id:
        date_compact = args.date.replace("-", "")
        session_id = make_session_id(
            replay_date=date_compact if mode == "replay" else None,
        )

    ranked_count, collector_count = build_and_insert(
        date=args.date,
        ch_client=ch_client,
        database=args.database,
        window_ms=args.window_ms,
        mode=mode,
        session_id=session_id,
        filter_start_et=args.start_et,
        filter_end_et=args.end_et,
    )
    print(
        f"\nInserted {ranked_count:,} ranked + {collector_count:,} collector rows "
        f"(mode={mode}, session={session_id}, {args.start_et}-{args.end_et} ET)"
    )


def cmd_prepare(args):
    """Full pipeline: download → convert → check → build-snapshot."""
    from jerry_trader.services.backtest.data.checker import (
        DataStatus,
        check_date,
        print_check_result,
    )
    from jerry_trader.services.backtest.data.converter import convert_date_range
    from jerry_trader.services.backtest.data.downloader import download_date_range

    date = args.date
    data_types = ["trades_v1", "quotes_v1", "day_aggs_v1"]
    ch_client = _get_ch_client() if not args.no_ch else None

    print(f"\n{'=' * 60}")
    print(f"  PREPARE — {date}")
    print(f"{'=' * 60}")

    # Step 1: Download
    print(f"\n[1/4] Downloading...")
    try:
        download_date_range(date, date, data_types=data_types, max_workers=2)
    except Exception as e:
        print(f"  Download failed (non-fatal): {e}")

    # Step 2: Convert
    print(f"\n[2/4] Converting...")
    convert_date_range(date, date, data_types=["trades_v1", "quotes_v1"])

    # Step 3: Check
    print(f"\n[3/4] Checking...")
    result = check_date(date, ch_client=ch_client)
    print_check_result(result)

    # Step 4: Build snapshot if missing
    if result.snapshot.status != DataStatus.READY and ch_client:
        print(f"\n[4/4] Building snapshot...")
        from jerry_trader.services.backtest.data.snapshot_builder import (
            build_and_insert,
        )

        build_and_insert(date, ch_client, database=args.database)
    else:
        print(f"\n[4/4] Snapshot already exists — skipping")

    print(f"\n{'=' * 60}")
    print(f"  PREPARE COMPLETE — {result.summary}")
    print(f"{'=' * 60}\n")


def cmd_estimate(args):
    """Estimate download size and check disk space (dry run)."""
    from jerry_trader.services.backtest.data.downloader import print_download_estimate

    data_types = args.types.split(",") if args.types else None
    print_download_estimate(
        start_date=args.date,
        end_date=args.end_date or args.date,
        data_types=data_types,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Jerry Trader — Backtest Data Tools",
    )
    subparsers = parser.add_subparsers(dest="command")

    # Common args
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")
    common.add_argument(
        "--no-ch", action="store_true", help="Skip ClickHouse operations"
    )

    # check
    p_check = subparsers.add_parser(
        "check", parents=[common], help="Check data readiness"
    )
    p_check.add_argument(
        "--end-date", help="End date for range check (default: single date)"
    )
    p_check.set_defaults(func=cmd_check)

    # download
    p_dl = subparsers.add_parser(
        "download", parents=[common], help="Download from Polygon"
    )
    p_dl.add_argument("--end-date", help="End date for range (default: same as --date)")
    p_dl.add_argument(
        "--types",
        help="Comma-separated data types (default: trades_v1,quotes_v1,day_aggs_v1)",
    )
    p_dl.add_argument(
        "--workers", type=int, default=2, help="Parallel download workers"
    )
    p_dl.set_defaults(func=cmd_download)

    # convert
    p_cv = subparsers.add_parser(
        "convert", parents=[common], help="Convert CSV.gz to Parquet"
    )
    p_cv.add_argument("--end-date", help="End date for range (default: same as --date)")
    p_cv.add_argument(
        "--types", help="Comma-separated data types (default: trades_v1,quotes_v1)"
    )
    p_cv.set_defaults(func=cmd_convert)

    # build-snapshot
    p_bs = subparsers.add_parser(
        "build-snapshot", parents=[common], help="Build market_snapshot in ClickHouse"
    )
    p_bs.add_argument("--database", default="jerry_trader", help="ClickHouse database")
    p_bs.add_argument(
        "--window-ms", type=int, default=5000, help="Snapshot window in ms"
    )
    p_bs.add_argument(
        "--mode", default="replay", help="Mode tag (live/replay, default: replay)"
    )
    p_bs.add_argument(
        "--session-id", default="", help="Session ID (default: auto from date+mode)"
    )
    p_bs.add_argument(
        "--start-et",
        default="04:00",
        help="Start time filter ET (HH:MM, default: 04:00)",
    )
    p_bs.add_argument(
        "--end-et", default="09:30", help="End time filter ET (HH:MM, default: 09:30)"
    )
    p_bs.set_defaults(func=cmd_build_snapshot)

    # prepare (full pipeline)
    p_prep = subparsers.add_parser(
        "prepare", parents=[common], help="Full prepare pipeline"
    )
    p_prep.add_argument(
        "--database", default="jerry_trader", help="ClickHouse database"
    )
    p_prep.set_defaults(func=cmd_prepare)

    # estimate (dry run)
    p_est = subparsers.add_parser(
        "estimate",
        parents=[common],
        help="Estimate download size and disk space (dry run)",
    )
    p_est.add_argument(
        "--end-date", help="End date for range (default: same as --date)"
    )
    p_est.add_argument(
        "--types",
        help="Comma-separated data types (default: trades_v1,quotes_v1,day_aggs_v1)",
    )
    p_est.set_defaults(func=cmd_estimate)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
