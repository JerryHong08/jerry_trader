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
from datetime import datetime
from pathlib import Path

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
        verbose_check,
    )

    ch_client = _get_ch_client() if not args.no_ch else None

    # Verbose mode (single date only)
    if args.verbose and not args.end_date:
        result = check_date(args.date, ch_client=ch_client)
        print_check_result(result)
        if result.snapshot.status.name == "READY":
            verbose_check(args.date, ch_client, database=args.database)
        sys.exit(0)

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
    """Build market_snapshot_collector + market_snapshot from trades Parquet.

    By default, runs the full 3-stage pipeline:
      Stage 1: Aggregate trades/quotes into windows
      Stage 2: Join, rank, write to collector
      Stage 3: Process collector → market_snapshot (subscribed tickers)

    With --reprocess, skips Stage 1-2 and only re-runs Stage 3.
    """
    from jerry_trader.platform.config.session import make_session_id
    from jerry_trader.services.backtest.data.snapshot_builder import (
        build,
        process_from_collector,
    )

    ch_client = _get_ch_client()
    if ch_client is None:
        print(
            "ERROR: ClickHouse client not available. Set CLICKHOUSE_PASSWORD env var."
        )
        sys.exit(1)

    # --reprocess: skip Stage 1-2, only run process_from_collector
    if args.reprocess:
        try:
            total = process_from_collector(
                date=args.date,
                ch_client=ch_client,
                database=args.database,
                force=args.force,
            )
            print(f"\nReprocessed {total:,} rows in market_snapshot for {args.date}")
        except RuntimeError as e:
            print(f"Error: {e}")
            sys.exit(1)
        return

    # Full build (Stage 1-3)
    mode = args.mode
    session_id = args.session_id
    if not session_id:
        date_compact = args.date.replace("-", "")
        session_id = make_session_id(
            replay_date=date_compact if mode == "replay" else None,
        )

    collector_count, processed_count = build(
        date=args.date,
        ch_client=ch_client,
        database=args.database,
        window_ms=args.window_ms,
        mode=mode,
        session_id=session_id,
        filter_start_et=args.start_et,
        filter_end_et=args.end_et,
        force=args.force,
    )
    print(
        f"\nInserted {collector_count:,} collector + {processed_count:,} market_snapshot rows "
        f"(mode={mode}, session={session_id}, {args.start_et}-{args.end_et} ET)"
    )


def cmd_extract_trades(args):
    """Extract per-ticker partitioned parquet from per-date Polygon files.

    Converts:
      trades_v1/2026/03/2026-03-13.parquet (all tickers mixed)
    → trades_v1_partitioned/ISPC/2026-03-13.parquet (per-ticker)

    Uses DuckDB for memory-efficient streaming.
    """
    import duckdb

    from jerry_trader.platform.config.config import lake_data_dir

    date = args.date  # YYYY-MM-DD
    year, month, _ = date.split("-")
    source_base = Path(args.source_dir) if args.source_dir else Path(lake_data_dir)
    data_types = args.types.split(",") if args.types else ["trades_v1", "quotes_v1"]
    tickers_filter = args.tickers

    for data_type in data_types:
        source_file = (
            source_base / "us_stocks_sip" / data_type / year / month / f"{date}.parquet"
        )
        target_dir = source_base / "us_stocks_sip" / f"{data_type}_partitioned"

        if not source_file.exists():
            print(f"  Source not found: {source_file}")
            continue

        conn = duckdb.connect(":memory:")
        conn.execute("SET memory_limit='2GB'")

        # Get tickers
        if tickers_filter:
            tickers = tickers_filter
        else:
            result = conn.execute(
                f"SELECT DISTINCT ticker FROM read_parquet('{source_file}')"
            ).fetchall()
            tickers = [r[0] for r in result]

        print(f"Extracting {data_type}: {len(tickers)} tickers from {source_file.name}")

        total_rows = 0
        for ticker in tickers:
            ticker_dir = target_dir / ticker
            ticker_dir.mkdir(parents=True, exist_ok=True)
            target_file = ticker_dir / f"{date}.parquet"

            if target_file.exists():
                continue

            try:
                result = conn.execute(
                    f"""
                    COPY (
                        SELECT * FROM read_parquet('{source_file}')
                        WHERE ticker = '{ticker}'
                    ) TO '{target_file}' (FORMAT PARQUET, COMPRESSION 'zstd')
                """
                )
                rows = result.fetchone()
                if rows:
                    total_rows += rows[0]
            except Exception as e:
                print(f"  Error extracting {ticker}: {e}")

        conn.close()
        print(
            f"  {data_type}: extracted {total_rows:,} rows for {len(tickers)} tickers"
        )


def cmd_import_ticks(args):
    """Import trades and/or quotes from Polygon parquet into ClickHouse.

    Reads per-date parquet files, filters to specified tickers (or auto-detects
    from PreFilter), and inserts into CH trades/quotes tables.
    """
    import os

    import duckdb

    from jerry_trader.platform.config.config import lake_data_dir

    ch_client = _get_ch_client()
    if ch_client is None:
        print("ERROR: ClickHouse client not available.")
        sys.exit(1)

    date = args.date  # YYYY-MM-DD
    year, month, _ = date.split("-")
    database = args.database
    data_types = args.types.split(",") if args.types else ["trades_v1", "quotes_v1"]

    # Resolve tickers
    if args.tickers:
        tickers = args.tickers
    else:
        # Auto-detect from PreFilter (all subscribed tickers from market_snapshot)
        r = ch_client.query(
            f"SELECT DISTINCT symbol FROM {database}.market_snapshot FINAL "
            "WHERE date = {date:String}",
            parameters={"date": date},
        )
        tickers = [row[0] for row in r.result_rows]
        if not tickers:
            print(
                f"No tickers found in market_snapshot for {date}. Run process-snapshot first."
            )
            sys.exit(1)

    print(
        f"Importing for {len(tickers)} tickers: {tickers[:10]}{'...' if len(tickers) > 10 else ''}"
    )

    source_base = Path(args.source_dir) if args.source_dir else Path(lake_data_dir)

    tickers_sql = "(" + ",".join(f"'{t}'" for t in tickers) + ")"

    for data_type in data_types:
        source_file = (
            source_base / "us_stocks_sip" / data_type / year / month / f"{date}.parquet"
        )

        if not source_file.exists():
            print(f"  Source not found: {source_file}")
            continue

        table_name = "trades" if data_type == "trades_v1" else "quotes"

        # Check existing
        r = ch_client.query(
            f"SELECT count() FROM {database}.{table_name} FINAL "
            "WHERE date = {date:String}",
            parameters={"date": date},
        )
        existing = r.result_rows[0][0]
        if existing > 0 and not args.force:
            print(
                f"  {table_name} already has {existing:,} rows for {date}. Use --force to overwrite."
            )
            continue
        if existing > 0:
            ch_client.command(
                f"ALTER TABLE {database}.{table_name} DELETE WHERE date = '{date}'"
            )
            ch_client.command(f"OPTIMIZE TABLE {database}.{table_name} FINAL")

        print(f"  Importing {data_type} → {table_name}...")

        conn = duckdb.connect(":memory:")
        conn.execute("SET memory_limit='4GB'")

        total = 0
        insert_batch_size = 100_000

        for ticker in tickers:
            if table_name == "trades":
                rows = conn.execute(
                    f"""
                    SELECT
                        ticker, '{date}',
                        sip_timestamp, coalesce(participant_timestamp, 0),
                        coalesce(price, 0), coalesce(size, 0),
                        coalesce(exchange, 0), coalesce(conditions, ''),
                        coalesce(correction, 0), coalesce(tape, 0),
                        coalesce(trf_id, 0), coalesce(trf_timestamp, 0),
                        coalesce(sequence_number, 0)
                    FROM read_parquet('{source_file}')
                    WHERE ticker = '{ticker}'
                    ORDER BY sip_timestamp
                """
                ).fetchall()
                if not rows:
                    continue
                for i in range(0, len(rows), insert_batch_size):
                    ch_client.insert(
                        table=f"{database}.{table_name}",
                        data=rows[i : i + insert_batch_size],
                        column_names=[
                            "ticker",
                            "date",
                            "sip_timestamp",
                            "participant_timestamp",
                            "price",
                            "size",
                            "exchange",
                            "conditions",
                            "correction",
                            "tape",
                            "trf_id",
                            "trf_timestamp",
                            "sequence_number",
                        ],
                    )
            else:
                rows = conn.execute(
                    f"""
                    SELECT
                        ticker, '{date}',
                        sip_timestamp, coalesce(participant_timestamp, 0),
                        coalesce(bid_price, 0), coalesce(bid_size, 0),
                        coalesce(ask_price, 0), coalesce(ask_size, 0),
                        coalesce(bid_exchange, 0), coalesce(ask_exchange, 0),
                        coalesce(conditions, ''), coalesce(indicators, ''),
                        coalesce(tape, 0), coalesce(trf_timestamp, 0),
                        coalesce(sequence_number, 0)
                    FROM read_parquet('{source_file}')
                    WHERE ticker = '{ticker}'
                    ORDER BY sip_timestamp
                """
                ).fetchall()
                if not rows:
                    continue
                for i in range(0, len(rows), insert_batch_size):
                    ch_client.insert(
                        table=f"{database}.{table_name}",
                        data=rows[i : i + insert_batch_size],
                        column_names=[
                            "ticker",
                            "date",
                            "sip_timestamp",
                            "participant_timestamp",
                            "bid_price",
                            "bid_size",
                            "ask_price",
                            "ask_size",
                            "bid_exchange",
                            "ask_exchange",
                            "conditions",
                            "indicators",
                            "tape",
                            "trf_timestamp",
                            "sequence_number",
                        ],
                    )

            total += len(rows) if rows else 0
            if total % 500_000 < len(rows):
                print(
                    f"    {total:,} rows ({tickers.index(ticker)+1}/{len(tickers)} tickers)..."
                )

        conn.close()
        print(f"  {table_name}: imported {total:,} rows for {len(tickers)} tickers")

    # Summary
    for table_name in ["trades", "quotes"]:
        r = ch_client.query(
            f"SELECT count(), count(DISTINCT ticker) FROM {database}.{table_name} FINAL "
            "WHERE date = {date:String}",
            parameters={"date": date},
        )
        if r.result_rows:
            print(
                f"  {table_name}: {r.result_rows[0][0]:,} rows, {r.result_rows[0][1]} tickers"
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
    p_check.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed ticker-level stats (single date only)",
    )
    p_check.add_argument(
        "--database",
        default="jerry_trader",
        help="ClickHouse database for verbose check",
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
    p_bs.add_argument(
        "--force",
        action="store_true",
        help="Delete existing data for this date before building",
    )
    p_bs.add_argument(
        "--reprocess",
        action="store_true",
        help="Skip collector build, only re-process collector → market_snapshot",
    )
    p_bs.set_defaults(func=cmd_build_snapshot)

    # extract-trades
    p_xt = subparsers.add_parser(
        "extract-trades",
        parents=[common],
        help="Extract per-ticker partitioned trades/quotes from per-date parquet",
    )
    p_xt.add_argument(
        "--tickers",
        nargs="+",
        help="Only extract specific tickers (default: all in source file)",
    )
    p_xt.add_argument(
        "--source-dir",
        default="",
        help="Override source directory (default: auto-detect from config)",
    )
    p_xt.add_argument(
        "--types",
        default="trades_v1,quotes_v1",
        help="Data types to extract (default: trades_v1,quotes_v1)",
    )
    p_xt.set_defaults(func=cmd_extract_trades)

    # import-ticks
    p_it = subparsers.add_parser(
        "import-ticks",
        parents=[common],
        help="Import trades/quotes from Polygon parquet into ClickHouse",
    )
    p_it.add_argument(
        "--tickers",
        nargs="+",
        help="Specific tickers (default: auto from market_snapshot)",
    )
    p_it.add_argument(
        "--source-dir",
        default="",
        help="Override source directory (default: auto from config)",
    )
    p_it.add_argument(
        "--types",
        default="trades_v1,quotes_v1",
        help="Data types to import (default: trades_v1,quotes_v1)",
    )
    p_it.add_argument("--database", default="jerry_trader", help="ClickHouse database")
    p_it.add_argument("--force", action="store_true", help="Overwrite existing data")
    p_it.set_defaults(func=cmd_import_ticks)

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

    # parquet-to-ch (migrate legacy parquet to CH)
    p_migrate = subparsers.add_parser(
        "parquet-to-ch",
        parents=[common],
        help="Migrate legacy parquet snapshots to ClickHouse",
    )
    p_migrate.add_argument(
        "--database", default="jerry_trader", help="ClickHouse database"
    )
    p_migrate.add_argument(
        "--mode", default="replay", help="Mode tag (default: replay)"
    )
    p_migrate.add_argument(
        "--force", action="store_true", help="Overwrite existing data"
    )
    p_migrate.set_defaults(func=cmd_parquet_to_ch)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    args.func(args)


def cmd_parquet_to_ch(args):
    """Migrate legacy parquet snapshot files to CH market_snapshot_collector."""
    import glob
    import os

    import polars as pl

    from jerry_trader.platform.config.config import cache_dir

    date_str = args.date.replace("-", "")
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    parquet_dir = os.path.join(cache_dir, "market_mover", year, month, day)

    if not os.path.exists(parquet_dir):
        logger.error(f"Directory not found: {parquet_dir}")
        return

    files = sorted(glob.glob(os.path.join(parquet_dir, "*_market_snapshot.parquet")))
    if not files:
        logger.error(f"No parquet files found in {parquet_dir}")
        return

    ch_client = _get_ch_client(args.database)
    mode = args.mode

    # Check existing data count
    existing = ch_client.query(
        "SELECT count() FROM market_snapshot_collector FINAL "
        "WHERE date = {date:String} AND mode = {mode:String}",
        parameters={"date": date_str, "mode": mode},
    )
    existing_count = existing.result_rows[0][0] if existing.result_rows else 0
    if existing_count > 0 and not args.force:
        logger.warning(
            f"Found {existing_count:,} existing rows for {date_str}/{mode}. "
            "Use --force to overwrite."
        )
        return

    total_rows = 0
    # Column mapping: old parquet names → CH collector column names
    column_map = {
        "todaysChangePerc": "changePercent",
        "min_av": "volume",
        "lastTrade_p": "price",
        "prevDay_c": "prev_close",
        "prevDay_v": "prev_volume",
        "min_vw": "vwap",
        "lastQuote_p": "bid",
        "lastQuote_P": "ask",
        "lastQuote_s": "bid_size",
        "lastQuote_S": "ask_size",
    }

    _COLLECTOR_COLUMNS = [
        "ticker",
        "timestamp",
        "price",
        "changePercent",
        "volume",
        "prev_close",
        "prev_volume",
        "vwap",
        "bid",
        "ask",
        "bid_size",
        "ask_size",
    ]

    for i, f in enumerate(files):
        try:
            df = pl.read_parquet(f)

            # Extract timestamp from filename (YYYYMMDDHHMMSS_market_snapshot.parquet)
            basename = os.path.basename(f)
            ts_str = basename.split("_")[0]
            from datetime import datetime as dt
            from zoneinfo import ZoneInfo

            file_dt = dt(
                int(ts_str[:4]),
                int(ts_str[4:6]),
                int(ts_str[6:8]),
                int(ts_str[8:10]),
                int(ts_str[10:12]),
                int(ts_str[12:14]),
                tzinfo=ZoneInfo("America/New_York"),
            )
            timestamp_ms = int(file_dt.timestamp() * 1000)

            # Rename columns
            rename_cols = {
                old: new for old, new in column_map.items() if old in df.columns
            }
            df = df.rename(rename_cols)

            # Add timestamp column
            df = df.with_columns(pl.lit(timestamp_ms).alias("timestamp"))

            # Select only the columns we need
            available = [c for c in _COLLECTOR_COLUMNS if c in df.columns]
            df = df.select(available)

            # Add metadata columns
            df = df.with_columns(
                [
                    pl.lit(date_str).alias("date"),
                    pl.lit(mode).alias("mode"),
                ]
            )

            # Insert to CH
            rows = []
            for row in df.iter_rows(named=True):
                rows.append(
                    [
                        row.get("ticker", ""),
                        int(row.get("timestamp", 0)),
                        float(row.get("price", 0) or 0),
                        float(row.get("changePercent", 0) or 0),
                        float(row.get("volume", 0) or 0),
                        float(row.get("prev_close", 0) or 0),
                        float(row.get("prev_volume", 0) or 0),
                        float(row.get("vwap", 0) or 0),
                        float(row.get("bid", 0) or 0),
                        float(row.get("ask", 0) or 0),
                        float(row.get("bid_size", 0) or 0),
                        float(row.get("ask_size", 0) or 0),
                        date_str,
                        mode,
                    ]
                )

            ch_client.insert(
                table="market_snapshot_collector",
                data=rows,
                column_names=_COLLECTOR_COLUMNS + ["date", "mode"],
            )
            total_rows += len(rows)

            if (i + 1) % 100 == 0:
                logger.info(
                    f"Migrated {i + 1}/{len(files)} files ({total_rows:,} rows)"
                )

        except Exception as e:
            logger.error(f"Error migrating {f}: {e}")
            continue

    logger.info(f"Migration complete: {total_rows:,} rows from {len(files)} files")


if __name__ == "__main__":
    main()
