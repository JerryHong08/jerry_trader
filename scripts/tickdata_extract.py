#!/usr/bin/env python3
"""
Migrate Polygon Parquet data to Hive-partitioned structure using DuckDB.

This version uses DuckDB for memory-efficient processing of large Parquet files.
DuckDB streams data from disk without loading everything into memory.

Before: lake/us_stocks_sip/quotes_v1/2025/11/2025-11-10.parquet (all tickers mixed)
After:  lake/us_stocks_sip/quotes_v1_partitioned/GOOGL/2025-11-10.parquet (per-ticker, flattened)

Data path is loaded from basic_config.yaml via jerry_trader.config.lake_data_dir

Usage:
    python scripts/migrate_parquet_to_hive_duckdb.py --data-type quotes_v1
    python scripts/migrate_parquet_to_hive_duckdb.py --data-type trades_v1
    python scripts/migrate_parquet_to_hive_duckdb.py --data-type all
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Set

# Add python package to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python" / "src"))

import duckdb
from tqdm import tqdm

from jerry_trader.config import lake_data_dir

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Base data directory loaded from basic_config.yaml
BASE_DATA_DIR = Path(lake_data_dir)


def get_unique_tickers(
    conn: duckdb.DuckDBPyConnection,
    parquet_file: Path,
    tickers_filter: List[str] = None,
) -> List[str]:
    """
    Get unique tickers from a Parquet file efficiently.

    Args:
        conn: DuckDB connection
        parquet_file: Path to Parquet file
        tickers_filter: Optional list of tickers to filter

    Returns:
        List of unique ticker symbols
    """
    query = f"SELECT DISTINCT ticker FROM read_parquet('{parquet_file}')"

    if tickers_filter:
        placeholders = ",".join([f"'{t}'" for t in tickers_filter])
        query += f" WHERE ticker IN ({placeholders})"

    result = conn.execute(query).fetchall()
    return [row[0] for row in result]


def migrate_single_file(
    conn: duckdb.DuckDBPyConnection,
    parquet_file: Path,
    source_dir: Path,
    target_dir: Path,
    tickers_filter: List[str] = None,
) -> tuple[int, Set[str]]:
    """
    Migrate one Parquet file, splitting by ticker.

    Uses DuckDB to stream data without loading full file into memory.

    Returns:
        (total_rows, set_of_tickers)
    """
    # Get tickers in this file
    tickers = get_unique_tickers(conn, parquet_file, tickers_filter)

    if not tickers:
        return 0, set()

    total_rows = 0
    # Use just the filename (date), not the full path with year/month
    date_filename = parquet_file.name  # e.g., "2025-11-10.parquet"

    # Process each ticker
    for ticker in tickers:
        # Query to read and filter for this ticker
        query = f"""
            COPY (
                SELECT * FROM read_parquet('{parquet_file}')
                WHERE ticker = '{ticker}'
            ) TO '{target_dir}/{ticker}/{date_filename}' (
                FORMAT PARQUET,
                CODEC 'ZSTD',
                COMPRESSION_LEVEL 3
            )
        """

        # Create target directory (just ticker folder, no year/month)
        ticker_dir = target_dir / ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Execute copy - DuckDB handles streaming
            result = conn.execute(query)
            rows = result.fetchone()[0] if result else 0
            total_rows += rows

            logger.debug(f"  {ticker}: {rows:,} rows -> {ticker}/{date_filename}")

        except Exception as e:
            logger.error(f"Failed to process {ticker} in {date_filename}: {e}")
            continue

    return total_rows, set(tickers)


def migrate_data_type(
    data_type: str,
    source_base: Path,
    target_base: Path,
    dry_run: bool = False,
    tickers_filter: List[str] = None,
    memory_limit: str = "2GB",
) -> None:
    """
    Migrate one data type using DuckDB.

    Args:
        data_type: "quotes_v1" or "trades_v1"
        source_base: Base directory for source data
        target_base: Base directory for target data
        dry_run: If True, only print what would be done
        tickers_filter: Optional list of tickers to migrate
        memory_limit: DuckDB memory limit (e.g., "2GB", "4GB")
    """
    source_dir = source_base / "us_stocks_sip" / data_type
    target_dir = target_base / "us_stocks_sip" / f"{data_type}_partitioned"

    if not source_dir.exists():
        logger.error(f"Source directory not found: {source_dir}")
        return

    logger.info(f"Migrating {data_type} using DuckDB")
    logger.info(f"  Source: {source_dir}")
    logger.info(f"  Target: {target_dir}")
    logger.info(f"  Memory limit: {memory_limit}")

    # Get all Parquet files
    parquet_files = sorted(source_dir.rglob("*.parquet"))
    if not parquet_files:
        logger.warning(f"No Parquet files found in {source_dir}")
        return

    logger.info(f"Found {len(parquet_files)} files to process")

    if dry_run:
        logger.info("DRY RUN - would process:")
        # Quick sample of tickers from first file
        conn = duckdb.connect(":memory:")
        try:
            sample_tickers = get_unique_tickers(conn, parquet_files[0], tickers_filter)
            logger.info(
                f"  Sample tickers in {parquet_files[0].name}: {sample_tickers[:10]}"
            )
            if len(sample_tickers) > 10:
                logger.info(f"    ... and {len(sample_tickers) - 10} more")
        except Exception as e:
            logger.warning(f"  Could not sample tickers: {e}")
        finally:
            conn.close()

        logger.info("  Files:")
        for f in parquet_files[:5]:
            logger.info(f"    - {f.relative_to(source_dir)}")
        if len(parquet_files) > 5:
            logger.info(f"    ... and {len(parquet_files) - 5} more")
        return

    # Create DuckDB connection with memory limit
    conn = duckdb.connect(":memory:")
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute("SET preserve_insertion_order=false")  # Better performance

    total_rows = 0
    total_tickers = set()

    try:
        for parquet_file in tqdm(parquet_files, desc=f"Processing {data_type}"):
            try:
                rows, tickers = migrate_single_file(
                    conn=conn,
                    parquet_file=parquet_file,
                    source_dir=source_dir,
                    target_dir=target_dir,
                    tickers_filter=tickers_filter,
                )
                total_rows += rows
                total_tickers.update(tickers)

            except Exception as e:
                logger.error(
                    f"Failed to process {parquet_file.relative_to(source_dir)}: {e}"
                )
                continue

    finally:
        conn.close()

    logger.info(f"✓ Migration complete for {data_type}")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Unique tickers: {len(total_tickers)}")
    logger.info(f"  Output directory: {target_dir}")


def verify_migration(
    data_type: str,
    source_base: Path,
    target_base: Path,
    sample_ticker: str = "AAPL",
    sample_date: str = "2025-11-10",
) -> bool:
    """
    Verify migration using DuckDB.
    """
    year, month, _ = sample_date.split("-")

    source_file = (
        source_base
        / "us_stocks_sip"
        / data_type
        / year
        / month
        / f"{sample_date}.parquet"
    )
    target_file = (
        target_base
        / "us_stocks_sip"
        / f"{data_type}_partitioned"
        / sample_ticker
        / f"{sample_date}.parquet"
    )

    if not source_file.exists():
        logger.warning(f"Source file not found: {source_file}")
        return False

    if not target_file.exists():
        logger.error(f"Target file missing: {target_file}")
        return False

    try:
        conn = duckdb.connect(":memory:")

        # Count rows in source for this ticker
        source_count = conn.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet('{source_file}')
            WHERE ticker = '{sample_ticker}'
        """
        ).fetchone()[0]

        # Count rows in target
        target_count = conn.execute(
            f"""
            SELECT COUNT(*) FROM read_parquet('{target_file}')
        """
        ).fetchone()[0]

        conn.close()

        if source_count != target_count:
            logger.error(
                f"Row count mismatch: source={source_count}, target={target_count}"
            )
            return False

        logger.info(f"✓ Verification passed for {sample_ticker}/{sample_date}")
        logger.info(f"  Rows: {target_count:,}")
        return True

    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Migrate Polygon Parquet data using DuckDB (memory-efficient)"
    )
    parser.add_argument(
        "--data-type",
        choices=["quotes_v1", "trades_v1", "all"],
        required=True,
        help="Which data type to migrate",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=BASE_DATA_DIR,
        help=f"Source data directory (default: {BASE_DATA_DIR})",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=BASE_DATA_DIR,
        help="Target data directory (default: same as source)",
    )
    parser.add_argument(
        "--memory-limit",
        default="2GB",
        help="DuckDB memory limit (default: 2GB, adjust for WSL2)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be done",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Only migrate specific tickers (e.g., --tickers AAPL GOOGL)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify migration after completion",
    )
    parser.add_argument(
        "--verify-ticker",
        default="AAPL",
        help="Ticker for verification (default: AAPL)",
    )
    parser.add_argument(
        "--verify-date",
        default="2025-11-10",
        help="Date for verification in YYYY-MM-DD format (default: 2025-11-10)",
    )

    args = parser.parse_args()

    # Determine which data types to migrate
    data_types = (
        ["quotes_v1", "trades_v1"] if args.data_type == "all" else [args.data_type]
    )

    # Run migration
    for dt in data_types:
        migrate_data_type(
            data_type=dt,
            source_base=args.source_dir,
            target_base=args.target_dir,
            dry_run=args.dry_run,
            tickers_filter=args.tickers,
            memory_limit=args.memory_limit,
        )

    # Verify if requested
    if args.verify and not args.dry_run:
        logger.info("\n" + "=" * 60)
        logger.info("Running verification...")
        logger.info("=" * 60)

        # If specific tickers were migrated, use the first one for verification
        verify_ticker = args.verify_ticker
        if args.tickers and args.verify_ticker not in args.tickers:
            verify_ticker = args.tickers[0]
            logger.info(f"Using migrated ticker '{verify_ticker}' for verification")

        all_verified = True
        for dt in data_types:
            verified = verify_migration(
                data_type=dt,
                source_base=args.source_dir,
                target_base=args.target_dir,
                sample_ticker=verify_ticker,
                sample_date=args.verify_date,
            )
            all_verified = all_verified and verified

        if all_verified:
            logger.info("\n✓ All verifications passed!")
        else:
            logger.error("\n✗ Some verifications failed")
            return 1

    return 0


if __name__ == "__main__":
    exit(main())
