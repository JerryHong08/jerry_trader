"""Data checker for backtest — validates data readiness.

Checks that all required data (trades/quotes Parquet, ClickHouse market_snapshot)
exists and is complete for a given date.

Data pipeline stages:
  raw (CSV.gz) → lake (Parquet) → snapshot (ClickHouse)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from jerry_trader.platform.config.config import lake_data_dir, raw_data_dir
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.data.checker", log_to_file=True)


class DataStatus(str, Enum):
    READY = "READY"
    MISSING = "MISSING"
    INCOMPLETE = "INCOMPLETE"
    ERROR = "ERROR"


@dataclass
class FileCheck:
    """Result of checking a single data file."""

    path: str
    exists: bool = False
    size_bytes: int = 0
    row_count: int | None = None
    status: DataStatus = DataStatus.MISSING
    # Raw file info
    raw_path: str | None = None
    raw_exists: bool = False
    raw_size_bytes: int = 0


@dataclass
class SnapshotCheck:
    """Result of checking ClickHouse market_snapshot."""

    row_count: int = 0
    min_time_ms: int | None = None
    max_time_ms: int | None = None
    unique_tickers: int = 0
    status: DataStatus = DataStatus.MISSING


@dataclass
class DataCheckResult:
    """Aggregated data readiness check for a date."""

    date: str
    trades: FileCheck = field(default_factory=FileCheck)
    quotes: FileCheck = field(default_factory=FileCheck)
    snapshot: SnapshotCheck = field(default_factory=SnapshotCheck)

    @property
    def is_ready(self) -> bool:
        """True if trades exist and snapshot is available."""
        return (
            self.trades.status == DataStatus.READY
            and self.snapshot.status == DataStatus.READY
        )

    @property
    def has_raw(self) -> bool:
        """True if raw CSV.gz files exist."""
        return self.trades.raw_exists

    @property
    def summary(self) -> str:
        if self.is_ready:
            return "READY — all data present"
        parts = []

        # Show pipeline status: raw → parquet → snapshot
        if self.trades.raw_exists:
            if self.trades.status == DataStatus.READY:
                parts.append("trades:READY")
            else:
                parts.append("trades:raw_only")
        else:
            parts.append(f"trades:{self.trades.status.value}")

        if self.quotes.raw_exists:
            if self.quotes.status == DataStatus.READY:
                parts.append("quotes:READY")
            else:
                parts.append("quotes:raw_only")
        else:
            parts.append(f"quotes:{self.quotes.status.value}")

        if self.snapshot.status != DataStatus.READY:
            parts.append(f"snapshot:{self.snapshot.status.value}")

        return "MISSING — " + ", ".join(parts)


def _parquet_paths(date: str, data_type: str) -> tuple[Path, Path]:
    """Get both monolithic and partitioned Parquet paths for a date.

    Pattern (matching Rust ReplayConfig):
      Monolithic:    {lake}/us_stocks_sip/{data_type}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet
      Partitioned:   {lake}/us_stocks_sip/{data_type}_partitioned/{ticker}/{YYYY-MM-DD}.parquet

    For monolithic check, we look for the date-level file.
    For partitioned check, we look for the directory existence.
    """
    year, month, day = date.split("-")
    date_iso = date

    monolithic = (
        Path(lake_data_dir)
        / "us_stocks_sip"
        / data_type
        / year
        / month
        / f"{date_iso}.parquet"
    )
    partitioned_dir = Path(lake_data_dir) / "us_stocks_sip" / f"{data_type}_partitioned"
    return monolithic, partitioned_dir


def _raw_csvgz_path(date: str, data_type: str) -> Path:
    """Get the raw CSV.gz path for a date.

    Pattern: {raw}/us_stocks_sip/{data_type}/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz
    """
    year, month, day = date.split("-")
    return (
        Path(raw_data_dir)
        / "us_stocks_sip"
        / data_type
        / year
        / month
        / f"{date}.csv.gz"
    )


def check_parquet_file(date: str, data_type: str) -> FileCheck:
    """Check if a Parquet file exists for the given date and data type.

    Also checks raw CSV.gz status.

    Args:
        date: Date in YYYY-MM-DD format.
        data_type: e.g. "trades_v1", "quotes_v1".

    Returns:
        FileCheck with existence and size info (including raw file info).
    """
    monolithic, partitioned_dir = _parquet_paths(date, data_type)
    raw_path = _raw_csvgz_path(date, data_type)

    # Check raw file first
    raw_exists = raw_path.exists()
    raw_size = raw_path.stat().st_size if raw_exists else 0

    # Check monolithic file
    if monolithic.exists():
        size = monolithic.stat().st_size
        row_count = _parquet_row_count(str(monolithic))
        return FileCheck(
            path=str(monolithic),
            exists=True,
            size_bytes=size,
            row_count=row_count,
            status=(
                DataStatus.READY
                if row_count and row_count > 0
                else DataStatus.INCOMPLETE
            ),
            raw_path=str(raw_path),
            raw_exists=raw_exists,
            raw_size_bytes=raw_size,
        )

    # Check partitioned directory — any ticker files for this date?
    if partitioned_dir.exists():
        date_files = list(partitioned_dir.glob(f"*/{date}.parquet"))
        if date_files:
            total_size = sum(f.stat().st_size for f in date_files)
            return FileCheck(
                path=f"{partitioned_dir}/*/{date}.parquet ({len(date_files)} tickers)",
                exists=True,
                size_bytes=total_size,
                row_count=len(date_files),  # per-ticker file count, not row count
                status=DataStatus.READY,
                raw_path=str(raw_path),
                raw_exists=raw_exists,
                raw_size_bytes=raw_size,
            )

    # Parquet not found, but return raw info
    return FileCheck(
        path=str(monolithic),
        exists=False,
        status=DataStatus.MISSING,
        raw_path=str(raw_path),
        raw_exists=raw_exists,
        raw_size_bytes=raw_size,
    )


def _parquet_row_count(path: str) -> int | None:
    """Get row count from a Parquet file without loading all data."""
    try:
        import polars as pl

        df = pl.scan_parquet(path).select(pl.len()).collect()
        return df.item()
    except Exception as e:
        logger.warning(f"Failed to read Parquet row count from {path}: {e}")
        return None


def check_clickhouse_snapshot(
    date: str,
    ch_client: Any,
    database: str = "jerry_trader",
) -> SnapshotCheck:
    """Check ClickHouse market_snapshot_collector data for a date.

    snapshot_builder writes to:
    - market_snapshot_collector (full for Replay)

    Args:
        date: Date in YYYY-MM-DD format.
        ch_client: ClickHouse client.
        database: Database name.

    Returns:
        SnapshotCheck with row count and time range.
    """
    if ch_client is None:
        return SnapshotCheck(status=DataStatus.MISSING)

    try:
        # Check market_snapshot_collector (snapshot_builder output)
        result = ch_client.query(
            f"""
            SELECT
                count() as row_count,
                min(timestamp) as min_time,
                max(timestamp) as max_time,
                uniq(ticker) as tickers
            FROM {database}.market_snapshot_collector
            WHERE date = %(date)s
            """,
            parameters={"date": date},
        )

        if not result.result_rows:
            return SnapshotCheck(status=DataStatus.MISSING)

        row = result.result_rows[0]
        row_count = int(row[0])
        min_time = int(row[1]) if row[1] else None
        max_time = int(row[2]) if row[2] else None
        tickers = int(row[3])

        if row_count == 0:
            return SnapshotCheck(status=DataStatus.MISSING)

        return SnapshotCheck(
            row_count=row_count,
            min_time_ms=min_time,
            max_time_ms=max_time,
            unique_tickers=tickers,
            status=DataStatus.READY,
        )

    except Exception as e:
        logger.error(f"ClickHouse snapshot check failed: {e}")
        return SnapshotCheck(status=DataStatus.ERROR)


def check_date(
    date: str,
    ch_client: Any = None,
    database: str = "jerry_trader",
) -> DataCheckResult:
    """Check all data readiness for a date.

    Args:
        date: Date in YYYY-MM-DD format.
        ch_client: Optional ClickHouse client for snapshot check.
        database: ClickHouse database name.

    Returns:
        DataCheckResult with all checks.
    """
    logger.info(f"Checking data readiness for {date}")

    trades = check_parquet_file(date, "trades_v1")
    quotes = check_parquet_file(date, "quotes_v1")
    snapshot = check_clickhouse_snapshot(date, ch_client, database)

    result = DataCheckResult(
        date=date,
        trades=trades,
        quotes=quotes,
        snapshot=snapshot,
    )

    logger.info(f"  Trades: {trades.status.value} ({trades.path})")
    logger.info(f"  Quotes: {quotes.status.value} ({quotes.path})")
    logger.info(
        f"  Snapshot: {snapshot.status.value} "
        f"({snapshot.row_count} rows, {snapshot.unique_tickers} tickers)"
    )
    logger.info(f"  Summary: {result.summary}")

    return result


def print_check_result(result: DataCheckResult) -> None:
    """Print formatted check result to console."""
    print(f"\n{'=' * 60}")
    print(f"  Data Check — {result.date}")
    print(f"{'=' * 60}")

    # Trades
    _print_file_status("Trades", result.trades)

    # Quotes
    _print_file_status("Quotes", result.quotes)

    # Snapshot
    snap = result.snapshot
    status_mark = _status_mark(snap.status)
    print(f"\n  ClickHouse Snapshot: {status_mark}")
    if snap.status == DataStatus.READY:
        print(f"    Rows: {snap.row_count:,}")
        print(f"    Tickers: {snap.unique_tickers}")
        if snap.min_time_ms and snap.max_time_ms:
            print(f"    Time range: {snap.min_time_ms} → {snap.max_time_ms}")

    print(f"\n  Result: {result.summary}")
    print(f"{'=' * 60}\n")


def _print_file_status(label: str, check: FileCheck) -> None:
    """Print a file check status line with pipeline info."""
    # Show pipeline: raw → parquet
    raw_mark = _status_mark(DataStatus.READY) if check.raw_exists else "MISSING"
    parquet_mark = _status_mark(check.status)

    print(f"  {label}: raw={raw_mark} → parquet={parquet_mark}")

    # Raw info
    if check.raw_exists:
        raw_size_mb = check.raw_size_bytes / (1024 * 1024)
        print(f"    Raw: {check.raw_path} ({raw_size_mb:.1f} MB)")
    elif check.raw_path:
        print(f"    Raw: {check.raw_path} (not found)")

    # Parquet info
    if check.exists:
        size_mb = check.size_bytes / (1024 * 1024)
        print(f"    Parquet: {check.path} ({size_mb:.1f} MB)")
        if check.row_count is not None:
            print(f"    Rows: {check.row_count:,}")
    elif check.path:
        print(f"    Parquet: {check.path} (not found)")


def _status_mark(status: DataStatus) -> str:
    marks = {
        DataStatus.READY: "OK",
        DataStatus.MISSING: "MISSING",
        DataStatus.INCOMPLETE: "INCOMPLETE",
        DataStatus.ERROR: "ERROR",
    }
    return marks.get(status, "?")


def check_date_range(
    start_date: str,
    end_date: str,
    ch_client: Any = None,
    database: str = "jerry_trader",
) -> dict[str, DataCheckResult]:
    """Check data readiness for a date range.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        ch_client: Optional ClickHouse client for snapshot check.
        database: ClickHouse database name.

    Returns:
        Dict mapping date → DataCheckResult.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    results: dict[str, DataCheckResult] = {}
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        results[date_str] = check_date(date_str, ch_client, database)
        current += timedelta(days=1)

    return results


def print_date_range_summary(results: dict[str, DataCheckResult]) -> None:
    """Print a summary table for date range check."""
    print(f"\n{'=' * 80}")
    print(f"  Data Check — {len(results)} days")
    print(f"{'=' * 80}")

    # Header
    print(f"  {'Date':<12} {'Trades':<20} {'Quotes':<20} {'Snapshot':<10}")
    print(f"  {'-'*12} {'-'*20} {'-'*20} {'-'*10}")

    # Rows
    ready_count = 0
    for date, result in sorted(results.items()):
        trades_status = (
            "raw+parquet"
            if result.trades.exists and result.trades.raw_exists
            else "raw_only" if result.trades.raw_exists else "missing"
        )
        quotes_status = (
            "raw+parquet"
            if result.quotes.exists and result.quotes.raw_exists
            else "raw_only" if result.quotes.raw_exists else "missing"
        )
        snap_status = "OK" if result.snapshot.status == DataStatus.READY else "missing"

        print(f"  {date:<12} {trades_status:<20} {quotes_status:<20} {snap_status:<10}")

        if result.is_ready:
            ready_count += 1

    # Summary
    print(f"\n  Ready: {ready_count}/{len(results)} days")
    print(f"{'=' * 80}\n")
