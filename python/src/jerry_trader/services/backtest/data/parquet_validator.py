"""
Parquet Snapshot Validator — 检查历史 snapshot 数据完整性。

在迁移到 ClickHouse 之前，验证 Parquet snapshot 数据的质量：
- 时间范围检查：首尾 timestamp 是否覆盖完整交易时段
- 间隔分析：计算相邻 snapshot 间隔，识别异常 gap
- Schema 验证：列名、类型、必需字段是否存在
- 行数统计：每个 snapshot 的 ticker 数量是否合理
- 输出报告：列出有问题的日期及其异常类型

Usage:
    # 验证所有日期
    python -m jerry_trader.services.backtest.data.parquet_validator

    # 验证特定日期
    python -m jerry_trader.services.backtest.data.parquet_validator --date 20260115

    # 输出详细报告
    python -m jerry_trader.services.backtest.data.parquet_validator --verbose
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from jerry_trader.platform.config.config import cache_dir
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import ms_to_hhmmss

logger = setup_logger(__name__, log_to_file=True)

# =============================================================================
# Constants
# =============================================================================

# Expected schema for Collector snapshot format
# Note: 'rank' column should NOT exist in raw collector data
EXPECTED_COLUMNS: dict[str, type] = {
    "ticker": pl.String,
    "todaysChange": pl.Float64,
    "todaysChangePerc": pl.Float64,
    "updated": pl.Int64,  # epoch nanoseconds
    "day_c": pl.Float64,
    "day_h": pl.Float64,
    "day_l": pl.Float64,
    "day_o": pl.Float64,
    "day_v": pl.Float64,
    "day_vw": pl.Float64,
    "prevDay_c": pl.Float64,
    # Optional columns (may not exist in all files)
    "lastQuote_p": pl.Float64,  # bid
    "lastQuote_P": pl.Float64,  # ask
    "lastQuote_s": pl.Int64,  # bid_size
    "lastQuote_S": pl.Int64,  # ask_size
}

# Columns that should NOT exist in raw collector data
UNEXPECTED_COLUMNS = ["rank", "changePercent", "change", "relativeVolume"]

# Trading hours (ET)
PREMARKET_OPEN = (4, 0)  # 04:00 ET
MARKET_CLOSE = (20, 0)  # 20:00 ET (after-hours end)

# Interval thresholds
EXPECTED_INTERVAL_MS = 5000  # 5 seconds between snapshots
MAX_GAP_MS = 30000  # 30 seconds - flag as anomaly
MIN_TICKERS = 100  # Minimum expected tickers per snapshot
MAX_TICKERS = 15000  # Maximum expected tickers per snapshot


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SnapshotInfo:
    """Information about a single snapshot file."""

    filepath: str
    timestamp_ms: int  # From filename
    row_count: int
    column_count: int
    has_unexpected_columns: bool = False
    unexpected_columns: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)


@dataclass
class DateValidationResult:
    """Validation result for a single date."""

    date: str  # YYYYMMDD
    snapshot_count: int
    time_range: tuple[int, int]  # (start_ms, end_ms) from filenames

    # Interval analysis
    intervals_ms: list[int] = field(default_factory=list)
    min_interval_ms: int = 0
    max_interval_ms: int = 0
    avg_interval_ms: float = 0.0
    gap_count: int = 0  # Intervals > MAX_GAP_MS

    # Row count analysis
    min_rows: int = 0
    max_rows: int = 0
    avg_rows: float = 0.0
    low_row_count: int = 0  # Snapshots with < MIN_TICKERS
    high_row_count: int = 0  # Snapshots with > MAX_TICKERS

    # Schema issues
    schema_issues: list[str] = field(default_factory=list)
    unexpected_columns_found: list[str] = field(default_factory=list)

    # Overall status
    is_valid: bool = True
    issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
        return {
            "date": self.date,
            "snapshot_count": self.snapshot_count,
            "time_range": {
                "start": ms_to_hhmmss(self.time_range[0]),
                "end": ms_to_hhmmss(self.time_range[1]),
            },
            "intervals": {
                "min_ms": self.min_interval_ms,
                "max_ms": self.max_interval_ms,
                "avg_ms": round(self.avg_interval_ms, 1),
                "gap_count": self.gap_count,
            },
            "rows": {
                "min": self.min_rows,
                "max": self.max_rows,
                "avg": round(self.avg_rows, 1),
                "low_count": self.low_row_count,
                "high_count": self.high_row_count,
            },
            "schema_issues": self.schema_issues,
            "unexpected_columns": self.unexpected_columns_found,
            "is_valid": self.is_valid,
            "issues": self.issues,
        }


# =============================================================================
# Validator
# =============================================================================


class ParquetSnapshotValidator:
    """Validate Parquet snapshot data integrity."""

    def __init__(self, market_mover_dir: str | None = None):
        """
        Initialize validator.

        Args:
            market_mover_dir: Root directory for market_mover data.
                             Defaults to cache_dir/market_mover.
        """
        self.market_mover_dir: str = market_mover_dir or os.path.join(
            cache_dir, "market_mover"
        )
        logger.info(f"ParquetSnapshotValidator initialized: {self.market_mover_dir}")

    def validate_date(self, date: str) -> DateValidationResult:
        """
        Validate snapshot data for a specific date.

        Args:
            date: Date string in YYYYMMDD format

        Returns:
            DateValidationResult with validation details
        """
        year = date[:4]
        month = date[4:6]
        day = date[6:8]

        date_dir = os.path.join(self.market_mover_dir, year, month, day)

        if not os.path.exists(date_dir):
            result = DateValidationResult(
                date=date, snapshot_count=0, time_range=(0, 0)
            )
            result.is_valid = False
            result.issues.append(f"Directory not found: {date_dir}")
            return result

        # Find all snapshot files (both parquet and csv)
        files = []
        for ext in ["parquet", "csv"]:
            files.extend(
                [
                    f
                    for f in os.listdir(date_dir)
                    if f.endswith(f"_market_snapshot.{ext}")
                ]
            )

        if not files:
            result = DateValidationResult(
                date=date, snapshot_count=0, time_range=(0, 0)
            )
            result.is_valid = False
            result.issues.append("No snapshot files found")
            return result

        files.sort()

        # Parse timestamps from filenames
        snapshots: list[SnapshotInfo] = []
        for filename in files:
            filepath = os.path.join(date_dir, filename)
            snapshot = self._parse_snapshot_file(filepath, filename)
            if snapshot:
                snapshots.append(snapshot)

        if not snapshots:
            result = DateValidationResult(
                date=date, snapshot_count=0, time_range=(0, 0)
            )
            result.is_valid = False
            result.issues.append("Failed to parse any snapshot files")
            return result

        # Build result
        timestamps = [s.timestamp_ms for s in snapshots]
        result = DateValidationResult(
            date=date,
            snapshot_count=len(snapshots),
            time_range=(min(timestamps), max(timestamps)),
        )

        # Analyze intervals
        self._analyze_intervals(snapshots, result)

        # Analyze row counts
        self._analyze_row_counts(snapshots, result)

        # Check for edge cases
        self._check_edge_cases(snapshots, result)

        # Check schema issues
        self._check_schema_issues(snapshots, result)

        # Determine overall validity
        result.is_valid = (
            len(result.issues) == 0
            and result.gap_count == 0
            and result.low_row_count == 0
            and len(result.schema_issues) == 0
        )

        return result

    def validate_all(self) -> list[DateValidationResult]:
        """
        Validate all available dates.

        Returns:
            List of DateValidationResult for each date found
        """
        results: list[DateValidationResult] = []
        dates = self._find_all_dates()

        logger.info(f"Found {len(dates)} dates to validate")

        for date in sorted(dates):
            logger.debug(f"Validating {date}...")
            result = self.validate_date(date)
            results.append(result)

            if not result.is_valid:
                logger.warning(f"{date}: INVALID - {result.issues}")

        return results

    def generate_report(self, results: list[DateValidationResult]) -> dict[str, Any]:
        """
        Generate a summary report from validation results.

        Args:
            results: List of validation results

        Returns:
            Dict with summary statistics and issues
        """
        valid_count = sum(1 for r in results if r.is_valid)
        invalid_count = len(results) - valid_count

        # Collect all issues
        all_issues: dict[str, list[str]] = {}
        for r in results:
            if not r.is_valid:
                all_issues[r.date] = r.issues

        # Collect unexpected columns across all dates
        all_unexpected: dict[str, int] = {}
        for r in results:
            for col in r.unexpected_columns_found:
                all_unexpected[col] = all_unexpected.get(col, 0) + 1

        return {
            "summary": {
                "total_dates": len(results),
                "valid": valid_count,
                "invalid": invalid_count,
            },
            "invalid_dates": all_issues,
            "unexpected_columns": all_unexpected,
            "details": [r.to_dict() for r in results],
        }

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _find_all_dates(self) -> list[str]:
        """Find all available dates in market_mover directory."""
        dates: list[str] = []

        if not os.path.exists(self.market_mover_dir):
            logger.error(f"market_mover directory not found: {self.market_mover_dir}")
            return dates

        for year in os.listdir(self.market_mover_dir):
            year_dir = os.path.join(self.market_mover_dir, year)
            if not os.path.isdir(year_dir) or not year.isdigit():
                continue

            for month in os.listdir(year_dir):
                month_dir = os.path.join(year_dir, month)
                if not os.path.isdir(month_dir) or not month.isdigit():
                    continue

                for day in os.listdir(month_dir):
                    day_dir = os.path.join(month_dir, day)
                    if not os.path.isdir(day_dir) or not day.isdigit():
                        continue

                    dates.append(f"{year}{month.zfill(2)}{day.zfill(2)}")

        return dates

    def _parse_snapshot_file(self, filepath: str, filename: str) -> SnapshotInfo | None:
        """Parse a single snapshot file."""
        try:
            # Extract timestamp from filename (YYYYMMDDHHMMSS_market_snapshot.ext)
            timestamp_str = filename.split("_")[0]
            timestamp_ms = self._parse_timestamp_to_ms(timestamp_str)

            # Read file
            if filepath.endswith(".parquet"):
                df = pl.read_parquet(filepath)
            else:
                df = pl.read_csv(filepath)

            # Check for unexpected columns
            columns = set(df.columns)
            unexpected = [c for c in UNEXPECTED_COLUMNS if c in columns]
            missing = [
                c
                for c in EXPECTED_COLUMNS
                if c not in columns and c in ["ticker", "updated"]
            ]

            return SnapshotInfo(
                filepath=filepath,
                timestamp_ms=timestamp_ms,
                row_count=len(df),
                column_count=len(df.columns),
                has_unexpected_columns=len(unexpected) > 0,
                unexpected_columns=unexpected,
                missing_columns=missing,
            )

        except Exception as e:
            logger.error(f"Failed to parse {filepath}: {e}")
            return None

    def _parse_timestamp_to_ms(self, timestamp_str: str) -> int:
        """Parse YYYYMMDDHHMMSS to epoch milliseconds."""
        dt = datetime(
            int(timestamp_str[:4]),
            int(timestamp_str[4:6]),
            int(timestamp_str[6:8]),
            int(timestamp_str[8:10]),
            int(timestamp_str[10:12]),
            int(timestamp_str[12:14]),
            tzinfo=ZoneInfo("America/New_York"),
        )
        return int(dt.timestamp() * 1000)

    def _analyze_intervals(
        self, snapshots: list[SnapshotInfo], result: DateValidationResult
    ) -> None:
        """Analyze intervals between snapshots."""
        if len(snapshots) < 2:
            return

        timestamps = sorted([s.timestamp_ms for s in snapshots])
        intervals = [
            timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)
        ]

        result.intervals_ms = intervals
        result.min_interval_ms = min(intervals)
        result.max_interval_ms = max(intervals)
        result.avg_interval_ms = sum(intervals) / len(intervals)
        result.gap_count = sum(1 for i in intervals if i > MAX_GAP_MS)

        if result.gap_count > 0:
            gap_times = [
                (timestamps[i], intervals[i])
                for i in range(len(intervals))
                if intervals[i] > MAX_GAP_MS
            ]
            result.issues.append(
                f"Found {result.gap_count} gaps > {MAX_GAP_MS}ms: "
                f"{[ms_to_hhmmss(t) for t, _ in gap_times[:3]]}"
            )

    def _analyze_row_counts(
        self, snapshots: list[SnapshotInfo], result: DateValidationResult
    ) -> None:
        """Analyze row counts across snapshots."""
        row_counts = [s.row_count for s in snapshots]

        result.min_rows = min(row_counts)
        result.max_rows = max(row_counts)
        result.avg_rows = sum(row_counts) / len(row_counts)
        result.low_row_count = sum(1 for r in row_counts if r < MIN_TICKERS)
        result.high_row_count = sum(1 for r in row_counts if r > MAX_TICKERS)

        if result.low_row_count > 0:
            result.issues.append(
                f"{result.low_row_count} snapshots have < {MIN_TICKERS} tickers"
            )

        if result.high_row_count > 0:
            result.issues.append(
                f"{result.high_row_count} snapshots have > {MAX_TICKERS} tickers"
            )

    def _check_edge_cases(
        self, snapshots: list[SnapshotInfo], result: DateValidationResult
    ) -> None:
        """Check for edge cases like insufficient snapshots."""
        # Check if too few snapshots (likely incomplete collection)
        if len(snapshots) < 100:
            result.issues.append(
                f"Too few snapshots ({len(snapshots)}), expected > 100 for a full trading day"
            )

        # Check if only 1 snapshot (can't calculate intervals)
        if len(snapshots) == 1:
            result.issues.append("Only 1 snapshot found, cannot validate intervals")

        # Check time range coverage
        if len(snapshots) >= 2:
            timestamps = sorted([s.timestamp_ms for s in snapshots])
            duration_minutes = (timestamps[-1] - timestamps[0]) / 60000

            # A full trading day should have at least 5 hours of data
            if duration_minutes < 300:  # 5 hours = 300 minutes
                result.issues.append(
                    f"Insufficient time coverage ({duration_minutes:.0f} minutes), expected > 300 minutes"
                )

    def _check_schema_issues(
        self, snapshots: list[SnapshotInfo], result: DateValidationResult
    ) -> None:
        """Check for schema issues across snapshots."""
        # Check for missing required columns
        missing_set = set()
        for s in snapshots:
            missing_set.update(s.missing_columns)

        if missing_set:
            result.schema_issues.append(
                f"Missing required columns: {list(missing_set)}"
            )

        # Check for unexpected columns (like 'rank' that shouldn't exist in raw data)
        unexpected_set = set()
        for s in snapshots:
            unexpected_set.update(s.unexpected_columns)

        result.unexpected_columns_found = list(unexpected_set)

        if unexpected_set:
            result.issues.append(f"Unexpected columns found: {list(unexpected_set)}")


# =============================================================================
# CLI
# =============================================================================


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Parquet Snapshot Validator")
    parser.add_argument(
        "--date",
        type=str,
        help="Validate specific date (YYYYMMDD format)",
    )
    parser.add_argument(
        "--dir",
        type=str,
        help="Override market_mover directory",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="validation_report.json",
        help="Output JSON file path",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed output",
    )

    args = parser.parse_args()

    validator = ParquetSnapshotValidator(market_mover_dir=args.dir)

    if args.date:
        results = [validator.validate_date(args.date)]
    else:
        results = validator.validate_all()

    report = validator.generate_report(results)

    # Print summary
    print("\n" + "=" * 60)
    print("PARQUET SNAPSHOT VALIDATION REPORT")
    print("=" * 60)
    print(f"\nTotal dates: {report['summary']['total_dates']}")
    print(f"Valid: {report['summary']['valid']}")
    print(f"Invalid: {report['summary']['invalid']}")

    if report["invalid_dates"]:
        print("\nInvalid dates:")
        for date, issues in report["invalid_dates"].items():
            print(f"  {date}: {issues}")

    if report["unexpected_columns"]:
        print("\nUnexpected columns found:")
        for col, count in report["unexpected_columns"].items():
            print(f"  {col}: {count} dates")

    if args.verbose:
        print("\nDetailed results:")
        for detail in report["details"]:
            status = "VALID" if detail["is_valid"] else "INVALID"
            print(f"\n{detail['date']} [{status}]:")
            print(f"  Snapshots: {detail['snapshot_count']}")
            print(
                f"  Time range: {detail['time_range']['start']} - {detail['time_range']['end']}"
            )
            print(
                f"  Intervals: min={detail['intervals']['min_ms']}ms, "
                f"max={detail['intervals']['max_ms']}ms, "
                f"avg={detail['intervals']['avg_ms']}ms"
            )
            print(
                f"  Rows: min={detail['rows']['min']}, "
                f"max={detail['rows']['max']}, "
                f"avg={detail['rows']['avg']}"
            )
            if detail["issues"]:
                print(f"  Issues: {detail['issues']}")

    # Save report to JSON
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nReport saved to: {args.output}")


if __name__ == "__main__":
    main()
