"""CSV.gz to Parquet Converter for backtest data.

Adapted from quant101's proven CSVGZToParquetConverter:
- Polars streaming (scan_csv + sink_parquet) for memory efficiency
- Polygon's float→int volume quirk (post 2026-02-22)
- Schema validation with actual column detection
"""

from __future__ import annotations

import gzip
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.data.converter", log_to_file=True)

# =============================================================================
# Schemas (synced with quant101/src/data/schemas.py)
# =============================================================================

CSV_FLOAT_TO_INT_CASTS = {"volume": pl.UInt64}

STOCK_TRADES_SCHEMA = {
    "ticker": pl.String,
    "conditions": pl.String,
    "correction": pl.Int32,
    "exchange": pl.Int32,
    "id": pl.Int64,
    "participant_timestamp": pl.Int64,
    "price": pl.Float64,
    "sequence_number": pl.Int64,
    "sip_timestamp": pl.Int64,
    "size": pl.Float32,
    "tape": pl.Int32,
    "trf_id": pl.Int64,
    "trf_timestamp": pl.Int64,
}

STOCK_QUOTES_SCHEMA = {
    "Ticker": pl.String,
    "ask_exchange": pl.Int32,
    "ask_price": pl.Float64,
    "ask_size": pl.UInt32,
    "bid_exchange": pl.Int32,
    "bid_price": pl.Float64,
    "bid_size": pl.UInt32,
    "conditions": pl.String,
    "indicators": pl.String,
    "participant_timestamp": pl.Int64,
    "sequence_number": pl.Int64,
    "sip_timestamp": pl.Int64,
    "tape": pl.Int32,
    "trf_timestamp": pl.Int64,
}

MINUTE_AGGS_SCHEMA = {
    "ticker": pl.String,
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "window_start": pl.Int64,
    "transactions": pl.UInt32,
}

DAY_AGGS_SCHEMA = {
    "ticker": pl.String,
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "window_start": pl.Int64,
    "transactions": pl.UInt32,
}

SCHEMAS = {
    "minute_aggs_v1": MINUTE_AGGS_SCHEMA,
    "day_aggs_v1": DAY_AGGS_SCHEMA,
    "stock_trades_v1": STOCK_TRADES_SCHEMA,
    "stock_quotes_v1": STOCK_QUOTES_SCHEMA,
}


# =============================================================================
# Path helpers
# =============================================================================


def _output_path(data_type: str, date: str) -> Path:
    """Build output Parquet path matching Rust ReplayConfig pattern.

    Pattern: {lake}/us_stocks_sip/{data_type}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet
    """
    from jerry_trader.platform.config.config import lake_data_dir

    year, month = date.split("-")[0], date.split("-")[1]
    return (
        Path(lake_data_dir)
        / "us_stocks_sip"
        / data_type
        / year
        / month
        / f"{date}.parquet"
    )


def _input_path(data_type: str, date: str) -> Path:
    """Build input CSV.gz path.

    Pattern: {raw}/us_stocks_sip/{data_type}/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz
    """
    from jerry_trader.platform.config.config import raw_data_dir

    year, month = date.split("-")[0], date.split("-")[1]
    return (
        Path(raw_data_dir)
        / "us_stocks_sip"
        / data_type
        / year
        / month
        / f"{date}.csv.gz"
    )


# =============================================================================
# Detection
# =============================================================================


def detect_data_type(file_path: str) -> Optional[str]:
    """Detect data type from file path and header content."""
    if "minute_agg" in file_path or "minute_candlestick" in file_path:
        return "minute_aggs_v1"
    elif "day_agg" in file_path or "day_candlestick" in file_path:
        return "day_aggs_v1"
    elif "trade" in file_path:
        if "stock" in file_path or "/stocks/" in file_path or "us_stocks" in file_path:
            return "stock_trades_v1"
        else:
            try:
                with gzip.open(file_path, "rt") as f:
                    first_line = f.readline().strip().lower()
                    if "trf_id" in first_line or "tape" in first_line:
                        return "stock_trades_v1"
            except Exception:
                pass
            return "stock_trades_v1"
    elif "quote" in file_path:
        if "stock" in file_path or "/stocks/" in file_path or "us_stocks" in file_path:
            return "stock_quotes_v1"
        else:
            try:
                with gzip.open(file_path, "rt") as f:
                    first_line = f.readline().strip().lower()
                    if "tape" in first_line or "indicators" in first_line:
                        return "stock_quotes_v1"
            except Exception:
                pass
            return "stock_quotes_v1"
    return None


# =============================================================================
# Core conversion (identical to quant101's CSVGZToParquetConverter logic)
# =============================================================================


def _convert_file(
    csv_gz_path: str | Path,
    parquet_path: str | Path,
    data_type: Optional[str] = None,
    compression: str = "zstd",
    compression_level: int = 3,
) -> Optional[str]:
    """Convert a single CSV.gz to Parquet using Polars streaming.

    This is the same logic as quant101's convert_single_file().
    """
    csv_gz_path = str(csv_gz_path)
    parquet_path = str(parquet_path)

    if not os.path.exists(csv_gz_path):
        logger.error(f"File not found: {csv_gz_path}")
        return None

    # Auto-detect data type if not provided
    if data_type is None:
        data_type = detect_data_type(csv_gz_path)

    if data_type is None:
        logger.error("Could not detect data type")
        return None

    # Look up schema
    schema = (
        SCHEMAS.get(data_type) if isinstance(SCHEMAS.get(data_type), dict) else None
    )
    if schema is None:
        logger.error(f"No schema for data type '{data_type}'")
        return None

    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    logger.info(f"Converting: {csv_gz_path} -> {parquet_path}")
    logger.info(f"Detected data type: {data_type}")

    try:
        # Read header to get actual columns
        with gzip.open(csv_gz_path, "rt") as f:
            header = f.readline().strip()
            actual_columns = [col.strip() for col in header.split(",")]

        # Adjust schema to match actual columns
        adjusted_schema = {}
        post_cast = {}
        for col in actual_columns:
            if col in schema:
                target = schema[col]
                if col in CSV_FLOAT_TO_INT_CASTS:
                    adjusted_schema[col] = pl.Float64
                    post_cast[col] = target
                else:
                    adjusted_schema[col] = target
            else:
                adjusted_schema[col] = pl.String

        lazy_df = pl.scan_csv(
            csv_gz_path,
            quote_char='"',
            schema_overrides=adjusted_schema,
            try_parse_dates=False,
            has_header=True,
            ignore_errors=True,
        )

        if post_cast:
            lazy_df = lazy_df.with_columns(
                [
                    pl.col(c).round(0).cast(dt)
                    for c, dt in post_cast.items()
                    if c in actual_columns
                ]
            )

        logger.info("Lazy_Dataframe scan_csv successfully.")

        lazy_df.sink_parquet(
            parquet_path,
            compression=compression,
            compression_level=compression_level,
            statistics=False,
        )

        size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
        logger.info(
            f"Successfully converted to {parquet_path}, "
            f"size: {size_mb:.1f} MB, compression_level: {compression_level}"
        )

        return parquet_path

    except Exception as e:
        logger.error(f"Error converting {csv_gz_path}: {e}")
        return None


# =============================================================================
# Public API
# =============================================================================


def convert_single(
    csv_gz_path: str | Path,
    data_type: str,
    output_path: str | Path | None = None,
    compression: str = "zstd",
    compression_level: int = 3,
) -> Optional[Path]:
    """Convert a single CSV.gz file to Parquet.

    Args:
        csv_gz_path: Path to CSV.gz file.
        data_type: Data type for schema (e.g. "trades_v1").
        output_path: Output Parquet path (auto-generated if None).
        compression: Parquet compression algorithm.
        compression_level: Compression level.

    Returns:
        Path to created Parquet file, or None if failed.
    """
    csv_gz_path = Path(csv_gz_path)
    if not csv_gz_path.exists():
        logger.warning(f"File not found: {csv_gz_path}")
        return None

    date_str = csv_gz_path.stem.replace(".csv", "")

    if output_path is None:
        output_path = _output_path(data_type, date_str)

    output_path = Path(output_path)

    # Skip if parquet already exists
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"Skipping (already exists): {output_path.name} ({size_mb:.1f} MB)")
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # For legacy names (trades_v1, quotes_v1), let auto-detection resolve
    schema_data_type = None if data_type in ("trades_v1", "quotes_v1") else data_type

    result = _convert_file(
        csv_gz_path=csv_gz_path,
        parquet_path=output_path,
        data_type=schema_data_type,
        compression=compression,
        compression_level=compression_level,
    )

    return Path(result) if result else None


def convert_date(
    date: str,
    data_types: list[str] | None = None,
    compression: str = "zstd",
    compression_level: int = 3,
) -> list[Path]:
    """Convert CSV.gz to Parquet for a date.

    Args:
        date: Date in YYYY-MM-DD format.
        data_types: List of data types to convert. Defaults to ["trades_v1", "quotes_v1"].

    Returns:
        List of output Parquet paths.
    """
    if data_types is None:
        data_types = ["trades_v1", "quotes_v1"]

    results: list[Path] = []
    for data_type in data_types:
        csv_gz = _input_path(data_type, date)
        if not csv_gz.exists():
            logger.warning(f"CSV.gz not found for {data_type}: {csv_gz}")
            continue

        out = convert_single(
            csv_gz,
            data_type=data_type,
            compression=compression,
            compression_level=compression_level,
        )
        if out:
            results.append(out)

    logger.info(f"Converted {len(results)}/{len(data_types)} files for {date}")
    return results


def convert_date_range(
    start_date: str,
    end_date: str,
    data_types: list[str] | None = None,
    compression: str = "zstd",
    compression_level: int = 3,
) -> dict[str, list[Path]]:
    """Convert CSV.gz to Parquet for a date range.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        data_types: List of data types to convert. Defaults to ["trades_v1", "quotes_v1"].

    Returns:
        Dict mapping date -> list of output Parquet paths.
    """
    if data_types is None:
        data_types = ["trades_v1", "quotes_v1"]

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    all_results: dict[str, list[Path]] = {}
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        all_results[date_str] = convert_date(
            date_str,
            data_types=data_types,
            compression=compression,
            compression_level=compression_level,
        )
        current += timedelta(days=1)

    return all_results
