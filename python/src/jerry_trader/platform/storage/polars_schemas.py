"""Polars Schema Definitions

Infrastructure-level schema definitions for Polars DataFrames.
Used for data validation and type enforcement.
"""

from typing import Dict

import polars as pl
from pydantic import ValidationError

from jerry_trader.domain.market.snapshot import SnapshotMessage

# Snapshot data schema for Parquet optimization
MASSIVE_SNAPSHOT_SCHEMA = {
    "ticker": pl.Utf8,
    "todaysChange": pl.Float64,
    "todaysChangePerc": pl.Float64,
    "updated": pl.Int64,
    # day fields
    "day_c": pl.Float64,
    "day_h": pl.Float64,
    "day_l": pl.Float64,
    "day_o": pl.Float64,
    "day_v": pl.Int64,  # volume should be integer
    "day_vw": pl.Float64,
    # prevDay fields
    "prevDay_c": pl.Float64,
    "prevDay_h": pl.Float64,
    "prevDay_l": pl.Float64,
    "prevDay_o": pl.Float64,
    "prevDay_v": pl.Int64,  # volume should be integer
    "prevDay_vw": pl.Float64,
    # min fields
    "min_av": pl.Int64,  # accumulated volume should be integer
    "min_c": pl.Float64,
    "min_h": pl.Float64,
    "min_l": pl.Float64,
    "min_n": pl.Int64,
    "min_o": pl.Float64,
    "min_t": pl.Int64,
    "min_v": pl.Int64,  # volume should be integer
    "min_vw": pl.Float64,
    # lastTrade fields
    "lastTrade_c": pl.Utf8,  # array stored as JSON string
    "lastTrade_i": pl.Utf8,
    "lastTrade_p": pl.Float64,
    "lastTrade_s": pl.Int64,  # size/volume should be integer
    "lastTrade_t": pl.Int64,
    "lastTrade_x": pl.Int64,
    # lastQuote fields
    "lastQuote_P": pl.Float64,
    "lastQuote_S": pl.Int64,  # ask size should be integer
    "lastQuote_p": pl.Float64,
    "lastQuote_s": pl.Int64,  # bid size should be integer
    "lastQuote_t": pl.Int64,
}


# Canonical Polars schema for processed snapshot DataFrames.
# All numeric columns are pinned to Float64 so that concat / vertical
# operations never hit Int64-vs-Float64 mismatches (e.g. changePercent=0
# being inferred as Int64 by pl.read_json).
SNAPSHOT_NUMERIC_SCHEMA: Dict[str, pl.DataType] = {
    "changePercent": pl.Float64,
    "volume": pl.Float64,
    "price": pl.Float64,
    "prev_close": pl.Float64,
    "prev_volume": pl.Float64,
    "vwap": pl.Float64,
    "bid": pl.Float64,
    "ask": pl.Float64,
    "bid_size": pl.Float64,
    "ask_size": pl.Float64,
}


def enforce_snapshot_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Cast columns to the canonical SNAPSHOT_NUMERIC_SCHEMA types.

    Normalizes an incoming DataFrame so that downstream concat / vertical
    operations never hit schema mismatches.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with enforced schema
    """
    casts = {
        col: dtype
        for col, dtype in SNAPSHOT_NUMERIC_SCHEMA.items()
        if col in df.columns and df.schema[col] != dtype
    }
    return df.cast(casts) if casts else df


def validate_snapshot_schema(df: pl.DataFrame) -> tuple[bool, str]:
    """Fast DataFrame schema validation

    Args:
        df: DataFrame to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check columns
    missing = set(MASSIVE_SNAPSHOT_SCHEMA.keys()) - set(df.columns)
    if missing:
        return False, f"Missing columns: {missing}"

    # Check types
    for col, expected_type in MASSIVE_SNAPSHOT_SCHEMA.items():
        if df[col].dtype != expected_type:
            return (
                False,
                f"Column '{col}' type mismatch: {df[col].dtype} vs {expected_type}",
            )

    # Check for nulls
    null_counts = df.null_count()
    null_cols = [
        col for col in MASSIVE_SNAPSHOT_SCHEMA.keys() if null_counts[col][0] > 0
    ]
    if null_cols:
        return False, f"Null values in columns: {null_cols}"

    return True, ""


def spot_check_snapshot_with_pydantic(df: pl.DataFrame, sample_size: int = 3) -> bool:
    """Deep validation using Pydantic (sample check)

    Args:
        df: DataFrame to validate
        sample_size: Number of rows to sample

    Returns:
        True if validation passes, False otherwise
    """
    if len(df) == 0:
        return False

    sample_size = min(sample_size, len(df))
    records = df.head(sample_size).to_dicts()

    for i, record in enumerate(records):
        try:
            SnapshotMessage(**record)
        except ValidationError as e:
            print(f"❌ Pydantic validation failed for row {i}: {e}")
            return False

    return True
