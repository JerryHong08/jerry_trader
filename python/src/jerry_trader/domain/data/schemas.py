"""
Centralized schema definitions for Polygon.io market data.

Single source of truth for column names and types across:
  - CSV.gz → Parquet conversion  (csvgz_to_parquet.py)
  - Data loading / scanning       (data_loader.py)
  - Downstream analytics          (indicators, alpha, etc.)

When Polygon changes its wire format (e.g. volume became Float64 after
2026-02-22), update the schema HERE and every consumer picks it up.
"""

from typing import Dict

import polars as pl

# ── helpers ──────────────────────────────────────────────────────────────────

SchemaDict = Dict[str, pl.DataType]


def _ohlcv_base(volume_dtype: pl.DataType = pl.UInt64) -> SchemaDict:
    """Shared OHLCV columns for aggregate bars."""
    return {
        "ticker": pl.String,
        "volume": volume_dtype,
        "open": pl.Float32,
        "close": pl.Float32,
        "high": pl.Float32,
        "low": pl.Float32,
        "window_start": pl.Int64,
        "transactions": pl.UInt32,
    }


# ── raw / storage schemas (used by csvgz_to_parquet + scan_parquet) ──────────

MINUTE_AGGS_SCHEMA: SchemaDict = _ohlcv_base(pl.UInt64)

DAY_AGGS_SCHEMA: SchemaDict = _ohlcv_base(pl.UInt64)

STOCK_TRADES_SCHEMA: SchemaDict = {
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

STOCK_QUOTES_SCHEMA: SchemaDict = {
    "Ticker": pl.String,  # Note: capital T in Polygon data
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

OPTION_TRADES_SCHEMA: SchemaDict = {
    "ticker": pl.String,
    "conditions": pl.String,
    "correction": pl.Int32,
    "exchange": pl.Int32,
    "participant_timestamp": pl.Int64,
    "price": pl.Float64,
    "sip_timestamp": pl.Int64,
    "size": pl.UInt32,
}

OPTION_QUOTES_SCHEMA: SchemaDict = {
    "ticker": pl.String,
    "ask_exchange": pl.Int32,
    "ask_price": pl.Float64,
    "ask_size": pl.UInt32,
    "bid_exchange": pl.Int32,
    "bid_price": pl.Float64,
    "bid_size": pl.UInt32,
    "sequence_number": pl.Int64,
    "sip_timestamp": pl.Int64,
}

# ── registry keyed by Polygon data-type name ────────────────────────────────

SCHEMAS: Dict[str, SchemaDict | str] = {
    "minute_aggs_v1": MINUTE_AGGS_SCHEMA,
    "day_aggs_v1": DAY_AGGS_SCHEMA,
    "stock_trades_v1": STOCK_TRADES_SCHEMA,
    "stock_quotes_v1": STOCK_QUOTES_SCHEMA,
    "option_trades_v1": OPTION_TRADES_SCHEMA,
    "option_quotes_v1": OPTION_QUOTES_SCHEMA,
    # Legacy names — auto-detected at runtime
    "trades_v1": "auto_detect",
    "quotes_v1": "auto_detect",
}


# ── CSV-time helpers ─────────────────────────────────────────────────────────

# Columns that Polygon may deliver as float in CSV (e.g. "21470.0") but should
# be stored as integer in Parquet.  csvgz_to_parquet reads these as Float64
# then casts to the target type listed here, truncating any fractional part.
CSV_FLOAT_TO_INT_CASTS: SchemaDict = {
    "volume": pl.UInt64,
}


def get_schema(data_type: str) -> SchemaDict | None:
    """Return schema dict for *data_type*, or None if unknown / auto_detect."""
    s = SCHEMAS.get(data_type)
    return s if isinstance(s, dict) else None
