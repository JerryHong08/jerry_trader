"""
Ticker utilities for mapping, filtering, and caching stock tickers.
"""

import datetime
import os
from typing import Optional

import polars as pl

from jerry_trader.config import all_tickers_dir, get_asset_overview_data

# ============================================================================
# Ticker Mapping (FIGI-based connected components)
# ============================================================================


def get_mapped_tickers() -> pl.DataFrame:
    """
    Create ticker mapping using FIGI-based connected components.

    Groups tickers that share the same FIGI (composite or share_class),
    useful for tracking ticker name changes and delistings.

    Returns:
        DataFrame with columns:
        - group_id: Unique group identifier
        - ticker: Individual ticker
        - tickers: List of all tickers in the group
        - latest_ticker: Most recent ticker name
        - all_types: List of security types
        - all_delisted_utc: List of delisting dates
        - all_last_updated_utc: List of update timestamps
    """
    # ======================
    # 1. Load the data
    # ======================
    df = get_asset_overview_data(asset="stocks").lazy()

    # ======================
    # 2. fill null FIGI
    # ======================
    df = (
        df.with_columns(
            pl.when(pl.col("composite_figi").is_null())
            .then(pl.concat_str([pl.lit("NULL_"), pl.arange(0, pl.len())]))
            .otherwise(pl.col("composite_figi"))
            .alias("composite_figi"),
            pl.when(pl.col("share_class_figi").is_null())
            .then(pl.concat_str([pl.lit("NULL_"), pl.arange(0, pl.len())]))
            .alias("share_class_figi"),
            pl.col("type").fill_null("UNKNOWN_TYPE"),
        )
        .group_by([pl.all().exclude("last_updated_utc")])
        .agg(pl.col("last_updated_utc").max())
        .sort("last_updated_utc")
        .unique(subset=["ticker", "cik"], keep="last")
    )

    # ======================
    # 3. create bipartite graph edges (ticker <-> figi + type constrain)
    # ======================
    edges = (
        df.select(["ticker", "type", "composite_figi", "share_class_figi"])
        .unpivot(index=["ticker", "type"], value_name="figi")
        .filter(~pl.col("figi").str.starts_with("NULL_"))
        .select(["ticker", "type", "figi"])
        .unique()
    )

    # ======================
    # 4. Find connected components and create mapping {ticker -> group_id}
    # ======================
    groups = df.select(["ticker", "type"]).with_columns(
        (pl.col("ticker") + "_" + pl.col("type")).hash().alias("group_id")
    )

    changed = True
    while changed:
        t2f = edges.join(groups, on=["ticker", "type"], how="left")
        f2g = t2f.group_by(["figi", "type"]).agg(
            pl.col("group_id").min().alias("group_id")
        )
        new_groups = (
            edges.join(f2g, on=["figi", "type"], how="left")
            .select(["ticker", "type", "group_id"])
            .group_by(["ticker", "type"])
            .agg(pl.col("group_id").min())
        )

        updated = groups.join(
            new_groups, on=["ticker", "type"], how="left", suffix="_new"
        )
        updated = updated.with_columns(
            pl.min_horizontal("group_id", "group_id_new").alias("group_id")
        ).select(["ticker", "type", "group_id"])

        updated_df = updated.collect()
        changed = not updated_df.equals(groups.collect())
        groups = updated_df.lazy()

    # ======================
    # 5. join back to original dataframe
    # ======================
    df = df.join(groups, on=["ticker", "type"], how="left")

    # ======================
    # 6. groupby aggregation
    # ======================
    result = (
        df.group_by("group_id")
        .agg(
            [
                pl.col("ticker").sort_by("last_updated_utc").alias("all_tickers_names"),
                pl.col("type").sort_by("last_updated_utc").alias("all_types"),
                pl.col("ticker")
                .sort_by("last_updated_utc")
                .last()
                .alias("latest_ticker"),
                pl.col("delisted_utc")
                .sort_by("last_updated_utc")
                .alias("all_delisted_utc"),
                pl.col("last_updated_utc").sort().alias("all_last_updated_utc"),
            ]
        )
        .with_columns(pl.col("all_tickers_names").alias("ticker"))
        .explode(["ticker"])
        .rename({"all_tickers_names": "tickers"})
    ).collect()

    return result


# ============================================================================
# Common Stocks Filter (with caching)
# ============================================================================

_common_stocks_cache: Optional[pl.LazyFrame] = None
_common_stocks_cache_date: Optional[str] = None


def get_common_stocks(filter_date: str = "2015-01-01") -> pl.LazyFrame:
    """
    Get common stocks (CS and ADRC) as a LazyFrame with caching.

    Cache is invalidated when filter_date changes.
    Returns LazyFrame for optimal query planning when joining.

    Args:
        filter_date: Filter out stocks delisted before this date (YYYY-MM-DD)

    Returns:
        LazyFrame with columns: ticker
    """
    global _common_stocks_cache, _common_stocks_cache_date

    if _common_stocks_cache is None or _common_stocks_cache_date != filter_date:
        all_tickers_file = os.path.join(all_tickers_dir, "all_stocks_*.parquet")
        all_tickers = pl.scan_parquet(all_tickers_file)

        _common_stocks_cache = all_tickers.filter(
            (pl.col("type").is_in(["CS", "ADRC"]))
            & (
                pl.col("delisted_utc").is_null()
                | (
                    pl.col("delisted_utc")
                    .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .dt.date()
                    > datetime.datetime.strptime(filter_date, "%Y-%m-%d").date()
                )
            )
        ).select("ticker")
        _common_stocks_cache_date = filter_date

    return _common_stocks_cache


def get_common_stocks_full(filter_date: str = "2015-01-01") -> pl.DataFrame:
    """
    Get common stocks with full details (ticker, active, composite_figi).

    Use this when you need more than just ticker for filtering.
    Not cached - use get_common_stocks() for cached joins.
    """
    all_tickers_file = os.path.join(all_tickers_dir, "all_stocks_*.parquet")
    all_tickers = pl.read_parquet(all_tickers_file)

    return all_tickers.filter(
        (pl.col("type").is_in(["CS", "ADRC"]))
        & (
            pl.col("delisted_utc").is_null()
            | (
                pl.col("delisted_utc")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                .dt.date()
                > datetime.datetime.strptime(filter_date, "%Y-%m-%d").date()
            )
        )
    ).select(["ticker", "active", "composite_figi"])


def clear_common_stocks_cache():
    """Clear the common stocks cache (useful for testing or forced refresh)."""
    global _common_stocks_cache, _common_stocks_cache_date
    _common_stocks_cache = None
    _common_stocks_cache_date = None


if __name__ == "__main__":
    # Test ticker mapping
    result = get_mapped_tickers()
    print(f"Mapped tickers: {result.shape}")
    print(result.head())

    # Test common stocks
    common_stocks = get_common_stocks().collect()
    print(f"\nTotal common stocks: {common_stocks.shape}")
    print(common_stocks.head(10))
