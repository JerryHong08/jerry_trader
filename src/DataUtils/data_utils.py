import datetime
import os
from functools import lru_cache
from typing import Optional

import polars as pl

from config import all_tickers_dir

# Module-level cache for common stocks (shared across all callers)
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
        all_tickers = pl.scan_parquet(all_tickers_file)  # Lazy read

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
        ).select(
            "ticker"
        )  # Only select what's needed for joins
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


def generate_backtest_date(
    start_date: str,
    reverse: bool,
    reverse_limit: str = None,
    period: str = "week",
    reverse_limit_count: int = 52,
):

    backtest_dates = []
    if not reverse:
        current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        today = datetime.datetime.now()

        while current_date <= today:
            backtest_dates.append(current_date.strftime("%Y-%m-%d"))

            if period == "week":
                current_date += datetime.timedelta(weeks=1)
            elif period == "month":
                if current_date.month == 12:
                    current_date = current_date.replace(
                        year=current_date.year + 1, month=1
                    )
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            elif period == "day":
                current_date += datetime.timedelta(days=1)
    else:
        current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        if reverse_limit:
            limit_date = datetime.datetime.strptime(reverse_limit, "%Y-%m-%d")
        else:
            limit_date = None

        count = 0
        while True:
            if reverse_limit and current_date < limit_date:
                break
            if not reverse_limit and count >= reverse_limit_count:
                break

            backtest_dates.append(current_date.strftime("%Y-%m-%d"))

            if period == "week":
                current_date -= datetime.timedelta(weeks=1)
            elif period == "month":
                if current_date.month == 1:
                    current_date = current_date.replace(
                        year=current_date.year - 1, month=12
                    )
                else:
                    current_date = current_date.replace(month=current_date.month - 1)
            elif period == "day":
                current_date -= datetime.timedelta(days=1)

            count += 1

    return backtest_dates


def load_irx_data(start, end):
    try:
        irx = pl.read_parquet("I:IRXday.parquet")
        irx = irx.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.replace_time_zone("America/New_York")
            .dt.replace(hour=0, minute=0, second=0)
            .cast(pl.Datetime("ns", "America/New_York"))
            .alias("date")
        )

        irx = irx.filter(
            (
                pl.col("date").dt.date()
                >= datetime.datetime.strptime(start, "%Y-%m-%d").date()
            )
            & (
                pl.col("date").dt.date()
                <= datetime.datetime.strptime(end, "%Y-%m-%d").date()
            )
        ).sort("date")

        irx = (
            irx.with_columns(
                (pl.col("close") / 25200).alias("irx_rate"),
            )
        ).select(["date", "irx_rate"])

        return irx
    except Exception as e:
        print(f"Load IRX data failed: {e}")
        return None


def load_spx_benchmark(start, end):
    """Load SPX benchmark data"""
    try:
        spx = pl.read_parquet("I:SPXday.parquet")
        spx = spx.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.convert_time_zone("America/New_York")
            .dt.replace(hour=0, minute=0, second=0)
            .cast(pl.Datetime("ns", "America/New_York"))
            .alias("date")
        )

        spx = spx.filter(
            (
                pl.col("date").dt.date()
                >= datetime.datetime.strptime(start, "%Y-%m-%d").date()
            )
            & (
                pl.col("date").dt.date()
                <= datetime.datetime.strptime(end, "%Y-%m-%d").date()
            )
        ).sort("date")

        # normalization
        spx = spx.with_columns(
            (pl.col("close") / pl.col("close").first()).alias("benchmark_return")
        ).select(["date", "close", "benchmark_return"])

        return spx

    except Exception as e:
        print(f"加载SPX基准数据失败: {e}")
        return None


if __name__ == "__main__":
    # Test the caching function
    common_stocks = get_common_stocks().collect()
    with pl.Config(tbl_rows=10, tbl_cols=50):
        print(f"Total common stocks: {common_stocks.shape}")
        print(common_stocks.head(10))
