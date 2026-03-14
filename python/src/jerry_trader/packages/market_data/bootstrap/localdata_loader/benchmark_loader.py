"""
Benchmark data loaders for indices like IRX (Treasury) and SPX (S&P 500).
"""

import datetime
import os

import polars as pl

from jerry_trader.platform.config.config import indices_day_aggs_dir


def load_irx_data(start: str, end: str) -> pl.DataFrame | None:
    """
    Load IRX (13-week Treasury Bill) data for risk-free rate calculations.

    Args:
        start: Start date in format 'YYYY-MM-DD'
        end: End date in format 'YYYY-MM-DD'

    Returns:
        DataFrame with columns: date, irx_rate (daily risk-free rate)
        None if loading fails
    """
    try:
        irx_file = os.path.join(indices_day_aggs_dir, "I_IRX_day.parquet")
        irx = pl.read_parquet(irx_file)

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

        # Convert IRX to daily rate (IRX is annualized, divide by 252 trading days * 100)
        irx = irx.with_columns(
            (pl.col("close") / 25200).alias("irx_rate"),
        ).select(["date", "irx_rate"])

        return irx

    except Exception as e:
        print(f"Load IRX data failed: {e}")
        return None


def load_spx_benchmark(start: str, end: str) -> pl.DataFrame | None:
    """
    Load SPX (S&P 500) benchmark data.

    Args:
        start: Start date in format 'YYYY-MM-DD'
        end: End date in format 'YYYY-MM-DD'

    Returns:
        DataFrame with columns: date, close, benchmark_return (normalized from 1.0)
        None if loading fails
    """
    try:
        spx_file = os.path.join(indices_day_aggs_dir, "I_SPX_day.parquet")
        spx = pl.read_parquet(spx_file)

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

        # Normalize to start at 1.0
        spx = spx.with_columns(
            (pl.col("close") / pl.col("close").first()).alias("benchmark_return")
        ).select(["date", "close", "benchmark_return"])

        return spx

    except Exception as e:
        print(f"Failed to load SPX data: {e}")
        return None


if __name__ == "__main__":
    # Test loading benchmark data
    start = "2025-01-01"
    end = "2025-02-01"

    print(f"Loading IRX data from {start} to {end}...")
    irx = load_irx_data(start, end)
    if irx is not None:
        print(irx.head())

    print(f"\nLoading SPX data from {start} to {end}...")
    spx = load_spx_benchmark(start, end)
    if spx is not None:
        print(spx.head())
