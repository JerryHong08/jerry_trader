"""
Data Loader Module - Utilities for data loading, ticker management, and date calculations.

Modules:
    ticker_utils: Ticker mapping and filtering (get_mapped_tickers, get_common_stocks)
    date_utils: Date and calendar utilities (resolve_date_range, generate_backtest_dates)
    benchmark_loader: Benchmark data loading (load_irx_data, load_spx_benchmark)
    path_loader: Data path calculation (DataPathFetcher)
"""

# Benchmark loaders
from data.loader.benchmark_loader import load_irx_data, load_spx_benchmark

# Date utilities
from data.loader.date_utils import (
    generate_backtest_date,
    generate_backtest_dates,
    resolve_date_range,
)

# Path loader
from data.loader.path_loader import DataPathFetcher

# Ticker utilities
from data.loader.ticker_utils import (
    clear_common_stocks_cache,
    get_common_stocks,
    get_common_stocks_full,
    get_mapped_tickers,
)

__all__ = [
    # Ticker utilities
    "get_mapped_tickers",
    "get_common_stocks",
    "get_common_stocks_full",
    "clear_common_stocks_cache",
    # Date utilities
    "resolve_date_range",
    "generate_backtest_dates",
    "generate_backtest_date",
    # Benchmark loaders
    "load_irx_data",
    "load_spx_benchmark",
    # Path loader
    "DataPathFetcher",
]
