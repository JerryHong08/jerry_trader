"""
Data Loader Module - Utilities for data loading, ticker management, and date calculations.

Modules:
    ticker_utils: Ticker mapping and filtering (get_mapped_tickers, get_common_stocks)
    date_utils: Date and calendar utilities (resolve_date_range, generate_backtest_dates)
    benchmark_loader: Benchmark data loading (load_irx_data, load_spx_benchmark)
    path_loader: Data path calculation (DataPathFetcher)

Note:
    All imports are guarded so that missing optional deps (e.g. s3fs)
    do not prevent importing individual submodules like data_loader.py.
"""

from __future__ import annotations

import logging as _logging

_log = _logging.getLogger(__name__)

# ── Lazy / guarded re-exports ────────────────────────────────────────
# These are convenience re-exports only. All consumers should prefer
# importing directly from the submodule (e.g. from .date_utils import ...).

try:
    from .benchmark_loader import load_irx_data, load_spx_benchmark
except Exception:  # noqa: BLE001
    _log.debug("benchmark_loader not available (missing deps?)")

try:
    from .date_utils import (
        generate_backtest_date,
        generate_backtest_dates,
        resolve_date_range,
    )
except Exception:  # noqa: BLE001
    _log.debug("date_utils not available (missing deps?)")

try:
    from .path_loader import DataPathFetcher
except Exception:  # noqa: BLE001
    _log.debug("path_loader not available (missing deps like s3fs?)")

try:
    from .ticker_utils import (
        clear_common_stocks_cache,
        get_common_stocks,
        get_common_stocks_full,
        get_mapped_tickers,
    )
except Exception:  # noqa: BLE001
    _log.debug("ticker_utils not available (missing deps?)")

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
