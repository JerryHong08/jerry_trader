"""Backtest configuration and service-level data containers.

BacktestConfig: top-level config for a backtest run.
PreFilterConfig: candidate pre-filter settings.
TickerData: in-memory data container for a single ticker's data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.domain.market.bar import Bar


@dataclass
class PreFilterConfig:
    """Configuration for candidate pre-filtering.

    Controls which stocks are selected from market_snapshot for backtesting.

    Attributes:
        top_n: Rank threshold (stocks must have rank <= top_n at some point).
        min_gain_pct: Minimum changePercent at entry.
        new_entry_only: Only select stocks that newly entered top N (exclude
            stocks already in top N at the first snapshot).
        min_price: Minimum price at entry.
        max_price: Maximum price at entry.
        min_volume: Minimum volume at entry.
        min_relative_volume: Minimum relativeVolumeDaily at entry.
        exclude_etf: Filter via get_common_stocks(backtest_date).
    """

    top_n: int = 20
    min_gain_pct: float = 2.0
    new_entry_only: bool = True
    min_price: float = 0.5
    max_price: float = 500.0
    min_volume: float = 0.0
    min_relative_volume: float = 0.0
    exclude_etf: bool = True


# Default return horizons in milliseconds
DEFAULT_HORIZONS_MS: list[int] = [
    30_000,  # 30s
    60_000,  # 1m
    120_000,  # 2m
    300_000,  # 5m
    600_000,  # 10m
    900_000,  # 15m
    1_800_000,  # 30m
    3_600_000,  # 60m
]

# Horizon label lookup
_HORIZON_LABELS: dict[int, str] = {
    30_000: "30s",
    60_000: "1m",
    120_000: "2m",
    300_000: "5m",
    600_000: "10m",
    900_000: "15m",
    1_800_000: "30m",
    3_600_000: "60m",
}


def horizon_label(ms: int) -> str:
    """Convert horizon ms to human-readable label."""
    return _HORIZON_LABELS.get(ms, f"{ms // 60_000}m")


@dataclass
class BacktestConfig:
    """Top-level configuration for a single backtest run.

    Attributes:
        date: Backtest date in YYYY-MM-DD format.
        rules_dir: Directory containing DSL rule YAML files.
        gain_threshold: Minimum gain % for pre-filter (convenience shortcut
            for pre_filter.min_gain_pct).
        slippage_buffer: Buffer on ask price for slippage model (e.g. 0.001 = 0.1%).
        default_slippage: Fallback slippage when no ask price available.
        horizons_ms: Return horizons to compute (ms offsets from trigger).
        pre_filter: Pre-filter configuration.
        output_clickhouse: Write results to ClickHouse backtest_results table.
        output_console: Print results to terminal.
        clickhouse_config: ClickHouse connection config dict (or None for no CH).
        trades_dir: Directory containing trades Parquet files.
        quotes_dir: Directory containing quotes Parquet files.
    """

    date: str
    rules_dir: str = "config/rules/"
    gain_threshold: float = 2.0
    slippage_buffer: float = 0.001
    default_slippage: float = 0.002
    horizons_ms: list[int] = field(default_factory=lambda: list(DEFAULT_HORIZONS_MS))
    cooldown_ms: int = 60_000  # Min gap between same-rule triggers for same ticker
    session_start: str = "040000"  # Pre-market start HHMMSS (ET)
    session_end: str = "093000"  # Pre-market end HHMMSS (ET)
    metrics_end: str = (
        ""  # End time for return horizon data loading (default: session_end + max horizon)
    )
    pre_filter: PreFilterConfig = field(default_factory=PreFilterConfig)
    output_clickhouse: bool = True
    output_console: bool = True
    clickhouse_config: dict[str, Any] | None = None
    trades_dir: str = ""
    quotes_dir: str = ""

    def __post_init__(self):
        # Sync gain_threshold into pre_filter if pre_filter still has default
        if self.pre_filter.min_gain_pct == 2.0 and self.gain_threshold != 2.0:
            self.pre_filter.min_gain_pct = self.gain_threshold

    def session_range_ms(self) -> tuple[int, int]:
        """Return (start_ms, end_ms) epoch milliseconds for the session window."""
        from jerry_trader.shared.time.timezone import hhmm_to_epoch_ms

        date_yyyymmdd = self.date.replace("-", "")
        start_ms = hhmm_to_epoch_ms(date_yyyymmdd, self.session_start)
        end_ms = hhmm_to_epoch_ms(date_yyyymmdd, self.session_end)
        return start_ms, end_ms

    def data_range_ms(self) -> tuple[int, int]:
        """Return (start_ms, end_ms) for data loading.

        Same as session_range_ms for start, but extends end to cover
        the longest return horizon after session_end.
        """
        from jerry_trader.shared.time.timezone import hhmm_to_epoch_ms

        date_yyyymmdd = self.date.replace("-", "")
        start_ms = hhmm_to_epoch_ms(date_yyyymmdd, self.session_start)

        if self.metrics_end:
            end_ms = hhmm_to_epoch_ms(date_yyyymmdd, self.metrics_end)
        else:
            _, session_end_ms = self.session_range_ms()
            max_horizon = max(self.horizons_ms) if self.horizons_ms else 3_600_000
            end_ms = session_end_ms + max_horizon

        return start_ms, end_ms


@dataclass
class TickerData:
    """In-memory data container for a single ticker's backtest data.

    Loaded by DataLoader from Parquet (trades/quotes) and ClickHouse (bars).

    Attributes:
        symbol: Ticker symbol.
        trades: List of (timestamp_ms, price, size) tuples.
        quotes: List of (timestamp_ms, bid, ask, bid_size, ask_size) tuples.
        bars_1m: 1-minute OHLCV bars from ClickHouse.
        candidate: The Candidate that selected this ticker.
    """

    symbol: str
    trades: list[tuple[int, float, int]] = field(default_factory=list)
    quotes: list[tuple[int, float, float, int, int]] = field(default_factory=list)
    bars_1m: list[Bar] = field(default_factory=list)
    candidate: Candidate | None = None
