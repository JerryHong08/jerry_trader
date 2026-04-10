"""Data loader for batch backtest.

Loads trades, quotes (from Parquet via Rust), and 1m bars (from ClickHouse)
for a list of candidate tickers on a given date.
"""

from __future__ import annotations

from typing import Any

from jerry_trader._rust import load_quotes_from_parquet, load_trades_from_parquet
from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.domain.market import Bar
from jerry_trader.services.backtest.config import BacktestConfig, TickerData
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.data_loader", log_to_file=True)


class DataLoader:
    """Load all data needed for a set of backtest candidates.

    Three data sources:
      1. Trades — Parquet via Rust load_trades_from_parquet
      2. Quotes — Parquet via Rust load_quotes_from_parquet
      3. Bars 1m — ClickHouse ohlcv_bars
    """

    def __init__(
        self,
        ch_client: Any | None = None,
        database: str = "jerry_trader",
    ):
        self._ch = ch_client
        self._database = database

    def load(
        self,
        candidates: list[Candidate],
        config: BacktestConfig,
    ) -> dict[str, TickerData]:
        """Load all data for the given candidates.

        Args:
            candidates: Pre-filtered candidate list.
            config: BacktestConfig with trades_dir, quotes_dir, date.

        Returns:
            dict mapping symbol → TickerData.
        """
        date_yyyymmdd = config.date.replace("-", "")
        result: dict[str, TickerData] = {}

        # 1. Load trades + quotes from Parquet (per ticker)
        for candidate in candidates:
            symbol = candidate.symbol
            trades = self._load_trades(symbol, date_yyyymmdd, config.trades_dir)
            quotes = self._load_quotes(symbol, date_yyyymmdd, config.quotes_dir)

            result[symbol] = TickerData(
                symbol=symbol,
                trades=trades,
                quotes=quotes,
                candidate=candidate,
            )

        # 2. Load 1m bars from ClickHouse (batch for all candidates)
        if self._ch:
            symbols = [c.symbol for c in candidates]
            bars_map = self._load_bars_batch(symbols, config.date)
            for symbol, bars in bars_map.items():
                if symbol in result:
                    result[symbol].bars_1m = bars

        total_trades = sum(len(td.trades) for td in result.values())
        total_quotes = sum(len(td.quotes) for td in result.values())
        total_bars = sum(len(td.bars_1m) for td in result.values())
        logger.info(
            f"DataLoader: loaded data for {len(result)} tickers — "
            f"{total_trades} trades, {total_quotes} quotes, {total_bars} bars"
        )

        return result

    @staticmethod
    def _load_trades(
        symbol: str, date_yyyymmdd: str, trades_dir: str
    ) -> list[tuple[int, float, int]]:
        """Load trades from Parquet via Rust."""
        if not trades_dir:
            return []
        try:
            return load_trades_from_parquet(trades_dir, symbol, date_yyyymmdd)
        except Exception as e:
            logger.warning(f"Trades load failed for {symbol}: {e}")
            return []

    @staticmethod
    def _load_quotes(
        symbol: str, date_yyyymmdd: str, quotes_dir: str
    ) -> list[tuple[int, float, float, int, int]]:
        """Load quotes from Parquet via Rust."""
        if not quotes_dir:
            return []
        try:
            return load_quotes_from_parquet(quotes_dir, symbol, date_yyyymmdd)
        except Exception as e:
            logger.warning(f"Quotes load failed for {symbol}: {e}")
            return []

    def _load_bars_batch(self, symbols: list[str], date: str) -> dict[str, list[Bar]]:
        """Load 1m bars from ClickHouse for multiple symbols."""
        if not self._ch or not symbols:
            return {}

        query = f"""
            SELECT ticker, timeframe, open, high, low, close,
                   volume, trade_count, vwap, bar_start, bar_end, session
            FROM {self._database}.ohlcv_bars FINAL
            WHERE ticker IN %(symbols)s
              AND toDate(bar_start) = %(date)s
              AND timeframe = '1m'
            ORDER BY ticker, bar_start
        """

        try:
            result = self._ch.query(
                query,
                parameters={"symbols": symbols, "date": date},
            )
        except Exception as e:
            logger.error(f"Bars batch query failed: {e}")
            return {}

        bars_map: dict[str, list[Bar]] = {}
        for row in result.result_rows:
            try:
                bar = Bar(
                    symbol=row[0],
                    timeframe=row[1],
                    open=float(row[2]),
                    high=float(row[3]),
                    low=float(row[4]),
                    close=float(row[5]),
                    volume=float(row[6]),
                    trade_count=int(row[7]),
                    vwap=float(row[8]),
                    bar_start=int(row[9]),
                    bar_end=int(row[10]),
                    session=row[11],
                )
                bars_map.setdefault(bar.symbol, []).append(bar)
            except (IndexError, ValueError, TypeError) as e:
                logger.warning(f"Bars: skipping malformed row — {e}")

        return bars_map
