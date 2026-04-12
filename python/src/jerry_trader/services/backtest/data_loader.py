"""Data loader for batch backtest.

Loads trades and quotes from ClickHouse (preferred) or Parquet (fallback),
and 1m bars from ClickHouse for a list of candidate tickers on a given date.
"""

from __future__ import annotations

from typing import Any

from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.domain.market import Bar
from jerry_trader.services.backtest.config import BacktestConfig, TickerData
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.data_loader", log_to_file=True)


class DataLoader:
    """Load all data needed for a set of backtest candidates.

    Data sources (in priority order):
      1. Trades — ClickHouse trades table (preferred), Parquet fallback
      2. Quotes — ClickHouse quotes table (preferred), Parquet fallback
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
            config: BacktestConfig with date, trades_dir, quotes_dir, session times.

        Returns:
            dict mapping symbol → TickerData.
        """
        date = config.date
        start_ms, end_ms = config.data_range_ms()
        symbols = [c.symbol for c in candidates]
        result: dict[str, TickerData] = {}

        # 1. Bulk load trades + quotes from CH (single query each)
        trades_map: dict[str, list] = {}
        quotes_map: dict[str, list] = {}
        missing_trades: list[str] = []
        missing_quotes: list[str] = []

        if self._ch:
            trades_map = self._load_trades_batch_ch(symbols, date, start_ms, end_ms)
            quotes_map = self._load_quotes_batch_ch(symbols, date, start_ms, end_ms)

        # 2. Build TickerData, fall back to parquet for missing tickers
        try:
            from tqdm import tqdm

            cand_iter = tqdm(candidates, desc="Loading tickers", unit="ticker")
        except ImportError:
            cand_iter = candidates

        for candidate in cand_iter:
            symbol = candidate.symbol

            trades = trades_map.get(symbol, [])
            if not trades and config.trades_dir:
                trades = self._load_trades_parquet(
                    symbol, date.replace("-", ""), config.trades_dir
                )

            quotes = quotes_map.get(symbol, [])
            if not quotes and config.quotes_dir:
                quotes = self._load_quotes_parquet(
                    symbol, date.replace("-", ""), config.quotes_dir
                )

            result[symbol] = TickerData(
                symbol=symbol,
                trades=trades,
                quotes=quotes,
                candidate=candidate,
            )

        # 3. Load 1m bars from ClickHouse (batch for all candidates)
        if self._ch:
            bars_map = self._load_bars_batch(symbols, date)
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

    # =========================================================================
    # ClickHouse loaders (preferred)
    # =========================================================================

    def _load_trades_batch_ch(
        self,
        symbols: list[str],
        date: str,
        start_ms: int = 0,
        end_ms: int = 0,
    ) -> dict[str, list[tuple[int, float, int]]]:
        """Load trades for all symbols in a single CH query.

        Returns dict mapping ticker → [(timestamp_ms, price, size), ...].
        """
        if not self._ch or not symbols:
            return {}
        try:
            time_filter = ""
            params: dict[str, Any] = {"date": date, "symbols": symbols}
            if start_ms > 0 and end_ms > 0:
                time_filter = (
                    f"AND sip_timestamp >= %(start_ns)s "
                    f"AND sip_timestamp < %(end_ns)s "
                )
                params["start_ns"] = start_ms * 1_000_000
                params["end_ns"] = end_ms * 1_000_000

            result = self._ch.query(
                f"SELECT ticker, sip_timestamp, price, size "
                f"FROM {self._database}.trades FINAL "
                f"WHERE date = %(date)s AND ticker IN %(symbols)s "
                f"{time_filter}"
                f"AND (exchange != 4 "
                f"     OR sip_timestamp - participant_timestamp <= 1000000000) "
                f"ORDER BY ticker, sip_timestamp",
                parameters=params,
            )
            trades_map: dict[str, list[tuple[int, float, int]]] = {}
            for row in result.result_rows:
                trades_map.setdefault(row[0], []).append(
                    (int(row[1] // 1_000_000), float(row[2]), int(row[3]))
                )
            logger.debug(
                f"CH trades batch: {len(trades_map)} tickers, {len(result.result_rows):,} rows"
            )
            return trades_map
        except Exception as e:
            logger.error(f"CH trades batch query failed: {e}")
            return {}

    def _load_quotes_batch_ch(
        self,
        symbols: list[str],
        date: str,
        start_ms: int = 0,
        end_ms: int = 0,
    ) -> dict[str, list[tuple[int, float, float, int, int]]]:
        """Load quotes for all symbols in a single CH query.

        Returns dict mapping ticker → [(timestamp_ms, bid, ask, bid_size, ask_size), ...].
        """
        if not self._ch or not symbols:
            return {}
        try:
            time_filter = ""
            params: dict[str, Any] = {"date": date, "symbols": symbols}
            if start_ms > 0 and end_ms > 0:
                time_filter = (
                    f"AND sip_timestamp >= %(start_ns)s "
                    f"AND sip_timestamp < %(end_ns)s "
                )
                params["start_ns"] = start_ms * 1_000_000
                params["end_ns"] = end_ms * 1_000_000

            result = self._ch.query(
                f"SELECT ticker, sip_timestamp, bid_price, ask_price, bid_size, ask_size "
                f"FROM {self._database}.quotes FINAL "
                f"WHERE date = %(date)s AND ticker IN %(symbols)s "
                f"{time_filter}"
                f"ORDER BY ticker, sip_timestamp",
                parameters=params,
            )
            quotes_map: dict[str, list[tuple[int, float, float, int, int]]] = {}
            for row in result.result_rows:
                quotes_map.setdefault(row[0], []).append(
                    (
                        int(row[1] // 1_000_000),
                        float(row[2]),
                        float(row[3]),
                        int(row[4]),
                        int(row[5]),
                    )
                )
            logger.debug(
                f"CH quotes batch: {len(quotes_map)} tickers, {len(result.result_rows):,} rows"
            )
            return quotes_map
        except Exception as e:
            logger.error(f"CH quotes batch query failed: {e}")
            return {}

    def _load_trades_ch(
        self, symbol: str, date: str, start_ms: int = 0, end_ms: int = 0
    ) -> list[tuple[int, float, int]]:
        """Load trades from ClickHouse: (timestamp_ms, price, size).

        Filters out FINRA TRF (exchange=4) delayed reports where
        sip_timestamp - participant_timestamp > 1s. These stale trades
        pollute price-based factors during the 8:00 ET batch release.
        Real-time TRF trades (delay <1s) are kept.

        See roadmap/trf-trade-filtering.md for design rationale.
        """
        if not self._ch:
            return []
        try:
            time_filter = ""
            params: dict[str, str | int] = {"date": date, "ticker": symbol}
            if start_ms > 0 and end_ms > 0:
                time_filter = (
                    f"AND sip_timestamp >= {{start_ns:Int64}} "
                    f"AND sip_timestamp < {{end_ns:Int64}} "
                )
                params["start_ns"] = start_ms * 1_000_000
                params["end_ns"] = end_ms * 1_000_000

            result = self._ch.query(
                f"SELECT sip_timestamp, price, size "
                f"FROM {self._database}.trades FINAL "
                f"WHERE date = {{date:String}} AND ticker = {{ticker:String}} "
                f"{time_filter}"
                f"AND (exchange != 4 "
                f"     OR sip_timestamp - participant_timestamp <= 1000000000) "
                f"ORDER BY sip_timestamp",
                parameters=params,
            )
            return [
                (int(row[0] // 1_000_000), float(row[1]), int(row[2]))
                for row in result.result_rows
            ]
        except Exception as e:
            logger.debug(f"CH trades load failed for {symbol}: {e}")
            return []

    def _load_quotes_ch(
        self, symbol: str, date: str, start_ms: int = 0, end_ms: int = 0
    ) -> list[tuple[int, float, float, int, int]]:
        """Load quotes from ClickHouse: (timestamp_ms, bid, ask, bid_size, ask_size)."""
        if not self._ch:
            return []
        try:
            time_filter = ""
            params: dict[str, str | int] = {"date": date, "ticker": symbol}
            if start_ms > 0 and end_ms > 0:
                time_filter = (
                    f"AND sip_timestamp >= {{start_ns:Int64}} "
                    f"AND sip_timestamp < {{end_ns:Int64}} "
                )
                params["start_ns"] = start_ms * 1_000_000
                params["end_ns"] = end_ms * 1_000_000

            result = self._ch.query(
                f"SELECT sip_timestamp, bid_price, ask_price, bid_size, ask_size "
                f"FROM {self._database}.quotes FINAL "
                f"WHERE date = {{date:String}} AND ticker = {{ticker:String}} "
                f"{time_filter}"
                f"ORDER BY sip_timestamp",
                parameters=params,
            )
            return [
                (
                    int(row[0] // 1_000_000),
                    float(row[1]),
                    float(row[2]),
                    int(row[3]),
                    int(row[4]),
                )
                for row in result.result_rows
            ]
        except Exception as e:
            logger.debug(f"CH quotes load failed for {symbol}: {e}")
            return []

    # =========================================================================
    # Parquet loaders (fallback)
    # =========================================================================

    @staticmethod
    def _load_trades_parquet(
        symbol: str, date_yyyymmdd: str, trades_dir: str
    ) -> list[tuple[int, float, int]]:
        """Load trades from Parquet via Rust."""
        if not trades_dir:
            return []
        try:
            from jerry_trader._rust import load_trades_from_parquet

            return load_trades_from_parquet(trades_dir, symbol, date_yyyymmdd)
        except Exception as e:
            logger.warning(f"Trades load failed for {symbol}: {e}")
            return []

    @staticmethod
    def _load_quotes_parquet(
        symbol: str, date_yyyymmdd: str, quotes_dir: str
    ) -> list[tuple[int, float, float, int, int]]:
        """Load quotes from Parquet via Rust."""
        if not quotes_dir:
            return []
        try:
            from jerry_trader._rust import load_quotes_from_parquet

            return load_quotes_from_parquet(quotes_dir, symbol, date_yyyymmdd)
        except Exception as e:
            logger.warning(f"Quotes load failed for {symbol}: {e}")
            return []

    # =========================================================================
    # Bars (ClickHouse only)
    # =========================================================================

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
