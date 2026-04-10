"""Candidate pre-filter for batch backtest.

Queries market_snapshot in ClickHouse to find stocks that newly entered
the top N movers during a trading session.

Design decisions (see roadmap/backtest-prefilter-findings.md):
- mode field is 'live'/'replay', NOT 'premarket'/'regular'
- date format: YYYY-MM-DD
- Only selects stocks that NEWLY entered top N (excludes open-in-top20)
- Uses CTE to identify initial top N, then filters them out
"""

from __future__ import annotations

from typing import Any

from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.services.backtest.config import PreFilterConfig
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.pre_filter", log_to_file=True)

# ClickHouse database/table — configurable via method param
_DEFAULT_TABLE = "market_snapshot"


class PreFilter:
    """Find candidate stocks from market_snapshot for backtesting.

    Queries ClickHouse to identify stocks that newly entered the top N
    movers during a session, with configurable filters.
    """

    def __init__(self, ch_client: Any, database: str = "jerry_trader"):
        self._ch = ch_client
        self._database = database

    @property
    def _table(self) -> str:
        return f"{self._database}.{_DEFAULT_TABLE}"

    def find(
        self,
        date: str,
        config: PreFilterConfig | None = None,
    ) -> list[Candidate]:
        """Find candidate stocks for a given date.

        Args:
            date: Date in YYYY-MM-DD format (e.g. '2026-03-13').
            config: Pre-filter configuration. Uses defaults if None.

        Returns:
            List of Candidate objects, sorted by first_entry_ms.
        """
        config = config or PreFilterConfig()
        mode = "live"  # Default mode for backtest queries

        if config.new_entry_only:
            candidates = self._find_new_entries(date, mode, config)
        else:
            candidates = self._find_all_top_n(date, mode, config)

        # Apply post-query filters
        candidates = self._apply_filters(candidates, config)

        # Apply ETF/exclusion filter
        if config.exclude_etf:
            candidates = self._filter_common_stocks(candidates, date)

        logger.info(
            f"PreFilter: found {len(candidates)} candidates for {date} "
            f"(new_entry_only={config.new_entry_only}, "
            f"min_gain={config.min_gain_pct}%)"
        )

        return candidates

    def _find_new_entries(
        self, date: str, mode: str, config: PreFilterConfig
    ) -> list[Candidate]:
        """Find stocks that newly entered top N (excludes initial top N).

        Uses CTE to identify stocks in the initial snapshot's top N,
        then filters them out.
        """
        query = f"""
            WITH initial_top20 AS (
                SELECT DISTINCT symbol
                FROM {self._table} FINAL
                WHERE date = %(date)s
                  AND mode = %(mode)s
                  AND event_time_ms = (
                      SELECT min(event_time_ms)
                      FROM {self._table} FINAL
                      WHERE date = %(date)s
                        AND mode = %(mode)s
                        AND rank <= %(top_n)s
                  )
                  AND rank <= %(top_n)s
            )
            SELECT
                symbol,
                min(event_time_ms) as first_entry_ms,
                argMin(changePercent, event_time_ms) as gain_at_entry,
                argMin(price, event_time_ms) as price_at_entry,
                any(prev_close) as prev_close,
                argMin(volume, event_time_ms) as volume_at_entry,
                argMin(relativeVolumeDaily, event_time_ms) as relative_volume,
                max(changePercent) as max_gain
            FROM {self._table} FINAL
            WHERE date = %(date)s
              AND mode = %(mode)s
              AND rank <= %(top_n)s
              AND symbol NOT IN (SELECT symbol FROM initial_top20)
            GROUP BY symbol
            ORDER BY first_entry_ms
        """
        params = {
            "date": date,
            "mode": mode,
            "top_n": config.top_n,
        }
        return self._execute_query(query, params)

    def _find_all_top_n(
        self, date: str, mode: str, config: PreFilterConfig
    ) -> list[Candidate]:
        """Find all stocks that were in top N at any point."""
        query = f"""
            SELECT
                symbol,
                min(event_time_ms) as first_entry_ms,
                argMin(changePercent, event_time_ms) as gain_at_entry,
                argMin(price, event_time_ms) as price_at_entry,
                any(prev_close) as prev_close,
                argMin(volume, event_time_ms) as volume_at_entry,
                argMin(relativeVolumeDaily, event_time_ms) as relative_volume,
                max(changePercent) as max_gain
            FROM {self._table} FINAL
            WHERE date = %(date)s
              AND mode = %(mode)s
              AND rank <= %(top_n)s
            GROUP BY symbol
            ORDER BY first_entry_ms
        """
        params = {
            "date": date,
            "mode": mode,
            "top_n": config.top_n,
        }
        return self._execute_query(query, params)

    def _execute_query(self, query: str, params: dict) -> list[Candidate]:
        """Execute ClickHouse query and map results to Candidate objects."""
        try:
            result = self._ch.query(query, parameters=params)
        except Exception as e:
            logger.error(f"PreFilter: ClickHouse query failed - {e}")
            return []

        candidates = []
        for row in result.result_rows:
            try:
                candidates.append(
                    Candidate(
                        symbol=row[0],
                        first_entry_ms=int(row[1]),
                        gain_at_entry=float(row[2]),
                        price_at_entry=float(row[3]),
                        prev_close=float(row[4]),
                        volume_at_entry=float(row[5]),
                        relative_volume=float(row[6]),
                        max_gain=float(row[7]),
                    )
                )
            except (IndexError, ValueError, TypeError) as e:
                logger.warning(f"PreFilter: skipping malformed row - {e}")
                continue

        return candidates

    @staticmethod
    def _apply_filters(
        candidates: list[Candidate], config: PreFilterConfig
    ) -> list[Candidate]:
        """Apply price, volume, and gain filters."""
        filtered = candidates
        if config.min_gain_pct > 0:
            filtered = [c for c in filtered if c.gain_at_entry >= config.min_gain_pct]
        if config.min_price > 0:
            filtered = [c for c in filtered if c.price_at_entry >= config.min_price]
        if config.max_price < float("inf"):
            filtered = [c for c in filtered if c.price_at_entry <= config.max_price]
        if config.min_volume > 0:
            filtered = [c for c in filtered if c.volume_at_entry >= config.min_volume]
        if config.min_relative_volume > 0:
            filtered = [
                c for c in filtered if c.relative_volume >= config.min_relative_volume
            ]
        return filtered

    @staticmethod
    def _filter_common_stocks(
        candidates: list[Candidate], date: str
    ) -> list[Candidate]:
        """Filter out ETFs and non-common stocks using date-aware lookup."""
        try:
            from jerry_trader.shared.utils.data_utils import get_common_stocks

            filter_date = f"{date[:4]}-{date[5:7]}-{date[8:10]}"
            common = get_common_stocks(filter_date).select("ticker").collect()
            common_set = set(common["ticker"].to_list())

            before = len(candidates)
            candidates = [c for c in candidates if c.symbol in common_set]
            excluded = before - len(candidates)
            if excluded:
                logger.info(
                    f"PreFilter: excluded {excluded} non-common stocks "
                    f"(ETFs, ADRs, etc.)"
                )
            return candidates
        except Exception as e:
            logger.warning(f"PreFilter: common stocks filter failed, skipping - {e}")
            return candidates
