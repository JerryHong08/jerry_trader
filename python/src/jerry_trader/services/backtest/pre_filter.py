"""Candidate pre-filter for batch backtest.

Uses the SAME subscription logic as live processor:
1. Read from collector (all tickers, raw data)
2. Filter to common stocks
3. Per-window iteration with prev_df filling
4. Compute ordinal rank based on common stocks pool
5. Track subscription set (rank <= TOP_N with positive changePercent)

This ensures backtest candidates match what live processor would subscribe,
not the "事后重算" snapshot rank from market_snapshot table.

Optimized for memory: uses window partitioning instead of full DataFrame ops.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from jerry_trader.domain.backtest.types import Candidate
from jerry_trader.services.backtest.config import PreFilterConfig
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger("backtest.pre_filter", log_to_file=True)

TOP_N = 20  # Must match processor.TOP_N


class PreFilter:
    """Find candidate stocks for backtesting using subscription logic.

    Simulates the live processor's subscription behavior:
    - Reads from collector (raw data, all tickers)
    - Filters to common stocks
    - Computes per-window ordinal rank on common stocks pool
    - Tracks subscription set (rank <= TOP_N with positive changePercent)

    This matches live processor behavior, not the snapshot rank from market_snapshot.
    """

    def __init__(self, ch_client: Any, database: str = "jerry_trader"):
        self._ch = ch_client
        self._database = database

    def _check_collector_exists(self, date: str) -> int:
        """Check if market_snapshot_collector has data for this date.

        Returns row count. Raises RuntimeError if no data found.
        """
        try:
            r = self._ch.query(
                f"SELECT count() FROM {self._database}.market_snapshot_collector "
                "WHERE date = %(date)s",
                parameters={"date": date},
            )
            count = r.result_rows[0][0] if r.result_rows else 0
        except Exception:
            count = 0

        if count == 0:
            raise RuntimeError(
                f"No data in market_snapshot_collector for {date}. "
                f"Run 'build-snapshot --date {date}' first to populate it."
            )

        logger.info(f"PreFilter: using collector ({count:,} rows)")
        return count

    def find(
        self,
        date: str,
        config: PreFilterConfig | None = None,
    ) -> list[Candidate]:
        """Find candidate stocks for a given date using subscription logic.

        Args:
            date: Date in YYYY-MM-DD format (e.g. '2026-03-13').
            config: Pre-filter configuration. Uses defaults if None.

        Returns:
            List of Candidate objects, sorted by first_entry_ms.

        Raises:
            RuntimeError: If collector has no data for this date.
        """
        config = config or PreFilterConfig()

        self._check_collector_exists(date)

        # Step 1: Read all collector data into Polars
        logger.info("Reading collector data...")
        query = f"""
            SELECT ticker, timestamp, price, volume, prev_close, prev_volume,
                   changePercent
            FROM {self._database}.market_snapshot_collector
            WHERE date = %(date)s
            ORDER BY timestamp ASC, changePercent DESC
        """
        result = self._ch.query(query, parameters={"date": date})
        columns = list(result.column_names)
        df = pl.DataFrame(
            {
                col: [row[i] for row in result.result_rows]
                for i, col in enumerate(columns)
            }
        )
        logger.info(f"  Loaded {len(df):,} rows from collector")

        # Step 2: Filter to common stocks (same as live processor)
        if config.exclude_etf:
            from jerry_trader.shared.utils.data_utils import get_common_stocks

            common = get_common_stocks(date).select("ticker").collect()
            common_set = set(common["ticker"].to_list())
            before = len(df)
            df = df.filter(pl.col("ticker").is_in(common_set))
            logger.info(f"  Filtered to common stocks: {before:,} -> {len(df):,} rows")

        # Step 3: Pre-partition by timestamp (single scan, avoids repeated filters)
        logger.info("Partitioning by window...")
        window_map: dict[int, pl.DataFrame] = {}
        for part_df in df.partition_by("timestamp"):
            ts = int(part_df["timestamp"][0])
            # Only keep columns needed for subscription logic
            window_map[ts] = part_df.select(
                [
                    "ticker",
                    "changePercent",
                    "price",
                    "volume",
                    "prev_volume",
                    "prev_close",
                ]
            )
        windows = sorted(window_map.keys())
        logger.info(f"  {len(windows)} windows")

        # Free memory from full df
        del df

        # Step 4: Per-window iteration with prev_df filling + compute subscription ranks
        logger.info("Computing subscription ranks...")
        subscribed: set[str] = set()
        first_entry: dict[str, int] = {}
        entry_gain: dict[str, float] = {}
        entry_price: dict[str, float] = {}
        entry_volume: dict[str, float] = {}
        entry_rel_vol: dict[str, float] = {}

        prev_df: pl.DataFrame | None = None
        prev_tickers: set[str] = set()

        # Progress bar
        try:
            from tqdm import tqdm

            window_iter = tqdm(windows, desc="Processing windows", unit="win")
        except ImportError:
            window_iter = windows
            logger.info(f"  Processing {len(windows)} windows")

        for window_ms in window_iter:
            window_df = window_map[window_ms]
            if window_df.is_empty():
                continue

            current_tickers = set(window_df["ticker"].to_list())

            # prev_df filling: carry forward tickers from previous window
            if prev_df is not None and prev_tickers:
                # Only fill if some tickers disappeared from current window
                missing = prev_tickers - current_tickers
                if missing:
                    # Carry forward missing tickers with their last values
                    carry_forward = prev_df.filter(pl.col("ticker").is_in(missing))
                    window_df = pl.concat([window_df, carry_forward], how="vertical")

            # Sort by changePercent for ranking
            window_df = window_df.sort("changePercent", descending=True)

            # Compute ordinal rank (subscription rank on common stocks pool)
            window_df = window_df.with_columns(
                pl.col("changePercent")
                .rank(method="ordinal", descending=True)
                .cast(pl.Int32)
                .alias("subscription_rank"),
            )

            # Subscribe tickers in top N when max changePercent > 0
            max_change = window_df["changePercent"].max()
            if max_change is not None and max_change > 0:
                top_n_df = window_df.filter(pl.col("subscription_rank") <= TOP_N)
                for row in top_n_df.iter_rows(named=True):
                    ticker = row["ticker"]
                    if ticker not in subscribed:
                        subscribed.add(ticker)
                        first_entry[ticker] = window_ms
                        entry_gain[ticker] = float(row["changePercent"])
                        entry_price[ticker] = float(row["price"])
                        entry_volume[ticker] = float(row["volume"])
                        prev_vol = float(row.get("prev_volume", 0) or 0)
                        entry_rel_vol[ticker] = (
                            entry_volume[ticker] / prev_vol if prev_vol > 0 else 1.0
                        )

            # Update prev_df for next window (keep same columns for concat compatibility)
            prev_df = window_df.select(
                [
                    "ticker",
                    "changePercent",
                    "price",
                    "volume",
                    "prev_volume",
                    "prev_close",
                ]
            )
            prev_tickers = set(window_df["ticker"].to_list())

        logger.info(
            f"  Subscription set: {len(subscribed)} tickers entered top {TOP_N}"
        )

        # Step 5: Compute max_gain and peak_volume for each subscribed ticker
        # Reconstruct from window_map to avoid keeping full df in memory
        logger.info("Computing max stats...")
        subscribed_rows: list[dict] = []
        for ts, w_df in window_map.items():
            w_subscribed = w_df.filter(
                pl.col("ticker").is_in(subscribed) & (pl.col("changePercent") > 0)
            )
            for row in w_subscribed.iter_rows(named=True):
                subscribed_rows.append(
                    {
                        "ticker": row["ticker"],
                        "changePercent": row["changePercent"],
                        "volume": row["volume"],
                        "prev_close": row.get("prev_close", 0),
                    }
                )

        if subscribed_rows:
            stats_df = pl.DataFrame(subscribed_rows)
            max_stats = stats_df.group_by("ticker").agg(
                [
                    pl.col("changePercent").max().alias("max_gain"),
                    pl.col("volume").max().alias("peak_volume"),
                    pl.col("prev_close").first().alias("prev_close"),
                ]
            )
            max_stats_map = {
                row["ticker"]: row for row in max_stats.iter_rows(named=True)
            }
        else:
            max_stats_map = {}

        # Step 6: Build Candidate objects
        candidates = []
        for ticker in subscribed:
            if ticker not in first_entry:
                continue
            stats = max_stats_map.get(ticker, {})
            candidates.append(
                Candidate(
                    symbol=ticker,
                    first_entry_ms=first_entry[ticker],
                    gain_at_entry=entry_gain[ticker],
                    price_at_entry=entry_price[ticker],
                    prev_close=float(stats.get("prev_close", 0) or 0),
                    volume_at_entry=entry_volume[ticker],
                    relative_volume=entry_rel_vol[ticker],
                    max_gain=float(stats.get("max_gain", 0) or 0),
                    peak_volume=float(stats.get("peak_volume", 0) or 0),
                )
            )

        # Step 7: Apply new_entry_only filter
        if config.new_entry_only and windows:
            first_window = windows[0]
            first_df = window_map.get(first_window)
            if first_df is not None:
                # Sort and compute rank at first window
                first_df = first_df.sort("changePercent", descending=True).with_columns(
                    pl.col("changePercent")
                    .rank(method="ordinal", descending=True)
                    .cast(pl.Int32)
                    .alias("rank"),
                )
                initial_top_n = set(
                    first_df.filter(pl.col("rank") <= TOP_N)["ticker"].to_list()
                )
                before = len(candidates)
                candidates = [c for c in candidates if c.symbol not in initial_top_n]
                logger.info(
                    f"  new_entry_only: excluded {before - len(candidates)} initial top N"
                )

        # Step 8: Apply post-subscription filters
        candidates = self._apply_filters(candidates, config)

        # Sort by first entry time
        candidates.sort(key=lambda c: c.first_entry_ms)

        logger.info(
            f"PreFilter: found {len(candidates)} candidates for {date} "
            f"(subscription_set={len(subscribed)}, "
            f"new_entry_only={config.new_entry_only}, "
            f"min_gain={config.min_gain_pct}%)"
        )

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
