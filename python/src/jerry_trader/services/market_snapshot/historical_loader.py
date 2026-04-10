"""
HistoricalLoader — Bootstrap historical snapshots from ClickHouse.

Used for replay mode initialization:
1. Load historical data from ClickHouse (single bulk query)
2. Build internal state (subscription_set, volume_tracker, last_df)
3. Write enriched data to market_snapshot CH table (single bulk insert)
4. After bootstrap: send one frame to Redis OUTPUT Stream
5. Replayer continues from bootstrap end point
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import polars as pl

if TYPE_CHECKING:
    from jerry_trader.services.market_snapshot.processor import SnapshotProcessor

from jerry_trader.platform.config.session import session_to_influx_tags
from jerry_trader.services.market_snapshot.processor import (
    compute_derived_metrics,
    compute_ranks,
    compute_weighted_mid_price,
)
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.parse import _parse_transfrom_timetamp

logger = setup_logger(__name__, log_to_file=True)

# ClickHouse market_snapshot columns (must match table schema)
_CH_SNAPSHOT_COLUMNS = [
    "symbol",
    "date",
    "mode",
    "session_id",
    "event_time_ms",
    "event_time",
    "price",
    "changePercent",
    "volume",
    "prev_close",
    "prev_volume",
    "vwap",
    "bid",
    "ask",
    "bid_size",
    "ask_size",
    "rank",
    "competition_rank",
    "change",
    "relativeVolumeDaily",
    "relativeVolume5min",
]


def _ms_to_time(ms: int) -> str:
    """Convert epoch ms to HH:MM:SS for logging."""
    return datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S")


class HistoricalLoader:
    """Load historical snapshots from ClickHouse for bootstrap.

    Reads from market_snapshot_collector table and builds processor internal
    state (subscription set, volume tracker, last_df), then writes enriched
    data to market_snapshot table in a single bulk insert.
    """

    def __init__(
        self,
        processor: "SnapshotProcessor",
        ch_client: Any,
        session_id: str,
    ):
        self.processor = processor
        self.ch_client = ch_client
        self.session_id = session_id
        self.date_tag, self.mode_tag = session_to_influx_tags(session_id)

        logger.info(
            f"HistoricalLoader initialized: session_id={session_id}, "
            f"date={self.date_tag}, mode={self.mode_tag}"
        )

    # =========================================================================
    # Query API
    # =========================================================================

    def get_time_range(self) -> Tuple[int, int]:
        """Get the time range of available data in ClickHouse."""
        query = """
            SELECT min(timestamp), max(timestamp)
            FROM market_snapshot_collector FINAL
            WHERE date = {date:String} AND mode = {mode:String}
        """
        result = self.ch_client.query(
            query, parameters={"date": self.date_tag, "mode": self.mode_tag}
        )
        if result.result_rows and result.result_rows[0][0] is not None:
            return (result.result_rows[0][0], result.result_rows[0][1])
        return (0, 0)

    def get_window_count(self) -> int:
        """Get total number of distinct time windows."""
        query = """
            SELECT count(DISTINCT timestamp)
            FROM market_snapshot_collector FINAL
            WHERE date = {date:String} AND mode = {mode:String}
        """
        result = self.ch_client.query(
            query, parameters={"date": self.date_tag, "mode": self.mode_tag}
        )
        return result.result_rows[0][0] if result.result_rows else 0

    # =========================================================================
    # Bootstrap API
    # =========================================================================

    def bootstrap(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        speed: float = 0,
    ) -> Dict[str, Any]:
        """Bootstrap historical data from ClickHouse.

        Optimized pipeline:
        1. Bulk read all data from CH (1 query)
        2. Filter common stocks once for entire dataset
        3. Per-window: compute ranks + derived metrics + update subscription set
           (stateful operations — VolumeTracker, subscription ZSET, last_df)
        4. Bulk write enriched data to market_snapshot CH (1 insert)
        5. Send final frame to Redis OUTPUT Stream

        Returns:
            Dict with bootstrap stats
        """
        t0 = time.monotonic()

        logger.info(f"bootstrap - Starting: start={start_ms}, end={end_ms}")

        # Step 1: Bulk read all data
        all_df = self._query_all(start_ms, end_ms)
        if all_df.is_empty():
            logger.warning(
                f"bootstrap - No data found for {self.date_tag}/{self.mode_tag}"
            )
            return {"success": False, "windows_processed": 0}

        t_read = time.monotonic()
        raw_windows = sorted(all_df["timestamp"].unique().to_list())
        logger.info(
            f"bootstrap - Read {len(all_df):,} rows ({len(raw_windows)} windows) in {t_read - t0:.1f}s, "
            f"range: {_ms_to_time(raw_windows[0])} - {_ms_to_time(raw_windows[-1])}"
        )

        # Step 2: Filter common stocks once for entire dataset
        all_prepared = self._prepare_data_bulk(all_df)
        t_prepare = time.monotonic()
        logger.info(f"bootstrap - Prepared data in {t_prepare - t_read:.1f}s")

        # Re-extract windows AFTER prepare (timestamp column may have been converted to datetime)
        windows = sorted(all_prepared["timestamp"].unique().to_list())

        # Step 3: Per-window stateful processing (must be sequential)
        all_enriched_dfs: list[pl.DataFrame] = []
        processed = 0

        # In-memory subscription accumulation — batch ZADD at end instead of per-window Redis calls
        subscribed: dict[str, datetime] = {}  # {ticker: first_appearance_timestamp}
        last_enriched_df: pl.DataFrame | None = None
        last_timestamp: datetime | None = None
        prev_df: pl.DataFrame | None = None  # for per-window last_df filling

        for window_ms in windows:
            df = all_prepared.filter(pl.col("timestamp") == window_ms)
            if df.is_empty():
                continue

            # Fill missing tickers from previous window (same as live _prepare_data)
            if prev_df is not None and len(prev_df) != len(df):
                agg_cols = [c for c in df.columns if c != "ticker"]
                df = (
                    pl.concat([prev_df.lazy(), df.lazy()], how="vertical")
                    .sort("timestamp")
                    .group_by("ticker")
                    .agg([pl.col(c).last() for c in agg_cols])
                    .sort("changePercent", descending=True)
                    .collect()
                )
            prev_df = df

            # Ranks
            ranked_df = compute_ranks(df)

            # Derived metrics (updates VolumeTracker — stateful)
            timestamp = _parse_transfrom_timetamp(window_ms)
            enriched_df = compute_derived_metrics(
                ranked_df, timestamp, self.processor._volume_tracker
            )

            # Accumulate subscription in memory (no Redis per-window)
            max_change = enriched_df["changePercent"].max()
            if max_change is not None and max_change > 0:
                top_n = enriched_df.filter(pl.col("rank") <= self.processor.TOP_N)
                for ticker in top_n["ticker"].to_list():
                    if ticker not in subscribed:
                        subscribed[ticker] = timestamp

            # Update last_df for data filling
            self.processor.last_df = df

            # Collect for bulk CH write
            all_enriched_dfs.append(enriched_df)
            last_enriched_df = enriched_df
            last_timestamp = timestamp
            processed += 1

            if processed % 500 == 0:
                logger.info(f"bootstrap - Processed {processed}/{len(windows)} windows")

        t_process = time.monotonic()
        logger.info(
            f"bootstrap - Processed {processed} windows in {t_process - t_prepare:.1f}s"
        )

        # Batch write subscription set to Redis (single ZADD)
        if subscribed:
            self.processor.r.zadd(
                self.processor.SUBSCRIBED_SET_NAME,
                {t: ts.timestamp() for t, ts in subscribed.items()},
            )
            logger.info(
                f"bootstrap - Batch ZADD {len(subscribed)} tickers to subscription set"
            )

        # Step 4: Bulk write enriched data to market_snapshot CH
        if all_enriched_dfs:
            self._bulk_write_to_ch(pl.concat(all_enriched_dfs))

        t_write = time.monotonic()
        logger.info(f"bootstrap - Wrote to CH in {t_write - t_process:.1f}s")

        # Step 5: Send final frame to Redis OUTPUT Stream
        if last_enriched_df is not None and last_timestamp is not None:
            self._send_final_frame(last_enriched_df, last_timestamp)

        elapsed = time.monotonic() - t0
        logger.info(f"bootstrap - Done: {processed} windows in {elapsed:.1f}s")

        # Compute epoch ms for return value (windows may be datetime after prepare)
        if windows:
            first_ts = windows[0]
            last_ts = windows[-1]
            start_epoch_ms = (
                int(first_ts.timestamp() * 1000)
                if isinstance(first_ts, datetime)
                else int(first_ts)
            )
            end_epoch_ms = (
                int(last_ts.timestamp() * 1000)
                if isinstance(last_ts, datetime)
                else int(last_ts)
            )
        else:
            start_epoch_ms = 0
            end_epoch_ms = 0

        return {
            "success": True,
            "windows_processed": processed,
            "start_ms": start_epoch_ms,
            "end_ms": end_epoch_ms,
            "elapsed_s": round(elapsed, 1),
        }

    def seek_to(
        self,
        target_ms: int,
        clear_after: bool = True,
    ) -> Dict[str, Any]:
        """Seek to a specific time point."""
        logger.info(
            f"seek_to - Seeking to {_ms_to_time(target_ms)}, clear_after={clear_after}"
        )
        result = self.bootstrap(end_ms=target_ms)
        if clear_after and result.get("success"):
            self._clear_after_timestamp(target_ms)
        return result

    # =========================================================================
    # Bulk operations
    # =========================================================================

    def _prepare_data_bulk(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter common stocks for entire dataset at once.

        Unlike processor._prepare_data(), this skips per-window last_df filling
        (not needed during bootstrap — we build state, not serve live data).
        """
        from jerry_trader.platform.storage.polars_schemas import enforce_snapshot_schema
        from jerry_trader.shared.utils.data_utils import get_common_stocks

        df = enforce_snapshot_schema(df)

        # Convert timestamp column
        lf = df.lazy().with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                "America/New_York"
            )
        )

        # Filter date for common stocks — same for all windows
        filter_date = df.select(pl.col("timestamp").max()).to_series()[0]
        if hasattr(filter_date, "strftime"):
            filter_date_str = filter_date.strftime("%Y-%m-%d")
        else:
            filter_date_str = datetime.fromtimestamp(filter_date / 1000).strftime(
                "%Y-%m-%d"
            )

        try:
            common_stocks_lf = get_common_stocks(filter_date_str)
            filtered_df = (
                common_stocks_lf.join(lf, on="ticker", how="inner")
                .sort(["timestamp", "changePercent"], descending=[False, True])
                .collect()
            )
        except Exception as e:
            logger.error(f"_prepare_data_bulk - Error filtering common stocks: {e}")
            filtered_df = lf.sort(
                ["timestamp", "changePercent"], descending=[False, True]
            ).collect()

        filtered_df = compute_weighted_mid_price(filtered_df)
        return filtered_df

    def _bulk_write_to_ch(self, enriched_df: pl.DataFrame) -> int:
        """Write all enriched data to market_snapshot in one CH insert."""
        if self.ch_client is None or enriched_df.is_empty():
            return 0

        # Get all subscribed tickers for filtering
        all_subscribed = set(self.processor._get_all_subscribed_tickers())
        if not all_subscribed:
            logger.warning("_bulk_write_to_ch - No subscribed tickers, skipping write")
            return 0

        # Filter to subscribed tickers only
        subscribed_df = enriched_df.filter(pl.col("ticker").is_in(all_subscribed))

        # Build CH rows
        clickhouse_rows = []
        for row in subscribed_df.iter_rows(named=True):
            ts = row.get("timestamp")
            # timestamp might be datetime (after _prepare_data_bulk conversion) or int
            if isinstance(ts, datetime):
                event_time_ms = int(ts.timestamp() * 1000)
                event_time = ts
            else:
                event_time_ms = int(ts)
                event_time = _parse_transfrom_timetamp(ts)

            clickhouse_rows.append(
                [
                    row["ticker"],  # symbol
                    self.date_tag,  # date
                    self.mode_tag,  # mode
                    self.session_id,  # session_id
                    event_time_ms,  # event_time_ms
                    event_time,  # event_time
                    float(row.get("price", 0) or 0),
                    float(row.get("changePercent", 0) or 0),
                    float(row.get("volume", 0) or 0),
                    float(row.get("prev_close", 0) or 0),
                    float(row.get("prev_volume", 0) or 0),
                    float(row.get("vwap", 0) or 0),
                    float(row.get("bid", 0) or 0),
                    float(row.get("ask", 0) or 0),
                    float(row.get("bid_size", 0) or 0),
                    float(row.get("ask_size", 0) or 0),
                    int(row.get("rank", 0) or 0),
                    int(row.get("competition_rank", 0) or 0),
                    float(row.get("change", 0) or 0),
                    float(row.get("relativeVolumeDaily", 0) or 0),
                    float(row.get("relativeVolume5min", 0) or 0),
                ]
            )

        # Single bulk insert
        BATCH_SIZE = 100_000
        total = 0
        for i in range(0, len(clickhouse_rows), BATCH_SIZE):
            batch = clickhouse_rows[i : i + BATCH_SIZE]
            try:
                self.ch_client.insert(
                    table="market_snapshot",
                    data=batch,
                    column_names=_CH_SNAPSHOT_COLUMNS,
                )
                total += len(batch)
            except Exception as e:
                logger.error(f"_bulk_write_to_ch - CH insert failed at batch {i}: {e}")
                break

        logger.info(f"_bulk_write_to_ch - Inserted {total:,} rows into market_snapshot")
        return total

    # =========================================================================
    # Internal query methods
    # =========================================================================

    def _query_all(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> pl.DataFrame:
        """Query all snapshot data at once from ClickHouse."""
        query = """
            SELECT
                ticker, timestamp, price, volume, prev_close, prev_volume, vwap,
                bid, ask, bid_size, ask_size,
                changePercent, change, rank
            FROM market_snapshot_collector FINAL
            WHERE date = {date:String} AND mode = {mode:String}
        """
        params: Dict[str, Any] = {"date": self.date_tag, "mode": self.mode_tag}

        if start_ms is not None:
            query += " AND timestamp >= {start_ms:Int64}"
            params["start_ms"] = start_ms
        if end_ms is not None:
            query += " AND timestamp <= {end_ms:Int64}"
            params["end_ms"] = end_ms

        query += " ORDER BY timestamp ASC"

        result = self.ch_client.query(query, parameters=params)

        if not result.result_rows:
            return pl.DataFrame()

        rows = []
        for row in result.result_rows:
            (
                ticker,
                timestamp,
                price,
                volume,
                prev_close,
                prev_volume,
                vwap,
                bid,
                ask,
                bid_size,
                ask_size,
                changePercent,
                change,
                rank,
            ) = row
            rows.append(
                {
                    "ticker": ticker,
                    "timestamp": timestamp,
                    "price": float(price or 0.0),
                    "volume": float(volume or 0.0),
                    "prev_close": float(prev_close or 0.0),
                    "prev_volume": float(prev_volume or 0.0),
                    "vwap": float(vwap or 0.0),
                    "bid": float(bid or 0.0),
                    "ask": float(ask or 0.0),
                    "bid_size": float(bid_size or 0.0),
                    "ask_size": float(ask_size or 0.0),
                    "changePercent": float(changePercent or 0.0),
                    "change": float(change or 0.0),
                }
            )

        return pl.DataFrame(rows)

    def _send_final_frame(self, enriched_df: pl.DataFrame, timestamp: datetime) -> None:
        """Send final frame to Redis OUTPUT Stream after bootstrap.

        Takes the already-enriched DataFrame from the last window
        (no re-processing needed).
        """
        all_subscribed = self.processor._get_all_subscribed_tickers()
        self.processor._write_to_output_stream_and_ch(
            enriched_df, all_subscribed, timestamp
        )

        logger.info(
            f"_send_final_frame - Sent snapshot to OUTPUT Stream: "
            f"{len(all_subscribed)} tickers, timestamp={timestamp.isoformat()}"
        )

    def _clear_after_timestamp(self, timestamp_ms: int) -> None:
        """Clear data after timestamp in Redis and ClickHouse."""
        logger.info(
            f"_clear_after_timestamp - Clearing data after {_ms_to_time(timestamp_ms)}"
        )
