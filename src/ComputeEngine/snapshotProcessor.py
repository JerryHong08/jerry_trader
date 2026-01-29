"""
Unified Snapshot Processor for Market Mover Web Analyzer
Handles snapshot receiving, processing, membership management, and data storage.

Merges functionality from snapshotdataReceiver and snapshotdataAnalyzer (excluding state computation).

Architecture:
- Redis Stream Input: market_snapshot_stream:{date} (from collector)
- Redis Stream Output: market_snapshot_processed:{date} (for BFF and StateEngine)
- Redis Set: movers_subscribed_set:{date} (subscription tracking)
- InfluxDB market_snapshot: stores all subscribed tickers' historical snapshot data
"""

import glob
import json
import logging
import os
import socket
import time
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Thread
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import influxdb_client
import polars as pl
import redis
from influxdb_client.client.write_api import SYNCHRONOUS

from config import cache_dir
from DataUtils.backtest_utils import get_common_stocks
from DataUtils.transforms import _parse_transfrom_timetamp
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class SnapshotProcessor:
    """
    Unified snapshot processor combining receiving and processing:
    - Receives data from Redis stream or local files
    - Computes ranks and derived metrics
    - Manages subscription set
    - Writes to output Redis Stream and InfluxDB

    Does NOT handle state computation (delegated to StateEngine).
    """

    TOP_N = 20  # Number of top movers to track

    def __init__(
        self,
        replay_date: Optional[str] = None,
        suffix_id: Optional[str] = None,
        load_history: Optional[str] = None,
    ):

        self.run_mode = "replay" if replay_date else "live"
        self.db_date = (
            replay_date
            if replay_date
            else datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
        )
        self.db_id = self._derive_db_id(self.db_date, suffix_id)

        self.load_history = load_history

        # ---------- InfluxDB Configuration ----------
        token = os.environ.get("INFLUXDB_TOKEN")
        self.org = "jerryhong"
        self.bucket = "jerrymmm"
        url = "http://localhost:8086"

        self._influx_client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=self.org
        )
        self._write_api = self._influx_client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._influx_client.query_api()

        # ---------- Redis Configuration ----------
        self.r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

        # Input stream (from collector)
        self.INPUT_STREAM_NAME = f"market_snapshot_stream:{self.db_date}"
        if self.run_mode == "replay":
            self.INPUT_STREAM_NAME = f"market_snapshot_stream_replay:{self.db_date}"

        # Output stream (for BFF and StateEngine)
        self.OUTPUT_STREAM_NAME = f"market_snapshot_processed:{self.db_date}"

        # Set: tracks all tickers that have ever been in top 20 (for subscription)
        self.SUBSCRIBED_SET_NAME = f"movers_subscribed_set:{self.db_date}"

        # HSET for cursor recovery (used by _filter_files_after_timestamp)
        self.CURSOR_HSET_NAME = f"state_cursor:{self.db_date}"

        # Consumer group config
        self.CONSUMER_GROUP = "market_consumers"
        self.CONSUMER_NAME = f"processor_{socket.gethostname()}_{os.getpid()}"

        # Create consumer group if not exists
        try:
            self.r.xgroup_create(
                self.INPUT_STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        # In-memory state for data processing
        self.last_df = pl.DataFrame()

        # Volume tracking for relative volume calculation
        # Structure: {ticker: [(timestamp, volume), ...]}
        self._volume_history: Dict[str, List[Tuple[datetime, float]]] = defaultdict(
            list
        )

        # Reload volume history from InfluxDB for recovery
        self._reload_volume_history_from_influx()

        logger.info(
            f"__init__ - SnapshotProcessor initialized: mode={self.run_mode}, "
            f"db_id={self.db_id}, INPUT={self.INPUT_STREAM_NAME}, OUTPUT={self.OUTPUT_STREAM_NAME}"
        )

    @staticmethod
    def _derive_db_id(db_date: Optional[str], override: Optional[str]) -> str:
        if db_date and override:
            return f"{db_date}_{override}"
        if override:
            return override
        if db_date:
            return db_date
        return "na"

    # =========================================================================
    # PUBLIC API - Start Listener
    # =========================================================================

    def start(self):
        """Start the snapshot processor in a background thread."""
        listener_thread = Thread(target=self._stream_listener, daemon=True)
        listener_thread.start()
        logger.info("start - SnapshotProcessor listener thread started")
        return listener_thread

    def _stream_listener(self):
        """Main listener loop - receives from input stream, processes, writes to output."""
        logger.info(
            f"_stream_listener - Starting Redis Stream consumer {self.CONSUMER_NAME}..."
        )

        # Load historical data if requested
        if self.load_history:
            logger.info(
                f"_stream_listener - Loading historical data for {self.load_history}..."
            )
            try:
                self._load_historical_data(self.load_history)
                logger.info(
                    "_stream_listener - Finished loading historical data.\n"
                    "_stream_listener - Starting real-time listener..."
                )
            except Exception as e:
                logger.error(f"_stream_listener - Error during historical load: {e}")

        # Real-time consumption loop
        while True:
            try:
                self.r.ping()

                messages = self.r.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.CONSUMER_NAME,
                    {self.INPUT_STREAM_NAME: ">"},
                    count=1,
                    block=2000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            self._process_stream_message(message_id, message_data)
                else:
                    logger.debug("_stream_listener - No new messages, waiting...")

            except KeyboardInterrupt:
                logger.info("_stream_listener - Stopping listener...")
                break
            except Exception as e:
                logger.error(f"_stream_listener - Error: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(5)

        logger.info("_stream_listener - Listener stopped")

    def _process_stream_message(self, message_id, message_data):
        """Process a single message from the input stream."""
        try:
            json_data = message_data.get("data") or message_data.get(b"data")
            if isinstance(json_data, bytes):
                json_data = json_data.decode("utf-8")

            df = pl.read_json(
                json_data.encode() if isinstance(json_data, str) else json_data
            )
            result = self._process_snapshot(df, is_historical=False)

            # Acknowledge the message
            self.r.xack(self.INPUT_STREAM_NAME, self.CONSUMER_GROUP, message_id)

            logger.debug(
                f"_process_stream_message - Processed snapshot: "
                f"{result.get('new_subscriptions', [])} new subs, "
                f"{result.get('total_subscribed', 0)} total"
            )

        except Exception as e:
            logger.error(
                f"_process_stream_message - Error processing message {message_id}: {e}"
            )
            import traceback

            traceback.print_exc()

    # =========================================================================
    # HISTORICAL DATA LOADING
    # =========================================================================

    def _load_historical_data(self, date: str) -> None:
        """Load historical data from local files for the given date."""
        year = date[:4]
        month = date[4:6]
        day = date[6:8]

        market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, day)

        if not os.path.exists(market_mover_dir):
            logger.info(f"_load_historical_data - No historical data found for {date}")
            return

        all_files = glob.glob(os.path.join(market_mover_dir, "*_market_snapshot.csv"))
        all_files.sort()

        if not all_files:
            logger.warning(
                f"_load_historical_data - No snapshot files found in {market_mover_dir}"
            )
            return

        # Check for existing cursors for recovery
        min_cursor_ts = self._get_min_cursor()

        if min_cursor_ts:
            files_to_load = self._filter_files_after_timestamp(all_files, min_cursor_ts)
            logger.info(
                f"_load_historical_data - Recovery mode: Found cursor at {min_cursor_ts}, "
                f"loading {len(files_to_load)}/{len(all_files)} files after cursor..."
            )
        else:
            files_to_load = all_files
            logger.info(
                f"_load_historical_data - Fresh load: Loading all {len(files_to_load)} files..."
            )

        for file_path in files_to_load:
            try:
                df = pl.read_csv(file_path)
                self._process_snapshot(df, is_historical=True)
            except Exception as e:
                logger.error(f"_load_historical_data - Error loading {file_path}: {e}")

    def _get_min_cursor(self) -> Optional[datetime]:
        """Get the minimum cursor timestamp from Redis HSET for recovery."""
        try:
            cursors = self.r.hgetall(self.CURSOR_HSET_NAME)
            if not cursors:
                return None

            min_ts = None
            for ticker, cursor_ts in cursors.items():
                try:
                    ts = datetime.fromisoformat(cursor_ts)
                    if min_ts is None or ts < min_ts:
                        min_ts = ts
                except (ValueError, TypeError):
                    continue

            if min_ts:
                logger.info(
                    f"_get_min_cursor - Found minimum cursor at {min_ts.isoformat()}"
                )
            return min_ts

        except Exception as e:
            logger.error(f"_get_min_cursor - Error reading cursors: {e}")
            return None

    def _filter_files_after_timestamp(
        self, files: List[str], min_ts: datetime
    ) -> List[str]:
        """Filter files to only include those after the cursor timestamp."""
        file_with_timestamps = []

        for file_path in files:
            try:
                filename = os.path.basename(file_path)
                time_part = filename.split("_")[0]

                if len(time_part) == 14 and time_part.isdigit():
                    file_dt = datetime(
                        int(time_part[0:4]),
                        int(time_part[4:6]),
                        int(time_part[6:8]),
                        int(time_part[8:10]),
                        int(time_part[10:12]),
                        int(time_part[12:14]),
                        tzinfo=min_ts.tzinfo,
                    )
                    file_with_timestamps.append((file_path, file_dt))
                elif len(time_part) == 6 and time_part.isdigit():
                    file_dt = min_ts.replace(
                        hour=int(time_part[0:2]),
                        minute=int(time_part[2:4]),
                        second=int(time_part[4:6]),
                        microsecond=0,
                    )
                    file_with_timestamps.append((file_path, file_dt))
                else:
                    file_with_timestamps.append((file_path, None))
            except Exception as e:
                logger.warning(
                    f"_filter_files_after_timestamp - Error parsing {file_path}: {e}"
                )
                file_with_timestamps.append((file_path, None))

        file_with_timestamps.sort(key=lambda x: (x[1] is None, x[1]))

        # Find first file after cursor and skip it (already processed)
        first_after_cursor_idx = None
        for i, (file_path, file_dt) in enumerate(file_with_timestamps):
            if file_dt is not None and file_dt > min_ts:
                first_after_cursor_idx = i
                break

        if first_after_cursor_idx is None:
            return []

        start_idx = first_after_cursor_idx + 1
        if start_idx >= len(file_with_timestamps):
            return []

        return [fp for fp, _ in file_with_timestamps[start_idx:]]

    # =========================================================================
    # CORE PROCESSING
    # =========================================================================

    def _process_snapshot(self, df: pl.DataFrame, is_historical: bool = False) -> Dict:
        """
        Main processing entry point.

        Flow:
        1. Filter and prepare data
        2. Compute ranks
        3. Compute derived metrics
        4. Update subscription set
        5. Write to output Redis Stream and InfluxDB
        """
        # Step 0: Prepare data (filter common stocks, handle missing data)
        prepared_df = self._prepare_data(df)

        # Step 1: Extract timestamp
        timestamp = self._extract_timestamp(prepared_df)

        # Step 2: Compute ranks
        ranked_df = self._compute_ranks(prepared_df)

        # Step 3: Compute derived metrics
        enriched_df = self._compute_derived_metrics(ranked_df, timestamp)

        # Step 4: Update subscription set
        current_top_n = enriched_df.head(self.TOP_N)
        new_subscriptions = self._update_subscription_set(current_top_n, timestamp)

        # Step 5: Get all subscribed tickers and write to output stream + InfluxDB
        all_subscribed = self._get_all_subscribed_tickers()
        self._write_to_output_stream_and_influx(enriched_df, all_subscribed, timestamp)

        # Update last_df for data filling
        self.last_df = prepared_df

        result = {
            "timestamp": timestamp.isoformat(),
            "new_subscriptions": new_subscriptions,
            "total_subscribed": len(all_subscribed),
        }
        
        return result

    def _prepare_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter common stocks and fill missing data."""
        # Convert timestamp
        lf = df.lazy().with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                "America/New_York"
            )
        )

        # Get filter date for common stocks lookup
        filter_date = df.select(pl.col("timestamp").max()).to_series()[0]
        if hasattr(filter_date, "strftime"):
            filter_date_str = filter_date.strftime("%Y-%m-%d")
        else:
            from datetime import datetime as dt

            filter_date_str = dt.fromtimestamp(filter_date / 1000).strftime("%Y-%m-%d")

        try:
            common_stocks_lf = get_common_stocks(filter_date_str)
            filtered_df = (
                common_stocks_lf.join(lf, on="ticker", how="inner")
                .sort("changePercent", descending=True)
                .collect()
            )
        except Exception as e:
            logger.error(f"_prepare_data - Error filtering common stocks: {e}")
            filtered_df = lf.sort("changePercent", descending=True).collect()

        # Fill missing data from last snapshot if needed
        if len(self.last_df) > 0 and len(self.last_df) != len(filtered_df):
            # Dynamically get all columns except the grouping column
            all_columns = filtered_df.columns
            agg_columns = [col for col in all_columns if col != "ticker"]

            filled_df = (
                pl.concat([self.last_df.lazy(), filtered_df.lazy()], how="vertical")
                .sort("timestamp")
                .group_by(["ticker"])
                .agg([pl.col(col).last() for col in agg_columns])
                .sort("changePercent", descending=True)
                .collect()
            )
        else:
            filled_df = filtered_df

        return filled_df

    def _extract_timestamp(self, df: pl.DataFrame) -> datetime:
        """Extract and parse timestamp from DataFrame."""
        if "timestamp" in df.columns:
            timestamp_value = df["timestamp"].max()
        else:
            timestamp_value = None
        return _parse_transfrom_timetamp(timestamp_value)

    def _compute_ranks(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add competition ranking based on changePercent."""
        return df.sort("changePercent", descending=True).with_columns(
            pl.col("changePercent")
            .rank(method="min", descending=True)
            .cast(pl.Int32)
            .alias("rank")
        )

    def _compute_derived_metrics(
        self, ranked_df: pl.DataFrame, timestamp: datetime
    ) -> pl.DataFrame:
        """Compute change, relativeVolumeDaily, and relativeVolume5min."""
        # Compute change
        df = ranked_df.with_columns(
            (pl.col("price") - pl.col("prev_close")).alias("change")
        )

        # Compute relativeVolumeDaily
        if "prev_volume" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("prev_volume") > 0)
                .then(pl.col("volume") / pl.col("prev_volume"))
                .otherwise(0.0)
                .alias("relativeVolumeDaily")
            )
        else:
            df = df.with_columns(pl.lit(0.0).alias("relativeVolumeDaily"))

        # Compute relativeVolume5min
        relative_5min_values = []
        for row in df.iter_rows(named=True):
            ticker = row["ticker"]
            volume = row.get("volume", 0.0)
            rel_5min = self._compute_relative_volume_5min(ticker, timestamp, volume)
            relative_5min_values.append(rel_5min)

        df = df.with_columns(pl.Series("relativeVolume5min", relative_5min_values))

        return df

    def _compute_relative_volume_5min(
        self, ticker: str, timestamp: datetime, volume: float
    ) -> float:
        """Compute relativeVolume5min = last_1min_volume / last_5min_avg_volume."""
        self._volume_history[ticker].append((timestamp, volume))

        # Keep only last 6 minutes of data
        cutoff_time = timestamp - timedelta(minutes=6)
        self._volume_history[ticker] = [
            (ts, vol) for ts, vol in self._volume_history[ticker] if ts >= cutoff_time
        ]

        history = self._volume_history[ticker]
        if len(history) < 2:
            return 1.0

        # Calculate last 1 minute volume
        one_min_ago = timestamp - timedelta(minutes=1)
        volume_1min_ago = None
        for ts, vol in reversed(history):
            if ts <= one_min_ago:
                volume_1min_ago = vol
                break

        if volume_1min_ago is None:
            volume_1min_ago = history[0][1]

        last_1min_volume = max(0.0, volume - volume_1min_ago)

        # Calculate last 5 minute average
        five_min_ago = timestamp - timedelta(minutes=5)
        volume_5min_ago = None
        for ts, vol in history:
            if ts <= five_min_ago:
                volume_5min_ago = vol
            else:
                break

        if volume_5min_ago is None:
            volume_5min_ago = history[0][1]
            earliest_ts = history[0][0]
            time_span_minutes = max(
                1.0, (timestamp - earliest_ts).total_seconds() / 60.0
            )
        else:
            time_span_minutes = 5.0

        last_5min_total_volume = max(0.0, volume - volume_5min_ago)
        last_5min_avg_volume = last_5min_total_volume / time_span_minutes

        if last_5min_avg_volume > 0:
            return last_1min_volume / last_5min_avg_volume

        return 1.0

    # =========================================================================
    # SUBSCRIPTION MANAGEMENT
    # =========================================================================

    # Redis key for static data pending queue
    STATIC_PENDING_SET = "static:pending"
    NEWS_PENDING_SET = "static:pending:news"  # News-only refetch

    def _update_subscription_set(
        self, current_top_n: pl.DataFrame, timestamp: datetime
    ) -> List[str]:
        """Add new top N tickers to subscription ZSET with first_appearance_time as score.

        Also queues newly subscribed tickers for static data fetch.
        """
        new_subscriptions = []
        timestamp_score = timestamp.timestamp()  # Unix timestamp as score

        for row in current_top_n.iter_rows(named=True):
            ticker = row["ticker"]
            # ZADD with NX option: only add if not exists (preserves first appearance time)
            added = self.r.zadd(
                self.SUBSCRIBED_SET_NAME, {ticker: timestamp_score}, nx=True
            )
            if added:
                new_subscriptions.append(ticker)
                # Queue for static data fetch (fundamentals, float, news)
                self.r.sadd(self.STATIC_PENDING_SET, ticker)
                self.r.sadd(self.NEWS_PENDING_SET, ticker)
                logger.debug(
                    f"_update_subscription_set - New subscription: {ticker} at {timestamp}"
                )

        if new_subscriptions:
            logger.info(
                f"_update_subscription_set - New subscriptions: {new_subscriptions}, "
                f"queued for static data fetch"
            )

        return new_subscriptions

    def _get_all_subscribed_tickers(self) -> List[str]:
        """Get all subscribed tickers from Redis ZSET."""
        return list(self.r.zrange(self.SUBSCRIBED_SET_NAME, 0, -1))

    def get_subscribed_tickers(self) -> List[str]:
        """Public method to get subscribed tickers."""
        return self._get_all_subscribed_tickers()

    def get_top_n_tickers(self, n: int = 20) -> List[str]:
        """Get top N tickers by rank from the last snapshot in output stream."""
        entries = self.r.xrevrange(self.OUTPUT_STREAM_NAME, count=1)

        if not entries:
            return []

        entry_id, fields = entries[0]
        data_json = fields.get("data")
        if not data_json:
            return []

        try:
            tickers_data = json.loads(data_json)
        except json.JSONDecodeError:
            return []

        current_membership = [
            (item["symbol"], item["rank"])
            for item in tickers_data
            if item.get("rank", 999) <= n
        ]
        current_membership.sort(key=lambda x: x[1])
        return [ticker for ticker, rank in current_membership]

    # =========================================================================
    # OUTPUT: REDIS STREAM & INFLUXDB
    # =========================================================================

    def _write_to_output_stream_and_influx(
        self,
        enriched_df: pl.DataFrame,
        subscribed_tickers: List[str],
        timestamp: datetime,
    ) -> None:
        """Write to output Redis Stream and InfluxDB market_snapshot."""
        df_dict = {row["ticker"]: row for row in enriched_df.iter_rows(named=True)}

        influx_points = []
        timestamp_iso = timestamp.isoformat()
        stream_tickers_data = []

        # Fields to exclude from output
        exclude_fields = {"ticker", "timestamp"}

        # Fields that should remain as integers
        int_fields = {"competition_rank", "rank"}

        for ticker in subscribed_tickers:
            row = df_dict.get(ticker)
            if row is None:
                continue

            # Dynamically build stream data and InfluxDB point
            stream_data = {"symbol": ticker}
            point = (
                influxdb_client.Point("market_snapshot")
                .tag("symbol", ticker)
                .tag("run_mode", self.run_mode)
                .tag("db_id", self.db_id)
            )

            # Process all fields dynamically
            for field_name, field_value in row.items():
                if field_name in exclude_fields:
                    continue

                # Convert to appropriate type
                try:
                    if field_name in int_fields:
                        numeric_value = (
                            int(field_value) if field_value is not None else 0
                        )
                    elif isinstance(field_value, (int, float)):
                        numeric_value = float(field_value)
                    else:
                        numeric_value = (
                            float(field_value) if field_value is not None else 0.0
                        )
                except (ValueError, TypeError):
                    continue

                stream_data[field_name] = numeric_value
                point = point.field(field_name, numeric_value)

            stream_tickers_data.append(stream_data)
            point = point.time(timestamp)
            influx_points.append(point)

        # Write to output stream
        if stream_tickers_data:
            logger.debug(
                "_write_to_output_stream_and_influx - Writing to output stream"
            )
            stream_message = {
                "timestamp": timestamp_iso,
                "data": json.dumps(stream_tickers_data),
            }
            self.r.xadd(self.OUTPUT_STREAM_NAME, stream_message, maxlen=100)

        # Write to InfluxDB
        if influx_points:
            self._write_api.write(
                bucket=self.bucket, org=self.org, record=influx_points
            )
            logger.debug(
                f"_write_to_output_stream_and_influx - Wrote {len(influx_points)} points"
            )

        # Set 19-hour TTL on stream
        if self.r.ttl(self.OUTPUT_STREAM_NAME) < 0:
            self.r.expire(self.OUTPUT_STREAM_NAME, 19 * 3600)

    # =========================================================================
    # RECOVERY SUPPORT
    # =========================================================================

    def _reload_volume_history_from_influx(self) -> None:
        """Reload volume history from InfluxDB for recovery."""
        subscribed = self._get_all_subscribed_tickers()
        if not subscribed:
            logger.info("_reload_volume_history_from_influx - No subscribed tickers")
            return

        range_start, range_end = self._get_reload_time_range(lookback_minutes=6)

        logger.info(
            f"_reload_volume_history_from_influx - Reloading for {len(subscribed)} tickers "
            f"(range: {range_start} to {range_end})..."
        )

        for ticker in subscribed:
            query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {range_start}, stop: {range_end})
                |> filter(fn: (r) => r["_measurement"] == "market_snapshot")
                |> filter(fn: (r) => r["symbol"] == "{ticker}")
                |> filter(fn: (r) => r["run_mode"] == "{self.run_mode}")
                |> filter(fn: (r) => r["db_id"] == "{self.db_id}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"], desc: false)
            """
            try:
                tables = self._query_api.query(query, org=self.org)
                for table in tables:
                    for record in table.records:
                        ts = record.get_time()
                        volume = record.values.get("volume", 0)
                        if volume is not None:
                            self._volume_history[ticker].append((ts, float(volume)))
            except Exception as e:
                logger.error(
                    f"_reload_volume_history_from_influx - Error for {ticker}: {e}"
                )

        tickers_with_history = sum(1 for v in self._volume_history.values() if v)
        logger.info(
            f"_reload_volume_history_from_influx - Reloaded for {tickers_with_history} tickers"
        )

    def _get_reload_time_range(self, lookback_minutes: int = 0) -> Tuple[str, str]:
        """Get time range for InfluxDB reload queries."""
        if self.run_mode == "replay":
            cursors = self.r.hgetall(self.CURSOR_HSET_NAME)

            if cursors:
                min_ts = None
                for ticker, cursor_ts in cursors.items():
                    try:
                        ts = datetime.fromisoformat(cursor_ts)
                        if min_ts is None or ts < min_ts:
                            min_ts = ts
                    except (ValueError, TypeError):
                        continue

                if min_ts:
                    if lookback_minutes > 0:
                        range_start = (
                            min_ts - timedelta(minutes=lookback_minutes)
                        ).isoformat()
                    else:
                        range_start = f"{self.db_date[:4]}-{self.db_date[4:6]}-{self.db_date[6:8]}T00:00:00Z"
                    range_end = min_ts.isoformat()
                    return range_start, range_end

            range_start = (
                f"{self.db_date[:4]}-{self.db_date[4:6]}-{self.db_date[6:8]}T00:00:00Z"
            )
            range_end = "now()"
            return range_start, range_end
        else:
            if lookback_minutes > 0:
                return f"-{lookback_minutes}m", "now()"
            else:
                return "-1d", "now()"

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def close(self):
        """Clean up resources."""
        if self._influx_client:
            self._influx_client.close()
        logger.info("close - SnapshotProcessor closed")
