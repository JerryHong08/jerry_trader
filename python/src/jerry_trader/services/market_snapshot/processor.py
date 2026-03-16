"""
Unified Snapshot Processor for Market Mover Web Analyzer
Handles snapshot receiving, processing, membership management, and data storage.

Merges functionality from snapshotdataReceiver and snapshotdataAnalyzer (excluding state computation).

Architecture:
- Redis Stream Input: market_snapshot_stream:{date} (from collector)
- Redis Stream Output: market_snapshot_processed:{date} (for BFF and StateEngine)
- Redis Set: movers_subscribed_set:{date} (subscription tracking)
- InfluxDB market_snapshot: stores all subscribed tickers' historical snapshot data

Computation is delegated to core.snapshot.compute (or Rust via _bridge).
"""

import glob
import json
import logging
import os
import socket
import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import clickhouse_connect
import influxdb_client
import polars as pl
import redis
from dotenv import load_dotenv
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()
from jerry_trader._rust import (
    VolumeTracker,
    compute_derived_metrics,
    compute_ranks,
    compute_weighted_mid_price,
)
from jerry_trader.platform.config.config import cache_dir
from jerry_trader.platform.config.session import (
    db_date_to_date,
    make_session_id,
    parse_session_id,
    session_to_influx_tags,
)
from jerry_trader.platform.storage.polars_schemas import enforce_snapshot_schema
from jerry_trader.shared.ids.redis_keys import (
    market_snapshot_processed,
    market_snapshot_stream,
    movers_subscribed_set,
    news_pending,
    state_cursor,
    static_pending,
)
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.utils.data_utils import get_common_stocks
from jerry_trader.shared.utils.parse import _parse_transfrom_timetamp

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class SnapshotProcessor:
    """
    Unified snapshot processor combining receiving and processing:
    - Receives data from Redis stream or local files
    - Computes ranks and derived metrics (via core.snapshot.compute / Rust)
    - Manages subscription set
    - Writes to output Redis Stream and InfluxDB

    Does NOT handle state computation (delegated to StateEngine).
    """

    TOP_N = 20  # Number of top movers to track

    def __init__(
        self,
        load_history: Optional[str] = None,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
    ):

        # Unified session id — single source of truth for mode & date
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        self.load_history = load_history

        # ---------- InfluxDB Configuration ----------
        self._influx_client = None
        self._write_api = None
        self._query_api = None

        influx_cfg = influxdb_config or {}
        self.org = influx_cfg.get("org", "jerryhong")
        self.bucket = influx_cfg.get("bucket", "")
        self.influx_url = None

        if influx_cfg:
            influx_token_env = influx_cfg.get("influx_token_env")
            token = os.environ.get(influx_token_env) if influx_token_env else None

            influx_url_env = influx_cfg.get("influx_url_env")
            self.influx_url = (
                os.getenv(influx_url_env) if influx_url_env else "http://localhost:8086"
            )

            self._influx_client = influxdb_client.InfluxDBClient(
                url=self.influx_url, token=token, org=self.org
            )
            self._write_api = self._influx_client.write_api(write_options=SYNCHRONOUS)
            self._query_api = self._influx_client.query_api()
            logger.info(
                f"__init__ - InfluxDB configured: url={self.influx_url}, bucket={self.bucket}"
            )
        else:
            logger.info(
                "__init__ - InfluxDB not configured; ClickHouse-only mode enabled"
            )

        # ---------- ClickHouse Configuration (gradual migration) ----------
        self.ch_client = None
        ch_cfg = clickhouse_config or {}
        if ch_cfg:
            ch_host = ch_cfg.get("host", "localhost")
            ch_port = ch_cfg.get("port", 8123)
            ch_user = ch_cfg.get("user", "default")
            ch_db = ch_cfg.get("database", "jerry_trader")
            password_env = ch_cfg.get("password_env", "CLICKHOUSE_PASSWORD")
            ch_password = os.getenv(password_env, "")
            try:
                self.ch_client = clickhouse_connect.get_client(
                    host=ch_host,
                    port=ch_port,
                    username=ch_user,
                    password=ch_password,
                    database=ch_db,
                )
                self.ch_client.command("SELECT 1")
                logger.info(
                    f"__init__ - ClickHouse connected: {ch_host}:{ch_port}/{ch_db}"
                )
            except Exception as e:
                logger.warning(
                    f"__init__ - ClickHouse unavailable, keeping Influx-only mode: {e}"
                )
                self.ch_client = None

        if not self.ch_client and not self._influx_client:
            raise ValueError(
                "SnapshotProcessor requires at least one backend: influxdb or clickhouse"
            )

        # ---------- Redis Configuration ----------
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Input stream (from collector/replayer) — unified by session_id
        self.INPUT_STREAM_NAME = market_snapshot_stream(self.session_id)

        # Output stream (for BFF and StateEngine)
        self.OUTPUT_STREAM_NAME = market_snapshot_processed(self.session_id)

        # Set: tracks all tickers that have ever been in top 20 (for subscription)
        self.SUBSCRIBED_SET_NAME = movers_subscribed_set(self.session_id)

        # HSET for cursor recovery (used by _filter_files_after_timestamp)
        self.CURSOR_HSET_NAME = state_cursor(self.session_id)

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

        # Volume tracking — delegated to VolumeTracker (compute.py / Rust)
        self._volume_tracker = VolumeTracker()

        # Reload volume history for recovery (prefer ClickHouse, fallback InfluxDB)
        self._reload_volume_history()

        logger.info(
            f"__init__ - SnapshotProcessor initialized: mode={self.run_mode}, "
            f"session_id={self.session_id}, INPUT={self.INPUT_STREAM_NAME}, OUTPUT={self.OUTPUT_STREAM_NAME}"
            f", influxdb_url={self.influx_url}, bucket={self.bucket}, "
            f"clickhouse={'connected' if self.ch_client else 'unavailable'}"
        )

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
    # HISTORICAL DATA LOADING (will be deprecated in the future)
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
        2. Compute ranks (via _bridge → Rust or Python)
        3. Compute derived metrics (via _bridge → Rust or Python)
        4. Update subscription set
        5. Write to output Redis Stream and InfluxDB
        """
        # Step 0: Prepare data (filter common stocks, handle missing data)
        prepared_df = self._prepare_data(df)

        # Step 1: Extract timestamp
        timestamp = self._extract_timestamp(prepared_df)

        # Step 2: Compute ranks (delegated to compute module)
        ranked_df = compute_ranks(prepared_df)

        # Step 3: Compute derived metrics (delegated to compute module)
        enriched_df = compute_derived_metrics(
            ranked_df, timestamp, self._volume_tracker
        )

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
        # Normalise column types up-front so every downstream
        # operation (concat, arithmetic) sees a consistent schema.
        df = enforce_snapshot_schema(df)

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

        # Weighted mid-price (delegated to compute module)
        filled_df = compute_weighted_mid_price(filled_df)

        return filled_df

    def _extract_timestamp(self, df: pl.DataFrame) -> datetime:
        """Extract and parse timestamp from DataFrame."""
        if "timestamp" in df.columns:
            timestamp_value = df["timestamp"].max()
        else:
            timestamp_value = None
        return _parse_transfrom_timetamp(timestamp_value)

    # =========================================================================
    # SUBSCRIPTION MANAGEMENT
    # =========================================================================

    def _update_subscription_set(
        self, current_top_n: pl.DataFrame, timestamp: datetime
    ) -> List[str]:
        """Add new top N tickers to subscription ZSET with first_appearance_time as score.

        Also queues newly subscribed tickers for static data fetch.
        """
        new_subscriptions = []
        timestamp_score = timestamp.timestamp()  # Unix timestamp as score

        # Session-scoped pending sets
        static_pending_key = static_pending(self.session_id)
        news_pending_key = news_pending(self.session_id)

        for row in current_top_n.iter_rows(named=True):
            ticker = row["ticker"]
            # ZADD with NX option: only add if not exists (preserves first appearance time)
            added = self.r.zadd(
                self.SUBSCRIBED_SET_NAME, {ticker: timestamp_score}, nx=True
            )
            if added:
                new_subscriptions.append(ticker)
                # Queue for static data fetch (fundamentals, float, news)
                self.r.sadd(static_pending_key, ticker)
                self.r.sadd(news_pending_key, ticker)
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
        clickhouse_rows = []
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
            date_tag, mode_tag = session_to_influx_tags(self.session_id)
            influx_enabled = self._write_api is not None and bool(self.bucket)
            point = None
            if influx_enabled:
                point = (
                    influxdb_client.Point("market_snapshot")
                    .tag("symbol", ticker)
                    .tag("date", date_tag)
                    .tag("mode", mode_tag)
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
                if point is not None:
                    point = point.field(field_name, numeric_value)

            stream_tickers_data.append(stream_data)
            if point is not None:
                point = point.time(timestamp)
                influx_points.append(point)

            # Gradual migration: add ClickHouse row in parallel with Influx write
            event_time_ms = int(timestamp.timestamp() * 1000)
            clickhouse_rows.append(
                [
                    ticker,
                    date_tag,
                    mode_tag,
                    self.session_id,
                    event_time_ms,
                    timestamp,
                    float(stream_data.get("price", 0.0)),
                    float(stream_data.get("changePercent", 0.0)),
                    float(stream_data.get("volume", 0.0)),
                    float(stream_data.get("prev_close", 0.0)),
                    float(stream_data.get("prev_volume", 0.0)),
                    float(stream_data.get("vwap", 0.0)),
                    float(stream_data.get("bid", 0.0)),
                    float(stream_data.get("ask", 0.0)),
                    float(stream_data.get("bid_size", 0.0)),
                    float(stream_data.get("ask_size", 0.0)),
                    int(stream_data.get("rank", 0)),
                    int(stream_data.get("competition_rank", 0)),
                    float(stream_data.get("change", 0.0)),
                    float(stream_data.get("relativeVolumeDaily", 0.0)),
                    float(stream_data.get("relativeVolume5min", 0.0)),
                ]
            )

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
        if influx_points and self._write_api is not None:
            self._write_api.write(
                bucket=self.bucket, org=self.org, record=influx_points
            )
            logger.debug(
                f"_write_to_output_stream_and_influx - Wrote {len(influx_points)} points"
            )

        # Dual-write supplement: ClickHouse snapshot table
        if self.ch_client and clickhouse_rows:
            try:
                self.ch_client.insert(
                    table="market_snapshot",
                    data=clickhouse_rows,
                    column_names=[
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
                    ],
                )
                logger.debug(
                    f"_write_to_output_stream_and_influx - Wrote {len(clickhouse_rows)} rows to ClickHouse"
                )
            except Exception as e:
                logger.warning(
                    f"_write_to_output_stream_and_influx - ClickHouse write failed (Influx still written): {e}"
                )

        # Set 19-hour TTL on stream
        if self.r.ttl(self.OUTPUT_STREAM_NAME) < 0:
            self.r.expire(self.OUTPUT_STREAM_NAME, 19 * 3600)

    # =========================================================================
    # RECOVERY SUPPORT
    # =========================================================================

    def _reload_volume_history(self) -> None:
        """Reload volume history for recovery.

        Preference order:
        1) ClickHouse (new path)
        2) InfluxDB fallback (legacy path)
        """
        if self.ch_client:
            loaded = self._reload_volume_history_from_clickhouse()
            if loaded > 0:
                return
            logger.info(
                "_reload_volume_history - ClickHouse returned no data; falling back to InfluxDB"
            )

        if self._query_api is not None and self.bucket:
            self._reload_volume_history_from_influx()
        else:
            logger.info(
                "_reload_volume_history - InfluxDB not configured; skipping fallback reload"
            )

    def _reload_volume_history_from_clickhouse(self) -> int:
        """Reload volume history from ClickHouse for recovery.

        Returns total number of loaded points.
        """
        subscribed = self._get_all_subscribed_tickers()
        if not subscribed or not self.ch_client:
            return 0

        range_start, range_end = self._get_reload_time_range(lookback_minutes=6)
        start_ms = self._time_expr_to_epoch_ms(range_start)
        end_ms = self._time_expr_to_epoch_ms(range_end)

        logger.info(
            f"_reload_volume_history_from_clickhouse - Reloading for {len(subscribed)} tickers "
            f"(ms range: {start_ms} to {end_ms})..."
        )

        date_tag, mode_tag = session_to_influx_tags(self.session_id)
        total_loaded = 0

        for ticker in subscribed:
            try:
                query = """
                    SELECT event_time_ms, volume
                    FROM market_snapshot FINAL
                    WHERE symbol = {symbol:String}
                      AND date = {date:String}
                      AND mode = {mode:String}
                      AND event_time_ms >= {start_ms:Int64}
                      AND event_time_ms <= {end_ms:Int64}
                    ORDER BY event_time_ms ASC
                """
                result = self.ch_client.query(
                    query,
                    parameters={
                        "symbol": ticker,
                        "date": date_tag,
                        "mode": mode_tag,
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                    },
                )

                entries = []
                for event_time_ms, volume in result.result_rows:
                    ts = datetime.fromtimestamp(
                        event_time_ms / 1000.0, tz=ZoneInfo("UTC")
                    )
                    if volume is not None:
                        entries.append((ts, float(volume)))

                if entries:
                    self._volume_tracker.reload_history(ticker, entries)
                    total_loaded += len(entries)
            except Exception as e:
                logger.error(
                    f"_reload_volume_history_from_clickhouse - Error for {ticker}: {e}"
                )

        tickers_with_history = sum(
            1 for v in self._volume_tracker.history.values() if v
        )
        logger.info(
            f"_reload_volume_history_from_clickhouse - Reloaded {total_loaded} points for {tickers_with_history} tickers"
        )
        return total_loaded

    def _time_expr_to_epoch_ms(self, value: str) -> int:
        """Convert Influx-style time expression or ISO timestamp to epoch ms."""
        text = (value or "").strip()

        if text == "now()":
            return int(time.time() * 1000)

        if text.startswith("-") and len(text) >= 3:
            unit = text[-1]
            amount_str = text[1:-1]
            if amount_str.isdigit():
                amount = int(amount_str)
                seconds_by_unit = {
                    "s": 1,
                    "m": 60,
                    "h": 3600,
                    "d": 86400,
                    "w": 604800,
                }
                if unit in seconds_by_unit:
                    return int(time.time() * 1000) - (
                        amount * seconds_by_unit[unit] * 1000
                    )

        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Unsupported time expression: {value}") from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
        return int(parsed.timestamp() * 1000)

    def _reload_volume_history_from_influx(self) -> None:
        """Reload volume history from InfluxDB for recovery."""
        if self._query_api is None or not self.bucket:
            logger.info("_reload_volume_history_from_influx - InfluxDB unavailable")
            return

        subscribed = self._get_all_subscribed_tickers()
        if not subscribed:
            logger.info("_reload_volume_history_from_influx - No subscribed tickers")
            return

        range_start, range_end = self._get_reload_time_range(lookback_minutes=6)

        logger.info(
            f"_reload_volume_history_from_influx - Reloading for {len(subscribed)} tickers "
            f"(range: {range_start} to {range_end})..."
        )

        date_tag, mode_tag = session_to_influx_tags(self.session_id)
        for ticker in subscribed:
            query = f"""
            from(bucket: "{self.bucket}")
                |> range(start: {range_start}, stop: {range_end})
                |> filter(fn: (r) => r["_measurement"] == "market_snapshot")
                |> filter(fn: (r) => r["symbol"] == "{ticker}")
                |> filter(fn: (r) => r["date"] == "{date_tag}")
                |> filter(fn: (r) => r["mode"] == "{mode_tag}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> sort(columns: ["_time"], desc: false)
            """
            try:
                tables = self._query_api.query(query, org=self.org)
                entries = []
                for table in tables:
                    for record in table.records:
                        ts = record.get_time()
                        volume = record.values.get("volume", 0)
                        if volume is not None:
                            entries.append((ts, float(volume)))
                if entries:
                    self._volume_tracker.reload_history(ticker, entries)
            except Exception as e:
                logger.error(
                    f"_reload_volume_history_from_influx - Error for {ticker}: {e}"
                )

        tickers_with_history = sum(
            1 for v in self._volume_tracker.history.values() if v
        )
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
                        range_start = (
                            f"{db_date_to_date(self.db_date).isoformat()}T00:00:00Z"
                        )
                    range_end = min_ts.isoformat()
                    return range_start, range_end

            range_start = f"{db_date_to_date(self.db_date).isoformat()}T00:00:00Z"
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
        if self.ch_client:
            try:
                self.ch_client.close()
            except Exception:
                pass
        logger.info("close - SnapshotProcessor closed")
