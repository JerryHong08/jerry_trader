"""
Docstring for DataSupply.snapshotDataSupply.replayer
MarketSnapshotReplayer: Replays collector.py saved parquet/csv files as market snapshot data simulation.

Usage:
    replayer = MarketSnapshotReplayer(
        replay_date="20260115",
        suffix_id="test",
        redis_config={"host": "localhost", "port": 6379, "db": 0},
        influxdb_config={"influx_url_env": "INFLUXDB_URL"},
    )
    await replayer.start()
"""

import asyncio
import glob
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import influxdb_client
import polars as pl
import redis
from influxdb_client.client.delete_api import DeleteApi

from config import cache_dir
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


def _extract_timestamp_from_filename(filename: str) -> datetime:
    """Extract timestamps info and turn into datetime."""
    basename = os.path.basename(filename)
    timestamp_str = basename.split("_")[0]  # extract YYYYMMDDHHMMSS

    # parse the timestamp str
    year = int(timestamp_str[:4])
    month = int(timestamp_str[4:6])
    day = int(timestamp_str[6:8])
    hour = int(timestamp_str[8:10])
    minute = int(timestamp_str[10:12])
    second = int(timestamp_str[12:14])

    dt = datetime(year, month, day, hour, minute, second)
    return dt.replace(tzinfo=ZoneInfo("America/New_York"))


class MarketSnapshotReplayer:
    """
    Replays market snapshot data from saved parquet/csv files with timing control.

    Features:
    - Async-compatible with start()/stop() interface
    - Configurable speed multiplier
    - Resume from specific timestamp
    - Rollback and clear data utilities
    - Database config passed via constructor (like NewsWorker)
    """

    # InfluxDB defaults (can be overridden via influxdb_config)
    DEFAULT_INFLUX_ORG = "jerryhong"
    DEFAULT_INFLUX_BUCKET = "jerrymmm"

    def __init__(
        self,
        replay_date: str,
        suffix_id: Optional[str] = None,
        speed: float = 1.0,
        file_format: str = "parquet",
        start_from: Optional[str] = None,
        rollback_to: Optional[str] = None,
        clear: bool = False,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the market snapshot replayer.

        Args:
            replay_date: Replay date (YYYYMMDD)
            suffix_id: Optional suffix ID for InfluxDB tagging
            speed: Replay speed multiplier (default: 1.0)
            file_format: File format to read ('parquet' or 'csv')
            start_from: Resume from timestamp (HHMMSS), e.g., "050110"
            rollback_to: Rollback to timestamp (HHMMSS) before starting
            clear: Clear all data before starting (fresh start)
            redis_config: Redis connection config dict with host, port, db keys
            influxdb_config: InfluxDB connection config dict with influx_url_env key
        """
        self.replay_date = replay_date
        self.suffix_id = suffix_id
        self.speed = speed
        self.file_format = file_format
        self.start_from = start_from
        self.rollback_to = rollback_to
        self.clear = clear

        # Parse redis config
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)
        self.r = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        # Parse influxdb config
        influx_cfg = influxdb_config or {}
        influx_url_env = influx_cfg.get("influx_url_env")
        self.influx_url = (
            os.getenv(influx_url_env) if influx_url_env else "http://localhost:8086"
        )
        self.influx_org = influx_cfg.get("org", self.DEFAULT_INFLUX_ORG)
        self.influx_bucket = influx_cfg.get("bucket", self.DEFAULT_INFLUX_BUCKET)

        # Derive db_id for InfluxDB filtering
        self.db_id = f"{replay_date}_{suffix_id}" if suffix_id else f"{replay_date}"

        # Redis key names
        self.INPUT_STREAM = f"market_snapshot_stream_replay:{replay_date}"
        self.OUTPUT_STREAM = f"market_snapshot_processed:{replay_date}"
        self.STATE_STREAM = f"movers_state:{replay_date}"
        self.SUBSCRIBED_ZSET = f"movers_subscribed_set:{replay_date}"
        self.CURSOR_HSET = f"state_cursor:{replay_date}"

        self._running = False
        self._files_replayed = 0

        logger.info(
            f"MarketSnapshotReplayer initialized: date={replay_date}, suffix_id={suffix_id}, "
            f"speed={speed}x, format={file_format}, redis={redis_host}:{redis_port}"
        )

    async def start(self):
        """Start the replayer loop."""
        self._running = True
        logger.info("MarketSnapshotReplayer starting...")

        # Handle rollback if requested
        if self.rollback_to:
            self._rollback_to_timestamp(self.rollback_to)
            # After rollback, update start_from to resume from rollback point
            if not self.start_from:
                self.start_from = self.rollback_to

        # Handle clear if requested
        if self.clear:
            self._clear_all_data()

        # Run the replay
        await self._run_replay()

        logger.info(
            f"MarketSnapshotReplayer stopped. Replayed {self._files_replayed} files."
        )

    def stop(self):
        """Stop the replayer loop."""
        self._running = False

    async def _run_replay(self):
        """Run the replay loop with timing."""
        year = self.replay_date[:4]
        month = self.replay_date[4:6]
        date = self.replay_date[6:8]

        market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, date)

        if not os.path.exists(market_mover_dir):
            logger.warning(f"Directory not found: {market_mover_dir}")
            return

        file_extension = "parquet" if self.file_format == "parquet" else "csv"
        all_files = glob.glob(
            os.path.join(market_mover_dir, f"*_market_snapshot.{file_extension}")
        )

        if not all_files:
            logger.warning(
                f"No market snapshot {file_extension} files found in {market_mover_dir}"
            )
            return

        all_files.sort()
        logger.info(f"Found {len(all_files)} files to replay")

        # Extract timestamps
        file_timestamps = []
        for file in all_files:
            timestamp = _extract_timestamp_from_filename(file)
            file_timestamps.append((file, timestamp))

        # Filter files if start_from is provided
        if self.start_from:
            start_hour = int(self.start_from[:2])
            start_minute = int(self.start_from[2:4])
            start_second = int(self.start_from[4:6])

            start_from_dt = datetime(
                int(self.replay_date[:4]),
                int(self.replay_date[4:6]),
                int(self.replay_date[6:8]),
                start_hour,
                start_minute,
                start_second,
                tzinfo=ZoneInfo("America/New_York"),
            )

            original_count = len(file_timestamps)
            file_timestamps = [
                (f, ts) for f, ts in file_timestamps if ts > start_from_dt
            ]

            if not file_timestamps:
                logger.warning(f"No files found after {start_from_dt}")
                return

            logger.info(
                f"Resuming from {start_from_dt}, skipped {original_count - len(file_timestamps)} files"
            )

        first_file_time = file_timestamps[0][1]
        logger.info(f"Starting replay from {first_file_time}")

        for i, (file, file_timestamp) in enumerate(file_timestamps):
            if not self._running:
                logger.info("Replay stopped by user")
                break

            if i > 0:
                prev_timestamp = file_timestamps[i - 1][1]
                time_diff = (file_timestamp - prev_timestamp).total_seconds()
                adjusted_wait_time = time_diff / self.speed

                if adjusted_wait_time > 0:
                    logger.debug(
                        f"Waiting {adjusted_wait_time:.2f}s (original: {time_diff:.2f}s)"
                    )
                    await asyncio.sleep(adjusted_wait_time)

            logger.info(f"[{file_timestamp}] Reading file: {os.path.basename(file)}")

            try:
                # Read all columns from file
                if self.file_format == "parquet":
                    df = pl.read_parquet(file)
                else:
                    df = pl.read_csv(file)

                # Select only the columns needed for downstream processing
                df = df.select(
                    [
                        pl.col("ticker"),
                        pl.col("todaysChangePerc").alias("changePercent"),
                        pl.col("min_av").alias("volume"),
                        pl.col("lastTrade_p").alias("price"),
                        pl.col("prevDay_c").alias("prev_close"),
                        pl.col("prevDay_v").alias("prev_volume"),
                        pl.col("min_vw").alias("vwap"),
                    ]
                )

                # Add timestamp from filename
                file_timestamp_ms = int(file_timestamp.timestamp() * 1000)
                df = df.with_columns(pl.lit(file_timestamp_ms).alias("timestamp"))

                logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")

                payload = df.write_json()

                self.r.xadd(self.INPUT_STREAM, {"data": payload}, maxlen=10000)
                if self.r.ttl(self.INPUT_STREAM) < 0:
                    self.r.expire(self.INPUT_STREAM, 1 * 19 * 3600)

                self._files_replayed += 1

            except Exception as e:
                logger.error(f"Error processing file {file}: {e}")
                continue

    def _rollback_to_timestamp(self, rollback_time: str) -> None:
        """
        Rollback all Redis and InfluxDB data to a specific timestamp.

        This method cleans up:
        - market_snapshot_stream_replay:{date} - Redis Stream (entries after timestamp)
        - market_snapshot_processed:{date} - Redis Stream (entries after timestamp)
        - movers_state:{date} - Redis Stream (entries after timestamp)
        - movers_subscribed_set:{date} - Redis ZSET (tickers first appeared after timestamp)
        - state_cursor:{date} - Redis HSET (reset timestamps to rollback point)
        - InfluxDB market_snapshot measurement (data after timestamp)
        - InfluxDB movers_state measurement (data after timestamp)

        Args:
            rollback_time: The timestamp to rollback to (HHMMSS), e.g., "061652"
        """
        rollback_hour = int(rollback_time[:2])
        rollback_minute = int(rollback_time[2:4])
        rollback_second = int(rollback_time[4:6])

        rollback_dt = datetime(
            int(self.replay_date[:4]),
            int(self.replay_date[4:6]),
            int(self.replay_date[6:8]),
            rollback_hour,
            rollback_minute,
            rollback_second,
            tzinfo=ZoneInfo("America/New_York"),
        )

        rollback_ts = rollback_dt.timestamp()
        rollback_ts_ms = int(rollback_ts * 1000)

        logger.info("=" * 70)
        logger.info(f"Rolling back to {rollback_dt}")
        logger.info("=" * 70)

        # Rollback Redis Streams
        for stream_name in [self.INPUT_STREAM, self.OUTPUT_STREAM, self.STATE_STREAM]:
            self._rollback_stream(stream_name, rollback_ts_ms)

        # Rollback movers_subscribed_set
        if self.r.exists(self.SUBSCRIBED_ZSET):
            removed_count = self.r.zremrangebyscore(
                self.SUBSCRIBED_ZSET, rollback_ts + 0.001, "+inf"
            )
            logger.info(
                f"Removed {removed_count} tickers from {self.SUBSCRIBED_ZSET} (appeared after {rollback_dt})"
            )
        else:
            logger.info(f"ZSET {self.SUBSCRIBED_ZSET} does not exist, skipping")

        # Rollback state_cursor
        if self.r.exists(self.CURSOR_HSET):
            all_cursors = self.r.hgetall(self.CURSOR_HSET)
            fields_to_update = {}

            for symbol, cursor_value in all_cursors.items():
                try:
                    cursor_dt = datetime.fromisoformat(cursor_value)
                    if cursor_dt > rollback_dt:
                        fields_to_update[symbol] = rollback_dt.isoformat()
                except (ValueError, TypeError):
                    continue

            if fields_to_update:
                self.r.hset(self.CURSOR_HSET, mapping=fields_to_update)
                logger.info(
                    f"Reset {len(fields_to_update)} cursor entries in {self.CURSOR_HSET}"
                )
            else:
                logger.info(f"No cursor entries to reset in {self.CURSOR_HSET}")
        else:
            logger.info(f"HSET {self.CURSOR_HSET} does not exist, skipping")

        # Rollback InfluxDB data
        self._rollback_influxdb(rollback_dt)

        logger.info("=" * 70)
        logger.info(f"Rollback completed.")
        logger.info("=" * 70)

    def _rollback_stream(self, stream_name: str, rollback_ts_ms: int) -> int:
        """Remove entries after rollback timestamp from a Redis Stream."""
        if not self.r.exists(stream_name):
            logger.info(f"Stream {stream_name} does not exist, skipping")
            return 0

        entries = self.r.xrange(stream_name)
        entries_to_delete = []

        for entry_id, fields in entries:
            entry_ts_ms = int(entry_id.split("-")[0])
            if entry_ts_ms > rollback_ts_ms:
                entries_to_delete.append(entry_id)

        if entries_to_delete:
            self.r.xdel(stream_name, *entries_to_delete)
            logger.info(f"Deleted {len(entries_to_delete)} entries from {stream_name}")
        else:
            logger.info(f"No entries to delete from {stream_name}")

        return len(entries_to_delete)

    def _rollback_influxdb(self, rollback_dt: datetime) -> None:
        """Rollback InfluxDB data after the specified datetime."""
        token = os.environ.get("INFLUXDB_TOKEN")

        if not token:
            logger.warning("INFLUXDB_TOKEN not set, skipping InfluxDB rollback")
            return

        try:
            client = influxdb_client.InfluxDBClient(
                url=self.influx_url, token=token, org=self.influx_org
            )
            delete_api = client.delete_api()

            start_delete = rollback_dt + timedelta(seconds=1)
            stop_delete = rollback_dt.replace(hour=23, minute=59, second=59)

            logger.info(f"Deleting InfluxDB data from {start_delete} to {stop_delete}")

            # Delete from market_snapshot measurement
            predicate_snapshot = f'_measurement="market_snapshot" AND run_mode="replay" AND db_id="{self.db_id}"'
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_snapshot,
                bucket=self.influx_bucket,
                org=self.influx_org,
            )
            logger.info(f"Deleted InfluxDB market_snapshot data after {rollback_dt}")

            # Delete from movers_state measurement
            predicate_state = f'_measurement="movers_state" AND run_mode="replay" AND db_id="{self.db_id}"'
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_state,
                bucket=self.influx_bucket,
                org=self.influx_org,
            )
            logger.info(f"Deleted InfluxDB movers_state data after {rollback_dt}")

            client.close()

        except Exception as e:
            logger.error(f"InfluxDB rollback failed: {e}")

    def _clear_all_data(self) -> None:
        """Clear all Redis and InfluxDB data for the replay date (fresh start)."""
        logger.info("=" * 70)
        logger.info(f"Clearing all data for replay date: {self.replay_date}")
        logger.info("=" * 70)

        # Delete Redis keys
        for key in [
            self.INPUT_STREAM,
            self.OUTPUT_STREAM,
            self.STATE_STREAM,
            self.SUBSCRIBED_ZSET,
            self.CURSOR_HSET,
        ]:
            if self.r.exists(key):
                self.r.delete(key)
                logger.info(f"Deleted Redis key: {key}")
            else:
                logger.info(f"Redis key {key} does not exist, skipping")

        # Delete InfluxDB data
        token = os.environ.get("INFLUXDB_TOKEN")

        if not token:
            logger.warning("INFLUXDB_TOKEN not set, skipping InfluxDB clear")
            return

        try:
            client = influxdb_client.InfluxDBClient(
                url=self.influx_url, token=token, org=self.influx_org
            )
            delete_api = client.delete_api()

            start_delete = datetime(
                int(self.replay_date[:4]),
                int(self.replay_date[4:6]),
                int(self.replay_date[6:8]),
                0,
                0,
                0,
                tzinfo=ZoneInfo("America/New_York"),
            )
            stop_delete = start_delete + timedelta(days=1)

            logger.info(f"Deleting InfluxDB data from {start_delete} to {stop_delete}")

            # Delete from market_snapshot measurement
            predicate_snapshot = f'_measurement="market_snapshot" AND run_mode="replay" AND db_id="{self.db_id}"'
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_snapshot,
                bucket=self.influx_bucket,
                org=self.influx_org,
            )
            logger.info(f"Deleted InfluxDB market_snapshot data for {self.replay_date}")

            # Delete from movers_state measurement
            predicate_state = f'_measurement="movers_state" AND run_mode="replay" AND db_id="{self.db_id}"'
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_state,
                bucket=self.influx_bucket,
                org=self.influx_org,
            )
            logger.info(f"Deleted InfluxDB movers_state data for {self.replay_date}")

            client.close()

        except Exception as e:
            logger.error(f"InfluxDB clear failed: {e}")

        logger.info("=" * 70)
        logger.info("Clear completed. Ready for fresh replay.")
        logger.info("=" * 70)


# ============================================================================
# Standalone Functions (for backward compatibility with CLI)
# ============================================================================


def rollback_to_timestamp(
    replay_date: str,
    rollback_time: str,
    suffix_id: Optional[str] = None,
    redis_config: Optional[Dict[str, Any]] = None,
    influxdb_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Standalone rollback function for CLI usage."""
    replayer = MarketSnapshotReplayer(
        replay_date=replay_date,
        suffix_id=suffix_id,
        redis_config=redis_config,
        influxdb_config=influxdb_config,
    )
    replayer._rollback_to_timestamp(rollback_time)


def clear_all_data(
    replay_date: str,
    suffix_id: Optional[str] = None,
    redis_config: Optional[Dict[str, Any]] = None,
    influxdb_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Standalone clear function for CLI usage."""
    replayer = MarketSnapshotReplayer(
        replay_date=replay_date,
        suffix_id=suffix_id,
        redis_config=redis_config,
        influxdb_config=influxdb_config,
    )
    replayer._clear_all_data()


def read_market_snapshot_with_timing(
    replay_date: str,
    speed_multiplier: float = 1.0,
    start_from: Optional[str] = None,
    file_format: str = "parquet",
    redis_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Standalone replay function for CLI usage (blocking, sync wrapper)."""
    replayer = MarketSnapshotReplayer(
        replay_date=replay_date,
        speed=speed_multiplier,
        start_from=start_from,
        file_format=file_format,
        redis_config=redis_config,
    )
    asyncio.run(replayer.start())


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Market snapshot replayer with rollback support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Normal replay
    python replayer.py --date 20260115

    # Resume replay from a specific time
    python replayer.py --date 20260115 --start-from 061652

    # Rollback to a specific timestamp (clears data after that time)
    python replayer.py --date 20260115 --rollback-to 061652

    # Clear all data for a fresh start
    python replayer.py --date 20260115 --clear

    # With custom suffix_id (for InfluxDB tagging)
    python replayer.py --date 20260115 --suffix-id test --rollback-to 061652
        """,
    )
    parser.add_argument("--date", default="20251003", help="Replay date (YYYYMMDD)")
    parser.add_argument(
        "--speed", type=float, default=1.0, help="Speed multiplier (default: 1.0)"
    )
    parser.add_argument(
        "--format",
        type=str,
        default="parquet",
        choices=["parquet", "csv"],
        help="File format to replay (default: parquet)",
    )
    parser.add_argument(
        "--start-from",
        type=str,
        default=None,
        help="Resume from timestamp (HHMMSS), e.g., 050110 to start from 05:01:10",
    )
    parser.add_argument(
        "--rollback-to",
        type=str,
        default=None,
        help="Rollback all data to timestamp (HHMMSS), e.g., 061652 to rollback to 06:16:52",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear all data for the date (fresh start)",
    )
    parser.add_argument(
        "--suffix-id",
        type=str,
        default=None,
        help="Custom suffix ID for InfluxDB tagging (default: replay_{date})",
    )
    parser.add_argument("--redis-host", type=str, default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)

    args = parser.parse_args()

    # Build redis config from CLI args
    redis_config = {
        "host": args.redis_host,
        "port": args.redis_port,
        "db": args.redis_db,
    }

    # Handle rollback command
    if args.rollback_to:
        rollback_to_timestamp(args.date, args.rollback_to, args.suffix_id, redis_config)
    # Handle clear command
    elif args.clear:
        clear_all_data(args.date, args.suffix_id, redis_config)
    # Normal replay
    else:
        logger.info(f"Replaying market snapshots for date: {args.date}")
        logger.info(f"Speed: {args.speed}x")
        logger.info(f"Format: {args.format}")
        if args.start_from:
            logger.info(f"Starting from: {args.start_from}")

        replayer = MarketSnapshotReplayer(
            replay_date=args.date,
            suffix_id=args.suffix_id,
            speed=args.speed,
            file_format=args.format,
            start_from=args.start_from,
            redis_config=redis_config,
        )
        asyncio.run(replayer.start())
