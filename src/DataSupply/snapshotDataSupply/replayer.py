"""
Docstring for DataSupply.snapshotDataSupply.replayer
Replay collector.py saved csv file as marketsnapshot data simulation.
"""

import glob
import json
import logging
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import influxdb_client
import polars as pl
import redis
from influxdb_client.client.delete_api import DeleteApi

from config import cache_dir
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def extract_timestamp_from_filename(filename: str) -> datetime:
    """extract timestamps info and turn into datetime"""
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


def read_market_snapshot_with_timing(
    replay_date: str,
    speed_multiplier: float = 1.0,
    start_from: str | None = None,
    file_format: str = "parquet",
) -> None:
    """
    replay file in market_mover_dir for the given date with timing based on filenames.

    Args:
        replay_date: replay date YYYYMMDD
        speed_multiplier: replay speed
        start_from: optional timestamp (HHMMSS) to resume replay from (will start from the first file after this time)
        file_format: file format to read ('parquet' or 'csv'), default is 'parquet'
    """
    year = replay_date[:4]
    month = replay_date[4:6]
    date = replay_date[6:8]

    market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, date)

    if not os.path.exists(market_mover_dir):
        logger.warning(f"Directory not found: {market_mover_dir}")
        return

    file_extension = "parquet" if file_format == "parquet" else "csv"
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

    # extract timestamps
    file_timestamps = []
    for file in all_files:
        timestamp = extract_timestamp_from_filename(file)
        file_timestamps.append((file, timestamp))

    # filter files if start_from is provided
    if start_from:
        # parse start_from as HHMMSS
        start_hour = int(start_from[:2])
        start_minute = int(start_from[2:4])
        start_second = int(start_from[4:6])

        # create start_from datetime using the replay date
        start_from_dt = datetime(
            int(replay_date[:4]),
            int(replay_date[4:6]),
            int(replay_date[6:8]),
            start_hour,
            start_minute,
            start_second,
            tzinfo=ZoneInfo("America/New_York"),
        )

        # filter to only files after start_from
        original_count = len(file_timestamps)
        file_timestamps = [(f, ts) for f, ts in file_timestamps if ts > start_from_dt]

        if not file_timestamps:
            logger.warning(f"No files found after {start_from_dt}")
            return

        logger.info(
            f"Resuming from {start_from_dt}, skipped {original_count - len(file_timestamps)} files"
        )

    first_file_time = file_timestamps[0][1]

    logger.info(f"Starting replay from {first_file_time}")

    first_file = True

    for i, (file, file_timestamp) in enumerate(file_timestamps):
        if i > 0:
            prev_timestamp = file_timestamps[i - 1][1]
            time_diff = (file_timestamp - prev_timestamp).total_seconds()
            adjusted_wait_time = time_diff / speed_multiplier

            if adjusted_wait_time > 0:
                logger.debug(
                    f"Waiting {adjusted_wait_time:.2f}s (original: {time_diff:.2f}s)"
                )
                time.sleep(adjusted_wait_time)

        logger.info(f"[{file_timestamp}] Reading file: {os.path.basename(file)}")

        try:
            # Read all columns from file (CSV or Parquet)
            if file_format == "parquet":
                df = pl.read_parquet(file)
            else:
                df = pl.read_csv(file)

            # Select only the columns needed for downstream processing
            # Map new column names to original expected names
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

            STREAM_NAME = f"market_snapshot_stream_replay:{replay_date}"
            assert ":" in STREAM_NAME, "STREAM_NAME must include a date suffix!"

            message_id = r.xadd(STREAM_NAME, {"data": payload}, maxlen=10000)
            if r.ttl(STREAM_NAME) < 0:
                r.expire(STREAM_NAME, 1 * 19 * 3600)

        except Exception as e:
            logger.error(f"Error processing file {file}: {e}")
            continue


def rollback_to_timestamp(
    replay_date: str, rollback_time: str, suffix_id: str | None = None
) -> None:
    """
    Rollback all Redis and InfluxDB data to a specific timestamp.

    This function cleans up:
    - market_snapshot_stream_replay:{date} - Redis Stream (entries after timestamp)
    - market_snapshot_processed:{date} - Redis Stream (entries after timestamp)
    - movers_state:{date} - Redis Stream (entries after timestamp)
    - movers_subscribed_set:{date} - Redis ZSET (tickers first appeared after timestamp)
    - state_cursor:{date} - Redis HSET (reset timestamps to rollback point)
    - InfluxDB market_snapshot measurement (data after timestamp)
    - InfluxDB movers_state measurement (data after timestamp)

    Args:
        replay_date: The replay date (YYYYMMDD)
        rollback_time: The timestamp to rollback to (HHMMSS), e.g., "061652"
        suffix_id: Optional suffix ID for InfluxDB tagging
    """
    # Parse rollback timestamp
    rollback_hour = int(rollback_time[:2])
    rollback_minute = int(rollback_time[2:4])
    rollback_second = int(rollback_time[4:6])

    rollback_dt = datetime(
        int(replay_date[:4]),
        int(replay_date[4:6]),
        int(replay_date[6:8]),
        rollback_hour,
        rollback_minute,
        rollback_second,
        tzinfo=ZoneInfo("America/New_York"),
    )

    rollback_ts = rollback_dt.timestamp()
    rollback_ts_ms = int(rollback_ts * 1000)

    logger.info(f"=" * 70)
    logger.info(f"Rolling back to {rollback_dt}")
    logger.info(f"=" * 70)

    # Derive db_id for InfluxDB filtering
    db_id = f"{replay_date}_{suffix_id}" if suffix_id else f"{replay_date}"

    # Redis key names
    INPUT_STREAM = f"market_snapshot_stream_replay:{replay_date}"
    OUTPUT_STREAM = f"market_snapshot_processed:{replay_date}"
    STATE_STREAM = f"movers_state:{replay_date}"
    SUBSCRIBED_ZSET = f"movers_subscribed_set:{replay_date}"
    CURSOR_HSET = f"state_cursor:{replay_date}"

    # =========================================================================
    # 1. Rollback Redis Streams
    # =========================================================================

    def rollback_stream(stream_name: str) -> int:
        """Remove entries after rollback timestamp from a Redis Stream."""
        if not r.exists(stream_name):
            logger.info(f"Stream {stream_name} does not exist, skipping")
            return 0

        # Get all entries and find those after rollback time
        entries = r.xrange(stream_name)
        entries_to_delete = []

        for entry_id, fields in entries:
            # Redis stream ID format: timestamp-sequence (e.g., 1234567890123-0)
            entry_ts_ms = int(entry_id.split("-")[0])
            if entry_ts_ms > rollback_ts_ms:
                entries_to_delete.append(entry_id)

        if entries_to_delete:
            r.xdel(stream_name, *entries_to_delete)
            logger.info(f"Deleted {len(entries_to_delete)} entries from {stream_name}")
        else:
            logger.info(f"No entries to delete from {stream_name}")

        return len(entries_to_delete)

    deleted_input = rollback_stream(INPUT_STREAM)
    deleted_output = rollback_stream(OUTPUT_STREAM)
    deleted_state = rollback_stream(STATE_STREAM)

    # =========================================================================
    # 2. Rollback movers_subscribed_set (ZSET - remove tickers first appeared after timestamp)
    # =========================================================================

    if r.exists(SUBSCRIBED_ZSET):
        # ZREMRANGEBYSCORE removes members with score between min and max
        # We want to remove tickers that first appeared AFTER rollback timestamp
        removed_count = r.zremrangebyscore(SUBSCRIBED_ZSET, rollback_ts + 0.001, "+inf")
        logger.info(
            f"Removed {removed_count} tickers from {SUBSCRIBED_ZSET} (appeared after {rollback_dt})"
        )
    else:
        logger.info(f"ZSET {SUBSCRIBED_ZSET} does not exist, skipping")

    # =========================================================================
    # 3. Rollback state_cursor (HSET - reset cursor timestamps after rollback point)
    # =========================================================================

    if r.exists(CURSOR_HSET):
        all_cursors = r.hgetall(CURSOR_HSET)
        fields_to_update = {}

        for symbol, cursor_value in all_cursors.items():
            try:
                cursor_dt = datetime.fromisoformat(cursor_value)
                if cursor_dt > rollback_dt:
                    # This ticker's cursor is after rollback, reset to rollback time
                    fields_to_update[symbol] = rollback_dt.isoformat()
            except (ValueError, TypeError):
                continue

        if fields_to_update:
            r.hset(CURSOR_HSET, mapping=fields_to_update)
            logger.info(
                f"Reset {len(fields_to_update)} cursor entries in {CURSOR_HSET}"
            )
        else:
            logger.info(f"No cursor entries to reset in {CURSOR_HSET}")
    else:
        logger.info(f"HSET {CURSOR_HSET} does not exist, skipping")

    # =========================================================================
    # 4. Rollback InfluxDB data
    # =========================================================================

    token = os.environ.get("INFLUXDB_TOKEN")
    org = "jerryhong"
    bucket = "jerrymmm"
    url = "http://localhost:8086"

    if not token:
        logger.warning("INFLUXDB_TOKEN not set, skipping InfluxDB rollback")
    else:
        try:
            client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
            delete_api = client.delete_api()

            # Delete range: from rollback time to end of day
            start_delete = rollback_dt + timedelta(seconds=1)
            stop_delete = rollback_dt.replace(hour=23, minute=59, second=59)

            logger.info(f"Deleting InfluxDB data from {start_delete} to {stop_delete}")

            # Delete from market_snapshot measurement
            predicate_snapshot = f'_measurement="market_snapshot" AND run_mode="replay" AND db_id="{db_id}"'
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_snapshot,
                bucket=bucket,
                org=org,
            )
            logger.info(f"Deleted InfluxDB market_snapshot data after {rollback_dt}")

            # Delete from movers_state measurement
            predicate_state = (
                f'_measurement="movers_state" AND run_mode="replay" AND db_id="{db_id}"'
            )
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_state,
                bucket=bucket,
                org=org,
            )
            logger.info(f"Deleted InfluxDB movers_state data after {rollback_dt}")

            client.close()

        except Exception as e:
            logger.error(f"InfluxDB rollback failed: {e}")

    logger.info(f"=" * 70)
    logger.info(
        f"Rollback completed. You can now restart replay with --start-from {rollback_time}"
    )
    logger.info(f"=" * 70)


def clear_all_data(replay_date: str, suffix_id: str | None = None) -> None:
    """
    Clear all Redis and InfluxDB data for a replay date (fresh start).

    Args:
        replay_date: The replay date (YYYYMMDD)
        suffix_id: Optional suffix ID for InfluxDB tagging
    """
    logger.info(f"=" * 70)
    logger.info(f"Clearing all data for replay date: {replay_date}")
    logger.info(f"=" * 70)

    # Derive db_id for InfluxDB filtering
    db_id = f"{replay_date}_{suffix_id}" if suffix_id else f"{replay_date}"

    # Redis key names
    INPUT_STREAM = f"market_snapshot_stream_replay:{replay_date}"
    OUTPUT_STREAM = f"market_snapshot_processed:{replay_date}"
    STATE_STREAM = f"movers_state:{replay_date}"
    SUBSCRIBED_ZSET = f"movers_subscribed_set:{replay_date}"
    CURSOR_HSET = f"state_cursor:{replay_date}"

    # Delete Redis keys
    for key in [
        INPUT_STREAM,
        OUTPUT_STREAM,
        STATE_STREAM,
        SUBSCRIBED_ZSET,
        CURSOR_HSET,
    ]:
        if r.exists(key):
            r.delete(key)
            logger.info(f"Deleted Redis key: {key}")
        else:
            logger.info(f"Redis key {key} does not exist, skipping")

    # Delete InfluxDB data
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "jerryhong"
    bucket = "jerrymmm"
    url = "http://localhost:8086"

    if not token:
        logger.warning("INFLUXDB_TOKEN not set, skipping InfluxDB clear")
    else:
        try:
            client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
            delete_api = client.delete_api()

            # Parse date for range
            start_delete = datetime(
                int(replay_date[:4]),
                int(replay_date[4:6]),
                int(replay_date[6:8]),
                0,
                0,
                0,
                tzinfo=ZoneInfo("America/New_York"),
            )
            stop_delete = start_delete + timedelta(days=1)

            logger.info(f"Deleting InfluxDB data from {start_delete} to {stop_delete}")

            # Delete from market_snapshot measurement
            predicate_snapshot = f'_measurement="market_snapshot" AND run_mode="replay" AND db_id="{db_id}"'
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_snapshot,
                bucket=bucket,
                org=org,
            )
            logger.info(f"Deleted InfluxDB market_snapshot data for {replay_date}")

            # Delete from movers_state measurement
            predicate_state = (
                f'_measurement="movers_state" AND run_mode="replay" AND db_id="{db_id}"'
            )
            delete_api.delete(
                start=start_delete,
                stop=stop_delete,
                predicate=predicate_state,
                bucket=bucket,
                org=org,
            )
            logger.info(f"Deleted InfluxDB movers_state data for {replay_date}")

            client.close()

        except Exception as e:
            logger.error(f"InfluxDB clear failed: {e}")

    logger.info(f"=" * 70)
    logger.info(f"Clear completed. Ready for fresh replay.")
    logger.info(f"=" * 70)


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

    args = parser.parse_args()

    # Handle rollback command
    if args.rollback_to:
        rollback_to_timestamp(args.date, args.rollback_to, args.suffix_id)
    # Handle clear command
    elif args.clear:
        clear_all_data(args.date, args.suffix_id)
    # Normal replay
    else:
        logger.info(f"Replaying market snapshots for date: {args.date}")
        logger.info(f"Speed: {args.speed}x")
        logger.info(f"Format: {args.format}")
        if args.start_from:
            logger.info(f"Starting from: {args.start_from}")

        read_market_snapshot_with_timing(
            args.date, args.speed, args.start_from, args.format
        )
