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

import polars as pl
import redis

from config import cache_dir
from DataUtils.schema import (
    spot_check_SnapshotMsg_with_pydantic,
    validate_SnapshotMsg_schema,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

r = redis.Redis(host="localhost", port=6379, db=0)


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
    replay_date: str, speed_multiplier: float = 1.0, start_from: str | None = None
) -> None:
    """
    replay file in market_mover_dir for the given date with timing based on filenames.

    Args:
        replay_date: replay date YYYYMMDD
        speed_multiplier: replay speed
        start_from: optional timestamp (HHMMSS) to resume replay from (will start from the first file after this time)
    """
    year = replay_date[:4]
    month = replay_date[4:6]
    date = replay_date[6:8]

    market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, date)

    if not os.path.exists(market_mover_dir):
        logger.warning(f"Directory not found: {market_mover_dir}")
        return

    all_files = glob.glob(os.path.join(market_mover_dir, "*_market_snapshot.csv"))

    if not all_files:
        logger.warning(f"No market snapshot files found in {market_mover_dir}")
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
            df = pl.read_csv(file)
            file_timestamp_ms = int(file_timestamp.timestamp() * 1000)

            if "timestamp" in df.columns:
                df = df.drop("timestamp")

            df = df.with_columns(pl.lit(file_timestamp_ms).alias("timestamp"))

            is_valid, error_msg = validate_SnapshotMsg_schema(df)
            if not is_valid:
                logger.error(f"Schema validation failed: {error_msg}")
                continue

            if first_file:
                if not spot_check_SnapshotMsg_with_pydantic(df, sample_size=5):
                    logger.error(f"Pydantic validation failed for {file}")
                    continue
                first_file = False

            logger.info(f"Validated {len(df)} rows")

            payload = df.write_json()

            STREAM_NAME = f"market_snapshot_stream_replay:{replay_date}"
            assert ":" in STREAM_NAME, "STREAM_NAME must include a date suffix!"

            message_id = r.xadd(STREAM_NAME, {"data": payload}, maxlen=10000)
            if r.ttl(STREAM_NAME) < 0:
                r.expire(STREAM_NAME, 1 * 19 * 3600)

        except Exception as e:
            logger.error(f"Error processing file {file}: {e}")
            continue


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Market snapshot replayer")
    parser.add_argument("--date", default="20251003", help="Replay date (YYYYMMDD)")
    parser.add_argument(
        "--speed", type=float, default=1.0, help="Speed multiplier (default: 1.0)"
    )
    parser.add_argument(
        "--start-from",
        type=str,
        default=None,
        help="Resume from timestamp (HHMMSS), e.g., 050110 to start from 05:01:10",
    )

    args = parser.parse_args()

    logger.info(f"Replaying market snapshots for date: {args.date}")
    logger.info(f"Speed: {args.speed}x")
    if args.start_from:
        logger.info(f"Starting from: {args.start_from}")

    read_market_snapshot_with_timing(args.date, args.speed, args.start_from)
