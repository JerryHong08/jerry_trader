"""Polygon.io Flat Files Downloader for backtest data.

Adapted from quant101/src/data/fetcher/polygon_downloader.py.
Downloads historical market data (trades, quotes, day_aggs) from Polygon S3 endpoint.
"""

from __future__ import annotations

import gzip
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from dotenv import load_dotenv
from tqdm import tqdm

from jerry_trader.platform.config.config import raw_data_dir
from jerry_trader.shared.logging.logger import setup_logger

_ = load_dotenv()

logger = setup_logger("backtest.data.downloader", log_to_file=True)

ACCESS_KEY_ID = os.getenv("POLYGON_ACCESS_KEY_ID", os.getenv("ACCESS_KEY_ID", ""))
SECRET_ACCESS_KEY = os.getenv(
    "POLYGON_SECRET_ACCESS_KEY", os.getenv("SECRET_ACCESS_KEY", "")
)

ENDPOINT_URL = "https://files.polygon.io"
BUCKET_NAME = "flatfiles"

# Data types for US stocks SIP
US_STOCKS_DATA_TYPES = ["trades_v1", "quotes_v1", "day_aggs_v1"]


def _get_s3_client():
    """Create S3 client for Polygon flat files."""
    import boto3
    from botocore.config import Config

    if not ACCESS_KEY_ID or not SECRET_ACCESS_KEY:
        raise ValueError(
            "Polygon credentials not found. Set POLYGON_ACCESS_KEY_ID and "
            "POLYGON_SECRET_ACCESS_KEY environment variables."
        )

    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )
    return session.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        config=Config(signature_version="s3v4"),
    )


def _s3_key_for_date(data_type: str, date: str) -> str:
    """Build S3 key for a date's CSV.gz file.

    Pattern: us_stocks_sip/{data_type}/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz
    """
    year, month, day = date.split("-")
    return f"us_stocks_sip/{data_type}/{year}/{month}/{date}.csv.gz"


def _local_raw_path(data_type: str, date: str) -> str:
    """Build local path for raw CSV.gz download.

    Pattern: {raw_data_dir}/us_stocks_sip/{data_type}/{YYYY}/{MM}/{YYYY-MM-DD}.csv.gz
    """
    year, month, day = date.split("-")
    return os.path.join(
        raw_data_dir,
        "us_stocks_sip",
        data_type,
        year,
        month,
        f"{date}.csv.gz",
    )


def download_file(
    date: str,
    data_type: str,
    decompress: bool = False,
    max_retries: int = 3,
    show_progress: bool = True,
) -> str | None:
    """Download a single date's data file from Polygon.

    Args:
        date: Date in YYYY-MM-DD format.
        data_type: e.g. "trades_v1", "quotes_v1", "day_aggs_v1".
        decompress: If True, decompress .gz after download.
        max_retries: Maximum retry attempts.
        show_progress: Show progress bar.

    Returns:
        Local file path on success, None on failure.
    """
    s3_key = _s3_key_for_date(data_type, date)
    local_path = _local_raw_path(data_type, date)

    # Skip if already exists (final decompressed or complete .gz)
    final_path = (
        local_path[:-3] if decompress and local_path.endswith(".gz") else local_path
    )
    if os.path.exists(final_path):
        logger.info(f"Already exists: {final_path}")
        return final_path

    # Check for complete .gz file (if not decompressing)
    if not decompress and os.path.exists(local_path):
        # Verify it's complete by comparing with remote size
        s3 = _get_s3_client()
        try:
            obj = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            total_size = obj["ContentLength"]
            if os.path.getsize(local_path) >= total_size:
                logger.info(f"Already exists: {local_path}")
                return local_path
        except Exception:
            pass

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Clean up any stale temporary files from interrupted downloads
    # boto3 creates temp files like: file.gz.XXXXXX or file.gz.XXXXXX.XXXXXX
    temp_patterns = [
        local_path + ".tmp",
        local_path + ".partial",
    ]
    # Also clean up any files starting with local_path but with extra suffixes
    parent_dir = os.path.dirname(local_path)
    base_name = os.path.basename(local_path)
    if os.path.exists(parent_dir):
        for f in os.listdir(parent_dir):
            if f.startswith(base_name) and f != base_name:
                # This is a temp/partial file
                temp_file = os.path.join(parent_dir, f)
                logger.info(f"Cleaning up stale temp file: {temp_file}")
                try:
                    os.remove(temp_file)
                except Exception as e:
                    logger.warning(f"Failed to remove temp file {temp_file}: {e}")

    s3 = _get_s3_client()

    for attempt in range(1, max_retries + 1):
        try:
            # Check for partial download (resume support)
            downloaded_bytes = 0
            if os.path.exists(local_path):
                downloaded_bytes = os.path.getsize(local_path)

            # Get remote file size
            try:
                obj = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                total_size = obj["ContentLength"]
            except Exception as e:
                logger.error(f"Failed to get file info for {s3_key}: {e}")
                return None

            if downloaded_bytes >= total_size:
                logger.info(f"Download complete: {local_path}")
                break

            # Setup progress bar
            pbar = None
            if show_progress:
                pbar = tqdm(
                    total=total_size,
                    initial=downloaded_bytes,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"{date} {data_type}",
                    ncols=80,
                )

            if downloaded_bytes > 0:
                logger.info(
                    f"Resuming {date} {data_type} from {downloaded_bytes}/{total_size} bytes "
                    f"[attempt {attempt}/{max_retries}]"
                )
                # Resume with Range header
                response = s3.get_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                    Range=f"bytes={downloaded_bytes}-",
                )
                with open(local_path, "ab") as f:
                    for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
                        f.write(chunk)
                        if pbar:
                            pbar.update(len(chunk))
            else:
                logger.info(
                    f"Downloading {date} {data_type} ({total_size / 1e9:.1f} GB)"
                )

                # Use get_object instead of download_file for better resume support
                # download_file creates temp files that can't be resumed
                response = s3.get_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                )
                with open(local_path, "wb") as f:
                    for chunk in response["Body"].iter_chunks(chunk_size=1024 * 1024):
                        f.write(chunk)
                        if pbar:
                            pbar.update(len(chunk))

            if pbar:
                pbar.close()

            # Verify download
            actual_size = os.path.getsize(local_path)
            if actual_size < total_size:
                logger.warning(
                    f"Incomplete download: {actual_size}/{total_size} bytes, retrying..."
                )
                continue

            break

        except Exception as e:
            logger.error(f"Download error (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                return None
            time.sleep(min(30, 2**attempt))

    # Decompress if requested
    if decompress and local_path.endswith(".gz"):
        decompressed = local_path[:-3]
        with gzip.open(local_path, "rb") as f_in:
            with open(decompressed, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(local_path)
        logger.info(f"Decompressed: {decompressed}")
        return decompressed

    return local_path


def download_date(
    date: str,
    data_types: list[str] | None = None,
    max_workers: int = 2,
    decompress: bool = False,
) -> dict[str, str | None]:
    """Download all data types for a single date.

    Args:
        date: Date in YYYY-MM-DD format.
        data_types: Data types to download. Defaults to trades + quotes.
        max_workers: Parallel download threads.
        decompress: Decompress after download.

    Returns:
        Dict mapping data_type → local file path (or None if failed).
    """
    if data_types is None:
        data_types = ["trades_v1", "quotes_v1"]

    results: dict[str, str | None] = {}

    if max_workers <= 1:
        for dt in data_types:
            results[dt] = download_file(date, dt, decompress=decompress)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_type = {
                executor.submit(download_file, date, dt, decompress): dt
                for dt in data_types
            }
            for future in as_completed(future_to_type):
                dt = future_to_type[future]
                try:
                    results[dt] = future.result()
                except Exception as e:
                    logger.error(f"Download failed for {dt}: {e}")
                    results[dt] = None

    # Also download prev day's day_aggs for snapshot builder (prev_close)
    if "day_aggs_v1" not in data_types:
        prev_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        logger.info(f"Downloading prev day aggs for prev_close: {prev_date}")
        prev_path = download_file(prev_date, "day_aggs_v1", decompress=decompress)
        results["day_aggs_v1_prev"] = prev_path

    return results


def estimate_download_size(
    start_date: str,
    end_date: str,
    data_types: list[str] | None = None,
) -> dict[str, float]:
    """Estimate download size for a date range.

    Uses S3 head_object to get actual file sizes.

    Returns:
        Dict with 'total_gb', 'trades_gb', 'quotes_gb', 'day_aggs_gb', 'days'
    """
    if data_types is None:
        data_types = ["trades_v1", "quotes_v1", "day_aggs_v1"]

    s3 = _get_s3_client()

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    sizes: dict[str, float] = {"trades_v1": 0.0, "quotes_v1": 0.0, "day_aggs_v1": 0.0}
    days = 0

    current = start
    while current <= end:
        days += 1
        for dt in data_types:
            if dt not in sizes:
                continue
            s3_key = _s3_key_for_date(dt, current.strftime("%Y-%m-%d"))
            try:
                obj = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                sizes[dt] += float(obj["ContentLength"])
            except Exception:
                # File not found or error, use average estimate
                if dt == "trades_v1":
                    sizes[dt] += 3.2e9  # ~3.2 GB average
                elif dt == "quotes_v1":
                    sizes[dt] += 11e9  # ~11 GB average
                elif dt == "day_aggs_v1":
                    sizes[dt] += 50e6  # ~50 MB average
        current += timedelta(days=1)

    return {
        "total_gb": sum(sizes.values()) / 1e9,
        "trades_gb": sizes["trades_v1"] / 1e9,
        "quotes_gb": sizes["quotes_v1"] / 1e9,
        "day_aggs_gb": sizes["day_aggs_v1"] / 1e9,
        "days": days,
    }


def check_disk_space(path: str) -> dict[str, float]:
    """Check disk space for the given path.

    Returns:
        Dict with 'total_gb', 'used_gb', 'available_gb', 'percent_used'
    """
    import shutil

    stat = shutil.disk_usage(path)
    return {
        "total_gb": stat.total / 1e9,
        "used_gb": stat.used / 1e9,
        "available_gb": stat.free / 1e9,
        "percent_used": (stat.used / stat.total) * 100,
    }


def print_download_estimate(
    start_date: str,
    end_date: str,
    data_types: list[str] | None = None,
    data_dir: str | None = None,
) -> None:
    """Print download estimate with disk space check."""
    from jerry_trader.platform.config.config import raw_data_dir

    # Check disk space
    target_dir = data_dir or raw_data_dir
    disk = check_disk_space(target_dir)

    print(f"\n{'=' * 60}")
    print(f"  DOWNLOAD ESTIMATE")
    print(f"{'=' * 60}")
    print(f"  Date range: {start_date} to {end_date}")

    # Estimate download size
    print(f"\n  Fetching file sizes from Polygon S3...")
    estimate = estimate_download_size(start_date, end_date, data_types)

    print(f"\n  Estimated download sizes:")
    print(f"    trades_v1:   {estimate['trades_gb']:.2f} GB")
    print(f"    quotes_v1:   {estimate['quotes_gb']:.2f} GB")
    print(f"    day_aggs_v1: {estimate['day_aggs_gb']:.2f} GB")
    print(f"    ─────────────────────────")
    print(f"    TOTAL:       {estimate['total_gb']:.2f} GB")
    print(f"    Days:        {estimate['days']}")

    print(f"\n  Disk space ({target_dir}):")
    print(f"    Available:   {disk['available_gb']:.2f} GB")
    print(
        f"    Used:        {disk['used_gb']:.2f} GB / {disk['total_gb']:.2f} GB ({disk['percent_used']:.1f}%)"
    )

    # Check if enough space
    remaining = disk["available_gb"] - estimate["total_gb"]
    print(f"\n  After download:")
    print(f"    Remaining:   {remaining:.2f} GB")

    if remaining < 0:
        print(f"\n  ⚠️  WARNING: Not enough disk space!")
        print(f"     Need {abs(remaining):.2f} GB more")
    elif remaining < 50:
        print(f"\n  ⚠️  WARNING: Low disk space after download")
    else:
        print(f"\n  ✅ Sufficient disk space")

    print(f"{'=' * 60}\n")


def download_date_range(
    start_date: str,
    end_date: str,
    data_types: list[str] | None = None,
    max_workers: int = 2,
) -> dict[str, dict[str, str | None]]:
    """Download data for a date range.

    Returns:
        Dict mapping date → {data_type: path}.
    """
    if data_types is None:
        data_types = ["trades_v1", "quotes_v1"]

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    all_results: dict[str, dict[str, str | None]] = {}
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        logger.info(f"=== Downloading data for {date_str} ===")
        all_results[date_str] = download_date(date_str, data_types, max_workers)
        current += timedelta(days=1)

    return all_results
