#!/usr/bin/env python3
"""
Polygon.io Flat Files Downloader
Downloads historical market data from Polygon.io S3 endpoint
"""

import argparse
import gzip
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv
from tqdm import tqdm

from jerry_trader.platform.config.config import data_dir

load_dotenv()

ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

ENDPOINT_URL = "https://files.polygon.io"
BUCKET_NAME = "flatfiles"

# Base directory for downloaded files
BASE_DIR = os.path.join(data_dir, "raw")

# Asset classes and their data types
ASSET_CLASSES = {
    "us_stocks_sip": ["trades_v1", "quotes_v1", "minute_aggs_v1", "day_aggs_v1"],
    "us_options_opra": ["trades_v1", "quotes_v1", "minute_aggs_v1", "day_aggs_v1"],
    "us_indices": ["minute_aggs_v1", "day_aggs_v1"],
    "global_crypto": ["trades_v1", "quotes_v1", "minute_aggs_v1", "day_aggs_v1"],
    "global_forex": ["quotes_v1", "minute_aggs_v1", "day_aggs_v1"],
}


class PolygonDownloader:
    def __init__(self):
        """Initialize the S3 client with Polygon.io credentials"""
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )

        self.s3 = session.client(
            "s3",
            # region_name='cn-north-1',
            endpoint_url=ENDPOINT_URL,
            config=Config(
                signature_version="s3v4",
            ),
        )
        # import logging
        # boto3.set_stream_logger('botocore', level=logging.DEBUG)
        # Create base directory if it doesn't exist
        os.makedirs(BASE_DIR, exist_ok=True)

    def list_files(self, prefix="", max_files=None):
        """List files in the S3 bucket with the given prefix"""
        paginator = self.s3.get_paginator("list_objects_v2")
        files = []

        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])
                    if max_files and len(files) >= max_files:
                        return files

        return files

    def download_file(self, s3_key, decompress=True, max_retries=5):
        """Download a single file from S3 with progress bar, resume support, and retries.

        If the download is interrupted (SSL error, connection drop, etc.), the partial
        file is kept and subsequent retries resume from where they left off using
        HTTP Range requests.
        """
        local_path = os.path.join(BASE_DIR, s3_key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Determine the download target path (always .gz during download)
        if decompress and not local_path.endswith(".gz"):
            download_path = local_path + ".gz"
        else:
            download_path = local_path

        # Check if final file already exists (either compressed or decompressed)
        if decompress and local_path.endswith(".gz"):
            decompressed_path = local_path[:-3]
        elif decompress and s3_key.endswith(".gz"):
            decompressed_path = local_path
        else:
            decompressed_path = local_path

        if os.path.exists(decompressed_path):
            print(f"Skipping (already exists): {decompressed_path}")
            return decompressed_path

        if os.path.exists(local_path) and not decompress:
            print(f"Skipping (already exists): {local_path}")
            return local_path

        try:
            # Get remote file size
            obj = self.s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            total_size = obj["ContentLength"]
        except ClientError as e:
            print(f"S3 Client error getting file info for {s3_key}: {str(e)}")
            return None
        except Exception as e:
            print(f"Error getting file info for {s3_key}: {str(e)}")
            return None

        for attempt in range(1, max_retries + 1):
            try:
                # Check how much we already have (for resume)
                downloaded_bytes = 0
                if os.path.exists(download_path):
                    downloaded_bytes = os.path.getsize(download_path)

                if downloaded_bytes >= total_size:
                    print(f"Download already complete: {download_path}")
                    break

                if downloaded_bytes > 0:
                    print(
                        f"Resuming download from {downloaded_bytes / (1024**3):.2f} GB "
                        f"({downloaded_bytes * 100 / total_size:.1f}%) "
                        f"[attempt {attempt}/{max_retries}]"
                    )
                else:
                    print(f"Downloading: {s3_key} [attempt {attempt}/{max_retries}]")

                # Progress bar starting from already-downloaded bytes
                pbar = tqdm(
                    total=total_size,
                    initial=downloaded_bytes,
                    unit="B",
                    unit_scale=True,
                    desc=s3_key,
                    dynamic_ncols=True,
                )
                start_time = time.time()

                def progress_hook(bytes_amount):
                    pbar.update(bytes_amount)
                    elapsed = time.time() - start_time
                    speed = (pbar.n - downloaded_bytes) / elapsed if elapsed > 0 else 0
                    remaining = (total_size - pbar.n) / speed if speed > 0 else 0
                    pbar.set_postfix(
                        elapsed=f"{elapsed:.1f}s",
                        remaining=f"{remaining:.1f}s",
                        speed=f"{speed / 1024:.1f}KB/s",
                    )

                if downloaded_bytes > 0:
                    # Resume: use get_object with Range header and append to file
                    range_header = f"bytes={downloaded_bytes}-"
                    response = self.s3.get_object(
                        Bucket=BUCKET_NAME, Key=s3_key, Range=range_header
                    )
                    with open(download_path, "ab") as f:
                        chunk_size = 1024 * 1024  # 1MB chunks
                        body = response["Body"]
                        while True:
                            chunk = body.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            progress_hook(len(chunk))
                else:
                    # Fresh download
                    self.s3.download_file(
                        BUCKET_NAME,
                        s3_key,
                        download_path,
                        Callback=progress_hook,
                    )

                pbar.close()

                # Verify downloaded size
                actual_size = os.path.getsize(download_path)
                if actual_size < total_size:
                    print(
                        f"Warning: downloaded {actual_size} bytes but expected {total_size}. "
                        f"Will retry..."
                    )
                    continue

                # Download complete — break out of retry loop
                break

            except (EndpointConnectionError, ClientError) as e:
                pbar.close() if "pbar" in dir() else None
                print(
                    f"Connection error on attempt {attempt}/{max_retries} for {s3_key}: {str(e)}"
                )
                if attempt == max_retries:
                    print(
                        f"All {max_retries} attempts failed for {s3_key}. Partial file kept for resume."
                    )
                    return None
                wait = min(30, 2**attempt)
                print(f"Retrying in {wait}s...")
                time.sleep(wait)
            except Exception as e:
                pbar.close() if "pbar" in dir() else None
                print(
                    f"Error on attempt {attempt}/{max_retries} for {s3_key}: {str(e)}"
                )
                if attempt == max_retries:
                    print(
                        f"All {max_retries} attempts failed for {s3_key}. Partial file kept for resume."
                    )
                    return None
                wait = min(30, 2**attempt)
                print(f"Retrying in {wait}s...")
                time.sleep(wait)

        # Decompress if it's a .gz file and decompress is True
        if decompress and (local_path.endswith(".gz") or s3_key.endswith(".gz")):
            if not local_path.endswith(".gz"):
                compressed_path = local_path + ".gz"
            else:
                compressed_path = local_path
                local_path = local_path[:-3]

            with gzip.open(compressed_path, "rb") as f_in:
                with open(local_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(compressed_path)  # Remove compressed file
            print(f"Decompressed: {local_path}")
            return local_path

        return local_path

    def download_date_range(
        self, asset_class, data_type, start_date, end_date, decompress=True
    ):
        """Download files for a specific date range"""
        files_to_download = []
        current_date = start_date

        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            year = current_date.strftime("%Y")
            month = current_date.strftime("%m")

            # Construct S3 key
            s3_key = f"{asset_class}/{data_type}/{year}/{month}/{date_str}.csv.gz"
            files_to_download.append(s3_key)

            current_date += timedelta(days=1)

        # Download files in parallel
        downloaded_files = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {
                executor.submit(self.download_file, f, decompress): f
                for f in files_to_download
            }

            for future in as_completed(future_to_file):
                result = future.result()
                if result:
                    downloaded_files.append(result)

        return downloaded_files

    def download_recent(self, asset_class, data_type, days=7, decompress=True):
        """Download the most recent N days of data"""
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=days - 1)

        return self.download_date_range(
            asset_class, data_type, start_date, end_date, decompress
        )

    def download_specific_file(self, s3_key, decompress=True):
        """Download a specific file by its S3 key"""
        return self.download_file(s3_key, decompress)


def main():
    parser = argparse.ArgumentParser(description="Download Polygon.io Flat Files")
    parser.add_argument(
        "--asset-class",
        choices=list(ASSET_CLASSES.keys()),
        help="Asset class to download",
    )
    parser.add_argument("--data-type", help="Data type (e.g., trades_v1, quotes_v1)")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--recent-days", type=int, help="Download most recent N days")
    parser.add_argument("--specific-file", help="Download specific file by S3 key")
    parser.add_argument("--list", action="store_true", help="List available files")
    parser.add_argument("--prefix", help="Prefix for listing files")
    parser.add_argument(
        "--no-decompress", action="store_true", help="Keep files compressed"
    )

    args = parser.parse_args()

    downloader = PolygonDownloader()

    if args.list:
        prefix = args.prefix or ""
        files = downloader.list_files(prefix, max_files=100)
        print(f"\nFound {len(files)} files with prefix '{prefix}':")
        for f in files[:20]:  # Show first 20
            print(f"  {f}")
        if len(files) > 20:
            print(f"  ... and {len(files) - 20} more files")

    elif args.specific_file:
        # Extract year and month from the file name
        # Example: us_stocks_sip/minute_aggs_v1/2020-11-17.csv.gz
        s3_key = args.specific_file
        print(f"Original s3_key: {s3_key}")
        # Split to get the base path and filename
        base_path, filename = os.path.split(s3_key)
        # Extract year and month from filename
        date_part = filename.split(".")[0]  # "2020-11-17"
        year, month, _ = date_part.split("-")
        # Reconstruct the s3_key with year/month in the path
        s3_key = f"{base_path}/{year}/{month}/{filename}"
        print(f"Resolved s3_key: {s3_key}")

        result = downloader.download_specific_file(s3_key, args.no_decompress)
        if result:
            print(f"Successfully downloaded: {result}")

    elif args.asset_class and args.data_type:
        if args.recent_days:
            files = downloader.download_recent(
                args.asset_class,
                args.data_type,
                args.recent_days,
                args.no_decompress,
            )
            print(f"\nDownloaded {len(files)} files")

        elif args.start_date and args.end_date:
            start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
            files = downloader.download_date_range(
                args.asset_class, args.data_type, start, end, args.no_decompress
            )
            print(f"\nDownloaded {len(files)} files")

        else:
            print(
                "Please specify either --recent-days or both --start-date and --end-date"
            )

    else:
        print("\nUsage examples:")
        print(
            "  List files: python src/data/fetcher/polygon_downloader.py --list --prefix us_stocks_sip/trades_v1/2024/"
        )
        print(
            "  Download recent 7 days: python src/data/fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days 7"
        )
        print(
            "  Download date range: python src/data/fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type trades_v1 --start-date 2024-03-01 --end-date 2024-03-07"
        )
        print(
            "  Download specific file: python src/data/fetcher/polygon_downloader.py --specific-file us_stocks_sip/minute_aggs_v1/2024-03-07.csv.gz"
        )


if __name__ == "__main__":
    main()
