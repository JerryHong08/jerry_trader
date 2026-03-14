import glob
import os
from datetime import datetime
from typing import Optional

import s3fs
from dotenv import load_dotenv

load_dotenv()
from jerry_trader.platform.config.config import data_dir

ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")


class DataPathFetcher:
    """
    Calculate data directory paths based on asset type, timeframe, date range.

    Args:
        asset,
        data_type: e.g., 'day_aggs_v1', 'minute_aggs_v1'
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
        lake: Whether to use lake (parquet) or raw (csv.gz) data
        s3: Whether to fetch from S3 or local directory
    Returns:
        List of parquet file paths
    """

    def __init__(
        self,
        asset: str = "us_stocks_sip",
        data_type: str = "day_aggs_v1",
        start_date: str = "2023-01-01",
        end_date: str = "2023-02-01",
        lake: bool = True,
        s3: bool = False,
    ):

        self.asset = asset
        self.data_type = data_type
        self.start_date = start_date
        self.end_date = end_date
        self.lake = lake
        self.s3 = s3

        # Lazy: only create S3 filesystem when actually using S3
        self._fs = None

    @property
    def fs(self):
        """Lazy S3 filesystem — only imported/created on first access."""

        self._fs = s3fs.S3FileSystem(
            key=ACCESS_KEY_ID,
            secret=SECRET_ACCESS_KEY,
            endpoint_url="https://files.polygon.io",
            client_kwargs={"region_name": "us-east-1"},
        )
        return self._fs

    def data_dir_calculate(
        self,
        asset: Optional[str] = None,
        data_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lake: Optional[bool] = None,
        s3: Optional[bool] = None,
    ) -> list:

        asset = asset or self.asset
        data_type = data_type or self.data_type
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        lake = lake if lake is not None else self.lake
        s3 = s3 if s3 is not None else self.s3

        print(
            f"Calculating data paths for asset: {asset}, data_type: {data_type}, date range: {start_date} to {end_date}, lake: {lake}, s3: {s3}"
        )

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        data_dirs = []

        for y in range(start_dt.year, end_dt.year + 1):
            start_month = start_dt.month if y == start_dt.year else 1
            end_month = end_dt.month if y == end_dt.year else 12

            for m in range(start_month, end_month + 1):
                month_str = f"{m:02d}"
                if self.s3:
                    month_all_file = self.s3_all_files_paths(
                        self.asset, self.data_type, y, month_str
                    )
                else:
                    month_all_file = self.local_all_files_paths(
                        self.asset, self.data_type, y, month_str, lake=self.lake
                    )

                if not month_all_file:
                    continue  # skip if no files found for the month

                if m in [start_month, end_month]:
                    filtered_files = []
                    for file in month_all_file:
                        _, file_name = os.path.split(file)
                        file_name = file_name.split(".")[0]
                        try:
                            file_date = datetime.strptime(file_name, "%Y-%m-%d").date()
                        except ValueError:
                            continue
                        if (m == start_month and file_date < start_dt.date()) or (
                            m == end_month and file_date > end_dt.date()
                        ):
                            continue  # skip files outside the range
                        filtered_files.append(file)
                    data_dirs.extend(filtered_files)
                else:
                    data_dirs.extend(month_all_file)
        print(f"Found {len(data_dirs)} files in paths")
        return data_dirs

    def local_all_files_paths(
        self, asset: str, data_type: str, year: int, month_str: str, lake: bool = False
    ):
        try:
            # fetch all files in local directory
            file_pattern = f"{data_dir}/{'lake' if lake else 'raw'}/{asset}/{data_type}/{year}/{month_str}/*.{'parquet' if lake else 'csv.gz'}"
            month_all_file = glob.glob(file_pattern)
            return month_all_file
        except Exception as e:
            print(f"Error accessing local data directory: {e}")
            return []

    def s3_all_files_paths(self, asset: str, data_type: str, year: int, month_str: str):
        try:
            # fetch all files in the S3 directory
            s3_prefix = f"flatfiles/{asset}/{data_type}/{year}/{month_str}/"
            file_extension = "csv.gz"
            # print(f"Searching in S3 path: {s3_prefix}")
            all_files = self.fs.ls(s3_prefix)
            month_all_file = [
                f"s3://{f}" for f in all_files if f.endswith(file_extension)
            ]
            return month_all_file
        except Exception as e:
            print(f"Error accessing S3 data directory: {e}")
            return []


if __name__ == "__main__":

    path_fetcher = DataPathFetcher(
        asset="us_stocks_sip",
        data_type="day_aggs_v1",
        start_date="2023-01-01",
        end_date="2023-02-01",
        lake=True,
        s3=False,
    )
    paths = path_fetcher.data_dir_calculate()
    print(paths)
