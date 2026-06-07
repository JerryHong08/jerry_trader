import os
import time

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient

from jerry_trader.platform.config.config import splits_dir

load_dotenv()

polygon_api_key = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key)

updated_time = time.strftime("%Y%m%d")


def fetch_splits_and_save(out_dir=splits_dir):

    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"all_splits_{updated_time}.parquet")

    if os.path.exists(out_file):
        print(
            f"splits incremental already incrementally updated. {out_file} already exists."
        )
        return out_file

    try:
        files = [
            f
            for f in os.listdir(out_dir)
            if f.startswith("all_splits_") and f.endswith(".parquet")
        ]
        if not files:
            raise FileNotFoundError("No splits files found")
        last_splits_file_name = max(files)
        last_splits_file = os.path.join(out_dir, last_splits_file_name)
        splits_original = pl.read_parquet(last_splits_file)
        print("splits incremental updating...")
        print(f"Found existing splits file: {last_splits_file_name}")
    except (FileNotFoundError, pl.exceptions.ComputeError, ValueError):
        print("not find previous splits file, first time run")
        splits_original = pl.DataFrame()

    try:
        first_time = splits_original.is_empty()

        splits = []
        for i, s in enumerate(
            client.list_splits(
                order="desc",
                limit="300",  # make sure it reaches to today
                sort="execution_date",
            )
        ):

            split_dict = {
                "id": s.id,
                "execution_date": s.execution_date,
                "split_from": s.split_from,
                "split_to": s.split_to,
                "ticker": s.ticker,
            }
            splits.append(split_dict)

            if not first_time:
                # When incremental updating, don't need to fetch too much data.
                if len(splits) >= 299:
                    break

        splits_new = pl.DataFrame(splits)

        # splits_diff = splits_new.filter(~pl.col('id').is_in(splits_original['id'].implode()))

        # Ensure schema compatibility
        splits_new = splits_new.with_columns(
            [
                pl.col("split_from").cast(pl.Float64),
            ]
        )

        splits = pl.concat([splits_original, splits_new]).unique()
        for f in os.listdir(out_dir):
            if f.startswith("all_splits_") and f.endswith(".parquet"):
                os.remove(os.path.join(out_dir, f))
        splits.write_parquet(out_file, compression="snappy")
        print(f"Saved {out_file}. Splits data incremental update done.")

    except Exception as e:
        print(f"Error during splits incremental update: {e}")


fetch_splits_and_save(splits_dir)
