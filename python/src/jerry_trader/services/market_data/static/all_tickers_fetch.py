import os
import time

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient

from jerry_trader.platform.config.config import all_indices_dir, all_tickers_dir

load_dotenv()
polygon_api_key = os.getenv("POLYGON_API_KEY")
client = RESTClient(polygon_api_key)

updated_time = time.strftime("%Y%m%d", time.localtime())

ASSET_CONFIG = {
    "stocks": all_tickers_dir,
    "indices": all_indices_dir,
    "otc": all_tickers_dir,
}


def fetch_and_save(asset_type: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"all_{asset_type}_{updated_time}.parquet")

    if os.path.exists(out_file):
        print(f"{out_file} already exists.")
        return out_file

    all_records = []
    try:
        for active_flag in ["True", "False"]:
            tickers = []
            for t in client.list_tickers(
                market=asset_type,
                active=active_flag,
                order="asc",
                limit="1000",
                sort="ticker",
            ):
                tickers.append(t)
            all_records.extend(tickers)

        df = pl.DataFrame(all_records)
        # delete last updated all_ticker file(s)
        for f in os.listdir(out_dir):
            if f.startswith(f"all_{asset_type}_") and f.endswith(".parquet"):
                os.remove(os.path.join(out_dir, f))

        df.write_parquet(out_file, compression="snappy")
        print(f"Saved {out_file}")
    except Exception as e:
        print(f"Error fetching {asset_type} data: {e}")


for asset, outdir in ASSET_CONFIG.items():
    fetch_and_save(asset, outdir)

# with pl.Config(tbl_rows=50, tbl_cols=20):
#     asset = "indices"
#     all_asset = pl.read_parquet(
#         os.path.join(ASSET_CONFIG[asset], f"all_{asset}_{updated_time}.parquet")
#     )
#     print(all_asset.shape)
#     print(
#         all_asset.filter(
#             (pl.col("active") == True)
#             # & (pl.col('type').is_in(['ADRC', 'CS', 'ETF', 'ETN', 'ETS', 'ETV']))
#             & (pl.col("ticker").str.contains("SPX"))
#         )
#     )
