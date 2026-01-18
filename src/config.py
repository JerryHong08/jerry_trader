import os

import polars as pl

# ===================== data root =============================================
blackdisk_data_dir = "/mnt/blackdisk/quant_data/polygon_data"
nftsdisk_data_dir = "data/polygon_data"
data_dir = blackdisk_data_dir
lake_data_dir = "/mnt/blackdisk/quant_data/polygon_data/lake"
raw_data_dir = "/mnt/blackdisk/quant_data/polygon_data/raw"

# ==================== config of config =======================================
asset_dir_config = {
    "splits": ["raw/us_stocks_sip/splits", "id"],
    "otc": ["raw/us_stocks_sip/us_all_tickers", "ticker"],
    "stocks": ["raw/us_stocks_sip/us_all_tickers", "ticker"],
    "indices": ["raw/us_indices/us_all_indices", "ticker"],
}

# ===================== splits data with error correction =====================
splits_dir = os.path.join(data_dir, "raw/us_stocks_sip/splits")
splits_error_file_copy = os.path.join(
    data_dir, "raw/us_stocks_sip/splits/splits_error.csv"
)
splits_error_file = "src/data_fetcher/data_discrepancy_fixed/splits_error.csv"

# ===================== float shares =====================
float_shares_dir = os.path.join(data_dir, "raw/us_stocks_sip/float_shares")

# ===================== all_asset_overview with error correction ==============
all_tickers_dir = os.path.join(data_dir, "raw/us_stocks_sip/us_all_tickers")
all_indices_dir = os.path.join(data_dir, "raw/us_indices/us_all_indices")

# ===================== cache dir =============================================
cache_dir = os.path.join(data_dir, "processed")

# ===================== other data ============================================
sppc_dir = "/mnt/blackdisk/quant_data/kaggle_data/sppc"


# ===================== data_return_functions =================================
def get_asset_dir(asset):
    if asset not in asset_dir_config:
        raise ValueError(f"Unsupported asset type: {asset}")

    asset_dir = os.path.join(data_dir, asset_dir_config[asset][0])
    asset_error_file = os.path.join(
        "src/data_fetcher/data_discrepancy_fixed", f"{asset}_error.csv"
    )
    asset_error_file_copy = os.path.join(asset_dir, f"{asset}_error.csv")

    return asset_dir, asset_error_file, asset_error_file_copy


def get_asset_overview_data(asset):
    asset_dir, asset_error_file, asset_error_file_copy = get_asset_dir(asset)
    # print(f"Loading {asset} data from {asset_dir}")
    try:
        asset_file = os.path.join(
            asset_dir,
            max(
                [
                    f
                    for f in os.listdir(asset_dir)
                    if f.startswith(f"all_{asset}_") and f.endswith(".parquet")
                ]
            ),
        )
        asset_original = pl.read_parquet(asset_file)
        if os.path.exists(asset_error_file):
            # print(f"Applying error corrections from {asset_error_file}")
            asset_errors = pl.read_csv(asset_error_file)
            asset_errors.write_csv(
                asset_error_file_copy
            )  # make a copy, you can delete it if you want.
            error_type_remove = asset_errors.filter(pl.col("error_type") == "remove")
            error_type_add = asset_errors.filter(pl.col("error_type") == "add").select(
                pl.all().exclude("error_type")
            )

            identifiers = asset_dir_config[asset][1]
            filtered_original = asset_original.filter(
                ~pl.col(identifiers).is_in(error_type_remove[identifiers].implode())
            )

            # Ensure schema compatibility before concatenating
            if not error_type_add.is_empty():
                error_type_add = error_type_add.cast(filtered_original.schema)

            asset_data = pl.concat([filtered_original, error_type_add])
        else:
            asset_data = asset_original

    except (ValueError, FileNotFoundError, OSError) as e:
        print(f"Error loading {asset}: {e}")
        asset_file = None
        asset_original = pl.DataFrame()
        asset_errors = pl.DataFrame()
        asset_data = pl.DataFrame()

    return asset_data


splits_data = get_asset_overview_data(asset="splits")

if __name__ == "__main__":
    with pl.Config(tbl_rows=50, tbl_cols=20):
        asset = "splits"
        all_asset = get_asset_overview_data(asset)
        print(all_asset.head())
        print(all_asset.filter(pl.col("ticker").is_in(["XTIA"])))
