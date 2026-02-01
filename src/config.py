import os
from pathlib import Path

import polars as pl
import yaml

# ===================== data root =============================================
blackdisk_data_dir = "/mnt/blackdisk/quant_data/polygon_data"
oldman_data_dir = "/home/oldman/quant_data/polygon_data"


def _get_data_dir_from_config() -> str:
    """
    Get data_dir based on machine role from machine_config.yaml.
    Falls back to blackdisk_data_dir if config is not found.
    """
    config_path = Path(__file__).resolve().parents[2] / "machine_config.yaml"

    if not config_path.exists():
        return blackdisk_data_dir

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Get role from environment variable or config
        role = os.environ.get("MACHINE_ROLE", config["machine"]["role"])

        if role == "server":
            return config["server"]["data_dir"]
        else:
            return config["client"]["data_dir"]
    except Exception:
        return blackdisk_data_dir


# =====================================================================
data_dir = _get_data_dir_from_config()
lake_data_dir = os.path.join(data_dir, "lake")
raw_data_dir = os.path.join(data_dir, "raw")
cache_dir = os.path.join(data_dir, "processed")
low_volume_tickers_dir = os.path.join(cache_dir, "low_volume_tickers")

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
splits_error_file = "src/utils/data_discrepancy_fixed/splits_error.csv"

# ===================== float shares =====================
float_shares_dir = os.path.join(data_dir, "raw/us_stocks_sip/float_shares")

# ===================== all_asset_overview with error correction ==============
all_tickers_dir = os.path.join(data_dir, "raw/us_stocks_sip/us_all_tickers")
all_indices_dir = os.path.join(data_dir, "raw/us_indices/us_all_indices")

# ===================== other data ============================================
sppc_dir = "/mnt/blackdisk/quant_data/kaggle_data/sppc"


# ===================== data_return_functions =================================
def get_asset_dir(asset):
    if asset not in asset_dir_config:
        raise ValueError(f"Unsupported asset type: {asset}")

    asset_dir = os.path.join(data_dir, asset_dir_config[asset][0])
    asset_error_file = os.path.join(
        "src/utils/data_discrepancy_fixed", f"{asset}_error.csv"
    )
    asset_error_file_copy = os.path.join(asset_dir, f"{asset}_error.csv")

    return asset_dir, asset_error_file, asset_error_file_copy


def get_asset_overview_data(asset: str) -> pl.DataFrame:
    """
    Load asset overview data with error corrections applied.
    Args:
        asset str: 'splits', 'otc', 'stocks', 'indices'

    Returns:
        pl.DataFrame: Asset overview data with error corrections applied.
    """
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


from pathlib import Path

PROMPT_DIR = Path(__file__).resolve().parents[0] / "llmContext"


def load_prompt(name: str) -> str:
    path = PROMPT_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Prompt not found: {path}")
    return path.read_text(encoding="utf-8")


if __name__ == "__main__":
    with pl.Config(tbl_rows=50, tbl_cols=20):
        asset = "splits"
        all_asset = get_asset_overview_data(asset)
        print(all_asset.head())
        print(all_asset.filter(pl.col("ticker").is_in(["XTIA"])))
