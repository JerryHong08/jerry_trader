import logging
import os
from pathlib import Path

import polars as pl
import yaml

from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.paths import PROJECT_ROOT

logger = setup_logger(__name__, log_to_file=True, level=logging.WARNING)

# ===================== data root =============================================
# The canonical data root is read from basic_config.yaml (project root).
# basic_config.yaml is gitignored; copy basic_config.yaml.example to get started.

_CONFIG_FILENAME = "basic_config.yaml"

# ===================== LLM prompt loader =====================================
_LLM_CONTEXT_DIR = Path(__file__).parent / "llmContext"


def load_prompt(filename: str) -> str:
    """
    Read a prompt text file from the llmContext/ directory.

    Args:
        filename: Name of the prompt file (e.g. "news_processor_prompt_v2.txt").

    Returns:
        The prompt text content.

    Raises:
        FileNotFoundError: If the prompt file does not exist.
    """
    prompt_path = _LLM_CONTEXT_DIR / filename
    if not prompt_path.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {prompt_path}\n"
            f"Available prompts: {[f.name for f in _LLM_CONTEXT_DIR.iterdir() if f.is_file()]}"
        )
    return prompt_path.read_text(encoding="utf-8")


def _get_data_dir_from_config() -> str:
    """
    Read data_dir from basic_config.yaml.

    Resolution order:
      1. UPDATE_MODE env var 鈫?picks local or server data_dir
      2. update.mode in basic_config.yaml
    Raises FileNotFoundError if basic_config.yaml is missing.
    """
    config_path = PROJECT_ROOT / _CONFIG_FILENAME

    if not config_path.exists():
        raise FileNotFoundError(
            f"{_CONFIG_FILENAME} not found at {config_path}.\n"
            "Copy basic_config.yaml.example 鈫?basic_config.yaml and fill in your paths."
        )

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Get mode from environment variable or config
    mode = os.environ.get("UPDATE_MODE", config["update"]["mode"])

    if mode == "server":
        return config["multi_machine"]["server"]["data_dir"]
    else:
        # standalone and client both use the local data dir
        return config["data"]["data_dir"]


# =====================================================================
data_dir = _get_data_dir_from_config()
lake_data_dir = os.path.join(data_dir, "lake")
raw_data_dir = os.path.join(data_dir, "raw")
cache_dir = os.path.join(data_dir, "processed")
low_volume_tickers_dir = os.path.join(cache_dir, "low_volume_tickers")

# ===================== low volume tickers ================================
low_volume_tickers_csv = os.path.join(
    data_dir, "low_volume_tickers/low_volume_tickers.csv"
)
low_volume_state_parquet = os.path.join(
    data_dir, "low_volume_tickers/low_volume_state.parquet"
)
low_volume_history_parquet = os.path.join(
    data_dir, "low_volume_tickers/low_volume_history.parquet"
)

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
splits_error_file = "src/data/data_discrepancy_fixed/splits_error.csv"

# ===================== float shares =====================
float_shares_dir = os.path.join(data_dir, "raw/us_stocks_sip/float_shares")

# ===================== all_asset_overview with error correction ==============
all_tickers_dir = os.path.join(data_dir, "raw/us_stocks_sip/us_all_tickers")
all_indices_dir = os.path.join(data_dir, "raw/us_indices/us_all_indices")

# ===================== indices day aggregates ================================
indices_day_aggs_dir = os.path.join(
    data_dir, "raw/us_indices/us_indices_sip/day_aggs_v1"
)


# ===================== data_return_functions =================================
def get_asset_dir(asset):
    if asset not in asset_dir_config:
        raise ValueError(f"Unsupported asset type: {asset}")

    asset_dir = os.path.join(data_dir, asset_dir_config[asset][0])
    asset_error_file = os.path.join(
        "src/data/data_discrepancy_fixed", f"{asset}_error.csv"
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

        # Try to find error correction file: first the source file, then the copy
        error_file_to_use = None
        if os.path.exists(asset_error_file):
            error_file_to_use = asset_error_file
            logger.info(f"Using error corrections from {asset_error_file}")
        elif os.path.exists(asset_error_file_copy):
            error_file_to_use = asset_error_file_copy
            logger.info(f"Using error corrections from copy: {asset_error_file_copy}")

        if error_file_to_use:
            asset_errors = pl.read_csv(error_file_to_use)
            # Make a copy to the asset directory if using the source file
            if error_file_to_use == asset_error_file:
                asset_errors.write_csv(asset_error_file_copy)

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
            logger.warning(
                f"No error file found for {asset}, loading original data without corrections."
            )
            asset_data = asset_original

    except (ValueError, FileNotFoundError, OSError) as e:
        logger.error(f"Error loading {asset}: {e}")
        asset_file = None
        asset_original = pl.DataFrame()
        asset_errors = pl.DataFrame()
        asset_data = pl.DataFrame()

    return asset_data


_splits_data_cache = None


def get_splits_data() -> pl.DataFrame:
    """Lazy-load splits overview data (cached after first call)."""
    global _splits_data_cache
    if _splits_data_cache is None:
        _splits_data_cache = get_asset_overview_data(asset="splits")
    return _splits_data_cache


if __name__ == "__main__":
    with pl.Config(tbl_rows=50, tbl_cols=20):
        asset = "splits"
        all_asset = get_asset_overview_data(asset)
        print(all_asset.head())
        print(all_asset.filter(pl.col("ticker").is_in(["XTIA"])))
