"""
Batch-fetch company profiles for all US stocks via FMP and save to parquet.

Profiles are fetched for the deduplicated set of latest_ticker values across
ALL CS-type stocks (including delisted), using FIGI-based ticker mapping to
avoid duplicate fetches for renamed tickers.

Usage:
    # Fetch all US common stocks (including delisted)
    poetry run python -m jerry_trader.services.data_prep.scripts.fetch_all_profiles

    # Fetch specific tickers with custom concurrency
    poetry run python -m jerry_trader.services.data_prep.scripts.fetch_all_profiles \
        --symbols AAPL,TSLA,MSFT --concurrency 3

Output:
    {data_dir}/raw/us_stocks_sip/fundamentals_profiles/profiles_YYYYMMDD.parquet
"""

import argparse
import asyncio
import os
import time
from typing import Optional

import polars as pl

from jerry_trader.platform.config.config import (
    all_tickers_dir,
    fundamentals_profiles_dir,
)
from jerry_trader.services.data_prep.providers.fmp import fetch_profiles_batch
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


def load_tickers_to_fetch() -> list[str]:
    """Load all CS-type tickers, map to latest_ticker via FIGI, deduplicate.

    Includes delisted stocks so their profiles are available for historical
    backtests.  Uses mapped_tickers (FIGI-based connected components) to
    resolve renamed tickers to their current name, avoiding duplicate FMP
    API calls for LQR→YHC style renames.
    """
    from jerry_trader.services.market_data.bootstrap.ticker_utils import (
        get_mapped_tickers,
    )

    files = sorted(
        [
            f
            for f in os.listdir(all_tickers_dir)
            if f.startswith("all_stocks_") and f.endswith(".parquet")
        ]
    )
    if not files:
        raise FileNotFoundError(f"No all_stocks_*.parquet found in {all_tickers_dir}")

    latest_file = os.path.join(all_tickers_dir, files[-1])
    df = pl.read_parquet(latest_file)

    # All CS type — include delisted so their profiles survive in cache
    cs_tickers = df.filter(pl.col("type") == "CS")["ticker"].unique().sort().to_list()
    logger.info(f"All CS tickers in file: {len(cs_tickers)} (including delisted)")

    # Map to latest_ticker via FIGI groups to deduplicate renames
    mapped = get_mapped_tickers()
    cs_df = pl.DataFrame({"ticker": cs_tickers})
    latest = cs_df.join(
        mapped.select(["ticker", "latest_ticker"]), on="ticker", how="left"
    ).with_columns(pl.col("latest_ticker").fill_null(pl.col("ticker")))

    unique_latest = latest["latest_ticker"].unique().sort().to_list()
    dup_saved = len(cs_tickers) - len(unique_latest)
    logger.info(
        f"Unique latest_tickers to fetch: {len(unique_latest)} "
        f"(saved {dup_saved} duplicate fetches via FIGI mapping)"
    )
    return unique_latest


async def main(
    concurrency: int, symbols: Optional[list[str]] = None, force: bool = False
):
    updated_time = time.strftime("%Y%m%d")
    os.makedirs(fundamentals_profiles_dir, exist_ok=True)
    out_file = os.path.join(
        fundamentals_profiles_dir, f"profiles_{updated_time}.parquet"
    )

    if os.path.exists(out_file) and not force:
        logger.info(f"Profiles for {updated_time} already exist: {out_file}")
        return out_file

    if symbols:
        tickers = symbols
    else:
        tickers = load_tickers_to_fetch()

    logger.info(
        f"Fetching profiles for {len(tickers)} tickers (concurrency={concurrency})..."
    )
    df = await fetch_profiles_batch(tickers, concurrency=concurrency)

    for f in os.listdir(fundamentals_profiles_dir):
        if f.startswith("profiles_") and f.endswith(".parquet"):
            os.remove(os.path.join(fundamentals_profiles_dir, f))

    df.write_parquet(out_file, compression="snappy")
    logger.info(f"Saved {len(df)} profiles to {out_file}")
    return out_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch fetch FMP company profiles")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Max concurrent requests (default: 5)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Comma-separated tickers. Default: all US common stocks.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch even if today's file already exists.",
    )
    args = parser.parse_args()

    symbols_list = args.symbols.split(",") if args.symbols else None
    asyncio.run(main(args.concurrency, symbols_list, args.force))
