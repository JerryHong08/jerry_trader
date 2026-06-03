"""
Async batch fetchers for FMP (Financial Modeling Prep) endpoints.

Usage:
    df = await fetch_profiles_batch(["AAPL", "TSLA"], concurrency=5)
"""

import asyncio
import os
from typing import Optional

import httpx
import polars as pl
from dotenv import load_dotenv

load_dotenv()

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"
DEFAULT_CONCURRENCY = 5

PROFILE_COLUMNS = [
    "symbol",
    "companyName",
    "country",
    "sector",
    "industry",
    "marketCap",
    "range",
    "exchange",
    "ipoDate",
]


def _client_kwargs() -> dict:
    proxy_url = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    kwargs = {"timeout": httpx.Timeout(15.0)}
    if proxy_url:
        kwargs["proxy"] = proxy_url
    return kwargs


async def _fetch_one_profile(
    symbol: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> tuple[str, Optional[dict]]:
    async with semaphore:
        try:
            url = f"{FMP_BASE}/profile"
            params = {"symbol": symbol.upper(), "apikey": API_KEY}
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                return symbol, data[0]
            return symbol, None
        except Exception as e:
            logger.warning(f"Profile fetch failed for {symbol}: {e}")
            return symbol, None


async def fetch_profiles_batch(
    symbols: list[str],
    concurrency: int = DEFAULT_CONCURRENCY,
) -> pl.DataFrame:
    """Fetch company profiles for a list of tickers via FMP /stable/profile.

    Args:
        symbols: Ticker symbols (e.g. ['AAPL', 'TSLA']).
        concurrency: Max simultaneous requests.

    Returns:
        DataFrame with columns: symbol, companyName, country, sector,
        industry, marketCap, range, exchange, ipoDate.
    """
    semaphore = asyncio.Semaphore(concurrency)
    client_kwargs = _client_kwargs()

    async with httpx.AsyncClient(**client_kwargs) as client:
        tasks = [_fetch_one_profile(s, client, semaphore) for s in symbols]
        results = await asyncio.gather(*tasks)

    records = [data for _, data in results if data is not None]

    if not records:
        logger.warning("No profile data fetched.")
        return pl.DataFrame()

    df = pl.DataFrame(records)
    existing = [c for c in PROFILE_COLUMNS if c in df.columns]
    df = df.select(existing)
    logger.info(f"Fetched {len(df)} profiles.")
    return df
