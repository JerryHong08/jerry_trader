"""
Docstring for DataSupply.snapshot_data_supply.fundamentals_fetch
fundamentals fetcher for all kinds of fundamental data from web and local cache such as float shares, country.
"""

import asyncio
import os
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

import httpx
import polars as pl
import requests
from bs4 import BeautifulSoup

from jerry_trader.platform.config.config import float_shares_dir
from jerry_trader.schema import FloatShares, FloatSourceData
from jerry_trader.shared.utils.logger import setup_logger
from jerry_trader.shared.utils.parse import _parse_number, _parse_percent

logger = setup_logger(__name__, log_to_file=True)


class FloatSharesProvider:

    URL = "https://knowthefloat.com/ticker/{ticker}"
    RETRIES = 2
    TIMEOUT = 3

    @classmethod
    async def fetch_from_web(cls, ticker: str) -> Optional[FloatShares]:
        url = cls.URL.format(ticker=ticker)
        client = httpx.AsyncClient(timeout=cls.TIMEOUT)

        for attempt in range(cls.RETRIES + 1):
            try:
                resp = await client.get(url, timeout=5)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt < cls.RETRIES:
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    return None

        soup = BeautifulSoup(resp.content, "html.parser")
        cards = soup.find_all("div", class_="col-lg-3 col-md-6 col-sm-12")

        results: List[FloatSourceData] = []
        for card in cards:
            img = card.find("img")
            source = img["alt"] if img and "alt" in img.attrs else None
            if not source:
                continue

            float_val = _parse_number(
                card.find("div", class_="float-section").find("p").get_text(strip=True)
                if card.find("div", class_="float-section")
                else None
            )
            short_val = _parse_percent(
                card.find("div", class_="short-percent-section")
                .find("p")
                .get_text(strip=True)
                if card.find("div", class_="short-percent-section")
                else None
            )
            out_val = _parse_number(
                card.find("div", class_="outstanding-shares-section")
                .find("p")
                .get_text(strip=True)
                if card.find("div", class_="outstanding-shares-section")
                else None
            )

            results.append(
                FloatSourceData(
                    source=source,
                    float_shares=float_val,
                    short_percent=short_val,
                    outstanding_shares=out_val,
                )
            )

        print(f"Debug web results: {results}")

        return FloatShares(
            ticker=ticker.upper(),
            data=results,
        )

    def fetch_from_local(cls, ticker: str) -> Optional[FloatShares]:
        float_shares_file = os.path.join(
            float_shares_dir,
            max(
                [
                    f
                    for f in os.listdir(float_shares_dir)
                    if f.startswith(f"float_shares_") and f.endswith(".parquet")
                ]
            ),
        )

        if not float_shares_file:
            return None

        try:
            float_val = (
                pl.read_parquet(float_shares_file)
                .filter(pl.col("symbol") == ticker)
                .select("floatShares")
                .unique()
                .item()
            )
        except Exception:
            return None

        return FloatShares(
            ticker=ticker.upper(),
            data=[
                FloatSourceData(
                    source="local_fmp",
                    float_shares=float_val,
                    short_percent=None,
                    outstanding_shares=None,
                )
            ],
        )


class FundamentalsFetcher:
    """
    Docstring for FundamentalsFetcher
    FMP API response example:
    https://financialmodelingprep.com/stable/profile?symbol=AAPL&apikey=${FMP_API_KEY}
    [
        {
            "symbol": "AAPL",  √
            "price": 255.53,
            "marketCap": 3775801225282,  √
            "beta": 1.093,
            "lastDividend": 1.03,
            "range": "169.21-288.62",  √ 52 week range
            "change": -2.68,
            "changePercentage": -1.03791,
            "volume": 70054453,
            "averageVolume": 45960616,  √
            "companyName": "Apple Inc.",  √
            "currency": "USD",
            "cik": "0000320193",  √
            "isin": "US0378331005",
            "cusip": "037833100",
            "exchangeFullName": "NASDAQ Global Select",
            "exchange": "NASDAQ",  √
            "industry": "Consumer Electronics",  √
            "website": "https://www.apple.com",  √
            "description": "Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. The company offers iPhone, a line of smartphones; Mac, a line of personal computers; iPad, a line of multi-purpose tablets; and wearables, home, and accessories comprising AirPods, Apple TV, Apple Watch, Beats products, and HomePod. It also provides AppleCare support and cloud services; and operates various platforms, including the App Store that allow customers to discover and download applications and digital content, such as books, music, video, games, and podcasts, as well as advertising services include third-party licensing arrangements and its own advertising platforms. In addition, the company offers various subscription-based services, such as Apple Arcade, a game subscription service; Apple Fitness+, a personalized fitness service; Apple Music, which offers users a curated listening experience with on-demand radio stations; Apple News+, a subscription news and magazine service; Apple TV+, which offers exclusive original content; Apple Card, a co-branded credit card; and Apple Pay, a cashless payment service, as well as licenses its intellectual property. The company serves consumers, and small and mid-sized businesses; and the education, enterprise, and government markets. It distributes third-party applications for its products through the App Store. The company also sells its products through its retail and online stores, and direct sales force; and third-party cellular network carriers, wholesalers, retailers, and resellers. Apple Inc. was founded in 1976 and is headquartered in Cupertino, California.",  √
            "ceo": "Timothy D. Cook",  √
            "sector": "Technology",  √
            "country": "US",  √
            "fullTimeEmployees": "164000",  √
            "phone": "(408) 996-1010",
            "address": "One Apple Park Way",  √
            "city": "Cupertino",  √
            "state": "CA",  √
            "zip": "95014",
            "image": "https://images.financialmodelingprep.com/symbol/AAPL.png",  √
            "ipoDate": "1980-12-12",  √
            "defaultImage": false,
            "isEtf": false,
            "isActivelyTrading": true,
            "isAdr": false,
            "isFund": false
        }
    ]
    """

    def __init__(self):
        self.FMP_API_KEY = os.getenv("FMP_API_KEY")
        self.FMP_BASE_URL = "https://financialmodelingprep.com/stable/profile"

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; FinancialNewsBot/1.0)",
                "accept": "application/json",
            }
        )

    def fetch_fundamentals_fmp(self, symbol: str, timeout: int = 5) -> pl.DataFrame:
        """
        Fetch Fundamentals from FMP API

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            timeout: connection timeout
        Returns:
            Fundamentals DataFrame
        """
        params = {"symbol": symbol.upper(), "apikey": self.FMP_API_KEY}

        try:
            logger.info(f"Start fetching {symbol} fundamental data using FMP API.")
            response = self.session.get(
                self.FMP_BASE_URL, params=params, timeout=timeout
            )
            response.raise_for_status()

            data = response.json()

            # Check return type
            if not isinstance(data, list):
                raise ValueError(
                    f"API Response data not matched, want List, got: {type(data)}"
                )

            # Transform into DataFrame objects
            try:
                df = pl.DataFrame(data).select(
                    [
                        "symbol",
                        "companyName",
                        "country",
                        "sector",
                        "industry",
                        "marketCap",
                        "range",
                        "averageVolume",
                        "ipoDate",
                        "fullTimeEmployees",
                        "ceo",
                        "website",
                        "description",
                        "cik",
                        "exchange",
                        "address",
                        "city",
                        "state",
                        "image",
                    ]
                )
            except Exception as e:
                logger.error(f"Parse fundamental data error: {e}, raw data: {data}")

            logger.info(f"Successfully fetched  fundamental data from FMP")
            return df

        except Exception as e:
            logger.error(f"Failed to fetch fundamental data for {symbol}: {e}")
            raise
