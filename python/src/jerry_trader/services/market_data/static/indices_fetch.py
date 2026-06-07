import os
import time
from datetime import datetime, timedelta

import polars as pl
import requests
from dotenv import load_dotenv

from jerry_trader.platform.config.config import indices_day_aggs_dir

load_dotenv()


class IndicesFetcher:
    def __init__(self):
        self.api_key = os.getenv("POLYGON_API_KEY_indices")
        self.base_url = "https://api.polygon.io"
        self.fmp_api_key = os.getenv("FMP_API_KEY")
        self.fmp_base_url = "https://financialmodelingprep.com"

        # Use requests with proxy support
        self.session = requests.Session()
        proxy = os.environ.get("HTTP_PROXY")
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}

        # Ensure output directory exists
        os.makedirs(indices_day_aggs_dir, exist_ok=True)

    def _get_ticker_file_path(self, ticker: str, timespan: str) -> str:
        """Get the file path for a ticker's data file."""
        # Replace colon with underscore for filesystem compatibility (e.g., "I:SPX" -> "I_SPX")
        safe_ticker = ticker.replace(":", "_")
        return os.path.join(indices_day_aggs_dir, f"{safe_ticker}_{timespan}.parquet")

    @staticmethod
    def _ticker_to_fmp(ticker: str) -> str:
        """Convert Polygon index ticker (I:SPX) to FMP format (^SPX)."""
        return ticker.replace("I:", "^")

    def _fetch_fmp_data(self, ticker: str, start_: str) -> list | None:
        """Fetch index data from FMP as fallback. Returns Polygon-compatible rows."""
        fmp_ticker = self._ticker_to_fmp(ticker)
        url = f"{self.fmp_base_url}/stable/historical-price-eod/full"
        params = {
            "symbol": fmp_ticker,
            "from": start_,
            "to": self.end_,
            "apikey": self.fmp_api_key,
        }
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or not data:
                print(f"FMP returned no data for {fmp_ticker}")
                return None

            results = []
            for row in data:
                dt = datetime.strptime(row["date"], "%Y-%m-%d")
                results.append(
                    {
                        "o": row["open"],
                        "h": row["high"],
                        "l": row["low"],
                        "c": row["close"],
                        "t": int(dt.timestamp() * 1000),
                    }
                )
            print(f"FMP returned {len(results)} rows for {fmp_ticker}")
            return results
        except Exception as e:
            print(f"FMP fallback error for {ticker}: {e}")
            return None

    def fetch_tickers(
        self,
        tickers: list,
        end_date: str = None,
        timespan: str = "day",
    ):
        self.end_ = (
            end_date if end_date else time.strftime("%Y-%m-%d", time.localtime())
        )
        self.timespan = timespan

        for ticker in tickers:
            self.ticker_file_path = self._get_ticker_file_path(ticker, timespan)
            ticker_data = pl.DataFrame()

            # Update from existing data
            if os.path.exists(self.ticker_file_path):
                ticker_data = pl.read_parquet(self.ticker_file_path)
                update_data = ticker_data.with_columns(
                    pl.from_epoch(pl.col("timestamp"), time_unit="ms")
                ).sort("timestamp")

                last_updated = update_data["timestamp"].max().strftime("%Y-%m-%d")
                start_ = (
                    datetime.strptime(last_updated, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
                print(f"Start date set to {start_} based on existing data.")
            else:
                # Polygon only provides indices data since 2023
                start_ = "2022-12-31"
                print(
                    f"No existing data found for {ticker}. Start date set to {start_}."
                )

            print(f"Fetching {ticker} data from {start_} to {self.end_}...")
            self.fetch_ticker_data(ticker, start_, ticker_data)

    def fetch_ticker_data(
        self, ticker: str, start_: str, ticker_data: pl.DataFrame = None
    ):
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/{self.timespan}/{start_}/{self.end_}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.api_key,
        }

        all_results = []
        max_attempts = 3

        while url:
            page_failed = False
            for attempt in range(1, max_attempts + 1):
                try:
                    resp = self.session.get(url, params=params, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except requests.exceptions.HTTPError as e:
                    print(f"Polygon HTTP error for {ticker}: {e}")
                    page_failed = True
                    break
                except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                ) as e:
                    if attempt >= max_attempts:
                        print(
                            f"Polygon failed for {ticker} after {max_attempts} attempts: {e}"
                        )
                        page_failed = True
                        break
                    wait_time = 3 * attempt
                    print(
                        f"Connection error on attempt {attempt}/{max_attempts}: {type(e).__name__}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)

            if page_failed:
                # Only use FMP fallback when the first page fails
                if not all_results and self.fmp_api_key:
                    print(f"Trying FMP fallback for {ticker}...")
                    fmp_results = self._fetch_fmp_data(ticker, start_)
                    if fmp_results:
                        all_results = fmp_results
                        break  # FMP has no pagination
                    print(f"FMP fallback also failed for {ticker}.")
                return

            results = data.get("results", [])
            all_results.extend(results)

            # Handle pagination
            next_url = data.get("next_url")
            if next_url:
                url = next_url
                params = {"apiKey": self.api_key}  # next_url already has other params
            else:
                url = None

        if not all_results:
            print(f"No data fetched for {ticker}.")
            return

        aggs = pl.DataFrame(all_results).select(
            pl.col("o").alias("open"),
            pl.col("h").alias("high"),
            pl.col("l").alias("low"),
            pl.col("c").alias("close"),
            pl.col("t").alias("timestamp"),
        )

        aggs = aggs.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.truncate("1d")
            .dt.epoch(time_unit="ms")
            .alias("timestamp")
        )

        if not ticker_data.is_empty():
            aggs = pl.concat([ticker_data, aggs]).unique(
                subset=["timestamp"], keep="last"
            )

        aggs.write_parquet(
            self.ticker_file_path,
            compression="zstd",
            compression_level=3,
        )

        if not ticker_data.is_empty():
            print(f"Incremental update data for {ticker}.")
        else:
            print(f"Initial data for {ticker}.")

        with pl.Config(tbl_cols=20):
            aggs = aggs.with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                    "UTC"
                )
            ).sort("timestamp")

            print(aggs.head())
            print(aggs.tail())


# Default tickers to fetch
INDICES_TICKERS = [
    "I:SPX",
    "I:IRX",
]

if __name__ == "__main__":
    print(f"Indices data will be saved to: {indices_day_aggs_dir}")
    fetcher = IndicesFetcher()
    fetcher.fetch_tickers(INDICES_TICKERS)
