"""
Docstring for DataSupply.snapshotDataSupply.borrow_fee_fetch
Borrow Fee data provider for chartexchange.com
Supports both real-time and historical data extraction
"""

import json
import os
import re
from datetime import datetime
from io import StringIO
from typing import Dict, Optional
from urllib.parse import urljoin

import pandas as pd
import requests

from DataUtils.schema import BorrowFee


class BorrowFeeProvider:
    def __init__(self):
        self.compiled_pattern = re.compile(
            r"As of <span[^>]*>([^<]+)</span>[^<]*there were <span[^>]*>([\d,]+)</span>[^<]*shares[^<]*fee of <span[^>]*>([\d.]+)%</span>"
        )
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    def extract_realtime_borrow_fee(self, ticker: str) -> Optional[Dict]:
        """Extract current borrow fee from chartexchange.com"""
        url = f"https://chartexchange.com/symbol/nasdaq-{ticker.lower()}/borrow-fee/"
        try:
            response = requests.get(url, timeout=5, headers=self.headers)
            response.raise_for_status()
            match = self.compiled_pattern.search(response.text)

            if match:
                return {
                    "update_time": match.group(1),
                    "available_shares": int(match.group(2).replace(",", "")),
                    "borrow_fee": float(match.group(3)) / 100,
                }
        except Exception as e:
            print(f"Error extracting realtime borrow fee: {e}")
            return None

    def _find_correct_cx_table(self, html: str) -> Optional[str]:
        """Find cx_table instance containing download_data"""
        cx_table_pattern = r"new cx_table\(\s*({.*?})\s*\)"
        matches = list(re.finditer(cx_table_pattern, html, re.DOTALL))

        for match in matches:
            try:
                table_config = json.loads(match.group(1))
                if "download_data" in table_config:
                    return match.group(1)
            except json.JSONDecodeError:
                continue

        return None

    def _extract_download_params(self, url: str) -> Optional[Dict]:
        """Extract download parameters from HTML page"""
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()

            correct_table_json = self._find_correct_cx_table(response.text)
            if not correct_table_json:
                return None

            table_config = json.loads(correct_table_json)
            download_data = table_config.get("download_data", {})

            if not download_data:
                return None

            return {
                "base_url": download_data.get("url", ""),
                "params": download_data.get("params", {}),
            }

        except Exception as e:
            print(f"Error extracting download parameters: {e}")
            return None

    def download_historical_borrow_fee(
        self, ticker: str, save_path: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """Download historical borrow fee data as CSV"""
        url = f"https://chartexchange.com/symbol/nasdaq-{ticker.lower()}/borrow-fee/"
        download_info = self._extract_download_params(url)
        if not download_info:
            return None

        base_url = download_info["base_url"]
        params = download_info["params"]

        if not base_url.startswith("http"):
            base_url = urljoin(url, base_url)

        download_url = (
            base_url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
        )

        headers = {
            **self.headers,
            "Accept": "text/csv,application/csv,*/*",
            "Referer": url,
        }

        try:
            response = requests.get(download_url, headers=headers, timeout=30)
            response.raise_for_status()

            csv_content = response.text

            if not csv_content.strip() or "," not in csv_content.split("\n")[0]:
                print("Invalid CSV content received")
                return None

            if save_path:
                os.makedirs(
                    os.path.dirname(save_path) if os.path.dirname(save_path) else ".",
                    exist_ok=True,
                )
                with open(save_path, "w", encoding="utf-8") as f:
                    f.write(csv_content)
                print(f"CSV saved to: {save_path}")

            return pd.read_csv(StringIO(csv_content))

        except Exception as e:
            print(f"Error downloading historical data: {e}")
            return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Brorrow Fee Fetcher")
    parser.add_argument(
        "--ticker", default="None", help="ticker you want to fetch its borrrow fee"
    )

    args = parser.parse_args()
    ticker = args.ticker

    provider = BorrowFeeProvider()

    # realtime data
    realtime_data = provider.extract_realtime_borrow_fee(ticker)
    print(f"Realtime data: {realtime_data}")

    # historical csv file data
    updated_time = datetime.now().strftime("%Y-%m-%d")
    df = provider.download_historical_borrow_fee(
        ticker.lower(), f"{ticker}_{updated_time}_borrow_fee_data.csv"
    )
    if df is not None:
        print(f"Downloaded {len(df)} rows of historical data")
        print(df.head())
