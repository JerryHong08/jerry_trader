import asyncio
import os
import time

import httpx
import polars as pl
from dotenv import load_dotenv

from jerry_trader.platform.config.config import float_shares_dir

os.makedirs(float_shares_dir, exist_ok=True)

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")


async def fetch_page(page, client):
    url = "https://financialmodelingprep.com/stable/shares-float-all"
    params = {"page": page, "limit": 5000, "apikey": API_KEY}
    try:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return page, r.json()
    except Exception as e:
        print(f"⚠️ Page {page} error: {e}")
        return page, []


async def main(out_dir=float_shares_dir):
    updated_time = time.strftime("%Y%m%d")
    out_file = os.path.join(out_dir, f"float_shares_{updated_time}.parquet")

    if os.path.exists(out_file):
        print(
            f"float shares data already incrementally updated. {out_file} already exists."
        )
        return out_file

    batch_size = 5
    start_page = 0
    all_data = []

    async with httpx.AsyncClient(timeout=10, proxy=None) as client:
        while True:
            pages = list(range(start_page, start_page + batch_size))
            tasks = [fetch_page(p, client) for p in pages]

            results = await asyncio.gather(*tasks)

            stop = False
            for page, data in results:
                if data:
                    all_data.extend(data)
                else:
                    stop = True
                    print(f"🚧 Data is ended page {page}")
                    break

            if stop:
                break

            start_page += batch_size

    df = pl.DataFrame(all_data)
    for f in os.listdir(out_dir):
        if f.startswith("float_shares_") and f.endswith(".parquet"):
            os.remove(os.path.join(out_dir, f))
    df.write_parquet(out_file, compression="snappy")
    print(f"✔️ Done, Total {len(all_data)} rows saved into float_shares.parquet")


asyncio.run(main())
