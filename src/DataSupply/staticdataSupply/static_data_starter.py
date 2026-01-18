"""
Static Data Fetch Starter Script
Unified entry point for fetching borrow fees, fundamentals, and news data
"""

import argparse
import asyncio
import sys
from datetime import datetime
from typing import List, Optional

from DataSupply.staticdataSupply.borrow_fee_fetch import BorrowFeeProvider
from DataSupply.staticdataSupply.fundamentals_fetch import (
    FloatSharesProvider,
    FundamentalsFetcher,
)
from DataSupply.staticdataSupply.news_fetch import (
    API_NewsFetchers,
    MoomooStockResolver,
    NewsFormatter,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class StaticDataFetcher:
    """Unified fetcher for all static data types"""

    def __init__(self, ticker: str):
        self.ticker = ticker.upper()
        self.borrow_fee_provider = BorrowFeeProvider()
        self.float_shares_provider = FloatSharesProvider()
        self.fundamentals_fetcher = FundamentalsFetcher()
        self.momo_news = MoomooStockResolver()
        self.api_news = API_NewsFetchers()

    def fetch_borrow_fee(self, save_csv: bool = False) -> dict:
        """Fetch borrow fee data (realtime and historical)"""
        print(f"\n{'='*60}")
        print(f"📊 Fetching Borrow Fee Data for {self.ticker}")
        print(f"{'='*60}")

        results = {}

        # Realtime data
        realtime = self.borrow_fee_provider.extract_realtime_borrow_fee(self.ticker)
        if realtime:
            print(f"\n✅ Realtime Borrow Fee:")
            print(f"   Update Time: {realtime['update_time']}")
            print(f"   Available Shares: {realtime['available_shares']:,}")
            print(f"   Borrow Fee: {realtime['borrow_fee']:.2%}")
            results["realtime"] = realtime
        else:
            print(f"\n⚠️  No realtime borrow fee data found")

        # Historical data
        if save_csv:
            updated_time = datetime.now().strftime("%Y-%m-%d")
            csv_path = f"{self.ticker.lower()}_{updated_time}_borrow_fee_data.csv"
            df = self.borrow_fee_provider.download_historical_borrow_fee(
                self.ticker.lower(), csv_path
            )
            if df is not None:
                print(f"\n✅ Historical Data: {len(df)} rows saved to {csv_path}")
                print(f"   Preview:\n{df.head()}")
                results["historical"] = {"rows": len(df), "path": csv_path}
            else:
                print(f"\n⚠️  Failed to download historical data")

        return results

    async def fetch_float_shares(self, source: str = "local") -> dict:
        """Fetch float shares data"""
        print(f"\n{'='*60}")
        print(f"📈 Fetching Float Shares for {self.ticker}")
        print(f"{'='*60}")

        results = {}

        if source == "web":
            float_data = await self.float_shares_provider.fetch_from_web(self.ticker)
            if float_data:
                print(f"\n✅ Float Shares Data from Web:")
                for item in float_data.data:
                    print(f"\n   Source: {item.source}")
                    if item.float_shares:
                        print(f"   Float Shares: {item.float_shares:,.0f}")
                    if item.short_percent:
                        print(f"   Short Percent: {item.short_percent:.2%}")
                    if item.outstanding_shares:
                        print(f"   Outstanding Shares: {item.outstanding_shares:,.0f}")
                results["float_shares"] = float_data
            else:
                print(f"\n⚠️  No float shares data found from web")

        elif source == "local":
            float_data = self.float_shares_provider.fetch_from_local(self.ticker)
            if float_data:
                print(f"\n✅ Float Shares Data from Local:")
                for item in float_data.data:
                    print(f"   Source: {item.source}")
                    if item.float_shares:
                        print(f"   Float Shares: {item.float_shares:,.0f}")
                results["float_shares"] = float_data
            else:
                print(f"\n⚠️  No float shares data found locally")

        return results

    def fetch_company_info(self) -> dict:
        """Fetch company info/fundamentals from FMP"""
        print(f"\n{'='*60}")
        print(f"🏢 Fetching Company Info for {self.ticker}")
        print(f"{'='*60}")

        results = {}

        try:
            fmp_data = self.fundamentals_fetcher.fetch_fundamentals_fmp(self.ticker)
            if fmp_data is not None and len(fmp_data) > 0:
                print(f"\n✅ FMP Company Info:")
                print(f"{fmp_data}")
                results["company_info"] = fmp_data
            else:
                print(f"\n⚠️  No company info found from FMP")
        except Exception as e:
            print(f"\n❌ Error fetching FMP company info: {e}")
            logger.error(f"FMP company info error: {e}")

        return results

    def fetch_news(self, provider: str = "momo", limit: int = 5) -> dict:
        """Fetch news data"""
        print(f"\n{'='*60}")
        print(f"📰 Fetching News for {self.ticker} from {provider.upper()}")
        print(f"{'='*60}")

        results = {}
        articles = None

        try:
            if provider == "momo":
                articles = self.momo_news.get_news_momo(self.ticker, pageSize=limit)
            elif provider == "fmp":
                articles = self.api_news.fetch_news_fmp(self.ticker, limit=limit)
            elif provider == "benzinga":
                articles = self.api_news.fetch_news_benzinga(
                    self.ticker, page_size=limit, display_output="full"
                )

            if articles:
                print(f"\n✅ Found {len(articles)} articles:")
                print(NewsFormatter.format_json(articles))
                results["articles"] = articles
            else:
                print(f"\n⚠️  No news found for {self.ticker} on {provider}")

        except Exception as e:
            print(f"\n❌ Error fetching news: {e}")
            logger.error(f"News fetch error: {e}")

        return results


async def main():
    parser = argparse.ArgumentParser(
        description="Static Data Fetch Starter - Unified fetcher for borrow fees, float shares, company info, and news",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all data types for AAPL
  python static_data_starter.py --ticker AAPL --all

  # Fetch only borrow fee data
  python static_data_starter.py --ticker TSLA --borrow-fee

  # Fetch float shares from web
  python static_data_starter.py --ticker NVDA --float --source web

  # Fetch company info from FMP
  python static_data_starter.py --ticker AAPL --info

  # Fetch news from multiple providers
  python static_data_starter.py --ticker AAPL --news --provider momo --limit 10

  # Fetch multiple data types
  python static_data_starter.py --ticker GME --borrow-fee --float --info --news
        """,
    )

    # Required arguments
    parser.add_argument(
        "--ticker", required=True, help="Stock ticker symbol (e.g., AAPL, TSLA, NVDA)"
    )

    # Data type selection
    data_group = parser.add_argument_group("Data Types")
    data_group.add_argument(
        "--all",
        action="store_true",
        help="Fetch all data types (borrow fee, float shares, company info, news)",
    )
    data_group.add_argument(
        "--borrow-fee", action="store_true", help="Fetch borrow fee data"
    )
    data_group.add_argument(
        "--float", action="store_true", help="Fetch float shares data"
    )
    data_group.add_argument(
        "--info", action="store_true", help="Fetch company info/fundamentals from FMP"
    )
    data_group.add_argument("--news", action="store_true", help="Fetch news articles")

    # Options for borrow fee
    borrow_group = parser.add_argument_group("Borrow Fee Options")
    borrow_group.add_argument(
        "--save-csv",
        action="store_true",
        help="Save historical borrow fee data to CSV file",
    )

    # Options for float shares
    float_group = parser.add_argument_group("Float Shares Options")
    float_group.add_argument(
        "--source",
        choices=["web", "local"],
        default="local",
        help="Source for float shares data (default: local)",
    )

    # Options for news
    news_group = parser.add_argument_group("News Options")
    news_group.add_argument(
        "--provider",
        choices=["momo", "fmp", "benzinga"],
        default="momo",
        help="News provider (default: momo)",
    )
    news_group.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of news articles to fetch (default: 5)",
    )

    args = parser.parse_args()

    # Validate that at least one data type is selected
    if not (args.all or args.borrow_fee or args.float or args.info or args.news):
        parser.error(
            "Please specify at least one data type: --all, --borrow-fee, --float, --info, or --news"
        )

    # Initialize fetcher
    fetcher = StaticDataFetcher(args.ticker)

    print(f"\n{'#'*60}")
    print(f"# Static Data Fetcher for {args.ticker.upper()}")
    print(f"# {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    all_results = {}

    # Fetch borrow fee
    if args.all or args.borrow_fee:
        try:
            results = fetcher.fetch_borrow_fee(save_csv=args.save_csv)
            all_results["borrow_fee"] = results
        except Exception as e:
            logger.error(f"Borrow fee fetch failed: {e}")
            print(f"\n❌ Borrow fee fetch failed: {e}")

    # Fetch float shares
    if args.all or args.float:
        try:
            results = await fetcher.fetch_float_shares(source=args.source)
            all_results["float_shares"] = results
        except Exception as e:
            logger.error(f"Float shares fetch failed: {e}")
            print(f"\n❌ Float shares fetch failed: {e}")

    # Fetch company info
    if args.all or args.info:
        try:
            results = fetcher.fetch_company_info()
            all_results["company_info"] = results
        except Exception as e:
            logger.error(f"Company info fetch failed: {e}")
            print(f"\n❌ Company info fetch failed: {e}")

    # Fetch news
    if args.all or args.news:
        try:
            results = fetcher.fetch_news(provider=args.provider, limit=args.limit)
            all_results["news"] = results
        except Exception as e:
            logger.error(f"News fetch failed: {e}")
            print(f"\n❌ News fetch failed: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"📋 Fetch Summary")
    print(f"{'='*60}")
    print(f"Ticker: {args.ticker.upper()}")

    if "borrow_fee" in all_results:
        print(f"✅ Borrow Fee: {'Fetched' if all_results['borrow_fee'] else 'No data'}")
    if "float_shares" in all_results:
        print(
            f"✅ Float Shares: {'Fetched' if all_results['float_shares'] else 'No data'}"
        )
    if "company_info" in all_results:
        print(
            f"✅ Company Info: {'Fetched' if all_results['company_info'] else 'No data'}"
        )
    if "news" in all_results:
        print(f"✅ News: {'Fetched' if all_results['news'] else 'No data'}")

    print(f"\n✨ Done!\n")

    return all_results


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
