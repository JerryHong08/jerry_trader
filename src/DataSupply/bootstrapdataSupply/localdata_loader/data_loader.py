import glob
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import exchange_calendars as xcals
import polars as pl
from dotenv import load_dotenv

from config import data_dir, get_splits_data
from DataSupply.bootstrapdataSupply.localdata_loader.date_utils import (
    resolve_date_range,
)
from DataSupply.bootstrapdataSupply.localdata_loader.path_loader import DataPathFetcher
from DataSupply.bootstrapdataSupply.localdata_loader.ticker_utils import (
    get_mapped_tickers,
)

load_dotenv()

# Constants
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
PRICE_DECIMALS = 4
CACHE_DIR_TEMPLATE = "processed/{asset}/{data_type}"

# Session time boundaries
SESSION_TIMES = {
    "premarket": {"start": (4, 0), "end": (9, 30)},
    "regular": {"start": (9, 30), "end": (16, 0)},
    "afterhours": {"start": (16, 0), "end": (20, 0)},
}


@dataclass
class LoaderConfig:
    """Configuration for stock data loading"""

    tickers: Optional[List[str]]
    start_date: str
    end_date: Optional[str]
    timedelta: Optional[int]
    timeframe: str
    asset: str
    data_type: str
    full_hour: bool
    lake: bool
    use_s3: bool
    use_cache: bool
    use_duck_db: bool
    skip_low_volume: bool


class TimestampGenerator:
    """Handles generation of trading timestamps"""

    def __init__(self, calendar_name: str = "XNYS"):
        self.calendar = xcals.get_calendar(calendar_name)

    def generate(
        self, start_date: str, end_date: str, timeframe: str, full_hour: bool = False
    ) -> pl.DataFrame:
        """
        Generate trading timestamps for the specified date range.

        Args:
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'
            timeframe: '1m', '5m', '15m', '30m', '1h', '1d'
            full_hour: Include pre-market and after-hours (4:00-20:00)

        Returns:
            DataFrame with timestamps column
        """
        schedule = self.calendar.schedule.loc[start_date:end_date]
        df_schedule = pl.from_pandas(schedule.reset_index()).with_columns(
            [
                pl.col("open").dt.convert_time_zone("America/New_York"),
                pl.col("close").dt.convert_time_zone("America/New_York"),
            ]
        )

        if timeframe == "1d":
            return self._generate_daily_timestamps(df_schedule)
        else:
            return self._generate_intraday_timestamps(df_schedule, full_hour)

    def _generate_daily_timestamps(self, df_schedule: pl.DataFrame) -> pl.DataFrame:
        """Generate daily timestamps"""
        return (
            df_schedule.select(pl.col("open").dt.date().alias("trade_date"))
            .with_columns(
                pl.col("trade_date")
                .cast(pl.Datetime)
                .dt.replace_time_zone("America/New_York")
                .alias("timestamps")
            )
            .select("timestamps")
        )

    def _generate_intraday_timestamps(
        self, df_schedule: pl.DataFrame, full_hour: bool
    ) -> pl.DataFrame:
        """Generate intraday timestamps"""
        all_timestamps = []

        for row in df_schedule.iter_rows(named=True):
            is_half_day = row["close"].hour == 13
            time_segments = self._get_time_segments(row, full_hour, is_half_day)

            for start_time, end_time in time_segments:
                timestamps = self._generate_minute_range(start_time, end_time)
                all_timestamps.extend(timestamps)

        return (
            pl.DataFrame({"timestamps": all_timestamps})
            .with_columns(
                pl.col("timestamps")
                .dt.cast_time_unit("ns")
                .dt.convert_time_zone("America/New_York")
            )
            .sort("timestamps")
        )

    @staticmethod
    def _get_time_segments(
        row: Dict, full_hour: bool, is_half_day: bool
    ) -> List[Tuple]:
        """Determine time segments for a trading day"""
        if not full_hour:
            return [(row["open"], row["close"])]

        if is_half_day:
            return [
                (
                    row["open"].replace(hour=4, minute=0, second=0),
                    row["open"].replace(hour=13, minute=0, second=0),
                ),
                (
                    row["open"].replace(hour=16, minute=0, second=0),
                    row["open"].replace(hour=17, minute=0, second=0),
                ),
            ]

        return [
            (
                row["open"].replace(hour=4, minute=0, second=0),
                row["close"].replace(hour=20, minute=0, second=0),
            )
        ]

    @staticmethod
    def _generate_minute_range(start_time, end_time) -> List:
        """Generate minute-level timestamps between start and end"""
        return (
            pl.select(
                pl.datetime_range(
                    start_time, end_time, interval="1m", closed="left", time_unit="ns"
                ).alias("timestamps")
            )
            .get_column("timestamps")
            .to_list()
        )


class OHLCVResampler:
    """Handles resampling of OHLCV data"""

    @staticmethod
    def parse_timeframe(timeframe: str) -> int:
        """Parse timeframe string to minutes"""
        match = re.match(r"(\d+)([mhd])", timeframe.lower())
        if not match:
            raise ValueError(f"Invalid timeframe: {timeframe}")

        value, unit = int(match.group(1)), match.group(2)
        multipliers = {"m": 1, "h": 60, "d": 1440}
        return value * multipliers[unit]

    @staticmethod
    def get_session_start(timestamp_col: pl.Expr, session_type: str) -> pl.Expr:
        """Get session start time for the given session type"""
        session_hours = {
            "regular": "09:30:00",
            "premarket": "04:00:00",
            "afterhours": "16:00:00",
        }

        time_str = session_hours.get(session_type, "09:30:00")
        return pl.concat_str(
            [timestamp_col.dt.date().cast(pl.Utf8), pl.lit(f" {time_str}")]
        ).str.strptime(pl.Datetime(time_zone="America/New_York"), strict=True)

    def resample(self, lf: pl.LazyFrame, timeframe: str) -> pl.LazyFrame:
        """
        Resample OHLCV data to specified timeframe.
        Uses session-aware bucketing for accurate aggregation.
        """
        bar_minutes = self.parse_timeframe(timeframe)
        print(f"Resampling to {bar_minutes} minute intervals")

        # Add temporal columns and classify sessions
        lf = self._add_temporal_columns(lf)
        lf = self._classify_sessions(lf)
        lf = self._calculate_bar_ids(lf, bar_minutes)
        lf = self._calculate_bar_starts(lf, bar_minutes)

        # Aggregate by bars
        return self._aggregate_bars(lf)

    @staticmethod
    def _add_temporal_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Add date, hour, and minute columns"""
        return lf.with_columns(
            [
                pl.col("timestamps").dt.date().alias("trade_date"),
                pl.col("timestamps").dt.hour().alias("hour"),
                pl.col("timestamps").dt.minute().alias("minute"),
            ]
        )

    @staticmethod
    def _classify_sessions(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Classify each timestamp into trading session"""
        return lf.with_columns(
            pl.when((pl.col("hour") >= 4) & (pl.col("hour") < 9))
            .then(pl.lit("premarket"))
            .when((pl.col("hour") == 9) & (pl.col("minute") < 30))
            .then(pl.lit("premarket"))
            .when((pl.col("hour") == 9) & (pl.col("minute") >= 30))
            .then(pl.lit("regular"))
            .when((pl.col("hour") > 9) & (pl.col("hour") < 16))
            .then(pl.lit("regular"))
            .when((pl.col("hour") >= 16) & (pl.col("hour") < 20))
            .then(pl.lit("afterhours"))
            .otherwise(pl.lit("other"))
            .alias("session")
        )

    def _calculate_bar_ids(self, lf: pl.LazyFrame, bar_minutes: int) -> pl.LazyFrame:
        """Calculate bar IDs based on minutes from session start"""
        return lf.with_columns(
            pl.when(pl.col("session") == "premarket")
            .then(self._minutes_from_session(pl.col("timestamps"), "premarket"))
            .when(pl.col("session") == "regular")
            .then(self._minutes_from_session(pl.col("timestamps"), "regular"))
            .when(pl.col("session") == "afterhours")
            .then(self._minutes_from_session(pl.col("timestamps"), "afterhours"))
            .otherwise(pl.lit(0))
            .alias("minutes_from_session_start")
        ).with_columns(
            (pl.col("minutes_from_session_start") // bar_minutes).alias("bar_id")
        )

    def _minutes_from_session(
        self, timestamp_col: pl.Expr, session_type: str
    ) -> pl.Expr:
        """Calculate minutes elapsed from session start"""
        session_start = self.get_session_start(timestamp_col, session_type)
        return (timestamp_col - session_start).dt.total_minutes().cast(pl.Int64)

    def _calculate_bar_starts(self, lf: pl.LazyFrame, bar_minutes: int) -> pl.LazyFrame:
        """Calculate the start timestamp for each bar"""
        return lf.with_columns(
            pl.when(pl.col("session") == "premarket")
            .then(
                self.get_session_start(pl.col("timestamps"), "premarket")
                + pl.duration(minutes=pl.col("bar_id") * pl.lit(bar_minutes))
            )
            .when(pl.col("session") == "regular")
            .then(
                self.get_session_start(pl.col("timestamps"), "regular")
                + pl.duration(minutes=pl.col("bar_id") * pl.lit(bar_minutes))
            )
            .when(pl.col("session") == "afterhours")
            .then(
                self.get_session_start(pl.col("timestamps"), "afterhours")
                + pl.duration(minutes=pl.col("bar_id") * pl.lit(bar_minutes))
            )
            .otherwise(pl.col("timestamps"))
            .alias("bar_start")
        )

    @staticmethod
    def _aggregate_bars(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Aggregate data into OHLCV bars"""
        return (
            lf.group_by(["ticker", "trade_date", "session", "bar_id", "bar_start"])
            .agg(
                [
                    pl.col("open").first().alias("open"),
                    pl.col("high").max().alias("high"),
                    pl.col("low").min().alias("low"),
                    pl.col("close").last().alias("close"),
                    pl.col("volume").sum().alias("volume"),
                    pl.col("transactions").sum().alias("transactions"),
                ]
            )
            .sort(["ticker", "trade_date", "bar_start"])
            .drop(["trade_date", "session", "bar_id"])
            .rename({"bar_start": "timestamps"})
            .lazy()
        )


class SplitsAdjuster:
    """Handles split adjustments for price and volume data"""

    @staticmethod
    def adjust(
        lf: pl.LazyFrame, splits: pl.DataFrame, price_decimals: int = PRICE_DECIMALS
    ) -> pl.LazyFrame:
        """
        Apply split adjustments to OHLCV data.

        Args:
            lf: LazyFrame with OHLCV data
            splits: DataFrame with split information
            price_decimals: Number of decimals for price rounding

        Returns:
            Split-adjusted LazyFrame

        Note:
            Splits are filtered PER TICKER based on each ticker's actual date range.
            This prevents splits that occur after a ticker's last trading date from
            corrupting historical data (e.g., delisted stocks with subsequent reverse splits).
        """
        # Get per-ticker date ranges to filter splits correctly
        ticker_date_ranges = (
            lf.group_by("ticker")
            .agg(
                [
                    pl.col("timestamps").min().alias("date_min"),
                    pl.col("timestamps").max().alias("date_max"),
                ]
            )
            .collect()
        )

        if ticker_date_ranges.height == 0:
            return lf

        tickers = ticker_date_ranges["ticker"].to_list()

        if not tickers:
            return lf

        # Filter splits PER TICKER based on each ticker's actual trading date range
        # This prevents applying splits that occur after a ticker was delisted
        splits_filtered_list = []
        for row in ticker_date_ranges.iter_rows(named=True):
            ticker = row["ticker"]
            date_min = row["date_min"]
            date_max = row["date_max"]

            if date_min is None or date_max is None:
                continue

            ticker_splits = splits.filter(
                (pl.col("ticker") == ticker)
                & (
                    pl.col("execution_date")
                    .str.to_date()
                    .is_between(
                        date_min.date() - pl.duration(days=1),
                        date_max.date() + pl.duration(days=1),
                    )
                )
            )
            if ticker_splits.height > 0:
                splits_filtered_list.append(ticker_splits)

        if not splits_filtered_list:
            return lf

        splits_filtered = pl.concat(splits_filtered_list)

        # Calculate cumulative split ratios
        splits_with_factor = SplitsAdjuster._calculate_split_factors(splits_filtered)
        print(f"Applying splits for {splits_with_factor.height} events")

        # Join and adjust prices/volumes
        return SplitsAdjuster._apply_adjustments(lf, splits_with_factor, price_decimals)

    @staticmethod
    def _calculate_split_factors(splits_filtered: pl.DataFrame) -> pl.DataFrame:
        """Calculate cumulative split factors"""
        return (
            splits_filtered.with_columns(
                [
                    (
                        pl.col("execution_date").str.to_date() - pl.duration(days=1)
                    ).alias("split_date"),
                    (pl.col("split_from") / pl.col("split_to")).alias("split_ratio"),
                ]
            )
            .select(["ticker", "split_date", "split_ratio"])
            .sort(["ticker", "split_date"], descending=[False, True])
            .with_columns(
                pl.col("split_ratio")
                .cum_prod()
                .over("ticker")
                .alias("cumulative_split_ratio")
            )
            .group_by(["ticker", "split_date"])
            .agg(pl.col("cumulative_split_ratio").last())
            .sort(["ticker", "split_date"])
        )

    @staticmethod
    def _apply_adjustments(
        lf: pl.LazyFrame, splits_with_factor: pl.DataFrame, price_decimals: int
    ) -> pl.LazyFrame:
        """Apply split adjustments to prices and volumes"""
        return (
            lf.with_columns(pl.col("timestamps").dt.date().alias("date_only"))
            .join_asof(
                splits_with_factor.lazy(),
                left_on="date_only",
                right_on="split_date",
                by="ticker",
                strategy="forward",
            )
            .with_columns(
                pl.col("cumulative_split_ratio").fill_null(1.0).alias("factor")
            )
            .with_columns(
                [
                    (pl.col("open") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("open"),
                    (pl.col("high") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("high"),
                    (pl.col("low") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("low"),
                    (pl.col("close") * pl.col("factor"))
                    .round(price_decimals)
                    .alias("close"),
                    (pl.col("volume") / pl.col("factor"))
                    .round(0)
                    .cast(pl.Int64)
                    .alias("volume"),
                ]
            )
            .drop(["date_only", "cumulative_split_ratio", "factor"])
        )


class TickerAligner:
    """Handles ticker name alignment based on FIGI groups"""

    def __init__(self, mapped_tickers: pl.LazyFrame):
        self.mapped_tickers = mapped_tickers

    def align_tickers_list(self, tickers: pl.LazyFrame) -> pl.LazyFrame:
        """Align ticker list with mapped tickers"""
        return tickers.join(
            self.mapped_tickers.select(
                ["ticker", "tickers", "group_id", "latest_ticker", "all_delisted_utc"]
            ).filter(pl.col("group_id").is_not_null()),
            on="ticker",
            how="left",
        )

    def align_splits_data(self, df: pl.DataFrame) -> pl.LazyFrame:
        """Align splits data with latest ticker names"""
        return (
            df.lazy()
            .join(
                self.mapped_tickers.select(["ticker", "group_id", "latest_ticker"]),
                on="ticker",
                how="left",
            )
            .drop("ticker")
            .rename({"latest_ticker": "ticker"})
            .drop("group_id")
        )

    def align_ohlcv_data(self, lf: pl.LazyFrame) -> pl.DataFrame:
        """
        Align OHLCV data handling ticker name changes over time.
        Groups tickers by FIGI and handles multi-ticker scenarios.
        """
        # Join with mapped tickers
        lf = lf.join(
            self.mapped_tickers.select(
                [
                    "ticker",
                    "group_id",
                    "latest_ticker",
                    "tickers",
                    "all_last_updated_utc",
                    "all_delisted_utc",
                ]
            ),
            on="ticker",
            how="left",
        )

        # Separate single and multi-ticker groups
        multi_ticker_groups = self._identify_multi_ticker_groups(lf)
        single_ticker_data = lf.join(multi_ticker_groups, on="group_id", how="anti")
        multi_ticker_data = lf.join(multi_ticker_groups, on="group_id", how="semi")

        # Process each group type
        processed_single = self._process_single_ticker_data(single_ticker_data)
        processed_multi = self._process_multi_ticker_data(multi_ticker_data)

        # Combine results
        return self._combine_processed_data(processed_single, processed_multi, lf)

    @staticmethod
    def _identify_multi_ticker_groups(lf: pl.LazyFrame) -> pl.LazyFrame:
        """Identify groups with multiple tickers"""
        return (
            lf.group_by("group_id")
            .agg(
                [
                    pl.col("ticker").n_unique().alias("ticker_count"),
                    pl.col("ticker").unique().alias("unique_tickers"),
                ]
            )
            .filter(pl.col("ticker_count") > 1)
            .select("group_id")
        )

    @staticmethod
    def _process_single_ticker_data(lf: pl.LazyFrame) -> pl.DataFrame:
        """Process groups with single ticker"""
        collected = lf.collect()
        if collected.height > 0:
            return (
                collected.with_columns(pl.col("latest_ticker").alias("new_ticker"))
                .drop("ticker")
                .rename({"new_ticker": "ticker"})
            )
        return pl.DataFrame(schema=collected.schema)

    def _process_multi_ticker_data(self, lf: pl.LazyFrame) -> pl.DataFrame:
        """Process groups with multiple tickers (name changes)"""
        collected = lf.collect()
        if collected.height == 0:
            return pl.DataFrame(schema=collected.schema)

        processed_groups = []
        for group_id in collected.select("group_id").unique()["group_id"]:
            group_data = collected.filter(pl.col("group_id") == group_id)
            processed = self._process_ticker_group(group_data)
            if processed.height > 0:
                processed_groups.append(processed)

        if processed_groups:
            return pl.concat(processed_groups)
        return pl.DataFrame(schema=collected.schema)

    @staticmethod
    def _process_ticker_group(group_df: pl.DataFrame) -> pl.DataFrame:
        """Process a single multi-ticker group with temporal cutoffs"""
        first_row = group_df.row(0, named=True)
        ticker_order = first_row["tickers"]
        last_updated_list = first_row["all_last_updated_utc"]
        delisted_list = first_row["all_delisted_utc"]

        processed_data = []
        last_end_date = None

        for i, ticker in enumerate(ticker_order):
            ticker_data = group_df.filter(pl.col("ticker") == ticker).sort("timestamps")

            if ticker_data.height == 0:
                continue

            # Determine cutoff date
            cutoff = TickerAligner._determine_cutoff(
                last_updated_list[i], delisted_list[i]
            )

            # Apply temporal filters
            ticker_data = TickerAligner._apply_temporal_filters(
                ticker_data, last_end_date, cutoff
            )

            if ticker_data.height > 0:
                processed_data.append(ticker_data)
                last_end_date = ticker_data.select(
                    pl.col("timestamps").dt.date().max()
                ).item()

        if processed_data:
            combined = pl.concat(processed_data)
            latest_ticker = group_df["latest_ticker"][0]
            return combined.with_columns(pl.lit(latest_ticker).alias("ticker"))

        return pl.DataFrame(schema=group_df.schema)

    @staticmethod
    def _determine_cutoff(last_updated: str, delisted: str) -> Optional[datetime.date]:
        """Determine the cutoff date for ticker data"""
        cutoff_candidates = [d for d in (last_updated, delisted) if d is not None]
        if not cutoff_candidates:
            return None

        cutoff_str = min(cutoff_candidates)
        return datetime.fromisoformat(cutoff_str.replace("Z", "+00:00")).date()

    @staticmethod
    def _apply_temporal_filters(
        data: pl.DataFrame,
        last_end_date: Optional[datetime.date],
        cutoff: Optional[datetime.date],
    ) -> pl.DataFrame:
        """Apply temporal filters to avoid overlapping data"""
        if last_end_date is not None:
            data = data.filter(pl.col("timestamps").dt.date() > last_end_date)

        if cutoff is not None:
            data = data.filter(pl.col("timestamps").dt.date() <= cutoff)

        return data

    @staticmethod
    def _combine_processed_data(
        single: pl.DataFrame, multi: pl.DataFrame, original_lf: pl.LazyFrame
    ) -> pl.DataFrame:
        """Combine single and multi-ticker processed data"""
        if single.height > 0 and multi.height > 0:
            common_columns = [col for col in single.columns if col in multi.columns]
            final_data = pl.concat(
                [single.select(common_columns), multi.select(common_columns)]
            )
        elif multi.height > 0:
            final_data = multi
        elif single.height > 0:
            final_data = single
        else:
            return pl.DataFrame(schema=original_lf.collect_schema())

        # Remove metadata columns
        columns_to_keep = [
            col
            for col in final_data.columns
            if col
            not in [
                "group_id",
                "latest_ticker",
                "tickers",
                "all_last_updated_utc",
                "all_delisted_utc",
            ]
        ]

        return final_data.select(columns_to_keep).sort(["ticker", "timestamps"])


class CacheManager:
    """Manages caching of processed data"""

    def __init__(self, base_dir: str = data_dir):
        self.base_dir = Path(base_dir)

    @staticmethod
    def generate_key(
        tickers: Optional[List[str]],
        timeframe: str,
        asset: str,
        data_type: str,
        start_date: str,
        end_date: str,
        full_hour: bool,
    ) -> str:
        """Generate unique cache key from parameters"""
        cache_params = {
            "tickers": sorted([t for t in tickers if t]) if tickers else None,
            "timeframe": timeframe,
            "asset": asset,
            "data_type": data_type,
            "start_date": start_date,
            "end_date": end_date,
            "full_hour": full_hour,
        }
        params_str = json.dumps(cache_params, sort_keys=True, default=str)
        return hashlib.md5(params_str.encode()).hexdigest()

    def get_cache_path(self, asset: str, data_type: str, cache_key: str) -> Path:
        """Get cache file path"""
        cache_dir = self.base_dir / "processed" / asset / data_type
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"cache_{cache_key}.parquet"

    def load(self, cache_path: Path) -> Optional[pl.LazyFrame]:
        """Load data from cache"""
        if not cache_path.exists():
            return None

        try:
            print(f"Loading from cache: {cache_path}")
            cached_data = pl.scan_parquet(cache_path)
            data = cached_data.collect()
            print(
                f"Cache loaded: {len(data):,} rows, {data.estimated_size('mb'):.2f} MB"
            )
            return cached_data
        except Exception as e:
            print(f"Failed to load cache: {e}")
            return None

    def save(
        self, data: pl.LazyFrame, cache_path: Path, metadata: Dict[str, Any]
    ) -> Optional[pl.LazyFrame]:
        """Save data to cache with metadata"""
        try:
            print(f"Saving to cache: {cache_path}")
            collected = data.collect()
            collected.write_parquet(cache_path)

            print(
                f"Cache saved: {len(collected):,} rows, {collected.estimated_size('mb'):.2f} MB"
            )

            # Save metadata
            metadata["created_at"] = datetime.now().isoformat()
            metadata_path = cache_path.with_suffix(".json")
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2, default=str)

            return pl.scan_parquet(cache_path)
        except Exception as e:
            print(f"Failed to save cache: {e}")
            return None


class RawDataLoader:
    """Loads raw data from various sources"""

    def __init__(self):
        self.access_key = ACCESS_KEY_ID
        self.secret_key = SECRET_ACCESS_KEY

    def load(
        self,
        asset: str,
        data_type: str,
        start_date: str,
        end_date: str,
        lake: bool = True,
        use_s3: bool = False,
        use_duck_db: bool = False,
    ) -> pl.LazyFrame:
        """Load raw data using appropriate method"""
        path_fetcher = DataPathFetcher(
            asset, data_type, start_date, end_date, lake, use_s3
        )
        paths = path_fetcher.data_dir_calculate()
        print(f"Loading data from {len(paths)} paths")

        if use_duck_db:
            return self._load_with_duckdb(paths, use_s3)
        else:
            return self._load_with_polars(paths, use_s3)

    def _load_with_polars(self, paths: List[str], use_s3: bool) -> pl.LazyFrame:
        """Load data using Polars"""
        if all(f.endswith(".parquet") for f in paths):
            return pl.scan_parquet(paths)

        if use_s3:
            return pl.scan_csv(
                paths,
                storage_options={
                    "aws_access_key_id": self.access_key,
                    "aws_secret_access_key": self.secret_key,
                    "aws_endpoint": "https://files.polygon.io",
                    "aws_region": "us-east-1",
                },
            )

        return pl.scan_csv(paths)

    def _load_with_duckdb(self, paths: List[str], use_s3: bool) -> pl.LazyFrame:
        """Load data using DuckDB"""
        con = duckdb.connect()

        if use_s3:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute("SET s3_region='us-east-1';")
            con.execute("SET s3_endpoint='files.polygon.io';")
            # Escape single quotes in credentials to prevent SQL injection
            safe_access = self.access_key.replace("'", "''")
            safe_secret = self.secret_key.replace("'", "''")
            con.execute(f"SET s3_access_key_id='{safe_access}';")
            con.execute(f"SET s3_secret_access_key='{safe_secret}';")
            con.execute("SET s3_url_style='path';")
            query = f"SELECT * FROM read_csv_auto({paths})"
        else:
            paths_str = "['" + "', '".join(paths) + "']"
            query = f"SELECT * FROM read_parquet({paths_str})"

        return pl.DataFrame(con.execute(query).fetchdf()).lazy()


class StockDataLoader:
    """Main class for loading and processing stock data"""

    def __init__(self, mapped_tickers: Optional[pl.LazyFrame] = None):
        self.mapped_tickers = mapped_tickers or get_mapped_tickers().lazy()
        self.timestamp_gen = TimestampGenerator()
        self.resampler = OHLCVResampler()
        self.splits_adjuster = SplitsAdjuster()
        self.ticker_aligner = TickerAligner(self.mapped_tickers)
        self.cache_manager = CacheManager()
        self.raw_loader = RawDataLoader()

    def load(self, config: LoaderConfig) -> pl.LazyFrame:
        """
        Main entry point for loading stock data.

        Args:
            config: LoaderConfig with all parameters

        Returns:
            LazyFrame with processed OHLCV data
        """
        # Resolve date range
        start_date, end_date = self._resolve_dates(config)

        # Check cache
        if config.use_cache:
            cached = self._try_load_cache(config, start_date, end_date)
            if cached is not None:
                return cached

        print("Processing data from source...")

        # Load and process data
        lf = self._load_and_prepare_data(config, start_date, end_date)
        lf = self._align_and_adjust(lf, config)
        lf = self._fill_missing_and_resample(lf, config, start_date, end_date)

        # Save to cache
        if config.use_cache:
            return self._save_to_cache(lf, config, start_date, end_date)

        return lf

    def _resolve_dates(self, config: LoaderConfig) -> Tuple[str, str]:
        """Resolve start and end dates"""
        if config.end_date is None and config.timedelta:
            start, end = resolve_date_range(
                start_date=config.start_date, timedelta=config.timedelta
            )
            print(f"Date range: {start} → {end}")
            return start, end
        return config.start_date, config.end_date or config.start_date

    def _try_load_cache(
        self, config: LoaderConfig, start_date: str, end_date: str
    ) -> Optional[pl.LazyFrame]:
        """Try to load data from cache"""
        cache_key = CacheManager.generate_key(
            config.tickers,
            config.timeframe,
            config.asset,
            config.data_type,
            start_date,
            end_date,
            config.full_hour,
        )
        cache_path = self.cache_manager.get_cache_path(
            config.asset, config.data_type, cache_key
        )
        return self.cache_manager.load(cache_path)

    def _load_and_prepare_data(
        self, config: LoaderConfig, start_date: str, end_date: str
    ) -> pl.LazyFrame:
        """Load raw data and prepare tickers"""
        # Load raw data
        lf = self.raw_loader.load(
            config.asset,
            config.data_type,
            start_date,
            end_date,
            config.lake,
            config.use_s3,
            config.use_duck_db,
        )

        # Convert timestamps
        lf = lf.with_columns(
            pl.from_epoch(pl.col("window_start"), time_unit="ns")
            .dt.convert_time_zone("America/New_York")
            .alias("timestamps")
        ).sort("ticker", "timestamps")

        # Prepare ticker list
        tickers = self._prepare_tickers(lf, config)
        return lf.filter(pl.col("ticker").is_in(tickers))

    def _prepare_tickers(self, lf: pl.LazyFrame, config: LoaderConfig) -> List[str]:
        """Prepare and filter ticker list"""
        # Get all tickers if none specified
        if config.tickers is None:
            tickers = lf.select("ticker").unique().collect()["ticker"].to_list()
            print(f"Loading all tickers: {len(tickers)}")
        else:
            tickers = config.tickers

        # Align tickers
        aligned_tickers = self.ticker_aligner.align_tickers_list(
            pl.DataFrame({"ticker": tickers}).lazy()
        )

        print(
            f"Aligned groups: {aligned_tickers.select('ticker').unique().collect().n_unique()}"
        )

        # Skip low volume tickers if requested
        if config.skip_low_volume:
            aligned_tickers = self._filter_low_volume(aligned_tickers)

        # Extract final ticker list
        tickers = (
            aligned_tickers.select("tickers")
            .collect()
            .to_series()
            .explode()
            .unique()
            .to_list()
        )

        if not tickers:
            raise ValueError("No tickers remaining after filtering")

        print(f"Final ticker count: {len(tickers)}")
        return tickers

    @staticmethod
    def _filter_low_volume(aligned_tickers: pl.LazyFrame) -> pl.LazyFrame:
        """Filter out low volume tickers"""
        from config import low_volume_tickers_csv

        try:
            skipped = (
                pl.read_csv(low_volume_tickers_csv, truncate_ragged_lines=True)
                .filter(
                    (pl.col("max_duration_days") > 50)
                    | (pl.col("avg_turnover") < 60000)
                )
                .select(pl.col("ticker").unique())
            )
            skipped_aligned = TickerAligner(
                get_mapped_tickers().lazy()
            ).align_tickers_list(skipped.lazy())

            result = aligned_tickers.join(skipped_aligned, on="ticker", how="anti")
            print(f"Filtered {len(skipped)} low volume tickers")
            return result
        except Exception as e:
            print(f"Could not filter low volume tickers: {e}")
            return aligned_tickers

    def _align_and_adjust(self, lf: pl.LazyFrame, config: LoaderConfig) -> pl.LazyFrame:
        """Apply ticker alignment and split adjustments"""
        print("Aligning tickers...")
        lf = self.ticker_aligner.align_ohlcv_data(lf).lazy()

        print("Adjusting for splits...")
        splits_aligned = self.ticker_aligner.align_splits_data(get_splits_data())
        lf = self.splits_adjuster.adjust(lf, splits_aligned.collect())

        return lf

    def _fill_missing_and_resample(
        self, lf: pl.LazyFrame, config: LoaderConfig, start_date: str, end_date: str
    ) -> pl.LazyFrame:
        """Fill missing timestamps and resample if needed"""
        # Determine if daily or intraday
        match = re.match(r"(\d+)(mo|[mhdwqy])", config.timeframe.lower())
        if not match:
            raise ValueError(f"Invalid timeframe: {config.timeframe}")

        value, unit = int(match.group(1)), match.group(2)
        is_daily = unit in ["d", "w", "mo", "q", "y"]

        # Generate full timestamp range for each ticker
        print("Generating timestamp ranges...")
        time_range_lf = self._generate_ticker_timestamps(
            lf, config.timeframe, config.full_hour, is_daily
        )

        # Fill missing data with forward fill
        print("Filling missing timestamps...")
        lf_full = self._forward_fill_missing(lf, time_range_lf)

        # Resample if needed
        if config.timeframe not in ("1m", "1d"):
            print(f"Resampling to {config.timeframe}...")
            lf_full = self.resampler.resample(lf_full, config.timeframe)

        return lf_full

    def _generate_ticker_timestamps(
        self, lf: pl.LazyFrame, timeframe: str, full_hour: bool, is_daily: bool
    ) -> pl.LazyFrame:
        """Generate complete timestamp range for each ticker"""
        ticker_ranges = (
            lf.group_by("ticker")
            .agg(
                [
                    pl.col("timestamps").min().alias("first_trade_time"),
                    pl.col("timestamps").max().alias("last_trade_time"),
                ]
            )
            .collect()
        )

        all_ranges = []
        base_timeframe = "1d" if is_daily else "1m"

        for row in ticker_ranges.iter_rows(named=True):
            ticker = row["ticker"]
            start = row["first_trade_time"].strftime("%Y-%m-%d")
            end = row["last_trade_time"].strftime("%Y-%m-%d")

            timestamps = self.timestamp_gen.generate(
                start, end, base_timeframe, full_hour
            )

            ticker_range = pl.DataFrame(
                {
                    "ticker": [ticker] * len(timestamps),
                    "timestamps": timestamps["timestamps"].to_list(),
                }
            )
            all_ranges.append(ticker_range)

        return (
            pl.concat(all_ranges)
            .with_columns(pl.col("timestamps").dt.cast_time_unit("ns"))
            .lazy()
        )

    @staticmethod
    def _forward_fill_missing(
        lf: pl.LazyFrame, time_range_lf: pl.LazyFrame
    ) -> pl.LazyFrame:
        """Forward fill missing OHLCV data"""
        return (
            time_range_lf.join(lf, on=["ticker", "timestamps"], how="left")
            .with_columns(pl.col("close").forward_fill().alias("close_filled"))
            .with_columns(
                [
                    pl.when(pl.col("open").is_not_null())
                    .then(pl.col("open"))
                    .otherwise(pl.col("close_filled"))
                    .alias("open"),
                    pl.when(pl.col("high").is_not_null())
                    .then(pl.col("high"))
                    .otherwise(pl.col("close_filled"))
                    .alias("high"),
                    pl.when(pl.col("low").is_not_null())
                    .then(pl.col("low"))
                    .otherwise(pl.col("close_filled"))
                    .alias("low"),
                    pl.when(pl.col("close").is_not_null())
                    .then(pl.col("close"))
                    .otherwise(pl.col("close_filled"))
                    .alias("close"),
                    pl.col("volume").fill_null(0),
                    pl.col("transactions").fill_null(0),
                ]
            )
            .drop("close_filled")
        )

    def _save_to_cache(
        self, lf: pl.LazyFrame, config: LoaderConfig, start_date: str, end_date: str
    ) -> pl.LazyFrame:
        """Save processed data to cache"""
        cache_key = CacheManager.generate_key(
            config.tickers,
            config.timeframe,
            config.asset,
            config.data_type,
            start_date,
            end_date,
            config.full_hour,
        )
        cache_path = self.cache_manager.get_cache_path(
            config.asset, config.data_type, cache_key
        )

        metadata = {
            "tickers": config.tickers,
            "timeframe": config.timeframe,
            "asset": config.asset,
            "data_type": config.data_type,
            "start_date": start_date,
            "end_date": end_date,
            "full_hour": config.full_hour,
            "cache_key": cache_key,
        }

        result = self.cache_manager.save(lf, cache_path, metadata)
        return result if result is not None else lf


# Convenience function for backward compatibility
def stock_load_process(
    tickers: Optional[List[str]] = None,
    start_date: str = "",
    end_date: Optional[str] = None,
    timedelta: Optional[int] = None,
    timeframe: str = "1d",
    asset: str = "us_stocks_sip",
    data_type: str = "day_aggs_v1",
    full_hour: bool = False,
    lake: bool = True,
    use_s3: bool = False,
    use_cache: bool = True,
    use_duck_db: bool = False,
    skip_low_volume: bool = True,
) -> pl.LazyFrame:
    """
    Load and process stock OHLCV data with split adjustments and ticker alignment.

    Args:
        tickers: List of ticker symbols (None = all tickers)
        start_date: Start date 'YYYY-MM-DD'
        end_date: End date 'YYYY-MM-DD' (optional if timedelta provided)
        timedelta: Number of days from start_date (optional)
        timeframe: '1m', '5m', '15m', '30m', '1h', '1d', etc.
        asset: Asset type
        data_type: Data type identifier
        full_hour: Include pre/post market (4:00-20:00) for intraday
        lake: Use data lake
        use_s3: Load from S3
        use_cache: Enable caching
        use_duck_db: Use DuckDB for loading
        skip_low_volume: Filter low volume tickers

    Returns:
        LazyFrame with processed OHLCV data
    """
    config = LoaderConfig(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        timedelta=timedelta,
        timeframe=timeframe,
        asset=asset,
        data_type=data_type,
        full_hour=full_hour,
        lake=lake,
        use_s3=use_s3,
        use_cache=use_cache,
        use_duck_db=use_duck_db,
        skip_low_volume=skip_low_volume,
    )

    loader = StockDataLoader()
    return loader.load(config)


if __name__ == "__main__":
    # Example usage
    tickers = ["NVDA"]
    plot = True
    ticker_plot = tickers[0]
    timeframe = "1d"
    result = stock_load_process(
        tickers=tickers,
        start_date="2026-01-31",
        end_date="2026-02-07",
        timeframe=timeframe,
        use_cache=True,
        skip_low_volume=False,
    )

    print(result.collect())

    # from visualizer.plotter import plot_candlestick

    # if plot:
    #     plot_candlestick(
    #         result.filter(pl.col("ticker") == ticker_plot).collect().to_pandas(),
    #         ticker_plot,
    #         timeframe,
    #     )
