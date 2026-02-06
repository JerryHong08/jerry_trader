"""
Overview Chart Data Manager for GridTrader Frontend

This module manages chart data for the GridTrader frontend's OverviewChartModule.
Unlike the MMM version which uses a single color per ticker, GridTrader displays
each ticker's line with segmented colors representing different states (regimes).

Data Flow:
    InfluxDB (market_snapshot + movers_state) -> ChartDataManager -> BFF -> Frontend

Key Differences from MMM Version:
    - Each ticker line is segmented by state transitions
    - Each segment has its own data key (e.g., AAPL_seg0, AAPL_seg1)
    - Segments are colored based on the state at that time period
    - Frontend uses Recharts with multiple Line components per ticker
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import influxdb_client
import redis

from utils.logger import setup_logger
from utils.session import db_date_to_date, make_session_id, parse_session_id

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class GridTraderChartDataManager:
    """
    Manage and format chart data for GridTrader's OverviewChartModule.

    The key difference from MMM version:
    - GridTrader renders each ticker as multiple line segments
    - Each segment corresponds to a state period
    - Segments are colored by state (Best, Good, OnWatch, NotGood, Bad)
    """

    # State colors matching frontend STATE_COLORS
    STATE_COLORS: Dict[str, str] = {
        "Best": "#10b981",  # Green
        "Good": "#34d399",  # Emerald
        "OnWatch": "#3b82f6",  # Blue
        "NotGood": "#eab308",  # Yellow
        "Bad": "#6b7280",  # Gray
    }

    # Valid state values (backend now emits these directly)
    VALID_STATES = {"Best", "Good", "OnWatch", "NotGood", "Bad"}

    def __init__(
        self,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
    ):
        self._chart_data_dirty = True
        self._cached_chart_data_lw: Optional[Dict] = None
        self._last_query_time: Optional[datetime] = None

        # Unified session id — single source of truth for mode & date
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # ------- Redis Configuration -------
        redis_cfg = redis_config or {}
        redis_host = redis_cfg.get("host", "127.0.0.1")
        redis_port = redis_cfg.get("port", 6379)
        redis_db = redis_cfg.get("db", 0)

        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db, decode_responses=True
        )

        self.STREAM_NAME = f"market_snapshot_processed:{self.session_id}"
        self.SUBSCRIBED_SET_NAME = f"movers_subscribed_set:{self.session_id}"
        self.HSET_NAME = f"state_cursor:{self.session_id}"

        # ------- InfluxDB Configuration -------
        self.org = "jerryhong"
        influx_cfg = influxdb_config or {}
        self.bucket = influx_cfg.get("bucket", "jerryib_trade")

        # Token from env var
        influx_token_env = influx_cfg.get("influx_token_env")
        token = os.getenv(influx_token_env) if influx_token_env else None

        # URL from env var
        influx_url_env = influx_cfg.get("influx_url_env")
        self.influx_url = (
            os.getenv(influx_url_env) if influx_url_env else "http://localhost:8086"
        )
        logger.info(
            f"__init__ - Connecting to InfluxDB at {self.influx_url}, bucket={self.bucket}"
        )

        self._influx_client = influxdb_client.InfluxDBClient(
            url=self.influx_url, token=token, org=self.org
        )
        self._query_api = self._influx_client.query_api()

        logger.info(
            f"__init__ - GridTraderChartDataManager initialized: mode={self.run_mode}, session_id={self.session_id}"
        )

    def mark_dirty(self):
        """Mark chart data as needing refresh."""
        self._chart_data_dirty = True

    def _get_intraday_time_range(self) -> Tuple[str, str]:
        """
        Get the intraday time range for InfluxDB queries.
        Pre-market: 4:00 AM to 9:30 AM (330 minutes)
        Regular hours: 9:30 AM to 4:00 PM (390 minutes)
        """
        ny_tz = ZoneInfo("America/New_York")

        if self.run_mode == "replay":
            d = db_date_to_date(self.db_date)
            # Create datetime in NY timezone to get correct offset for that date
            start_dt = datetime(d.year, d.month, d.day, 4, 0, 0, tzinfo=ny_tz)
            end_dt = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=ny_tz)
            range_start = start_dt.isoformat()
            range_end = end_dt.isoformat()
        else:
            today = datetime.now(ny_tz).strftime("%Y-%m-%d")
            # Use ISO format with proper timezone for start
            start_dt = datetime.now(ny_tz).replace(
                hour=4, minute=0, second=0, microsecond=0
            )
            range_start = start_dt.isoformat()
            range_end = "now()"

        return range_start, range_end

    def get_subscribed_tickers(self) -> List[str]:
        """Get all subscribed tickers from Redis ZSET."""
        return list(self.redis_client.zrange(self.SUBSCRIBED_SET_NAME, 0, -1))

    def get_top_n_tickers(self, n: int = 20) -> List[str]:
        """
        Get top N tickers by rank from the last snapshot in Redis Stream.
        """
        entries = self.redis_client.xrevrange(self.STREAM_NAME, count=1)

        if not entries:
            return []

        entry_id, fields = entries[0]
        data_json = fields.get("data")
        if not data_json:
            return []

        try:
            tickers_data = json.loads(data_json)
        except json.JSONDecodeError:
            return []

        # Filter tickers with rank <= n and sort by rank
        current_membership = [
            (item["symbol"], item["rank"])
            for item in tickers_data
            if item.get("rank", 999) <= n
        ]
        current_membership.sort(key=lambda x: x[1])
        return [ticker for ticker, rank in current_membership]

    def get_ticker_history(
        self, ticker: str, since: Optional[datetime] = None, limit: int = 500
    ) -> List[Dict]:
        """
        Query historical snapshot data for a ticker from InfluxDB.
        Returns list of {timestamp, changePercent, price, volume, rank}
        """
        if since is not None:
            range_start = since.isoformat()
            range_end = (
                "now()"
                if self.run_mode != "replay"
                else f"{db_date_to_date(self.db_date).isoformat()}T23:59:59Z"
            )
        else:
            range_start, range_end = self._get_intraday_time_range()

        query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {range_start}, stop: {range_end})
            |> filter(fn: (r) => r["_measurement"] == "market_snapshot")
            |> filter(fn: (r) => r["symbol"] == "{ticker}")
            |> filter(fn: (r) => r["session_id"] == "{self.session_id}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        """

        try:
            tables = self._query_api.query(query, org=self.org)
            results = []
            for table in tables:
                for record in table.records:
                    results.append(
                        {
                            "timestamp": record.get_time(),
                            "changePercent": record.values.get("changePercent", 0.0),
                            "price": record.values.get("price", 0.0),
                            "volume": record.values.get("volume", 0),
                            "rank": record.values.get("rank", 0),
                        }
                    )
            return results
        except Exception as e:
            logger.error(f"Error querying history for {ticker}: {e}")
            return []

    def get_ticker_state_history(self, ticker: str) -> List[Dict]:
        """
        Query state history for a ticker from InfluxDB movers_state.
        Returns list of {timestamp, state, stateReason} sorted by timestamp.
        """
        range_start, range_end = self._get_intraday_time_range()

        query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {range_start}, stop: {range_end})
            |> filter(fn: (r) => r["_measurement"] == "movers_state")
            |> filter(fn: (r) => r["symbol"] == "{ticker}")
            |> filter(fn: (r) => r["session_id"] == "{self.session_id}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        """

        try:
            tables = self._query_api.query(query, org=self.org)
            results = []
            for table in tables:
                for record in table.records:
                    state = record.values.get("state", "OnWatch")
                    # Validate state is a valid frontend state
                    if state not in self.VALID_STATES:
                        state = "OnWatch"
                    state_reason = record.values.get("stateReason", "")
                    results.append(
                        {
                            "timestamp": record.get_time(),
                            "state": state,
                            "stateReason": state_reason,
                        }
                    )
            return results
        except Exception as e:
            logger.error(f"Error querying state history for {ticker}: {e}")
            return []

    def get_latest_rank_data(self) -> tuple[List[Dict], Optional[str]]:
        """
        Get latest rank list data from Redis stream.
        Returns tuple of (list matching frontend RankItem interface, timestamp string).

        Only emits columns that are in the stream message:
        ['symbol', 'rank', 'price', 'change', 'changePercent', 'volume', 'relativeVolume5min', 'relativeVolumeDaily', 'vwap'].

        State and other columns are updated separately by the state stream listener.
        """
        entries = self.redis_client.xrevrange(self.STREAM_NAME, count=1)

        if not entries:
            return [], None

        entry_id, fields = entries[0]
        data_json = fields.get("data")
        timestamp = fields.get(
            "timestamp"
        )  # This is the snapshot timestamp from backend

        if not data_json:
            return [], timestamp

        try:
            tickers_data = json.loads(data_json)
        except json.JSONDecodeError:
            return [], timestamp

        # Transform to frontend format
        # Only emit columns from stream message, frontend handles defaults for other columns
        result = []
        for item in tickers_data:
            result.append(
                {
                    "symbol": item.get("symbol", ""),
                    "rank": item.get("rank", 999),
                    "price": item.get("price", 0.0),
                    "change": item.get("change", 0.0),
                    "changePercent": item.get("changePercent", 0.0),
                    "volume": item.get("volume", 0),
                    "relativeVolume5min": item.get("relativeVolume5min", 1.0),
                    "relativeVolumeDaily": item.get("relativeVolumeDaily", 1.0),
                    "vwap": item.get("vwap", 0.0),
                }
            )

        # Sort by rank
        result.sort(key=lambda x: x.get("rank", 999))
        return result, timestamp

    def get_overview_chart_data_lw(
        self,
        force_refresh: bool = False,
        top_n: int = 20,
    ) -> Dict[str, Any]:
        """
        Get data formatted for Lightweight Charts OverviewChartModule.

        Backend sends top N tickers as a superset. Frontend controls visibility.

        Returns:
            {
                "seriesData": {
                    "AAPL": {
                        "data": [{"time": 1737300000, "value": 5.2}, ...],  # time in seconds
                        "states": [{"time": 1737300000, "state": "Best"}, ...]
                    },
                    ...
                },
                "rankData": [...RankItem data...],
                "timestamp": "..."
            }
        """
        # Return cached data if not dirty and not forced
        if (
            not force_refresh
            and not self._chart_data_dirty
            and self._cached_chart_data_lw is not None
        ):
            return self._cached_chart_data_lw

        # Get top N tickers from latest snapshot (backend sends superset)
        tickers = self.get_top_n_tickers(n=top_n)
        if not tickers:
            return {"seriesData": {}, "rankData": [], "timestamp": None}

        # Collect all data for each ticker
        series_data: Dict[str, Dict[str, List]] = {}

        for ticker in tickers:
            ticker_history = self.get_ticker_history(ticker)
            ticker_state_history = self.get_ticker_state_history(ticker)

            # Build per-ticker series data
            data_points: List[Dict] = []
            state_points: List[Dict] = []

            for point in ticker_history:
                ts = point["timestamp"]
                # Convert to seconds for Lightweight Charts (uses Unix timestamp in seconds)
                # Ensure plain Python int (not numpy.int64) for JSON serialization
                time_seconds = int(ts.timestamp())
                value = point.get("changePercent")

                if value is not None:
                    # Ensure value is a plain Python float for JSON serialization
                    value_float = float(value) if value is not None else 0.0
                    data_points.append({"time": time_seconds, "value": value_float})

                    # Find the state at this timestamp
                    current_state = "OnWatch"  # Default
                    for sh in ticker_state_history:
                        if sh["timestamp"] <= ts:
                            current_state = sh["state"]
                        else:
                            break
                    state_points.append({"time": time_seconds, "state": current_state})

            series_data[ticker] = {
                "data": data_points,
                "states": state_points,
            }

        # Get rank data for legend/display (returns tuple of data and timestamp)
        rank_data, snapshot_timestamp = self.get_latest_rank_data()

        result = {
            "seriesData": series_data,
            "rankData": rank_data,
            "timestamp": snapshot_timestamp,
        }

        # Cache the result
        self._cached_chart_data_lw = result
        self._chart_data_dirty = False
        self._last_query_time = datetime.now(ZoneInfo("America/New_York"))

        logger.debug(
            f"LW Overview chart data generated: {len(tickers)} tickers, "
            f"timestamp={result['timestamp']}"
        )

        return result

    def close(self):
        """Clean up resources."""
        if self._influx_client:
            self._influx_client.close()
        logger.info("GridTraderChartDataManager closed")
