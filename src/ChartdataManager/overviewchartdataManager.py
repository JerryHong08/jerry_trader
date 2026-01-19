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

    # Map backend states to frontend states
    # Since stateEngine is in development, we use simple mapping
    STATE_MAPPING: Dict[str, str] = {
        "new_entrant": "Best",
        "rising_fast": "Best",
        "rising": "Good",
        "stable": "OnWatch",
        "falling": "NotGood",
        "falling_fast": "Bad",
        # Direct mappings for when backend sends new state values
        "Best": "Best",
        "Good": "Good",
        "OnWatch": "OnWatch",
        "NotGood": "NotGood",
        "Bad": "Bad",
    }

    def __init__(
        self,
        replay_date: Optional[str] = None,
        suffix_id: Optional[str] = None,
    ):
        self._chart_data_dirty = True
        self._cached_chart_data: Optional[Dict] = None
        self._cached_rank_data: Optional[List[Dict]] = None
        self._last_query_time: Optional[datetime] = None

        self.run_mode = "replay" if replay_date else "live"
        self.db_date = (
            replay_date
            if replay_date
            else datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
        )
        self.db_id = self._derive_db_id(self.db_date, suffix_id)

        # ------- Redis Configuration -------
        self.redis_client = redis.Redis(
            host="localhost", port=6379, db=0, decode_responses=True
        )

        self.STREAM_NAME = f"market_snapshot_processed:{self.db_date}"
        self.SUBSCRIBED_SET_NAME = f"movers_subscribed_set:{self.db_date}"
        self.HSET_NAME = f"state_cursor:{self.db_date}"

        # ------- InfluxDB Configuration -------
        token = os.environ.get("INFLUXDB_TOKEN")
        self.org = "jerryhong"
        self.bucket = "jerrymmm"
        url = "http://localhost:8086"

        self._influx_client = influxdb_client.InfluxDBClient(
            url=url, token=token, org=self.org
        )
        self._query_api = self._influx_client.query_api()

        logger.info(
            f"GridTraderChartDataManager initialized: mode={self.run_mode}, db_id={self.db_id}"
        )

    @staticmethod
    def _derive_db_id(db_date: Optional[str], override: Optional[str]) -> str:
        if db_date and override:
            return f"{db_date}_{override}"
        if override:
            return override
        if db_date:
            return db_date
        return "na"

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
            year = int(self.db_date[:4])
            month = int(self.db_date[4:6])
            day = int(self.db_date[6:8])
            # Create datetime in NY timezone to get correct offset for that date
            start_dt = datetime(year, month, day, 4, 0, 0, tzinfo=ny_tz)
            end_dt = datetime(year, month, day, 16, 0, 0, tzinfo=ny_tz)
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
        """Get all subscribed tickers from Redis Set."""
        return list(self.redis_client.smembers(self.SUBSCRIBED_SET_NAME))

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

    def _map_state_to_frontend(self, backend_state: str) -> str:
        """Map backend state to frontend state."""
        return self.STATE_MAPPING.get(backend_state, "OnWatch")

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
                else f"{self.db_date[:4]}-{self.db_date[4:6]}-{self.db_date[6:8]}T23:59:59Z"
            )
        else:
            range_start, range_end = self._get_intraday_time_range()

        query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {range_start}, stop: {range_end})
            |> filter(fn: (r) => r["_measurement"] == "market_snapshot")
            |> filter(fn: (r) => r["symbol"] == "{ticker}")
            |> filter(fn: (r) => r["run_mode"] == "{self.run_mode}")
            |> filter(fn: (r) => r["db_id"] == "{self.db_id}")
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
            |> filter(fn: (r) => r["run_mode"] == "{self.run_mode}")
            |> filter(fn: (r) => r["db_id"] == "{self.db_id}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"])
        """

        try:
            tables = self._query_api.query(query, org=self.org)
            results = []
            for table in tables:
                for record in table.records:
                    backend_state = record.values.get("state", "stable")
                    frontend_state = self._map_state_to_frontend(backend_state)
                    state_reason = record.values.get("stateReason", "")
                    results.append(
                        {
                            "timestamp": record.get_time(),
                            "state": frontend_state,
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
        result = []
        for item in tickers_data:
            # Get the latest state and stateReason from state cursor
            symbol = item.get("symbol", "")
            state_key = f"{symbol}_state"
            state_reason_key = f"{symbol}_stateReason"
            backend_state = (
                self.redis_client.hget(self.HSET_NAME, state_key) or "stable"
            )
            state_reason = (
                self.redis_client.hget(self.HSET_NAME, state_reason_key) or ""
            )
            frontend_state = self._map_state_to_frontend(backend_state)

            result.append(
                {
                    "symbol": symbol,
                    "price": item.get("price", 0.0),
                    "change": item.get("change", 0.0),
                    "changePercent": item.get("changePercent", 0.0),
                    "volume": item.get("volume", 0),
                    "marketCap": item.get("marketCap", 0),
                    "state": frontend_state,
                    "stateReason": state_reason,
                    "float": item.get("float_share", 0),
                    "relativeVolumeDaily": item.get("relativeVolumeDaily", 1.0),
                    "relativeVolume5min": item.get("relativeVolume5min", 1.0),
                    "latestNewsTime": item.get("latestNewsTime"),  # May be None
                    "rank": item.get("rank", 999),
                }
            )

        # Sort by rank
        result.sort(key=lambda x: x.get("rank", 999))
        return result, timestamp

    def get_overview_chart_data(
        self,
        force_refresh: bool = False,
        top_n: int = 20,
        interval_minutes: int = 5,
    ) -> Dict[str, Any]:
        """
        Get data formatted for GridTrader's OverviewChartModule.

        Returns:
            {
                "data": [...time series data points...],
                "segmentInfo": {
                    "AAPL": [{"key": "AAPL_seg0", "color": "#10b981", "startIdx": 0, "endIdx": 10}, ...],
                    ...
                },
                "rankData": [...RankItem data...],
                "timestamp": "..."
            }

        Each data point contains:
            - date: time label (e.g., "9:30")
            - timestamp: epoch milliseconds
            - {symbol}_value: percent change value
            - {symbol}_state: state at that time
            - {symbol}_seg0, {symbol}_seg1, ...: segment values (null when not in segment)
        """
        # Return cached data if not dirty and not forced
        if (
            not force_refresh
            and not self._chart_data_dirty
            and self._cached_chart_data is not None
        ):
            return self._cached_chart_data

        # Get top N tickers
        tickers = self.get_top_n_tickers(n=top_n)
        if not tickers:
            return {"data": [], "segmentInfo": {}, "rankData": [], "timestamp": None}

        # Collect all data for each ticker
        ticker_histories: Dict[str, List[Dict]] = {}
        ticker_state_histories: Dict[str, List[Dict]] = {}

        for ticker in tickers:
            ticker_histories[ticker] = self.get_ticker_history(ticker)
            ticker_state_histories[ticker] = self.get_ticker_state_history(ticker)

        # Build unified time series
        all_timestamps = set()
        for history in ticker_histories.values():
            for point in history:
                all_timestamps.add(point["timestamp"])

        if not all_timestamps:
            return {"data": [], "segmentInfo": {}, "rankData": [], "timestamp": None}

        sorted_timestamps = sorted(all_timestamps)

        # Build data points
        data = []
        for ts in sorted_timestamps:
            # Format time label
            ts_local = ts.astimezone(ZoneInfo("America/New_York"))
            time_label = ts_local.strftime("%H:%M")

            data_point: Dict[str, Any] = {
                "date": time_label,
                "timestamp": int(ts.timestamp() * 1000),  # Epoch milliseconds
            }

            # Add data for each ticker
            for ticker in tickers:
                history = ticker_histories.get(ticker, [])
                state_history = ticker_state_histories.get(ticker, [])

                # Find the value at this timestamp (or interpolate)
                value = None
                for point in history:
                    if point["timestamp"] == ts:
                        value = point["changePercent"]
                        break

                # Find the state and stateReason at this timestamp
                current_state = "OnWatch"  # Default
                current_state_reason = ""
                for sh in state_history:
                    if sh["timestamp"] <= ts:
                        current_state = sh["state"]
                        current_state_reason = sh.get("stateReason", "")
                    else:
                        break

                data_point[f"{ticker}_value"] = value
                data_point[f"{ticker}_state"] = current_state
                data_point[f"{ticker}_stateReason"] = current_state_reason

            data.append(data_point)

        # Build segment info for each ticker
        segment_info: Dict[str, List[Dict]] = {}

        for ticker in tickers:
            segments: List[Dict] = []
            current_state: Optional[str] = None
            segment_start = 0
            segment_index = 0

            for idx, point in enumerate(data):
                point_state = point.get(f"{ticker}_state")
                point_value = point.get(f"{ticker}_value")

                if point_value is None:
                    continue

                if point_state != current_state:
                    if current_state is not None:
                        # Close previous segment
                        segment_key = f"{ticker}_seg{segment_index}"
                        segments.append(
                            {
                                "key": segment_key,
                                "color": self.STATE_COLORS.get(
                                    current_state, "#6b7280"
                                ),
                                "startIdx": segment_start,
                                "endIdx": idx,
                            }
                        )
                        segment_index += 1

                    # Start new segment
                    segment_start = idx
                    current_state = point_state

            # Close final segment
            if current_state is not None:
                segment_key = f"{ticker}_seg{segment_index}"
                segments.append(
                    {
                        "key": segment_key,
                        "color": self.STATE_COLORS.get(current_state, "#6b7280"),
                        "startIdx": segment_start,
                        "endIdx": len(data) - 1,
                    }
                )

            segment_info[ticker] = segments

        # Add segment values to data points
        for ticker, segments in segment_info.items():
            for segment in segments:
                for idx, point in enumerate(data):
                    if idx >= segment["startIdx"] and idx <= segment["endIdx"]:
                        point[segment["key"]] = point.get(f"{ticker}_value")
                    else:
                        point[segment["key"]] = None

        # Get rank data for legend/display (returns tuple of data and timestamp)
        rank_data, snapshot_timestamp = self.get_latest_rank_data()

        # Build result - use snapshot timestamp from data, not machine time
        result = {
            "data": data,
            "segmentInfo": segment_info,
            "rankData": rank_data,
            "timestamp": snapshot_timestamp,  # Use the actual snapshot timestamp
        }

        # Cache the result
        self._cached_chart_data = result
        self._chart_data_dirty = False
        self._last_query_time = datetime.now(ZoneInfo("America/New_York"))

        logger.debug(
            f"Overview chart data generated: {len(tickers)} tickers, "
            f"{len(data)} data points, timestamp={result['timestamp']}"
        )

        return result

    def close(self):
        """Clean up resources."""
        if self._influx_client:
            self._influx_client.close()
        logger.info("GridTraderChartDataManager closed")
