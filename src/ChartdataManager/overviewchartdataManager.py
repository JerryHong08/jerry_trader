"""
Chart Data Manager for Market Mover Visualization
Reads data from InfluxDB and formats for frontend chart rendering.
Decoupled from SnapshotAnalyzer to separate data processing from presentation.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import influxdb_client
import redis

from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class ChartDataManager:
    """
    Manage and format chart data for visualization.
    Reads from InfluxDB (market_snapshot, movers_state) and Redis (subscribed tickers).
    """

    # Color schemes for different states
    STATE_COLORS = {
        "new_entrant": "255, 0, 0",  # Red
        "rising_fast": "255, 165, 0",  # Orange
        "rising": "50, 205, 50",  # Lime green
        "stable": "100, 149, 237",  # Cornflower blue
        "falling": "255, 215, 0",  # Gold
        "falling_fast": "178, 34, 34",  # Firebrick
    }

    def __init__(
        self, replay_date: Optional[str] = None, suffix_id: Optional[str] = None
    ):
        self._chart_data_dirty = True
        self._cached_chart_data = None
        self._last_query_time = None

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
            f"ChartDataManager initialized: mode={self.run_mode}, db_id={self.db_id}"
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

    def _get_intraday_time_range(self) -> tuple:
        """
        Get the intraday time range for InfluxDB queries.

        In replay mode: returns the replay date's start and end of day
        In live mode: returns today's start to now

        Returns:
            Tuple of (range_start, range_end) as InfluxDB time strings
        """
        if self.run_mode == "replay":
            # Replay mode: use specific date
            year = self.db_date[:4]
            month = self.db_date[4:6]
            day = self.db_date[6:8]
            range_start = f"{year}-{month}-{day}T00:00:00Z"
            range_end = f"{year}-{month}-{day}T23:59:59Z"
        else:
            # Live mode: use today
            today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
            range_start = f"{today}T00:00:00-05:00"
            range_end = "now()"

        return range_start, range_end

    def get_subscribed_tickers(self) -> List[str]:
        """Get all subscribed tickers from Redis Set."""
        return list(self.redis_client.smembers(self.SUBSCRIBED_SET_NAME))

    def get_top_n_tickers(self, n: int = 20) -> List[str]:
        """
        Get top N tickers by rank from the last snapshot in Redis Stream.
        Reads the most recent message and returns tickers with rank <= n.
        """
        import json

        # Read the last entry from stream
        entries = self.redis_client.xrevrange(self.STREAM_NAME, count=1)

        if not entries:
            return []

        # Parse the latest message
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

    def get_ticker_latest_state(self, ticker: str) -> Optional[Dict]:
        """Query the latest state for a ticker from InfluxDB movers_state."""
        range_start, range_end = self._get_intraday_time_range()

        query = f"""
        from(bucket: "{self.bucket}")
            |> range(start: {range_start}, stop: {range_end})
            |> filter(fn: (r) => r["_measurement"] == "movers_state")
            |> filter(fn: (r) => r["symbol"] == "{ticker}")
            |> filter(fn: (r) => r["run_mode"] == "{self.run_mode}")
            |> filter(fn: (r) => r["db_id"] == "{self.db_id}")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            |> sort(columns: ["_time"], desc: true)
            |> limit(n: 1)
        """
        try:
            tables = self._query_api.query(query, org=self.org)
            for table in tables:
                for record in table.records:
                    return {
                        "state": record.values.get("state", "unknown"),
                        "rank": int(record.values.get("rank", 0)),
                        "percent_change": float(
                            record.values.get("percent_change", 0.0)
                        ),
                        "rank_velocity": int(record.values.get("rank_velocity", 0)),
                        "timestamp": record.get_time(),
                    }
            return None
        except Exception as e:
            logger.error(f"Error querying state for {ticker}: {e}")
            return None

    def get_ticker_history(
        self, ticker: str, since: Optional[datetime] = None, limit: int = 500
    ) -> List[Dict]:
        """
        Query historical snapshot data for a ticker from InfluxDB.
        Uses intraday time range (single day focus).
        """
        if since is not None:
            range_start = since.isoformat()
            range_end = (
                "now()"
                if not self.run_mode == "replay"
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
        """
        # |> limit(n: {limit})

        try:
            tables = self._query_api.query(query, org=self.org)
            results = []
            for table in tables:
                for record in table.records:
                    results.append(
                        {
                            "timestamp": record.get_time(),
                            "percent_change": record.values.get("changePercent", 0.0),
                            "current_price": record.values.get("price", 0.0),
                            "accumulated_volume": record.values.get("volume", 0),
                            "rank": record.values.get("rank", 0),
                        }
                    )
            return results
        except Exception as e:
            logger.error(f"Error querying history for {ticker}: {e}")
            return []

    def get_mmm_version_chart_data(
        self, force_refresh: bool = False, top_n: int = 20
    ) -> Dict:
        """
        Get data formatted for MMM chart visualization.
        Reads from InfluxDB and formats for frontend consumption.

        Args:
            force_refresh: Force refresh even if cache is valid
            top_n: Number of top tickers to display (default: 20)

        Returns:
            Dict with 'datasets' and 'highlights' for Chart.js
        """
        # Return cached data if not dirty and not forced
        if (
            not force_refresh
            and not self._chart_data_dirty
            and self._cached_chart_data is not None
        ):
            return self._cached_chart_data

        chart_data = {"datasets": [], "highlights": []}

        # Get top N tickers by current percent_change (not all subscribed)
        subscribed_tickers = self.get_top_n_tickers(n=top_n)

        for ticker in subscribed_tickers:
            # Get latest state
            state = self.get_ticker_latest_state(ticker)
            if state is None:
                continue

            # Get historical data
            history = self.get_ticker_history(ticker)
            if not history:
                continue

            rank = state.get("rank", 999)
            state_name = state.get("state", "stable")
            rank_velocity = state.get("rank_velocity", 0)
            is_highlighted = state_name in ("new_entrant", "rising_fast", "rising")

            # Build data points
            data_points = [
                {"x": h["timestamp"].isoformat(), "y": h["percent_change"]}
                for h in history
            ]

            # Get colors based on state
            border_color = self._get_state_color(state_name, with_alpha=True)
            bg_color = self._get_state_color(state_name, with_alpha=False)

            dataset = {
                "label": ticker,
                "data": data_points,
                "borderColor": border_color,
                "backgroundColor": bg_color,
                "borderWidth": 3 if is_highlighted else 1,
                "pointRadius": 2,
                "tension": 0.1,
                "rank": rank,
                "alpha": 1.0,
                "highlight": is_highlighted,
                "velocity": rank_velocity,
                "state": state_name,
                "metadata": {
                    "current_price": (
                        history[-1].get("current_price", 0.0) if history else 0.0
                    ),
                    "volume": (
                        history[-1].get("accumulated_volume", 0) if history else 0
                    ),
                },
            }

            chart_data["datasets"].append(dataset)

            if is_highlighted:
                chart_data["highlights"].append(
                    {
                        "ticker": ticker,
                        "rank": rank,
                        "velocity": rank_velocity,
                        "state": state_name,
                    }
                )

        # Sort datasets by rank
        chart_data["datasets"].sort(key=lambda d: d.get("rank", 999))

        # Cache the result
        self._cached_chart_data = chart_data
        self._chart_data_dirty = False
        self._last_query_time = datetime.now(ZoneInfo("America/New_York"))

        return chart_data

    def _get_state_color(self, state: str, with_alpha: bool) -> str:
        """Get color based on state."""
        base_color = self.STATE_COLORS.get(
            state, "100, 149, 237"
        )  # Default cornflower blue
        alpha = 0.3 if with_alpha else 1.0
        return f"rgba({base_color}, {alpha})"

    def _get_rank_color(
        self, rank: int, is_highlighted: bool, is_new_entrant: bool, with_alpha: bool
    ) -> str:
        """Generate color for stock based on rank and status (legacy method)."""
        if is_highlighted:
            if is_new_entrant:
                base_color = "255, 0, 0"  # Red for new entrants
            else:
                base_color = "255, 165, 0"  # Orange for fast movers
        else:
            if rank <= 5:
                base_color = f"{127 + (rank - 1) * 28}, {(rank - 1) * 51}, 255"
            elif rank <= 10:
                base_color = f"{100 + (rank-5) * 30}, 255, {200 - (rank-5) * 30}"
            elif rank <= 15:
                base_color = f"200, {200 - (rank-10) * 20}, {120 - (rank-10) * 15}"
            else:
                base_color = f"{10 + (20-rank) * 32}, {60 + (20-rank) * 32}, 255"

        alpha = 0.3 if with_alpha else 1.0
        return f"rgba({base_color}, {alpha})"

    def close(self):
        """Clean up resources."""
        if self._influx_client:
            self._influx_client.close()
        logger.info("ChartDataManager closed")
