"""
Overview Chart Data Manager for JerryTrader Frontend

This module manages chart data for the JerryTrader frontend's OverviewChartModule.

Data Flow:
    ClickHouse (market_snapshot) -> ChartDataManager -> BFF -> Frontend
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import clickhouse_connect
import redis

from jerry_trader.platform.config.session import (
    db_date_to_date,
    make_session_id,
    parse_session_id,
    session_to_influx_tags,
)
from jerry_trader.shared.ids.redis_keys import (
    market_snapshot_processed,
    movers_subscribed_set,
    state_cursor,
)
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class JerryTraderChartDataManager:
    """
    Manage and format chart data for JerryTrader's OverviewChartModule.

    Each ticker line is segmented by state transitions, colored by state
    (Best, Good, OnWatch, NotGood, Bad).
    """

    # Valid state values (backend emits these directly)
    VALID_STATES = {"Best", "Good", "OnWatch", "NotGood", "Bad"}

    def __init__(
        self,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        influxdb_config: Optional[Dict[str, Any]] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
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

        self.STREAM_NAME = market_snapshot_processed(self.session_id)
        self.SUBSCRIBED_SET_NAME = movers_subscribed_set(self.session_id)
        self.HSET_NAME = state_cursor(self.session_id)

        # ------- ClickHouse Configuration -------
        self.ch_client = None
        ch_cfg = clickhouse_config or {}
        if ch_cfg:
            ch_host = ch_cfg.get("host", "localhost")
            ch_port = ch_cfg.get("port", 8123)
            ch_user = ch_cfg.get("user", "default")
            ch_db = ch_cfg.get("database", "jerry_trader")
            password_env = ch_cfg.get("password_env", "CLICKHOUSE_PASSWORD")
            ch_password = os.getenv(password_env, "")
            try:
                self.ch_client = clickhouse_connect.get_client(
                    host=ch_host,
                    port=ch_port,
                    username=ch_user,
                    password=ch_password,
                    database=ch_db,
                )
                self.ch_client.command("SELECT 1")
                logger.info(
                    f"__init__ - ClickHouse connected for overview chart: {ch_host}:{ch_port}/{ch_db}"
                )
            except Exception as exc:
                logger.warning(
                    f"__init__ - ClickHouse unavailable for overview chart: {exc}"
                )
                self.ch_client = None

        logger.info(
            f"__init__ - JerryTraderChartDataManager initialized: mode={self.run_mode}, session_id={self.session_id}"
        )

    def mark_dirty(self):
        """Mark chart data as needing refresh."""
        self._chart_data_dirty = True

    def get_subscribed_tickers(self) -> List[str]:
        """Get all subscribed tickers from Redis ZSET."""
        return list(self.redis_client.zrange(self.SUBSCRIBED_SET_NAME, 0, -1))

    def get_top_n_tickers(self, n: int = 20) -> List[str]:
        """
        Get top N tickers by rank from the last snapshot in Redis Stream.
        Falls back to ClickHouse when Redis stream is empty (e.g. replay mode).
        """
        entries = self.redis_client.xrevrange(self.STREAM_NAME, count=1)

        if not entries:
            return self._get_top_n_tickers_clickhouse(n)

        entry_id, fields = entries[0]
        data_json = fields.get("data")
        if not data_json:
            return self._get_top_n_tickers_clickhouse(n)

        try:
            tickers_data = json.loads(data_json)
        except json.JSONDecodeError:
            return self._get_top_n_tickers_clickhouse(n)

        # Filter tickers with rank <= n and sort by rank
        current_membership = [
            (item["symbol"], item["rank"])
            for item in tickers_data
            if item.get("rank", 999) <= n
        ]
        current_membership.sort(key=lambda x: x[1])
        return [ticker for ticker, rank in current_membership]

    def _get_top_n_tickers_clickhouse(self, n: int = 20) -> List[str]:
        """Fallback: get top N tickers from ClickHouse latest snapshot."""
        if not self.ch_client:
            return []

        date_tag, mode_tag = session_to_influx_tags(self.session_id)

        try:
            query = f"""
                SELECT DISTINCT symbol FROM market_snapshot FINAL
                WHERE date = {{date:String}}
                  AND mode = {{mode:String}}
                  AND rank > 0
                  AND rank <= {n}
                ORDER BY rank ASC
                LIMIT {n}
            """
            result = self.ch_client.query(
                query,
                parameters={"date": date_tag, "mode": mode_tag},
            )
            return [row[0] for row in result.result_rows]
        except Exception as e:
            logger.error(f"Error getting top N tickers from ClickHouse: {e}")
            return []

    def get_ticker_history(
        self, ticker: str, since: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Query historical snapshot data for a ticker from ClickHouse.
        Returns list of {timestamp, changePercent, price, volume, rank}.
        """
        if not self.ch_client:
            return []

        range_start, range_end = self._get_time_range(since)
        start_ms = self._time_to_ms(range_start)
        end_ms = self._time_to_ms(range_end)

        date_tag, mode_tag = session_to_influx_tags(self.session_id)

        try:
            query = """
                SELECT event_time_ms, changePercent, price, volume, rank
                FROM market_snapshot FINAL
                WHERE symbol = {symbol:String}
                  AND date = {date:String}
                  AND mode = {mode:String}
                  AND event_time_ms >= {start_ms:Int64}
                  AND event_time_ms <= {end_ms:Int64}
                ORDER BY event_time_ms ASC
            """
            result = self.ch_client.query(
                query,
                parameters={
                    "symbol": ticker,
                    "date": date_tag,
                    "mode": mode_tag,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                },
            )

            rows = []
            for event_time_ms, change_pct, price, volume, rank in result.result_rows:
                ts = datetime.fromtimestamp(event_time_ms / 1000.0, tz=ZoneInfo("UTC"))
                rows.append(
                    {
                        "timestamp": ts,
                        "changePercent": float(change_pct or 0.0),
                        "price": float(price or 0.0),
                        "volume": float(volume or 0.0),
                        "rank": int(rank or 0),
                    }
                )

            return rows
        except Exception as e:
            logger.error(f"Error querying ClickHouse history for {ticker}: {e}")
            return []

    def get_ticker_state_history(self, ticker: str) -> List[Dict]:
        """
        Query state history for a ticker.
        Returns list of {timestamp, state, stateReason} sorted by timestamp.
        Currently returns empty — state history will be stored in ClickHouse
        when StateEngine is migrated.
        """
        # TODO: Migrate state history queries to ClickHouse
        return []

    def _get_time_range(self, since: Optional[datetime] = None) -> Tuple[str, str]:
        """Get time range for queries as ISO strings."""
        ny_tz = ZoneInfo("America/New_York")

        if since is not None:
            range_start = since.isoformat()
            if self.run_mode == "replay":
                d = db_date_to_date(self.db_date)
                range_end = f"{d.isoformat()}T23:59:59Z"
            else:
                range_end = "now()"
        elif self.run_mode == "replay":
            d = db_date_to_date(self.db_date)
            range_start = f"{d.isoformat()}T04:00:00Z"
            range_end = f"{d.isoformat()}T23:59:59Z"
        else:
            today = datetime.now(ny_tz)
            range_start = today.replace(
                hour=4, minute=0, second=0, microsecond=0
            ).isoformat()
            range_end = "now()"

        return range_start, range_end

    def _time_to_ms(self, ts: str) -> int:
        """Convert ISO timestamp or 'now()' to epoch milliseconds."""
        if ts == "now()":
            return int(datetime.now(ZoneInfo("UTC")).timestamp() * 1000)
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)

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
        timestamp = fields.get("timestamp")

        if not data_json:
            return [], timestamp

        try:
            tickers_data = json.loads(data_json)
        except json.JSONDecodeError:
            return [], timestamp

        result = []
        for item in tickers_data:
            result.append(
                {
                    "symbol": item.get("symbol", ""),
                    "rank": item.get("rank", 999),
                    "price": item.get("price", 0.0),
                    "changePercent": item.get("changePercent", 0.0),
                    "volume": item.get("volume", 0),
                    "relativeVolume5min": item.get("relativeVolume5min", 1.0),
                    "relativeVolumeDaily": item.get("relativeVolumeDaily", 1.0),
                    "vwap": item.get("vwap", 0.0),
                }
            )

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
                        "data": [{"time": 1737300000, "value": 5.2}, ...],
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
        logger.debug(f"Generating LW Overview chart data for tickers: {tickers}")
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
                time_seconds = int(ts.timestamp())
                value = point.get("changePercent")

                if value is not None:
                    value_float = float(value)
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

        # Get rank data for legend/display
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
        if self.ch_client:
            try:
                self.ch_client.close()
            except Exception:
                pass
        logger.info("JerryTraderChartDataManager closed")
