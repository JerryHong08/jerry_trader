"""
Pure ClickHouse connection factory and low-level OHLCV query helpers.

This module has NO dependencies on services/ — it only wraps
clickhouse_connect and provides raw query primitives that any service
(bar_builder, factor_engine, strategy) can use.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

import clickhouse_connect
from dotenv import load_dotenv

from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import ms_to_readable

load_dotenv()

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

_ms_to_readable = ms_to_readable

# Timeframe → duration in seconds mapping (shared constant)
TF_DURATION_SEC: Dict[str, int] = {
    "10s": 10,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
    "1M": 2592000,
}


def get_clickhouse_client(clickhouse_config: Optional[Dict] = None):
    """
    Create and return a connected clickhouse_connect client, or None on failure.

    Args:
        clickhouse_config: Dict with keys: host, port, user, database, password_env.

    Returns:
        A clickhouse_connect client instance, or None if unavailable.
    """
    ch_cfg = clickhouse_config or {}
    ch_host = ch_cfg.get("host", "localhost")
    ch_port = ch_cfg.get("port", 8123)
    ch_user = ch_cfg.get("user", "default")
    ch_db = ch_cfg.get("database", "jerry_trader")
    password_env = ch_cfg.get("password_env", "CLICKHOUSE_PASSWORD")
    ch_password = os.getenv(password_env, "")

    if not ch_password:
        logger.info("No ClickHouse password configured — client not created")
        return None

    try:
        client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_password,
            database=ch_db,
        )
        client.command("SELECT 1")  # connectivity check
        logger.info(f"ClickHouse connected: {ch_host}:{ch_port}/{ch_db}")
        return client
    except Exception as exc:
        logger.warning(f"ClickHouse unavailable: {exc}")
        return None


def query_ohlcv_bars(
    ch_client,
    ticker: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    limit: int = 5000,
) -> Optional[Dict]:
    """
    Query completed OHLCV bars from ClickHouse for a ticker + timeframe + time range.

    Returns a response dict with keys: ticker, timeframe, bars, barCount,
    barDurationSec, source, from, to — matching ChartDataService format.
    Returns None on error or if client is unavailable.
    """
    if not ch_client:
        return None

    start_dt = datetime.utcfromtimestamp(start_ms / 1000).date()
    end_dt = datetime.utcfromtimestamp(end_ms / 1000).date()

    logger.debug(
        f"query_ohlcv_bars - {ticker}/{timeframe}: "
        f"querying from {_ms_to_readable(start_ms)} to {_ms_to_readable(end_ms)}"
    )

    try:
        query = """
            SELECT bar_start, bar_end, open, high, low, close, volume,
                   trade_count, vwap, session
            FROM ohlcv_bars FINAL
            WHERE ticker = {ticker:String}
              AND timeframe = {timeframe:String}
              AND bar_start >= {start_ms:Int64}
              AND bar_start < {end_ms:Int64}
            ORDER BY bar_start ASC
            LIMIT {limit:UInt32}
        """
        result = ch_client.query(
            query,
            parameters={
                "ticker": ticker,
                "timeframe": timeframe,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "limit": limit,
            },
        )

        bars = []
        for row in result.result_rows:
            bar_start, bar_end, o, h, l, c, vol, tc, vwap, session = row
            bars.append(
                {
                    "time": bar_start // 1000,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": vol,
                }
            )

        dur_sec = TF_DURATION_SEC.get(timeframe, 60)
        return {
            "ticker": ticker,
            "timeframe": timeframe,
            "bars": bars,
            "barCount": len(bars),
            "barDurationSec": dur_sec,
            "source": "clickhouse",
            "from": str(start_dt),
            "to": str(end_dt),
        }
    except Exception as e:
        logger.error(f"query_ohlcv_bars - {ticker}/{timeframe}: query failed - {e}")
        return None


def query_tickers_with_bars_today(ch_client, start_ms: int, end_ms: int) -> Set[str]:
    """
    Return all tickers that have at least one bar between start_ms and end_ms.
    Returns empty set on error or if client unavailable.
    """
    if not ch_client:
        return set()
    try:
        result = ch_client.query(
            "SELECT DISTINCT ticker FROM ohlcv_bars "
            "WHERE bar_start >= {start_ms:Int64} "
            "  AND bar_start < {end_ms:Int64}",
            parameters={"start_ms": start_ms, "end_ms": end_ms},
        )
        return {row[0] for row in result.result_rows}
    except Exception as e:
        logger.warning(f"query_tickers_with_bars_today: query failed - {e}")
        return set()


def query_last_bar_start(
    ch_client, ticker: str, timeframe: str, start_ms: int, end_ms: int
) -> Optional[int]:
    """
    Return the MAX bar_start (ms) for ticker+timeframe in [start_ms, end_ms).
    Returns None if no bars exist or on error.
    """
    if not ch_client:
        return None
    try:
        query = """
            SELECT MAX(bar_start) as last_start
            FROM ohlcv_bars FINAL
            WHERE ticker = {ticker:String}
              AND timeframe = {timeframe:String}
              AND bar_start >= {start_ms:Int64}
              AND bar_start < {end_ms:Int64}
        """
        result = ch_client.query(
            query,
            parameters={
                "ticker": ticker,
                "timeframe": timeframe,
                "start_ms": start_ms,
                "end_ms": end_ms,
            },
        )
        last_start = result.result_rows[0][0] if result.result_rows else None
        return last_start if last_start and last_start > 0 else None
    except Exception as e:
        logger.error(f"query_last_bar_start - {ticker}/{timeframe}: {e}")
        return None
