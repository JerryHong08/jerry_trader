"""
ClickHouse Client for OHLCV Bar Data

Handles ClickHouse queries, backfilling, and partial bar aggregation for
bar data served to the frontend.

Used by tickdata_server to provide historical bar data with Polygon fallback.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Set

import clickhouse_connect
from dotenv import load_dotenv

from jerry_trader.DataManager.chart_data_service import ChartDataService
from jerry_trader.DataManager.ohlcv_writer import polygon_bars_to_dicts, write_bars
from jerry_trader.utils.logger import setup_logger
from jerry_trader.utils.session import make_session_id, parse_session_id
from jerry_trader.utils.timezone import ms_to_readable

load_dotenv()

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class ClickHouseClient:
    """
    ClickHouse client for OHLCV bar queries and Polygon backfill.

    Provides methods to query historical bars from ClickHouse, backfill missing
    data from Polygon, and append partial bars from BarsBuilder.
    """

    # Timeframes with live partial-bar updates from Rust BarBuilder.
    # Only these have pending/partial bar state at runtime.
    BARS_BUILDER_TIMEFRAMES = {"10s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"}

    # Map frontend timeframe names → canonical ClickHouse timeframe keys.
    _TF_TO_CH: Dict[str, str] = {
        "10s": "10s",
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1D": "1d",
        "1W": "1w",
        "1M": "1M",
        "1d": "1d",
        "1w": "1w",
    }

    _TF_DURATION_SEC: Dict[str, int] = {
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

    _ms_to_readable = staticmethod(ms_to_readable)

    def __init__(
        self,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
        bars_builder: Optional[Any] = None,
    ):
        # Unified session id — single source of truth for mode & date
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        # BarsBuilder reference — used to fetch partial (in-progress) bars
        self._bars_builder = bars_builder

        # Polygon historical data source (used for ClickHouse backfill)
        self.chart_data_service = ChartDataService(
            redis_config=redis_config,
            session_id=self.session_id,
        )

        # ── ClickHouse (bar queries) ─────────────────────────────────
        ch_cfg = clickhouse_config or {}
        ch_host = ch_cfg.get("host", "localhost")
        ch_port = ch_cfg.get("port", 8123)
        ch_user = ch_cfg.get("user", "default")
        ch_db = ch_cfg.get("database", "jerry_trader")
        password_env = ch_cfg.get("password_env", "CLICKHOUSE_PASSWORD")
        ch_password = os.getenv(password_env, "")

        self.ch_client = None
        if ch_password:
            try:
                self.ch_client = clickhouse_connect.get_client(
                    host=ch_host,
                    port=ch_port,
                    username=ch_user,
                    password=ch_password,
                    database=ch_db,
                )
                self.ch_client.command("SELECT 1")  # connectivity check
                logger.info(f"ClickHouse connected: {ch_host}:{ch_port}/{ch_db}")
            except Exception as exc:
                logger.warning(
                    f"ClickHouse unavailable, falling back to ChartDataService: {exc}"
                )
                self.ch_client = None
        else:
            logger.info(
                "No ClickHouse password — bar queries fall back to ChartDataService"
            )

        logger.info(
            f"ClickHouseClient initialized: session_id={self.session_id}, "
            f"run_mode={self.run_mode}, "
            f"clickhouse={'connected' if self.ch_client else 'unavailable'}"
        )

    def _append_partial_bar(
        self,
        result: Dict,
        ticker: str,
        builder_tf: str,
    ) -> None:
        """Append pending (unflushed) + in-progress bars from BarsBuilder.

        The bars_builder keeps completed bars in ``_pending_bars`` for
        up to ~50 ms before flushing them to ClickHouse.  If a REST
        request lands in that window, ClickHouse won't have them yet.
        We also append the current partial (in-progress) bar so the
        frontend seeds ``currentBarRef`` with correct OHLCV.

        This guarantees zero gaps between historical bars and the
        real-time trade-tick stream.
        """
        if not self._bars_builder:
            return

        bars = result.get("bars", [])
        last_time = bars[-1]["time"] if bars else 0

        try:
            # 1. Insert any completed bars not yet flushed to ClickHouse
            pending = self._bars_builder.get_pending_bars(ticker, builder_tf)
            for pbar in pending:
                pbar_time = pbar["bar_start"] // 1000  # ms → seconds

                logger.debug(f"pbar_time:{pbar_time}, last_time:{last_time}")

                if pbar_time > last_time:
                    bars.append(
                        {
                            "time": pbar_time,
                            "open": pbar["open"],
                            "high": pbar["high"],
                            "low": pbar["low"],
                            "close": pbar["close"],
                            "volume": pbar["volume"],
                        }
                    )
                    last_time = pbar_time
                elif pbar_time == last_time:
                    # Pending bar is more recent than ClickHouse copy
                    bars[-1] = {
                        "time": pbar_time,
                        "open": pbar["open"],
                        "high": pbar["high"],
                        "low": pbar["low"],
                        "close": pbar["close"],
                        "volume": pbar["volume"],
                    }

            # 2. Append the current in-progress (partial) bar
            partial = self._bars_builder.get_partial_bar(ticker, builder_tf)
            if partial is not None:
                partial_time = partial["bar_start"] // 1000
                partial_trades = partial.get("trade_count", 0)
                # Check if partial has real OHLC variation (not flat)
                has_ohlc_range = partial["high"] != partial["low"]
                is_meaningful = partial_trades >= 2 or has_ohlc_range

                if partial_time > last_time and is_meaningful:
                    # New bar: only append if it has meaningful OHLC
                    bars.append(
                        {
                            "time": partial_time,
                            "open": partial["open"],
                            "high": partial["high"],
                            "low": partial["low"],
                            "close": partial["close"],
                            "volume": partial["volume"],
                        }
                    )
                elif partial_time == last_time and is_meaningful:
                    # Same bar: replace if it has meaningful OHLC
                    bars[-1] = {
                        "time": partial_time,
                        "open": partial["open"],
                        "high": partial["high"],
                        "low": partial["low"],
                        "close": partial["close"],
                        "volume": partial["volume"],
                    }
                else:
                    # Flat partial bar (H=L, trade_count<2) — skip to preserve data quality
                    logger.debug(
                        f"_append_partial_bar - {ticker}/{builder_tf}: "
                        f"skipping flat partial bar (time={'new' if partial_time > last_time else 'same'}, "
                        f"trades={partial_trades}, O={partial['open']:.2f}, "
                        f"H={partial['high']:.2f}, L={partial['low']:.2f}, C={partial['close']:.2f})"
                    )

            result["bars"] = bars
            result["barCount"] = len(bars)

            logger.debug(
                f"_append_partial_bar - {ticker}/{builder_tf}: "
                f"appended {len(pending)} pending + {'1 partial' if partial else '0 partial'} bar(s)"
            )
        except Exception as e:
            logger.debug(f"_append_partial_bar - {ticker}/{builder_tf}: error - {e}")

    def _query_bars_clickhouse(
        self,
        ticker: str,
        builder_tf: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 5000,
    ) -> Optional[Dict]:
        """Query completed bars from ClickHouse.

        Returns a response dict matching ChartDataService format, or None on error.
        """
        if not self.ch_client:
            return None

        # Default date range based on timeframe
        from jerry_trader import clock

        if to_date:
            end_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
        else:
            end_dt = clock.now_datetime().date()

        if from_date:
            start_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
        else:
            # Auto-range: short TFs get fewer days, long TFs get more
            dur = self._TF_DURATION_SEC.get(builder_tf, 60)
            if dur <= 60:  # 10s, 1m
                days_back = 2
            elif dur <= 900:  # 5m, 15m
                days_back = 5
            elif dur <= 14400:  # 1h, 4h
                days_back = 30
            elif dur <= 86400:  # 1d
                days_back = 365
            else:  # 1w, 1M
                days_back = 1825
            start_dt = end_dt - timedelta(days=days_back)

        start_ms = int(
            datetime.combine(start_dt, datetime.min.time()).timestamp() * 1000
        )
        end_ms = int(
            datetime.combine(
                end_dt + timedelta(days=1), datetime.min.time()
            ).timestamp()
            * 1000
        )

        logger.debug(
            f"_query_bars_clickhouse - {ticker}/{builder_tf}: "
            f"querying from {self._ms_to_readable(start_ms)} to {self._ms_to_readable(end_ms)}"
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
            result = self.ch_client.query(
                query,
                parameters={
                    "ticker": ticker,
                    "timeframe": builder_tf,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "limit": limit,
                },
            )

            bars = []
            for row in result.result_rows:
                bar_start, bar_end, o, h, l, c, vol, tc, vwap, session = row
                # lightweight-charts expects `time` in seconds (UTC)
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

            dur_sec = self._TF_DURATION_SEC.get(builder_tf, 60)
            return {
                "ticker": ticker,
                "timeframe": builder_tf,
                "bars": bars,
                "barCount": len(bars),
                "barDurationSec": dur_sec,
                "source": "clickhouse",
                "from": str(start_dt),
                "to": str(end_dt),
            }
        except Exception as e:
            logger.error(
                f"_query_bars_clickhouse - {ticker}/{builder_tf}: query failed - {e}"
            )
            return None

    def custom_bar_backfill(
        self,
        ticker: str,
        frontend_tf: str,
        builder_tf: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 5000,
    ) -> int:
        """Fetch pre-aggregated bars from Polygon via ChartDataService and
        persist them into ClickHouse.

        Fetches data as recent as Polygon has available (including today).
        For intraday TFs, trades_backfill will later overwrite today's bars
        with higher-accuracy trade-built data via ReplacingMergeTree.

        Returns the number of bars written.
        """
        result = self.chart_data_service.get_bars(
            ticker=ticker,
            timeframe=frontend_tf,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            use_cache=False,  # always fetch fresh — data goes to ClickHouse
        )
        if not result or not result.get("bars"):
            return 0

        dur_sec = self._TF_DURATION_SEC.get(builder_tf, 60)
        bar_dicts = polygon_bars_to_dicts(ticker, builder_tf, result["bars"], dur_sec)

        # Daily+ bars have bar_start at midnight ET which falls in the
        # "closed session" window (20:00–04:00 ET).  Skip session filtering
        # for those timeframes so they don't get silently dropped.
        is_daily_or_above = dur_sec >= 86400

        try:
            n = write_bars(
                self.ch_client,
                bar_dicts,
                source="polygon_backfill",
                filter_closed=not is_daily_or_above,
            )
            logger.info(
                f"custom_bar_backfill - {ticker}/{builder_tf}: "
                f"backfilled {n} bars to ClickHouse "
                f"(source={result.get('source', '?')})"
            )
            return n
        except Exception as e:
            logger.error(
                f"custom_bar_backfill - {ticker}/{builder_tf}: insert failed - {e}"
            )
            return 0

    # Timeframes to backfill from Polygon on subscription.
    # Excludes 10s (no Polygon support — trades_backfill handles it).
    _CUSTOM_BAR_BACKFILL_TFS: list = [
        # (frontend_tf, builder_tf)
        ("1m", "1m"),
        ("5m", "5m"),
        ("15m", "15m"),
        ("30m", "30m"),
        ("1h", "1h"),
        ("4h", "4h"),
        ("1D", "1d"),
        ("1W", "1w"),
        ("1M", "1M"),
    ]

    def custom_bar_backfill_all(
        self,
        ticker: str,
    ) -> int:
        """Backfill ALL timeframes (except 10s) from Polygon at once.

        Called on ticker subscription to warm up ClickHouse for fast
        frontend chart rendering. Fetches as recent as Polygon has
        available. trades_backfill will later overwrite today's intraday
        bars with trade-built data via ReplacingMergeTree(inserted_at).

        Returns total number of bars written across all timeframes.
        """
        total = 0
        for frontend_tf, builder_tf in self._CUSTOM_BAR_BACKFILL_TFS:
            try:
                n = self.custom_bar_backfill(ticker, frontend_tf, builder_tf)
                total += n
            except Exception as e:
                logger.error(f"custom_bar_backfill_all - {ticker}/{builder_tf}: {e}")
        logger.info(
            f"custom_bar_backfill_all - {ticker}: "
            f"backfilled {total} total bars across "
            f"{len(self._CUSTOM_BAR_BACKFILL_TFS)} timeframes"
        )
        return total

    def tickers_with_bars_today(self) -> Set[str]:
        """Return all tickers that have bars in ClickHouse for today (ET).

        Called once at startup to bootstrap _backfill_done, so that
        re-subscribing after a service restart skips redundant
        custom_bar_backfill_all for tickers already warmed up.
        """
        if not self.ch_client:
            return set()
        try:
            from zoneinfo import ZoneInfo

            from jerry_trader import clock as _clock

            now_et = _clock.now_datetime().astimezone(ZoneInfo("America/New_York"))
            today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_start = today_start + timedelta(days=1)
            start_ms = int(today_start.timestamp() * 1000)
            end_ms = int(tomorrow_start.timestamp() * 1000)

            result = self.ch_client.query(
                "SELECT DISTINCT ticker FROM ohlcv_bars "
                "WHERE bar_start >= {start_ms:Int64} "
                "  AND bar_start < {end_ms:Int64}",
                parameters={
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                },
            )
            tickers = {row[0] for row in result.result_rows}
            if tickers:
                logger.info(
                    f"tickers_with_bars_today: found {len(tickers)} tickers "
                    f"with bars today: {sorted(tickers)}"
                )
            return tickers
        except Exception as e:
            logger.warning(f"tickers_with_bars_today: query failed - {e}")
            return set()

    def get_last_bar_start(
        self, ticker: str, timeframe: str, date: str
    ) -> Optional[int]:
        """Return the last bar_start (ms) for a ticker+timeframe on a specific date.

        Args:
            ticker: Stock symbol
            timeframe: Bar timeframe (e.g., '1m', '5m', '1h')
            date: Date string in YYYYMMDD format

        Returns:
            Last bar_start timestamp in milliseconds, or None if no bars exist.

        Used by bars_builder_service to determine bootstrap start point:
        - None → first subscription → bootstrap from 4 AM ET session start
        - Valid timestamp → re-subscription → bootstrap from last bar_start
          (ensures partial bars written during unsubscribe are rebuilt
          with complete trade data rather than overwritten with a gap)
        """
        if not self.ch_client:
            return None

        start_dt = datetime.strptime(date, "%Y%m%d")
        end_dt = start_dt + timedelta(days=1)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        try:
            query = """
                SELECT MAX(bar_start) as last_start
                FROM ohlcv_bars FINAL
                WHERE ticker = {ticker:String}
                  AND timeframe = {timeframe:String}
                  AND bar_start >= {start_ms:Int64}
                  AND bar_start < {end_ms:Int64}
            """
            result = self.ch_client.query(
                query,
                parameters={
                    "ticker": ticker,
                    "timeframe": timeframe,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                },
            )
            last_start = result.result_rows[0][0] if result.result_rows else None
            if last_start and last_start > 0:
                logger.debug(
                    f"get_last_bar_start - {ticker}/{timeframe}: "
                    f"last_start={self._ms_to_readable(last_start)} on {date}"
                )
                return last_start
            logger.debug(
                f"get_last_bar_start - {ticker}/{timeframe}: no bars on {date}"
            )
            return None
        except Exception as e:
            logger.error(f"get_last_bar_start - {ticker}/{timeframe}: {e}")
            return None

    def cleanup(self):
        """Clean up resources on shutdown."""
        if self.ch_client:
            try:
                self.ch_client.close()
            except Exception:
                pass
        logger.info("ClickHouseClient resources cleaned up")
