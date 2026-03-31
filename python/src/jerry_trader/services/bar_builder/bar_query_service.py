"""
Bar query service: orchestrates ClickHouse bar queries, Polygon backfill,
and partial-bar appending from BarsBuilder.

This is the service-layer wrapper. Pure ClickHouse query primitives live in
platform/storage/clickhouse.py and can be used independently by other services
(FactorEngine, StateEngine) without pulling in bar_builder dependencies.
"""

import logging
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from jerry_trader.platform.config.session import make_session_id, parse_session_id
from jerry_trader.platform.messaging.event_bus import Event as BusEvent
from jerry_trader.platform.messaging.event_bus import EventBus
from jerry_trader.platform.storage.clickhouse import (
    TF_DURATION_SEC,
    get_clickhouse_client,
    query_last_bar_start,
    query_ohlcv_bars,
    query_tickers_with_bars_today,
)
from jerry_trader.platform.storage.ohlcv_writer import polygon_bars_to_dicts, write_bars
from jerry_trader.services.bar_builder.chart_data_service import ChartDataService
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import ms_to_readable

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

# Thread-local storage for per-thread ClickHouse clients
_thread_local = threading.local()


class ClickHouseClient:
    """
    Bar query orchestrator: ClickHouse + Polygon fallback + BarsBuilder partial bars.

    Coordinates:
    - Querying completed bars from ClickHouse
    - Backfilling missing bars from Polygon via ChartDataService
    - Appending pending/partial bars from the live BarsBuilder

    For raw ClickHouse queries without bar_builder dependencies, use
    platform.storage.clickhouse directly.
    """

    BARS_BUILDER_TIMEFRAMES = {"10s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"}

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

    _TF_DURATION_SEC = TF_DURATION_SEC
    _ms_to_readable = staticmethod(ms_to_readable)

    def __init__(
        self,
        session_id: Optional[str] = None,
        redis_config: Optional[Dict[str, Any]] = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
        bars_builder: Optional[Any] = None,
        event_bus: Optional[EventBus] = None,
    ):
        self.session_id = session_id or make_session_id()
        self.db_date, self.run_mode = parse_session_id(self.session_id)

        self._bars_builder = bars_builder
        self._event_bus = event_bus

        self.chart_data_service = ChartDataService(
            redis_config=redis_config,
            session_id=self.session_id,
        )

        # Store config for per-thread client creation (thread-safe)
        self._ch_config = clickhouse_config

        # Test connectivity once at startup (creates temporary client)
        test_client = get_clickhouse_client(clickhouse_config)
        if test_client:
            test_client.close()
            logger.info(
                f"ClickHouseClient initialized: session_id={self.session_id}, "
                f"run_mode={self.run_mode}, clickhouse=connected"
            )
        else:
            logger.info(
                "ClickHouse unavailable — bar queries fall back to ChartDataService"
            )

    def _get_thread_ch_client(self):
        """Get or create a per-thread ClickHouse client.

        Per-thread clients prevent "concurrent queries within same session" errors
        when multiple threads query ClickHouse simultaneously.
        """
        if not hasattr(_thread_local, "ch_client") or _thread_local.ch_client is None:
            _thread_local.ch_client = get_clickhouse_client(self._ch_config)
        return _thread_local.ch_client

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
            logger.debug(
                f"_append_partial_bar - {ticker}/{builder_tf}: "
                f"BarsBuilder not available (expected for remote ChartBFF)"
            )
            return

        bars = result.get("bars", [])
        last_time = bars[-1]["time"] if bars else 0

        try:
            # 1. Insert any completed bars not yet flushed to ClickHouse
            pending = self._bars_builder.get_pending_bars(ticker, builder_tf)
            for pbar in pending:
                pbar_time = pbar["bar_start"] // 1000

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
                has_ohlc_range = partial["high"] != partial["low"]
                is_meaningful = partial_trades >= 2 or has_ohlc_range

                if partial_time > last_time and is_meaningful:
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
                    bars[-1] = {
                        "time": partial_time,
                        "open": partial["open"],
                        "high": partial["high"],
                        "low": partial["low"],
                        "close": partial["close"],
                        "volume": partial["volume"],
                    }
                else:
                    logger.debug(
                        f"_append_partial_bar - {ticker}/{builder_tf}: "
                        f"skipping flat partial bar (time={'new' if partial_time > last_time else 'same'}, "
                        f"partial time is {self._ms_to_readable(partial_time)}, "
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
        """Query completed bars from ClickHouse. Returns ChartDataService-format dict or None."""
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            return None

        from jerry_trader import clock

        if to_date:
            end_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
        else:
            end_dt = clock.now_datetime().date()

        if from_date:
            start_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
        else:
            dur = self._TF_DURATION_SEC.get(builder_tf, 60)
            if dur <= 60:
                days_back = 2
            elif dur <= 900:
                days_back = 5
            elif dur <= 14400:
                days_back = 30
            elif dur <= 86400:
                days_back = 365
            else:
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

        return query_ohlcv_bars(ch_client, ticker, builder_tf, start_ms, end_ms, limit)

    _CUSTOM_BAR_BACKFILL_TFS: list = [
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

    def custom_bar_backfill(
        self,
        ticker: str,
        frontend_tf: str,
        builder_tf: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: int = 5000,
    ) -> int:
        """Fetch pre-aggregated bars from Polygon via ChartDataService and persist to ClickHouse."""
        result = self.chart_data_service.get_bars(
            ticker=ticker,
            timeframe=frontend_tf,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            use_cache=False,
        )
        if not result or not result.get("bars"):
            return 0

        dur_sec = self._TF_DURATION_SEC.get(builder_tf, 60)
        bar_dicts = polygon_bars_to_dicts(ticker, builder_tf, result["bars"], dur_sec)
        is_daily_or_above = dur_sec >= 86400

        try:
            # Use per-thread client via config to avoid concurrent query errors
            n = write_bars(
                self._ch_config,
                bar_dicts,
                source="polygon_backfill",
                filter_closed=not is_daily_or_above,
            )
            logger.info(
                f"custom_bar_backfill - {ticker}/{builder_tf}: "
                f"backfilled {n} bars to ClickHouse (source={result.get('source', '?')})"
            )

            # Publish BarBackfillCompleted event to EventBus
            if n > 0 and self._event_bus:
                self._event_bus.publish(
                    BusEvent.bar_backfill_completed(
                        symbol=ticker,
                        timeframe=builder_tf,
                        bar_count=n,
                        source=result.get("source", "unknown"),
                    )
                )

            return n
        except Exception as e:
            logger.error(
                f"custom_bar_backfill - {ticker}/{builder_tf}: insert failed - {e}"
            )
            return 0

    def custom_bar_backfill_all(self, ticker: str) -> int:
        """Backfill ALL timeframes (except 10s) from Polygon at once."""
        total = 0
        for frontend_tf, builder_tf in self._CUSTOM_BAR_BACKFILL_TFS:
            try:
                n = self.custom_bar_backfill(ticker, frontend_tf, builder_tf)
                total += n
            except Exception as e:
                logger.error(f"custom_bar_backfill_all - {ticker}/{builder_tf}: {e}")
        logger.info(
            f"custom_bar_backfill_all - {ticker}: backfilled {total} total bars "
            f"across {len(self._CUSTOM_BAR_BACKFILL_TFS)} timeframes"
        )
        return total

    def tickers_with_bars_today(self) -> set[str]:
        """Return all tickers that have bars in ClickHouse for today (ET)."""
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            return set()

        from datetime import timedelta
        from zoneinfo import ZoneInfo

        from jerry_trader import clock as _clock

        now_et = _clock.now_datetime().astimezone(ZoneInfo("America/New_York"))
        today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_start = today_start + timedelta(days=1)
        start_ms = int(today_start.timestamp() * 1000)
        end_ms = int(tomorrow_start.timestamp() * 1000)

        tickers = query_tickers_with_bars_today(ch_client, start_ms, end_ms)
        if tickers:
            logger.info(
                f"tickers_with_bars_today: found {len(tickers)} tickers "
                f"with bars today: {sorted(tickers)}"
            )
        return tickers

    def get_last_bar_start(self, ticker: str, timeframe: str, date: str) -> int | None:
        """Return the last bar_start (ms) for a ticker+timeframe on a specific date."""
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            return None

        start_dt = datetime.strptime(date, "%Y%m%d")
        end_dt = start_dt + timedelta(days=1)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        last_start = query_last_bar_start(
            ch_client, ticker, timeframe, start_ms, end_ms
        )
        if last_start:
            logger.debug(
                f"get_last_bar_start - {ticker}/{timeframe}: "
                f"last_start={self._ms_to_readable(last_start)} on {date}"
            )
        else:
            logger.debug(
                f"get_last_bar_start - {ticker}/{timeframe}: no bars on {date}"
            )
        return last_start

    def cleanup(self):
        """Clean up resources on shutdown.

        Note: Per-thread ClickHouse clients are automatically garbage collected
        when threads exit. We don't track them individually for cleanup.
        """
        logger.info("ClickHouseClient resources cleaned up")
