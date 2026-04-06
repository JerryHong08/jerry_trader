"""ClickHouse storage adapter for signal events."""

import json
import threading
import uuid
from typing import Any, Optional

from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)

_thread_local = threading.local()


class SignalStorage:
    """ClickHouse storage for signal trigger events.

    Records trigger events from SignalEngine with rule metadata,
    factor snapshot, and optional entry price. Returns are computed
    offline via SQL JOIN with ohlcv bars.
    """

    TABLE_NAME = "signal_events"

    def __init__(
        self,
        session_id: str,
        ch_client: Any = None,
        clickhouse_config: Optional[dict] = None,
    ):
        self._ch_config = clickhouse_config
        self._shared_client = ch_client
        self.session_id = session_id

        if self._ch_config:
            from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

            test_client = get_clickhouse_client(self._ch_config)
            if test_client:
                test_client.close()
                logger.info("SignalStorage: ClickHouse connectivity verified")
            else:
                logger.warning("SignalStorage: ClickHouse unavailable")

        self._ensure_table()

    def _get_thread_ch_client(self):
        if self._shared_client:
            return self._shared_client

        if not hasattr(_thread_local, "ch_client") or _thread_local.ch_client is None:
            from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

            _thread_local.ch_client = get_clickhouse_client(self._ch_config)
        return _thread_local.ch_client

    def _ensure_table(self) -> None:
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            logger.warning("SignalStorage: No ClickHouse client, skipping table check")
            return

        try:
            result = ch_client.query(f"EXISTS TABLE {self.TABLE_NAME}")
            exists = result.result_rows[0][0] if result.result_rows else 0

            if exists:
                logger.info(f"SignalStorage: Table '{self.TABLE_NAME}' verified")
            else:
                logger.warning(
                    f"SignalStorage: Table '{self.TABLE_NAME}' does not exist. "
                    f"Run: clickhouse-client --password <pw> < sql/clickhouse_signal_events.sql"
                )
        except Exception as e:
            logger.error(f"SignalStorage: Failed to check table existence - {e}")

    def write_signal_event(
        self,
        rule_id: str,
        rule_version: int,
        symbol: str,
        timeframe: str,
        trigger_time_ns: int,
        factors: dict[str, float],
        trigger_price: Optional[float] = None,
    ) -> bool:
        """Write a signal trigger event to ClickHouse.

        Args:
            rule_id: Rule identifier.
            rule_version: Rule version number.
            symbol: Ticker symbol.
            timeframe: Factor timeframe (e.g. "trade").
            trigger_time_ns: Trigger timestamp in nanoseconds.
            factors: Factor values at trigger time.
            trigger_price: Optional price at trigger time.

        Returns:
            True if write succeeded.
        """
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            return False

        event_id = str(uuid.uuid4())
        factors_json = json.dumps(factors, default=str)

        row = [
            event_id,
            rule_id,
            rule_version,
            symbol,
            timeframe,
            trigger_time_ns,
            trigger_price,
            factors_json,
            self.session_id,
        ]

        try:
            ch_client.insert(
                self.TABLE_NAME,
                data=[row],
                column_names=[
                    "id",
                    "rule_id",
                    "rule_version",
                    "ticker",
                    "timeframe",
                    "trigger_time",
                    "trigger_price",
                    "factors",
                    "session",
                ],
            )
            logger.info(
                f"SignalStorage: wrote event {rule_id}/{symbol} "
                f"at ts={trigger_time_ns}"
            )
            return True
        except Exception as e:
            logger.error(f"SignalStorage: Failed to write signal event - {e}")
            return False
