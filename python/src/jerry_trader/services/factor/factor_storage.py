"""ClickHouse storage adapter for factors."""

import logging
import threading
from typing import Any, Dict, Optional

from jerry_trader.domain.factor import FactorSnapshot
from jerry_trader.shared.logging.logger import setup_logger
from jerry_trader.shared.time.timezone import ms_to_readable

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)

# Thread-local storage for per-thread ClickHouse clients
_thread_local = threading.local()


class FactorStorage:
    """ClickHouse storage for computed factors.

    Handles persistence of FactorSnapshot to ClickHouse with proper
    schema management and batch writing.

    Uses per-thread ClickHouse clients to avoid "concurrent queries within
    same session" errors when multiple threads access ClickHouse simultaneously.
    """

    TABLE_NAME = "factors"
    _ms_to_readable = staticmethod(ms_to_readable)

    def __init__(
        self,
        session_id: str,
        ch_client: Any = None,
        clickhouse_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize storage with ClickHouse client/config and session ID.

        Args:
            session_id: Session identifier for this run
            ch_client: ClickHouse client from clickhouse_connect (legacy, optional)
            clickhouse_config: ClickHouse connection config for per-thread clients
        """
        self._ch_config = clickhouse_config
        self._shared_client = ch_client  # Legacy support
        self.session_id = session_id

        # Test connectivity once at startup (creates temporary client)
        if self._ch_config:
            from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

            test_client = get_clickhouse_client(self._ch_config)
            if test_client:
                test_client.close()
                logger.info("FactorStorage: ClickHouse connectivity verified")
            else:
                logger.warning("FactorStorage: ClickHouse unavailable")

        self._ensure_table()

    def _get_thread_ch_client(self):
        """Get or create a per-thread ClickHouse client.

        Per-thread clients prevent "concurrent queries within same session" errors
        when multiple threads query ClickHouse simultaneously.
        """
        # If we have a shared client (legacy mode), use it
        if self._shared_client:
            return self._shared_client

        # Otherwise use per-thread clients
        if not hasattr(_thread_local, "ch_client") or _thread_local.ch_client is None:
            from jerry_trader.platform.storage.clickhouse import get_clickhouse_client

            _thread_local.ch_client = get_clickhouse_client(self._ch_config)
        return _thread_local.ch_client

    def _ensure_table(self) -> None:
        """Verify factors table exists.

        Note: Table should be created via sql/clickhouse_factors.sql
        This method only checks existence and logs a warning if missing.
        """
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            logger.warning("FactorStorage: No ClickHouse client, skipping table check")
            return

        try:
            # Check if table exists
            result = ch_client.query(
                f"EXISTS TABLE {self.TABLE_NAME}",
            )
            exists = result.result_rows[0][0] if result.result_rows else 0

            if exists:
                logger.info(f"FactorStorage: Table '{self.TABLE_NAME}' verified")
            else:
                logger.warning(
                    f"FactorStorage: Table '{self.TABLE_NAME}' does not exist. "
                    f"Run: clickhouse-client --password <pw> < sql/clickhouse_factors.sql"
                )
        except Exception as e:
            logger.error(f"FactorStorage: Failed to check table existence - {e}")

    def write_factor_snapshot(
        self, snapshot: FactorSnapshot, timeframe: str = "trade"
    ) -> int:
        """Write factor snapshot to ClickHouse.

        Args:
            snapshot: FactorSnapshot with computed factors
            timeframe: Timeframe for these factors (e.g., 'trade', '1m', '5m')

        Returns:
            Number of factor rows written
        """
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            return 0

        if not snapshot.factors:
            return 0

        rows = [
            [
                snapshot.symbol,
                timeframe,
                snapshot.timestamp_ns,
                self.session_id,
                name,
                value,
            ]
            for name, value in snapshot.factors.items()
        ]

        try:
            ch_client.insert(
                self.TABLE_NAME,
                data=rows,
                column_names=[
                    "ticker",
                    "timeframe",
                    "timestamp_ns",
                    "session",
                    "factor_name",
                    "factor_value",
                ],
            )
            logger.debug(
                f"FactorStorage: Wrote {len(rows)} factors for {snapshot.symbol}/{timeframe} "
                f"at ts={self._ms_to_readable(snapshot.timestamp_ns // 1_000_000)}"
            )
            return len(rows)
        except Exception as e:
            logger.error(f"FactorStorage: Failed to write factors - {e}")
            return 0

    def write_batch(self, snapshots: list[tuple[FactorSnapshot, str]]) -> int:
        """Write multiple factor snapshots in a single batch.

        Args:
            snapshots: List of (FactorSnapshot, timeframe) tuples

        Returns:
            Total number of factor rows written
        """
        ch_client = self._get_thread_ch_client()
        if not ch_client or not snapshots:
            return 0

        rows = []
        for snapshot, timeframe in snapshots:
            for name, value in snapshot.factors.items():
                rows.append(
                    [
                        snapshot.symbol,
                        timeframe,
                        snapshot.timestamp_ns,
                        self.session_id,
                        name,
                        value,
                    ]
                )

        if not rows:
            return 0

        try:
            ch_client.insert(
                self.TABLE_NAME,
                data=rows,
                column_names=[
                    "ticker",
                    "timeframe",
                    "timestamp_ns",
                    "session",
                    "factor_name",
                    "factor_value",
                ],
            )
            # Log sample of what was written for debugging
            if rows:
                sample = rows[0]
                logger.info(
                    f"FactorStorage: Batch wrote {len(rows)} rows, sample: "
                    f"ticker={sample[0]}, timeframe={sample[1]}, "
                    f"timestamp_ns={sample[2]}, session={sample[3]}, "
                    f"factor_name={sample[4]}"
                )
            return len(rows)
        except Exception as e:
            logger.error(f"FactorStorage: Batch write failed - {e}")
            return 0

    def query_factors(
        self,
        ticker: str,
        start_ns: int | None = None,
        end_ns: int | None = None,
        factor_names: list[str] | None = None,
        timeframe: str | None = None,
    ) -> list[dict]:
        """Query historical factors from ClickHouse.

        Args:
            ticker: Symbol to query
            start_ns: Start timestamp in nanoseconds (optional, no filter if None)
            end_ns: End timestamp in nanoseconds (optional, no filter if None)
            factor_names: Optional list of factor names to filter
            timeframe: Optional timeframe filter (e.g., 'trade', '1m', '5m')

        Returns:
            List of dicts with ticker, timeframe, timestamp_ns, factor_name, factor_value
        """
        ch_client = self._get_thread_ch_client()
        if not ch_client:
            return []

        try:
            factor_filter = ""
            timeframe_filter = ""
            time_filter = ""
            params: dict[str, Any] = {
                "ticker": ticker,
            }

            # Add time range filters only if provided
            if start_ns is not None:
                time_filter += " AND timestamp_ns >= {start_ns:Int64}"
                params["start_ns"] = start_ns
            if end_ns is not None:
                time_filter += " AND timestamp_ns < {end_ns:Int64}"
                params["end_ns"] = end_ns

            if factor_names:
                factor_filter = "AND factor_name IN {factor_names:Array(String)}"
                params["factor_names"] = factor_names

            if timeframe:
                timeframe_filter = "AND timeframe = {timeframe:String}"
                params["timeframe"] = timeframe

            query = f"""
                SELECT ticker, timeframe, timestamp_ns, factor_name, factor_value
                FROM {self.TABLE_NAME} FINAL
                WHERE ticker = {{ticker:String}}
                  AND session = {{session:String}}
                  {time_filter}
                  {timeframe_filter}
                  {factor_filter}
                ORDER BY timestamp_ns ASC
            """

            params["session"] = self.session_id
            logger.info(
                f"FactorStorage: Query for {ticker}/{timeframe} "
                f"session={self.session_id}, start_ns={start_ns}, end_ns={end_ns}, "
                f"factor_names={factor_names}"
            )

            result = ch_client.query(query, parameters=params)
            logger.info(
                f"FactorStorage: Query returned {len(result.result_rows)} rows for {ticker}/{timeframe}"
            )

            return [
                {
                    "ticker": row[0],
                    "timeframe": row[1],
                    "timestamp_ns": row[2],
                    "factor_name": row[3],
                    "factor_value": row[4],
                }
                for row in result.result_rows
            ]
        except Exception as e:
            logger.error(f"FactorStorage: Query failed - {e}")
            return []
