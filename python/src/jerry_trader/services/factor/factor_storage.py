"""ClickHouse storage adapter for factors."""

import logging
from typing import Any

from jerry_trader.domain.factor import FactorSnapshot
from jerry_trader.shared.logging.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class FactorStorage:
    """ClickHouse storage for computed factors.

    Handles persistence of FactorSnapshot to ClickHouse with proper
    schema management and batch writing.
    """

    TABLE_NAME = "factors"

    def __init__(self, ch_client: Any, session_id: str):
        """Initialize storage with ClickHouse client and session ID.

        Args:
            ch_client: ClickHouse client from clickhouse_connect
            session_id: Session identifier for this run
        """
        self.ch_client = ch_client
        self.session_id = session_id
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Verify factors table exists.

        Note: Table should be created via sql/clickhouse_factors.sql
        This method only checks existence and logs a warning if missing.
        """
        if not self.ch_client:
            logger.warning("FactorStorage: No ClickHouse client, skipping table check")
            return

        try:
            # Check if table exists
            result = self.ch_client.query(
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

    def write_factor_snapshot(self, snapshot: FactorSnapshot) -> int:
        """Write factor snapshot to ClickHouse.

        Args:
            snapshot: FactorSnapshot with computed factors

        Returns:
            Number of factor rows written
        """
        if not self.ch_client:
            return 0

        if not snapshot.factors:
            return 0

        rows = [
            [
                snapshot.symbol,
                snapshot.timestamp_ns,
                self.session_id,
                name,
                value,
            ]
            for name, value in snapshot.factors.items()
        ]

        try:
            self.ch_client.insert(
                self.TABLE_NAME,
                data=rows,
                column_names=[
                    "ticker",
                    "timestamp_ns",
                    "session",
                    "factor_name",
                    "factor_value",
                ],
            )
            logger.debug(
                f"FactorStorage: Wrote {len(rows)} factors for {snapshot.symbol} "
                f"at ts={snapshot.timestamp_ns}"
            )
            return len(rows)
        except Exception as e:
            logger.error(f"FactorStorage: Failed to write factors - {e}")
            return 0

    def write_batch(self, snapshots: list[FactorSnapshot]) -> int:
        """Write multiple factor snapshots in a single batch.

        Args:
            snapshots: List of FactorSnapshot objects

        Returns:
            Total number of factor rows written
        """
        if not self.ch_client or not snapshots:
            return 0

        rows = []
        for snapshot in snapshots:
            for name, value in snapshot.factors.items():
                rows.append(
                    [
                        snapshot.symbol,
                        snapshot.timestamp_ns,
                        self.session_id,
                        name,
                        value,
                    ]
                )

        if not rows:
            return 0

        try:
            self.ch_client.insert(
                self.TABLE_NAME,
                data=rows,
                column_names=[
                    "ticker",
                    "timestamp_ns",
                    "session",
                    "factor_name",
                    "factor_value",
                ],
            )
            logger.debug(f"FactorStorage: Batch wrote {len(rows)} factor rows")
            return len(rows)
        except Exception as e:
            logger.error(f"FactorStorage: Batch write failed - {e}")
            return 0

    def query_factors(
        self,
        ticker: str,
        start_ns: int,
        end_ns: int,
        factor_names: list[str] | None = None,
    ) -> list[dict]:
        """Query historical factors from ClickHouse.

        Args:
            ticker: Symbol to query
            start_ns: Start timestamp in nanoseconds
            end_ns: End timestamp in nanoseconds
            factor_names: Optional list of factor names to filter

        Returns:
            List of dicts with ticker, timestamp_ns, factor_name, factor_value
        """
        if not self.ch_client:
            return []

        try:
            factor_filter = ""
            params: dict[str, Any] = {
                "ticker": ticker,
                "start_ns": start_ns,
                "end_ns": end_ns,
            }

            if factor_names:
                factor_filter = "AND factor_name IN {factor_names:Array(String)}"
                params["factor_names"] = factor_names

            query = f"""
                SELECT ticker, timestamp_ns, factor_name, factor_value
                FROM {self.TABLE_NAME} FINAL
                WHERE ticker = {{ticker:String}}
                  AND timestamp_ns >= {{start_ns:Int64}}
                  AND timestamp_ns < {{end_ns:Int64}}
                  {factor_filter}
                ORDER BY timestamp_ns ASC
            """

            result = self.ch_client.query(query, parameters=params)

            return [
                {
                    "ticker": row[0],
                    "timestamp_ns": row[1],
                    "factor_name": row[2],
                    "factor_value": row[3],
                }
                for row in result.result_rows
            ]
        except Exception as e:
            logger.error(f"FactorStorage: Query failed - {e}")
            return []
