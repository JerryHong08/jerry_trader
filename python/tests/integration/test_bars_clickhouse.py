"""
Smoke test: Rust BarBuilder → ClickHouse write path.

Bypasses UnifiedTickManager and Redis.  Directly feeds trades into BarBuilder,
collects completed bars, inserts them via the same _flush_to_clickhouse logic,
and reads them back from ClickHouse.

Requires:
    - ClickHouse running on localhost:8123
    - CLICKHOUSE_PASSWORD env var set
    - jerry_trader.ohlcv_bars table exists (sql/clickhouse_ohlcv.sql)
"""

import os
import time

import clickhouse_connect
import pytest

from jerry_trader._rust import BarBuilder

# ── helpers ──────────────────────────────────────────────────────────────

CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "default"
CH_DB = "jerry_trader"
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

COLUMNS = [
    "ticker",
    "timeframe",
    "bar_start",
    "bar_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
    "session",
]


def _get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )


def _bar_to_row(bar: dict) -> list:
    return [bar[c] for c in COLUMNS]


# ── Skip if ClickHouse is unavailable ────────────────────────────────────


@pytest.fixture(scope="module")
def ch_client():
    """Module-scoped ClickHouse client; skip the entire module if unavailable."""
    if not CH_PASSWORD:
        pytest.skip("CLICKHOUSE_PASSWORD not set")
    try:
        client = _get_ch_client()
        # quick connectivity check
        client.command("SELECT 1")
    except Exception as exc:
        pytest.skip(f"ClickHouse unavailable: {exc}")
    yield client
    client.close()


@pytest.fixture(autouse=True)
def _clean_test_rows(ch_client):
    """Delete TEST_* rows after each test so tests are idempotent."""
    yield
    ch_client.command("ALTER TABLE ohlcv_bars DELETE WHERE ticker LIKE 'TEST_%'")
    # ReplacingMergeTree mutations are async; force sync for determinism
    ch_client.command("OPTIMIZE TABLE ohlcv_bars FINAL")


# ── Tests ────────────────────────────────────────────────────────────────


class TestBarBuilderClickHouseWritePath:
    """End-to-end: generate bars with Rust BarBuilder, write to ClickHouse."""

    def test_single_completed_bar_10s(self, ch_client):
        """Feed enough trades to complete a 10s bar, insert, read back."""
        builder = BarBuilder(["10s"])

        # 2025-01-06 14:30:00 UTC = 09:30:00 ET (regular session, Monday)
        base = 1_736_171_400_000

        # Trades inside first 10s window
        completed = []
        completed += builder.ingest_trade("TEST_A", 100.0, 10.0, base + 0)
        completed += builder.ingest_trade("TEST_A", 102.0, 5.0, base + 3000)
        completed += builder.ingest_trade("TEST_A", 99.0, 8.0, base + 7000)
        # Nothing completed yet (same window)
        assert len(completed) == 0

        # Trade at t=10s triggers completion of first bar
        completed += builder.ingest_trade("TEST_A", 101.0, 6.0, base + 10_000)
        assert len(completed) == 1

        bar = completed[0]
        assert bar["ticker"] == "TEST_A"
        assert bar["timeframe"] == "10s"
        assert bar["open"] == 100.0
        assert bar["high"] == 102.0
        assert bar["low"] == 99.0
        assert bar["close"] == 99.0
        assert bar["trade_count"] == 3

        # Write to ClickHouse
        rows = [_bar_to_row(bar)]
        ch_client.insert(table="ohlcv_bars", data=rows, column_names=COLUMNS)

        # Read back
        result = ch_client.query(
            "SELECT * FROM ohlcv_bars FINAL "
            "WHERE ticker = 'TEST_A' AND timeframe = '10s'"
        )
        assert len(result.result_rows) == 1
        row = dict(zip(result.column_names, result.result_rows[0]))
        assert row["open"] == 100.0
        assert row["high"] == 102.0
        assert row["low"] == 99.0
        assert row["close"] == 99.0
        assert row["trade_count"] == 3

    def test_multiple_bars_1m(self, ch_client):
        """Generate several 1m bars, batch-insert, count rows."""
        builder = BarBuilder(["1m"])

        # Within regular session: 2025-01-06 14:30 UTC = 09:30 ET
        base = 1_736_171_400_000  # Mon 2025-01-06 14:30:00 UTC

        completed = []
        for minute in range(4):  # 4 minutes of trades
            for sec in range(0, 60, 5):
                ts = base + (minute * 60 + sec) * 1000
                completed += builder.ingest_trade(
                    "TEST_B", 200.0 + minute + sec * 0.01, 1.0, ts
                )

        # Flush any in-progress bar
        completed += builder.flush()

        # Should have at least 3 completed (minute 0, 1, 2) + 1 flushed (minute 3)
        assert len(completed) >= 3

        rows = [_bar_to_row(b) for b in completed]
        ch_client.insert(table="ohlcv_bars", data=rows, column_names=COLUMNS)

        result = ch_client.query(
            "SELECT count() FROM ohlcv_bars FINAL "
            "WHERE ticker = 'TEST_B' AND timeframe = '1m'"
        )
        count = result.result_rows[0][0]
        assert count == len(completed)

    def test_query_bars_round_trip(self, ch_client):
        """Insert bars and use parameterized query (same as BarsBuilderService.query_bars)."""
        builder = BarBuilder(["5m"])

        # 2025-01-06 15:00 UTC = 10:00 ET (regular session)
        base = 1_736_173_200_000

        completed = []
        for i in range(15):  # 15 minutes → 3 completed 5m bars
            ts = base + i * 60_000
            completed += builder.ingest_trade("TEST_C", 300.0 + i, 2.0, ts)
        completed += builder.flush()

        assert len(completed) >= 2  # at least 2 completed + 1 flushed

        rows = [_bar_to_row(b) for b in completed]
        ch_client.insert(table="ohlcv_bars", data=rows, column_names=COLUMNS)

        # Parameterized query (matches BarsBuilderService.query_bars)
        query = """
            SELECT ticker, timeframe, bar_start, bar_end,
                   open, high, low, close,
                   volume, trade_count, vwap, session
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
                "ticker": "TEST_C",
                "timeframe": "5m",
                "start_ms": base,
                "end_ms": base + 20 * 60_000,
                "limit": 100,
            },
        )
        bars = [dict(zip(COLUMNS, row)) for row in result.result_rows]
        assert len(bars) >= 2
        assert all(b["ticker"] == "TEST_C" for b in bars)
        assert all(b["timeframe"] == "5m" for b in bars)
        # Bars should be in ascending order
        starts = [b["bar_start"] for b in bars]
        assert starts == sorted(starts)

    def test_upsert_idempotent(self, ch_client):
        """ReplacingMergeTree deduplicates same (ticker, timeframe, bar_start)."""
        builder = BarBuilder(["10s"])

        # 2025-01-06 14:30:00 UTC = 09:30:00 ET (regular session, Monday)
        base = 1_736_171_400_000
        builder.ingest_trade("TEST_D", 50.0, 1.0, base)
        completed = builder.ingest_trade("TEST_D", 55.0, 1.0, base + 10_000)
        assert len(completed) == 1

        row = [_bar_to_row(completed[0])]

        # Insert same bar twice
        ch_client.insert(table="ohlcv_bars", data=row, column_names=COLUMNS)
        time.sleep(0.1)
        ch_client.insert(table="ohlcv_bars", data=row, column_names=COLUMNS)

        # OPTIMIZE FINAL forces merge → deduplicate
        ch_client.command("OPTIMIZE TABLE ohlcv_bars FINAL")

        result = ch_client.query(
            "SELECT count() FROM ohlcv_bars FINAL "
            "WHERE ticker = 'TEST_D' AND timeframe = '10s'"
        )
        assert result.result_rows[0][0] == 1
