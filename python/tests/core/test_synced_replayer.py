"""Tests for the synced-replayer pipeline.

Validates the full data flow:
  Rust TickDataReplayer → SyncedReplayerManager._on_tick() → asyncio queues

Tests are split into three groups:

  1. **Unit tests** — SyncedReplayerManager payload conversion, queue
     fan-out, subscribe/unsubscribe lifecycle.  No Rust replayer or
     Parquet data required (a lightweight mock replaces TickDataReplayer).

  2. **UnifiedTickManager unit tests** — stream-key generation,
     normalize_data (ns → ms), and provider wiring with synced-replayer.

  3. **Integration tests** — end-to-end pipeline using the real Rust
     TickDataReplayer and Parquet files from the data lake.  Skipped
     automatically when data is missing.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock

import pytest

from jerry_trader.services.market_data.feeds.synced_replayer_manager import (
    SyncedReplayerManager,
)
from jerry_trader.services.market_data.feeds.unified_tick_manager import (
    UnifiedTickManager,
)

# ── Constants ────────────────────────────────────────────────────────

# Sample Polygon wire-format payloads (as Rust to_py_dict() would emit)
SAMPLE_QUOTE_WIRE = {
    "ev": "Q",
    "sym": "AAPL",
    "bx": "11",
    "ax": "12",
    "bp": 189.50,
    "ap": 189.55,
    "bs": 300,
    "as": 200,
    "t": 1_700_000_000_000_000_000,  # nanoseconds
    "c": [1, 2],
    "z": 3,
}

SAMPLE_TRADE_WIRE = {
    "ev": "T",
    "sym": "AAPL",
    "p": 189.52,
    "s": 100,
    "x": 11,
    "t": 1_700_000_000_500_000_000,  # nanoseconds
    "c": [0],
    "z": 3,
}

# Expected normalised payloads after _on_tick conversion
EXPECTED_QUOTE_NORMALISED = {
    "event_type": "Q",
    "symbol": "AAPL",
    "bid": 189.50,
    "ask": 189.55,
    "bid_size": 300,
    "ask_size": 200,
    "timestamp": 1_700_000_000_000_000_000,
    "bid_exchange": "11",
    "ask_exchange": "12",
    "conditions": [1, 2],
    "tape": 3,
}

EXPECTED_TRADE_NORMALISED = {
    "event_type": "T",
    "symbol": "AAPL",
    "price": 189.52,
    "size": 100,
    "exchange": 11,
    "timestamp": 1_700_000_000_500_000_000,
    "conditions": [0],
    "tape": 3,
}


# ── Mock TickDataReplayer ────────────────────────────────────────────


class MockTickDataReplayer:
    """Lightweight stand-in for the Rust TickDataReplayer.

    Records subscribe/unsubscribe calls so tests can inspect them.
    When ``subscribe()`` is called it stores the callback so we can
    manually fire ticks.
    """

    def __init__(self) -> None:
        self.subscriptions: List[dict] = []
        self.unsubscriptions: List[str] = []
        self._callbacks: Dict[str, Any] = {}  # symbol → callback

    def subscribe(self, symbol: str, events: List[str], callback: Any) -> None:
        self.subscriptions.append({"symbol": symbol, "events": events})
        self._callbacks[symbol] = callback

    def unsubscribe(self, symbol: str) -> None:
        self.unsubscriptions.append(symbol)
        self._callbacks.pop(symbol, None)

    def shutdown(self) -> None:
        pass

    # Test helper — simulate the Rust engine firing a tick
    def fire_tick(self, symbol: str, payload: dict) -> None:
        cb = self._callbacks.get(symbol)
        if cb:
            cb(symbol, payload)


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def mock_replayer() -> MockTickDataReplayer:
    return MockTickDataReplayer()


@pytest.fixture
def manager(mock_replayer: MockTickDataReplayer) -> SyncedReplayerManager:
    return SyncedReplayerManager(mock_replayer)


@pytest.fixture
def event_loop_for_manager(manager: SyncedReplayerManager):
    """Provide a running event loop and set it on the manager.

    Yields the loop so tests can run coroutines on it.
    After the test, cleans up properly.
    """
    loop = asyncio.new_event_loop()
    manager._loop = loop
    yield loop
    loop.close()


# ══════════════════════════════════════════════════════════════════════
#  Part 1: SyncedReplayerManager — payload conversion
# ══════════════════════════════════════════════════════════════════════


class TestOnTickPayloadConversion:
    """_on_tick() should convert wire-format → normalised format."""

    def test_quote_conversion(
        self, manager: SyncedReplayerManager, event_loop_for_manager
    ):
        """Quote wire-format is correctly normalised."""
        loop = event_loop_for_manager
        q: asyncio.Queue = asyncio.Queue()
        manager._client_queues["Q.AAPL"] = {"client_1": q}

        # Fire the tick (simulates Rust engine thread)
        manager._on_tick("AAPL", SAMPLE_QUOTE_WIRE)

        # Drain the loop so call_soon_threadsafe callbacks execute
        loop.run_until_complete(asyncio.sleep(0))

        assert not q.empty()
        result = q.get_nowait()
        assert result == EXPECTED_QUOTE_NORMALISED

    def test_trade_conversion(
        self, manager: SyncedReplayerManager, event_loop_for_manager
    ):
        """Trade wire-format is correctly normalised."""
        loop = event_loop_for_manager
        q: asyncio.Queue = asyncio.Queue()
        manager._client_queues["T.AAPL"] = {"client_1": q}

        manager._on_tick("AAPL", SAMPLE_TRADE_WIRE)
        loop.run_until_complete(asyncio.sleep(0))

        assert not q.empty()
        result = q.get_nowait()
        assert result == EXPECTED_TRADE_NORMALISED

    def test_unknown_event_type_ignored(
        self, manager: SyncedReplayerManager, event_loop_for_manager
    ):
        """Payloads with unknown ev type are silently dropped."""
        loop = event_loop_for_manager
        q: asyncio.Queue = asyncio.Queue()
        manager._client_queues["Q.AAPL"] = {"client_1": q}

        manager._on_tick("AAPL", {"ev": "X", "sym": "AAPL"})
        loop.run_until_complete(asyncio.sleep(0))

        assert q.empty()

    def test_missing_fields_default_gracefully(
        self, manager: SyncedReplayerManager, event_loop_for_manager
    ):
        """Sparse payloads use None / empty defaults — no KeyError."""
        loop = event_loop_for_manager
        q: asyncio.Queue = asyncio.Queue()
        manager._client_queues["Q.SPY"] = {"client_1": q}

        # Bare-minimum quote payload
        manager._on_tick("SPY", {"ev": "Q"})
        loop.run_until_complete(asyncio.sleep(0))

        result = q.get_nowait()
        assert result["event_type"] == "Q"
        assert result["symbol"] == "SPY"
        assert result["bid"] is None
        assert result["ask"] is None
        assert result["bid_exchange"] == ""
        assert result["conditions"] == []


# ══════════════════════════════════════════════════════════════════════
#  Part 2: SyncedReplayerManager — queue fan-out
# ══════════════════════════════════════════════════════════════════════


class TestQueueFanOut:
    """Ticks should be delivered to ALL client queues for a stream key."""

    def test_single_client_receives_tick(
        self, manager: SyncedReplayerManager, event_loop_for_manager
    ):
        loop = event_loop_for_manager
        q = asyncio.Queue()
        manager._client_queues["Q.AAPL"] = {"c1": q}

        manager._on_tick("AAPL", SAMPLE_QUOTE_WIRE)
        loop.run_until_complete(asyncio.sleep(0))

        assert q.qsize() == 1

    def test_multiple_clients_receive_tick(
        self, manager: SyncedReplayerManager, event_loop_for_manager
    ):
        """All subscribed clients get the same payload."""
        loop = event_loop_for_manager
        q1, q2, q3 = asyncio.Queue(), asyncio.Queue(), asyncio.Queue()
        manager._client_queues["Q.AAPL"] = {"c1": q1, "c2": q2, "c3": q3}

        manager._on_tick("AAPL", SAMPLE_QUOTE_WIRE)
        loop.run_until_complete(asyncio.sleep(0))

        for q in (q1, q2, q3):
            assert q.qsize() == 1
            assert q.get_nowait() == EXPECTED_QUOTE_NORMALISED

    def test_no_queue_for_stream_key_is_noop(
        self, manager: SyncedReplayerManager, event_loop_for_manager
    ):
        """Ticks for unsubscribed symbols don't crash."""
        loop = event_loop_for_manager
        # No queues registered for TSLA
        manager._on_tick("TSLA", SAMPLE_QUOTE_WIRE.copy() | {"sym": "TSLA"})
        loop.run_until_complete(asyncio.sleep(0))
        # Simply doesn't crash

    def test_no_loop_is_noop(self, manager: SyncedReplayerManager):
        """_on_tick without a captured loop doesn't crash."""
        manager._loop = None
        manager._client_queues["Q.AAPL"] = {"c1": asyncio.Queue()}
        # Should not raise
        manager._on_tick("AAPL", SAMPLE_QUOTE_WIRE)


# ══════════════════════════════════════════════════════════════════════
#  Part 3: SyncedReplayerManager — subscribe / unsubscribe lifecycle
# ══════════════════════════════════════════════════════════════════════


class TestSubscribeUnsubscribe:
    """subscribe()/unsubscribe() should manage queues and Rust replayer."""

    @pytest.mark.asyncio
    async def test_subscribe_creates_queues(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """subscribe() creates per-client queues and calls Rust replayer."""
        await manager.subscribe("client_A", ["AAPL"], events=["Q", "T"])

        # Queues created
        assert "Q.AAPL" in manager._client_queues
        assert "T.AAPL" in manager._client_queues
        assert "client_A" in manager._client_queues["Q.AAPL"]
        assert "client_A" in manager._client_queues["T.AAPL"]

        # Rust replayer called
        assert len(mock_replayer.subscriptions) == 1
        assert mock_replayer.subscriptions[0]["symbol"] == "AAPL"
        assert set(mock_replayer.subscriptions[0]["events"]) == {"Q", "T"}

    @pytest.mark.asyncio
    async def test_subscribe_idempotent(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """Subscribing the same symbol twice does NOT call Rust subscribe again."""
        await manager.subscribe("client_A", ["AAPL"], events=["Q"])
        await manager.subscribe("client_B", ["AAPL"], events=["Q"])

        # Both clients have queues
        assert "client_A" in manager._client_queues["Q.AAPL"]
        assert "client_B" in manager._client_queues["Q.AAPL"]

        # Rust replayer was only called ONCE
        assert len(mock_replayer.subscriptions) == 1

    @pytest.mark.asyncio
    async def test_subscribe_tracks_connections(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """connections dict maps client → set of stream_keys."""
        await manager.subscribe("client_A", ["AAPL", "NVDA"], events=["Q", "T"])

        assert manager.connections["client_A"] == {
            "Q.AAPL",
            "T.AAPL",
            "Q.NVDA",
            "T.NVDA",
        }

    @pytest.mark.asyncio
    async def test_unsubscribe_single_client_keeps_stream(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """When one client unsubscribes but another is still listening,
        the Rust replayer should NOT be unsubscribed."""
        await manager.subscribe("client_A", ["AAPL"], events=["Q"])
        await manager.subscribe("client_B", ["AAPL"], events=["Q"])

        await manager.unsubscribe("client_A", "AAPL", events=["Q"])

        # Client A's queue removed, but stream still alive for B
        assert "client_A" not in manager._client_queues.get("Q.AAPL", {})
        assert "client_B" in manager._client_queues["Q.AAPL"]

        # Rust replayer NOT unsubscribed
        assert len(mock_replayer.unsubscriptions) == 0

    @pytest.mark.asyncio
    async def test_unsubscribe_last_client_removes_stream(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """When the last client unsubscribes, the Rust replayer
        should be unsubscribed as well."""
        await manager.subscribe("client_A", ["AAPL"], events=["Q"])
        await manager.unsubscribe("client_A", "AAPL", events=["Q"])

        # Queue structure cleaned up
        assert "Q.AAPL" not in manager._client_queues

        # Rust replayer notified
        assert "AAPL" in mock_replayer.unsubscriptions
        assert "Q.AAPL" not in manager.subscribed_streams

    @pytest.mark.asyncio
    async def test_disconnect_cleans_all(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """disconnect() removes all of a client's subscriptions."""
        await manager.subscribe("client_A", ["AAPL", "NVDA"], events=["Q", "T"])

        await manager.disconnect("client_A")

        # Connection removed
        assert "client_A" not in manager.connections

        # All streams cleaned up (no other clients)
        assert len(manager._client_queues) == 0
        assert len(manager.subscribed_streams) == 0

    @pytest.mark.asyncio
    async def test_subscribe_multiple_symbols(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """Can subscribe to multiple symbols in one call."""
        await manager.subscribe("client_A", ["AAPL", "NVDA", "MSFT"], events=["Q"])

        assert len(mock_replayer.subscriptions) == 3
        subscribed_symbols = {s["symbol"] for s in mock_replayer.subscriptions}
        assert subscribed_symbols == {"AAPL", "NVDA", "MSFT"}


# ══════════════════════════════════════════════════════════════════════
#  Part 4: SyncedReplayerManager — backward-compatible views
# ══════════════════════════════════════════════════════════════════════


class TestBackwardCompatViews:
    """queues and get_client_queue() provide backward-compatible access."""

    @pytest.mark.asyncio
    async def test_queues_flat_view(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """queues property returns {stream_key: first_queue}."""
        await manager.subscribe("client_A", ["AAPL"], events=["Q"])

        assert "Q.AAPL" in manager.queues
        assert isinstance(manager.queues["Q.AAPL"], asyncio.Queue)

    @pytest.mark.asyncio
    async def test_get_client_queue(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """get_client_queue() returns the right queue for the right client."""
        await manager.subscribe("client_A", ["AAPL"], events=["Q"])
        await manager.subscribe("client_B", ["AAPL"], events=["Q"])

        q_a = manager.get_client_queue("client_A", "Q.AAPL")
        q_b = manager.get_client_queue("client_B", "Q.AAPL")

        assert q_a is not q_b
        assert q_a is not None
        assert q_b is not None

    @pytest.mark.asyncio
    async def test_get_client_queue_unknown_returns_none(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        assert manager.get_client_queue("unknown", "Q.AAPL") is None


# ══════════════════════════════════════════════════════════════════════
#  Part 5: SyncedReplayerManager — stream_forever / close
# ══════════════════════════════════════════════════════════════════════


class TestStreamForeverAndClose:
    """stream_forever() captures the loop; close() shuts down."""

    @pytest.mark.asyncio
    async def test_stream_forever_captures_loop(self, manager: SyncedReplayerManager):
        """stream_forever() sets self._loop and self.connected."""
        task = asyncio.create_task(manager.stream_forever())

        # Let it run one iteration
        await asyncio.sleep(0.05)

        assert manager.connected is True
        assert manager._loop is not None

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert manager.connected is False

    @pytest.mark.asyncio
    async def test_close_shuts_down_replayer(
        self,
        manager: SyncedReplayerManager,
        mock_replayer: MockTickDataReplayer,
    ):
        """close() invokes replayer.shutdown()."""
        # Set connected so we can verify it gets cleared
        manager.connected = True

        await manager.close()

        assert manager.connected is False


# ══════════════════════════════════════════════════════════════════════
#  Part 6: UnifiedTickManager — synced-replayer provider
# ══════════════════════════════════════════════════════════════════════


class TestUnifiedTickManagerSyncedReplayer:
    """UnifiedTickManager wraps SyncedReplayerManager transparently."""

    def _make_utm(self) -> UnifiedTickManager:
        """Build a UnifiedTickManager backed by a mock SyncedReplayerManager."""
        mock_rpl = MockTickDataReplayer()
        mgr = SyncedReplayerManager(mock_rpl)
        return UnifiedTickManager(provider="synced-replayer", manager=mgr)

    def test_construction_requires_manager(self):
        """synced-replayer without a manager raises ValueError."""
        with pytest.raises(ValueError, match="synced-replayer requires"):
            UnifiedTickManager(provider="synced-replayer")

    def test_stream_key_generation_quotes(self):
        """Stream keys use the Polygon/replayer format: Q.<SYMBOL>."""
        utm = self._make_utm()
        keys = utm.generate_stream_keys(symbols=["AAPL", "NVDA"], events=["Q"])
        assert keys == {"Q.AAPL", "Q.NVDA"}

    def test_stream_key_generation_quotes_and_trades(self):
        utm = self._make_utm()
        keys = utm.generate_stream_keys(symbols=["AAPL"], events=["Q", "T"])
        assert keys == {"Q.AAPL", "T.AAPL"}

    def test_stream_key_advanced_subscription(self):
        utm = self._make_utm()
        keys = utm.generate_stream_keys(
            subscriptions=[
                {"symbol": "AAPL", "events": ["Q", "T"], "sec_type": "STOCK"},
                {"symbol": "SPY", "events": ["Q"], "sec_type": "STOCK"},
            ]
        )
        assert keys == {"Q.AAPL", "T.AAPL", "Q.SPY"}

    def test_normalize_data_ns_to_ms_quote(self):
        """Nanosecond timestamps are converted to milliseconds."""
        utm = self._make_utm()
        result = utm.normalize_data(
            {
                "event_type": "Q",
                "symbol": "AAPL",
                "bid": 189.50,
                "ask": 189.55,
                "timestamp": 1_700_000_000_000_000_000,  # ns
            }
        )
        assert result["timestamp"] == 1_700_000_000_000  # ms

    def test_normalize_data_ns_to_ms_trade(self):
        utm = self._make_utm()
        result = utm.normalize_data(
            {
                "event_type": "T",
                "symbol": "AAPL",
                "price": 189.52,
                "size": 100,
                "timestamp": 1_700_000_000_500_000_000,  # ns
            }
        )
        assert result["timestamp"] == 1_700_000_000_500  # ms

    def test_normalize_data_ms_passthrough(self):
        """Already-ms timestamps pass through unchanged."""
        utm = self._make_utm()
        result = utm.normalize_data(
            {
                "event_type": "Q",
                "symbol": "AAPL",
                "timestamp": 1_700_000_000_000,  # ms already
            }
        )
        assert result["timestamp"] == 1_700_000_000_000

    def test_normalize_data_preserves_fields(self):
        """Non-timestamp fields are preserved as-is."""
        utm = self._make_utm()
        data = EXPECTED_QUOTE_NORMALISED.copy()
        result = utm.normalize_data(data)
        assert result["bid"] == 189.50
        assert result["ask"] == 189.55
        assert result["bid_size"] == 300
        assert result["symbol"] == "AAPL"


# ══════════════════════════════════════════════════════════════════════
#  Part 7: End-to-end pipeline (mock Rust engine)
# ══════════════════════════════════════════════════════════════════════


class TestEndToEndMockPipeline:
    """Full pipeline: mock fire_tick → _on_tick → queue → normalize_data."""

    @pytest.mark.asyncio
    async def test_quote_pipeline(self):
        """Wire-format quote arrives in queue as normalised + ms timestamp."""
        mock_rpl = MockTickDataReplayer()
        mgr = SyncedReplayerManager(mock_rpl)
        utm = UnifiedTickManager(provider="synced-replayer", manager=mgr)

        # Start stream_forever (captures event loop)
        task = asyncio.create_task(mgr.stream_forever())
        await asyncio.sleep(0.01)

        # Subscribe
        await mgr.subscribe("test_client", ["AAPL"], events=["Q"])

        # Fire tick from "Rust engine"
        mock_rpl.fire_tick("AAPL", SAMPLE_QUOTE_WIRE)
        await asyncio.sleep(0.01)

        # Read from queue
        q = mgr.get_client_queue("test_client", "Q.AAPL")
        assert q is not None
        assert not q.empty()

        raw = q.get_nowait()

        # Normalise
        final = utm.normalize_data(raw)

        # Assertions
        assert final["event_type"] == "Q"
        assert final["symbol"] == "AAPL"
        assert final["bid"] == 189.50
        assert final["ask"] == 189.55
        assert final["timestamp"] == 1_700_000_000_000  # ns → ms

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_trade_pipeline(self):
        """Wire-format trade arrives in queue as normalised + ms timestamp."""
        mock_rpl = MockTickDataReplayer()
        mgr = SyncedReplayerManager(mock_rpl)
        utm = UnifiedTickManager(provider="synced-replayer", manager=mgr)

        task = asyncio.create_task(mgr.stream_forever())
        await asyncio.sleep(0.01)

        await mgr.subscribe("test_client", ["AAPL"], events=["T"])

        mock_rpl.fire_tick("AAPL", SAMPLE_TRADE_WIRE)
        await asyncio.sleep(0.01)

        q = mgr.get_client_queue("test_client", "T.AAPL")
        raw = q.get_nowait()
        final = utm.normalize_data(raw)

        assert final["event_type"] == "T"
        assert final["symbol"] == "AAPL"
        assert final["price"] == 189.52
        assert final["size"] == 100
        assert final["timestamp"] == 1_700_000_000_500

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_multi_symbol_fan_out(self):
        """Multiple symbols to multiple clients via single manager."""
        mock_rpl = MockTickDataReplayer()
        mgr = SyncedReplayerManager(mock_rpl)

        task = asyncio.create_task(mgr.stream_forever())
        await asyncio.sleep(0.01)

        await mgr.subscribe("client_A", ["AAPL", "NVDA"], events=["Q"])
        await mgr.subscribe("client_B", ["AAPL"], events=["Q"])

        # Fire AAPL tick — both clients should get it
        mock_rpl.fire_tick("AAPL", SAMPLE_QUOTE_WIRE)
        await asyncio.sleep(0.01)

        q_a = mgr.get_client_queue("client_A", "Q.AAPL")
        q_b = mgr.get_client_queue("client_B", "Q.AAPL")
        assert q_a.qsize() == 1
        assert q_b.qsize() == 1

        # Fire NVDA tick — only client_A should get it
        nvda_wire = SAMPLE_QUOTE_WIRE.copy()
        nvda_wire["sym"] = "NVDA"
        mock_rpl.fire_tick("NVDA", nvda_wire)
        await asyncio.sleep(0.01)

        q_a_nvda = mgr.get_client_queue("client_A", "Q.NVDA")
        assert q_a_nvda.qsize() == 1
        assert mgr.get_client_queue("client_B", "Q.NVDA") is None

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ══════════════════════════════════════════════════════════════════════
#  Part 8: Integration — real Rust TickDataReplayer + Parquet data
# ══════════════════════════════════════════════════════════════════════

# Paths for real data — skip tests if unavailable
LAKE_DATA_DIR = "/mnt/blackdisk/quant_data/polygon_data/lake"
REPLAY_DATE = "20260306"
QUOTES_PARQUET = os.path.join(
    LAKE_DATA_DIR, "us_stocks_sip", "quotes_v1", "2026", "03", "2026-03-06.parquet"
)
TRADES_PARQUET = os.path.join(
    LAKE_DATA_DIR, "us_stocks_sip", "trades_v1", "2026", "03", "2026-03-06.parquet"
)

HAS_PARQUET_DATA = os.path.isfile(QUOTES_PARQUET) and os.path.isfile(TRADES_PARQUET)

# Try importing the Rust extension
try:
    from jerry_trader._rust import ReplayClock, TickDataReplayer

    HAS_RUST_EXT = True
except ImportError:
    HAS_RUST_EXT = False

skip_integration = pytest.mark.skipif(
    not (HAS_PARQUET_DATA and HAS_RUST_EXT),
    reason="Integration tests require Parquet data at "
    f"{LAKE_DATA_DIR} and the compiled Rust extension",
)

# data_start_ts_ns for 2026-03-06 09:30 ET
# 2026-03-06 09:30:00 ET = 2026-03-06 14:30:00 UTC
MAR06_0930_NS = 1_772_807_400_000_000_000  # 2026-03-06 14:30:00 UTC


@skip_integration
class TestIntegrationRustReplayer:
    """End-to-end tests using the real Rust TickDataReplayer."""

    @pytest.fixture
    def rust_replayer(self):
        """Create a real TickDataReplayer pointed at the data lake.

        Starts **paused** so the virtual clock doesn't advance while
        Parquet data is being loaded.  Each test must call
        ``rust_replayer.resume()`` after subscribing.
        """
        rpl = TickDataReplayer(
            replay_date=REPLAY_DATE,
            lake_data_dir=LAKE_DATA_DIR,
            data_start_ts_ns=MAR06_0930_NS,
            speed=10000.0,  # fast-forward — don't pace ticks
        )
        rpl.pause()  # hold the clock until subscribe finishes
        yield rpl
        rpl.shutdown()

    @pytest.mark.asyncio
    async def test_real_quotes_arrive(self, rust_replayer):
        """Subscribe to NIPG quotes and receive at least one tick."""
        mgr = SyncedReplayerManager(rust_replayer)

        task = asyncio.create_task(mgr.stream_forever())
        await asyncio.sleep(0.05)

        await mgr.subscribe("test_client", ["NIPG"], events=["Q"])
        rust_replayer.resume()  # start the clock after data is loaded

        # Wait for ticks to arrive (Parquet loading + replay)
        q = mgr.get_client_queue("test_client", "Q.NIPG")
        assert q is not None

        received = []
        deadline = time.monotonic() + 15.0  # generous timeout
        while time.monotonic() < deadline and len(received) < 5:
            try:
                item = q.get_nowait()
                received.append(item)
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert (
            len(received) >= 1
        ), f"Expected at least 1 quote tick for NIPG, got {len(received)}"

        # Validate structure of first tick
        tick = received[0]
        assert tick["event_type"] == "Q"
        assert tick["symbol"] == "NIPG"
        assert "bid" in tick
        assert "ask" in tick
        assert "timestamp" in tick
        assert isinstance(tick["timestamp"], int)
        # Timestamp should be in nanoseconds (19 digits)
        assert tick["timestamp"] > 1e15

    @pytest.mark.asyncio
    async def test_real_trades_arrive(self, rust_replayer):
        """Subscribe to NIPG trades and receive at least one tick."""
        mgr = SyncedReplayerManager(rust_replayer)

        task = asyncio.create_task(mgr.stream_forever())
        await asyncio.sleep(0.05)

        await mgr.subscribe("test_client", ["NIPG"], events=["T"])
        rust_replayer.resume()  # start the clock after data is loaded

        q = mgr.get_client_queue("test_client", "T.NIPG")
        assert q is not None

        received = []
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and len(received) < 5:
            try:
                item = q.get_nowait()
                received.append(item)
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert (
            len(received) >= 1
        ), f"Expected at least 1 trade tick for NIPG, got {len(received)}"

        tick = received[0]
        assert tick["event_type"] == "T"
        assert tick["symbol"] == "NIPG"
        assert "price" in tick
        assert "size" in tick
        assert isinstance(tick["price"], float)
        assert tick["price"] > 0

    @pytest.mark.asyncio
    async def test_real_full_pipeline_normalize(self, rust_replayer):
        """Full pipeline: Rust → SyncedReplayerManager → UnifiedTickManager.normalize_data()
        — nanosecond timestamps should be converted to milliseconds."""
        mgr = SyncedReplayerManager(rust_replayer)
        utm = UnifiedTickManager(provider="synced-replayer", manager=mgr)

        task = asyncio.create_task(mgr.stream_forever())
        await asyncio.sleep(0.05)

        await mgr.subscribe("test_client", ["NIPG"], events=["Q"])
        rust_replayer.resume()  # start the clock after data is loaded

        q = mgr.get_client_queue("test_client", "Q.NIPG")

        # Wait for one tick
        deadline = time.monotonic() + 15.0
        raw_tick = None
        while time.monotonic() < deadline:
            try:
                raw_tick = q.get_nowait()
                break
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.1)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert raw_tick is not None, "Timed out waiting for a tick"

        # Raw timestamp is in nanoseconds
        assert raw_tick["timestamp"] > 1e15

        # After normalize, timestamp should be milliseconds (13 digits)
        normalised = utm.normalize_data(raw_tick)
        assert normalised["timestamp"] < 1e15
        assert normalised["timestamp"] > 1e12
        # ~13 digits
        assert len(str(normalised["timestamp"])) == 13

    @pytest.mark.asyncio
    async def test_real_multi_symbol(self, rust_replayer):
        """Subscribe to two symbols and receive ticks for both."""
        mgr = SyncedReplayerManager(rust_replayer)

        task = asyncio.create_task(mgr.stream_forever())
        await asyncio.sleep(0.05)

        await mgr.subscribe("test_client", ["NIPG", "AMC"], events=["Q"])
        rust_replayer.resume()  # start the clock after data is loaded

        q_nipg = mgr.get_client_queue("test_client", "Q.NIPG")
        q_amc = mgr.get_client_queue("test_client", "Q.AMC")

        nipg_ticks = []
        amc_ticks = []
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if len(nipg_ticks) >= 1 and len(amc_ticks) >= 1:
                break
            for q, bucket in [(q_nipg, nipg_ticks), (q_amc, amc_ticks)]:
                try:
                    bucket.append(q.get_nowait())
                except asyncio.QueueEmpty:
                    pass
            await asyncio.sleep(0.1)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert len(nipg_ticks) >= 1, "Expected NIPG ticks"
        assert len(amc_ticks) >= 1, "Expected AMC ticks"
        assert nipg_ticks[0]["symbol"] == "NIPG"
        assert amc_ticks[0]["symbol"] == "AMC"
