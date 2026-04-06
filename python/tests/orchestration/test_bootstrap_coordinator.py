"""Test suite for Bootstrap Coordinator V2.

Tests the unified bootstrap orchestration flow:
- Coordinator core state machine
- BarsBuilder integration (store_trades, on_bars_ready)
- FactorEngine integration (consumer registration, report_done)
- End-to-end flow (subscribe → bootstrap → ready)
"""

import gzip
import pickle
import threading
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest
import redis

from jerry_trader.services.orchestration.bootstrap_coordinator import (
    BOOTSTRAP_TIMEFRAMES,
    TRADES_ONLY_TIMEFRAMES,
    BootstrapCoordinator,
    TickerBootstrap,
    TimeframeBootstrap,
    TimeframeState,
    TradesBootstrapState,
    get_coordinator,
    set_coordinator,
)


class TestBootstrapCoordinatorCore:
    """Unit tests for BootstrapCoordinator core functionality."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        mock = MagicMock()
        mock.get.return_value = None
        mock.setex.return_value = True
        mock.delete.return_value = True
        return mock

    @pytest.fixture
    def coordinator(self, mock_redis):
        """Create a BootstrapCoordinator with mock Redis."""
        return BootstrapCoordinator(redis_client=mock_redis, event_bus=None)

    def test_start_bootstrap_creates_plan(self, coordinator):
        """Test start_bootstrap creates a proper bootstrap plan."""
        plan = coordinator.start_bootstrap("AAPL", timeframes=["10s", "1m"])

        assert plan.symbol == "AAPL"
        assert "10s" in plan.timeframes
        assert "1m" in plan.timeframes
        assert plan.trades_state == TradesBootstrapState.PENDING
        assert plan.timeframes["10s"].needs_trades is True
        assert plan.timeframes["1m"].needs_trades is True

    def test_start_bootstrap_10s_trades_only(self, coordinator):
        """Test 10s timeframe is marked as trades_only."""
        plan = coordinator.start_bootstrap("AAPL", timeframes=["10s"])

        assert plan.timeframes["10s"].bar_source == "trades_only"
        assert plan.timeframes["10s"].needs_trades is True

    def test_start_bootstrap_1m_trades_or_clickhouse(self, coordinator):
        """Test 1m timeframe supports both trades and clickhouse."""
        plan = coordinator.start_bootstrap("AAPL", timeframes=["1m"])

        assert plan.timeframes["1m"].bar_source == "trades_or_clickhouse"
        assert plan.timeframes["1m"].needs_trades is True

    def test_start_bootstrap_1d_ws_only(self, coordinator):
        """Test 1d timeframe is ws_only (no bootstrap needed)."""
        plan = coordinator.start_bootstrap("AAPL", timeframes=["1d"])

        assert plan.timeframes["1d"].bar_source == "ws_only"
        assert plan.timeframes["1d"].needs_trades is False
        assert plan.trades_state == TradesBootstrapState.NOT_NEEDED

    def test_start_bootstrap_prevents_duplicate(self, coordinator):
        """Test start_bootstrap returns existing plan if already in progress."""
        plan1 = coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        plan2 = coordinator.start_bootstrap("AAPL", timeframes=["5m"])

        # Should return the same plan, not create new one
        assert plan1 is plan2

    def test_start_bootstrap_resets_completed_event(self, coordinator):
        """Test cleanup + start_bootstrap clears Event on re-subscribe after completion."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])

        # Simulate completion - mark all consumers done
        coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")
        coordinator.report_done("AAPL", "tick_warmup", "factor_engine")
        coordinator.on_bars_ready("AAPL", "1m")
        coordinator.register_consumer("AAPL", "bar_warmup:1m", "factor_engine")
        coordinator.report_done("AAPL", "bar_warmup:1m", "factor_engine")

        # Verify bootstrap is ready and event is set
        assert coordinator.get_bootstrap("AAPL").is_ready()
        assert coordinator._ready_events["AAPL"].is_set()

        # Cleanup then re-subscribe should create fresh bootstrap
        coordinator.cleanup("AAPL")
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        assert not coordinator._ready_events["AAPL"].is_set()

    def test_store_trades_creates_key(self, coordinator, mock_redis):
        """Test store_trades creates Redis key with trades."""
        trades = [(1000, 100.0, 10), (2000, 101.0, 20)]
        key = coordinator.store_trades("AAPL", trades, from_ms=1000, first_ws_ts=3000)

        assert key.startswith("bootstrap:trades:AAPL:")
        mock_redis.setex.assert_called_once()

        # Verify trades were stored
        call_args = mock_redis.setex.call_args
        assert call_args[0][0] == key  # Redis key
        assert call_args[0][1] == 3600  # TTL

    def test_store_trades_updates_state(self, coordinator):
        """Test store_trades updates trades_state to READY."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        trades = [(1000, 100.0, 10)]

        coordinator.store_trades("AAPL", trades)

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert bootstrap.trades_state == TradesBootstrapState.READY
        assert bootstrap.trades == trades

    def test_get_trades_from_memory(self, coordinator):
        """Test get_trades returns trades from memory if available."""
        trades = [(1000, 100.0, 10)]
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        coordinator.store_trades("AAPL", trades)

        result = coordinator.get_trades("AAPL")
        assert result == trades

    def test_get_trades_from_redis(self, coordinator, mock_redis):
        """Test get_trades falls back to Redis if not in memory."""
        trades = [(1000, 100.0, 10)]
        compressed = gzip.compress(pickle.dumps(trades))
        mock_redis.get.return_value = compressed

        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        coordinator._bootstraps["AAPL"].trades_key = "bootstrap:trades:AAPL:test"

        result = coordinator.get_trades("AAPL")
        assert result == trades

    def test_on_bars_ready_updates_state(self, coordinator):
        """Test on_bars_ready updates timeframe state to READY."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m", "5m"])

        coordinator.on_bars_ready("AAPL", "1m")

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert bootstrap.timeframes["1m"].state == TimeframeState.READY
        assert bootstrap.timeframes["5m"].state == TimeframeState.PENDING

    def test_register_consumer(self, coordinator):
        """Test register_consumer adds consumer to tracking."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])

        coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert "factor_engine" in bootstrap.tick_consumers
        assert bootstrap.tick_consumers["factor_engine"] is False

    def test_register_bar_consumer(self, coordinator):
        """Test register_consumer for bar_warmup per timeframe."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])

        coordinator.register_consumer("AAPL", "bar_warmup:1m", "factor_engine")

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert "factor_engine" in bootstrap.timeframes["1m"].bar_consumers

    def test_report_done_tick_warmup(self, coordinator):
        """Test report_done marks tick_warmup complete."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")

        coordinator.report_done("AAPL", "tick_warmup", "factor_engine")

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert bootstrap.tick_consumers["factor_engine"] is True
        assert bootstrap.all_tick_consumers_done() is True

    def test_report_done_bar_warmup(self, coordinator):
        """Test report_done marks bar_warmup complete and updates state."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        coordinator.on_bars_ready("AAPL", "1m")  # Bars ready first
        coordinator.register_consumer("AAPL", "bar_warmup:1m", "factor_engine")

        coordinator.report_done("AAPL", "bar_warmup:1m", "factor_engine")

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert bootstrap.timeframes["1m"].bar_consumers["factor_engine"] is True
        assert bootstrap.timeframes["1m"].state == TimeframeState.DONE

    def test_is_ready_trades_and_bars_done(self, coordinator):
        """Test is_ready returns True when all consumers done."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])

        # Complete tick warmup
        coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")
        coordinator.report_done("AAPL", "tick_warmup", "factor_engine")

        # Complete bar warmup
        coordinator.on_bars_ready("AAPL", "1m")
        coordinator.register_consumer("AAPL", "bar_warmup:1m", "factor_engine")
        coordinator.report_done("AAPL", "bar_warmup:1m", "factor_engine")

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert bootstrap.is_ready() is True

    def test_is_ready_not_ready(self, coordinator):
        """Test is_ready returns False when consumers pending."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")
        # Don't report done

        bootstrap = coordinator.get_bootstrap("AAPL")
        assert bootstrap.is_ready() is False

    def test_wait_for_ticker_ready_timeout(self, coordinator):
        """Test wait_for_ticker_ready returns False on timeout."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])

        result = coordinator.wait_for_ticker_ready("AAPL", timeout=0.01)
        assert result is False

    def test_wait_for_ticker_ready_success(self, coordinator):
        """Test wait_for_ticker_ready returns True when ready."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])

        # Complete in background
        def complete():
            time.sleep(0.01)
            coordinator.register_consumer("AAPL", "tick_warmup", "fe")
            coordinator.report_done("AAPL", "tick_warmup", "fe")
            coordinator.on_bars_ready("AAPL", "1m")
            coordinator.register_consumer("AAPL", "bar_warmup:1m", "fe")
            coordinator.report_done("AAPL", "bar_warmup:1m", "fe")

        threading.Thread(target=complete, daemon=True).start()

        result = coordinator.wait_for_ticker_ready("AAPL", timeout=1.0)
        assert result is True

    def test_cleanup_removes_state(self, coordinator, mock_redis):
        """Test cleanup removes bootstrap state and Redis key."""
        coordinator.start_bootstrap("AAPL", timeframes=["1m"])
        coordinator.store_trades("AAPL", [(1000, 100.0, 10)])

        coordinator.cleanup("AAPL")

        assert coordinator.get_bootstrap("AAPL") is None
        mock_redis.delete.assert_called_once()


class TestBootstrapCoordinatorIntegration:
    """Integration tests for multi-service scenarios."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        mock = MagicMock()
        mock.get.return_value = None
        mock.setex.return_value = True
        mock.delete.return_value = True
        return mock

    @pytest.fixture
    def coordinator(self, mock_redis):
        """Create a BootstrapCoordinator with mock Redis."""
        return BootstrapCoordinator(redis_client=mock_redis, event_bus=None)

    def test_full_bootstrap_flow_10s_only(self, coordinator):
        """Test complete flow for 10s (trades only)."""
        # 1. ChartBFF starts bootstrap
        plan = coordinator.start_bootstrap("TSLA", timeframes=["10s"])
        assert plan.timeframes["10s"].needs_trades is True

        # 2. BarsBuilder stores trades
        trades = [(1000, 100.0, 10), (2000, 101.0, 20)]
        coordinator.store_trades("TSLA", trades)
        assert coordinator.get_trades("TSLA") == trades

        # 3. BarsBuilder notifies bars ready
        coordinator.on_bars_ready("TSLA", "10s")
        assert plan.timeframes["10s"].state == TimeframeState.READY

        # 4. FactorEngine registers and completes
        coordinator.register_consumer("TSLA", "tick_warmup", "factor_engine")
        coordinator.register_consumer("TSLA", "bar_warmup:10s", "factor_engine")

        coordinator.report_done("TSLA", "tick_warmup", "factor_engine")
        coordinator.report_done("TSLA", "bar_warmup:10s", "factor_engine")

        # 5. Verify ready
        assert plan.is_ready() is True

    def test_full_bootstrap_flow_mixed_timeframes(self, coordinator):
        """Test complete flow with mixed timeframes (10s, 1m, 1d)."""
        # 1. ChartBFF starts bootstrap for multiple timeframes
        coordinator.start_bootstrap("AAPL", timeframes=["10s", "1m", "1d"])

        # 2. BarsBuilder stores trades (needed for 10s and 1m)
        trades = [(1000, 100.0, 10)]
        coordinator.store_trades("AAPL", trades)

        # 3. BarsBuilder notifies bars ready per timeframe
        coordinator.on_bars_ready("AAPL", "10s")
        coordinator.on_bars_ready("AAPL", "1m")
        # 1d is ws_only, no bars ready needed

        # 4. FactorEngine completes all phases
        coordinator.register_consumer("AAPL", "tick_warmup", "factor_engine")
        coordinator.report_done("AAPL", "tick_warmup", "factor_engine")

        coordinator.register_consumer("AAPL", "bar_warmup:10s", "factor_engine")
        coordinator.register_consumer("AAPL", "bar_warmup:1m", "factor_engine")
        coordinator.report_done("AAPL", "bar_warmup:10s", "factor_engine")
        coordinator.report_done("AAPL", "bar_warmup:1m", "factor_engine")

        # 5. Verify ready
        assert coordinator.get_bootstrap("AAPL").is_ready() is True

    def test_re_subscribe_scenario(self, coordinator):
        """Test re-subscribe scenario: cleanup then start_bootstrap creates fresh state."""
        # First subscribe
        coordinator.start_bootstrap("MSFT", timeframes=["1m"])
        coordinator.store_trades("MSFT", [(1000, 100.0, 10)])
        coordinator.on_bars_ready("MSFT", "1m")
        coordinator.register_consumer("MSFT", "tick_warmup", "fe")
        coordinator.report_done("MSFT", "tick_warmup", "fe")
        coordinator.register_consumer("MSFT", "bar_warmup:1m", "fe")
        coordinator.report_done("MSFT", "bar_warmup:1m", "fe")

        assert coordinator.get_bootstrap("MSFT").is_ready() is True

        # Cleanup then re-subscribe (real flow: cleanup first, then fresh start)
        coordinator.cleanup("MSFT")
        coordinator.start_bootstrap("MSFT", timeframes=["1m"])
        bootstrap = coordinator.get_bootstrap("MSFT")

        # Should be fresh start
        assert bootstrap.timeframes["1m"].state == TimeframeState.PENDING
        assert not coordinator._ready_events["MSFT"].is_set()

    def test_concurrent_consumers(self, coordinator):
        """Test multiple consumers completing in any order."""
        coordinator.start_bootstrap("GOOGL", timeframes=["1m", "5m"])

        # Multiple services as consumers
        coordinator.register_consumer("GOOGL", "tick_warmup", "factor_engine")
        coordinator.register_consumer("GOOGL", "tick_warmup", "state_engine")

        # One completes
        coordinator.report_done("GOOGL", "tick_warmup", "factor_engine")
        assert not coordinator.get_bootstrap("GOOGL").all_tick_consumers_done()

        # Other completes
        coordinator.report_done("GOOGL", "tick_warmup", "state_engine")
        assert coordinator.get_bootstrap("GOOGL").all_tick_consumers_done()


class TestGlobalCoordinator:
    """Tests for global coordinator singleton."""

    def test_get_set_coordinator(self):
        """Test global get_coordinator/set_coordinator functions."""
        mock_redis = MagicMock()
        coordinator = BootstrapCoordinator(redis_client=mock_redis)

        # Initially None
        assert get_coordinator() is None

        # Set coordinator
        set_coordinator(coordinator)
        assert get_coordinator() is coordinator

        # Reset
        set_coordinator(None)
        assert get_coordinator() is None


class TestBootstrapPlan:
    """Tests for TickerBootstrap and TimeframeBootstrap dataclasses."""

    def test_ticker_bootstrap_to_dict(self):
        """Test TickerBootstrap.to_dict returns serializable data."""
        bootstrap = TickerBootstrap(
            symbol="AAPL",
            timeframes={
                "1m": TimeframeBootstrap(
                    timeframe="1m",
                    state=TimeframeState.READY,
                    needs_trades=True,
                    bar_source="trades_or_clickhouse",
                )
            },
            trades_state=TradesBootstrapState.READY,
            start_time_ns=1000,
            end_time_ns=2000,
        )

        data = bootstrap.to_dict()

        assert data["symbol"] == "AAPL"
        assert data["trades_state"] == "READY"
        assert "timeframes" in data
        assert data["timeframes"]["1m"]["state"] == "READY"
        assert data["elapsed_ms"] == 0  # (2000-1000) // 1_000_000 = 0 (less than 1ms)


class TestEdgeCases:
    """Edge case tests."""

    @pytest.fixture
    def mock_redis(self):
        return MagicMock()

    @pytest.fixture
    def coordinator(self, mock_redis):
        return BootstrapCoordinator(redis_client=mock_redis, event_bus=None)

    def test_unknown_timeframe(self, coordinator):
        """Test handling of unknown timeframe."""
        # Unknown timeframes should be treated as ws_only
        plan = coordinator.start_bootstrap("AAPL", timeframes=["unknown_tf"])

        assert plan.timeframes["unknown_tf"].bar_source == "ws_only"
        assert plan.timeframes["unknown_tf"].needs_trades is False

    def test_report_done_unknown_symbol(self, coordinator):
        """Test report_done with unknown symbol is no-op."""
        # Should not raise
        coordinator.report_done("UNKNOWN", "tick_warmup", "factor_engine")

    def test_register_consumer_unknown_symbol(self, coordinator):
        """Test register_consumer with unknown symbol is no-op."""
        # Should not raise
        coordinator.register_consumer("UNKNOWN", "tick_warmup", "factor_engine")

    def test_on_bars_ready_unknown_symbol(self, coordinator):
        """Test on_bars_ready with unknown symbol is no-op."""
        # Should not raise
        coordinator.on_bars_ready("UNKNOWN", "1m")

    def test_cleanup_unknown_symbol(self, coordinator):
        """Test cleanup with unknown symbol is no-op."""
        # Should not raise
        coordinator.cleanup("UNKNOWN")

    def test_empty_timeframes_list(self, coordinator):
        """Test start_bootstrap with empty timeframes."""
        plan = coordinator.start_bootstrap("AAPL", timeframes=[])

        assert plan.symbol == "AAPL"
        assert len(plan.timeframes) == 0
        assert plan.trades_state == TradesBootstrapState.NOT_NEEDED
        # Should be immediately ready
        assert plan.is_ready() is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
