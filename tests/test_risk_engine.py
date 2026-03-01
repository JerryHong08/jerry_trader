"""
Tests for the RiskManagement module.

Tests cover:
  - Pure-Python RiskEngine fallback (_PyRiskEngine)
  - C++ RiskEngine bridge (_CppRiskEngine) if library is available
  - RiskManager service (event handling and PnL calculation)

Run with:
    pytest tests/test_risk_engine.py -v
"""

import sys
import os

# Ensure src/ is on the path so imports work without an installed package.
SRC_DIR = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, SRC_DIR)

import pytest

from RiskManagement.risk_engine_bridge import _PyRiskEngine, load_risk_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_py_engine():
    return _PyRiskEngine()


# ---------------------------------------------------------------------------
# _PyRiskEngine unit tests (pure Python fallback)
# ---------------------------------------------------------------------------


class TestPyRiskEngine:
    def test_buy_increases_quantity(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 150.0)
        pos = eng.get_position("AAPL")
        assert pos is not None
        assert pos["quantity"] == 100
        assert pos["avg_fill_price"] == pytest.approx(150.0)

    def test_unrealized_pnl_positive(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 150.0)
        eng.update_price("AAPL", 155.0)
        pnl = eng.get_unrealized_pnl("AAPL")
        assert pnl == pytest.approx(500.0)

    def test_unrealized_pnl_negative(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 150.0)
        eng.update_price("AAPL", 145.0)
        pnl = eng.get_unrealized_pnl("AAPL")
        assert pnl == pytest.approx(-500.0)

    def test_unrealized_pnl_zero_before_price_update(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 150.0)
        # No price update yet
        pnl = eng.get_unrealized_pnl("AAPL")
        assert pnl == pytest.approx(0.0)

    def test_weighted_average_fill_price(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 100.0)
        eng.on_fill("AAPL", "BUY", 100, 200.0)
        pos = eng.get_position("AAPL")
        assert pos["quantity"] == 200
        assert pos["avg_fill_price"] == pytest.approx(150.0)

    def test_sell_reduces_quantity_and_books_realized_pnl(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 100.0)
        eng.on_fill("AAPL", "SELL", 50, 120.0)
        pos = eng.get_position("AAPL")
        assert pos["quantity"] == 50
        # Realized PnL = (120 - 100) * 50 = 1000
        assert eng.get_realized_pnl("AAPL") == pytest.approx(1000.0)

    def test_full_close_position(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 100.0)
        eng.on_fill("AAPL", "SELL", 100, 110.0)
        pos = eng.get_position("AAPL")
        assert pos["quantity"] == 0
        assert eng.get_realized_pnl("AAPL") == pytest.approx(1000.0)
        assert eng.get_unrealized_pnl("AAPL") == pytest.approx(0.0)

    def test_total_unrealized_pnl(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 100.0)
        eng.on_fill("TSLA", "BUY", 10, 200.0)
        eng.update_price("AAPL", 110.0)
        eng.update_price("TSLA", 210.0)
        total = eng.get_total_unrealized_pnl()
        # AAPL: (110-100)*100 = 1000; TSLA: (210-200)*10 = 100 → 1100
        assert total == pytest.approx(1100.0)

    def test_unknown_symbol_returns_zero(self):
        eng = make_py_engine()
        assert eng.get_unrealized_pnl("XYZ") == pytest.approx(0.0)
        assert eng.get_realized_pnl("XYZ") == pytest.approx(0.0)
        assert eng.get_position("XYZ") is None

    def test_invalid_quantity_raises(self):
        eng = make_py_engine()
        with pytest.raises(ValueError, match="quantity"):
            eng.on_fill("AAPL", "BUY", 0, 100.0)
        with pytest.raises(ValueError, match="quantity"):
            eng.on_fill("AAPL", "BUY", -10, 100.0)

    def test_invalid_price_raises(self):
        eng = make_py_engine()
        with pytest.raises(ValueError, match="fill_price"):
            eng.on_fill("AAPL", "BUY", 10, 0.0)

    def test_invalid_side_raises(self):
        eng = make_py_engine()
        with pytest.raises(ValueError, match="side"):
            eng.on_fill("AAPL", "LONG", 10, 100.0)

    def test_bot_sld_aliases(self):
        """IB uses BOT/SLD instead of BUY/SELL in execDetails."""
        eng = make_py_engine()
        eng.on_fill("AAPL", "BOT", 100, 150.0)
        pos = eng.get_position("AAPL")
        assert pos["quantity"] == 100
        eng.on_fill("AAPL", "SLD", 50, 160.0)
        pos = eng.get_position("AAPL")
        assert pos["quantity"] == 50
        assert eng.get_realized_pnl("AAPL") == pytest.approx(500.0)

    def test_reset_clears_all(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 150.0)
        eng.reset()
        assert eng.get_position("AAPL") is None
        assert eng.get_total_unrealized_pnl() == pytest.approx(0.0)

    def test_update_price_ignored_for_untracked_symbol(self):
        """Updating price for an untracked symbol should not create a position."""
        eng = make_py_engine()
        eng.update_price("AAPL", 150.0)
        assert eng.get_position("AAPL") is None

    def test_price_ignored_if_nonpositive(self):
        eng = make_py_engine()
        eng.on_fill("AAPL", "BUY", 100, 100.0)
        eng.update_price("AAPL", 0.0)
        eng.update_price("AAPL", -5.0)
        # Current price should remain 0.0 (initial value)
        assert eng.get_unrealized_pnl("AAPL") == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# load_risk_engine() factory test
# ---------------------------------------------------------------------------


class TestLoadRiskEngine:
    def test_factory_returns_working_engine(self):
        """The factory should always return a working engine (C++ or Python)."""
        eng = load_risk_engine()
        eng.on_fill("MSFT", "BUY", 50, 300.0)
        eng.update_price("MSFT", 310.0)
        assert eng.get_unrealized_pnl("MSFT") == pytest.approx(500.0)
        eng.reset()


# ---------------------------------------------------------------------------
# RiskManager integration tests (no actual IBKR / EventBus connection needed)
# ---------------------------------------------------------------------------


class TestRiskManager:
    def setup_method(self):
        """Isolate EventBus state by creating a fresh one per test."""
        import importlib
        import OrderManagement.adapter.event_bus as eb_module
        # Reset global singleton for test isolation
        eb_module._global_event_bus = None
        from RiskManagement.risk_manager import RiskManager
        self.RiskManager = RiskManager

    def teardown_method(self):
        import OrderManagement.adapter.event_bus as eb_module
        eb_module._global_event_bus = None

    def test_start_stop(self):
        rm = self.RiskManager()
        rm.start()
        assert rm._subscribed is True
        rm.stop()
        assert rm._subscribed is False

    def test_on_tick_updates_pnl(self):
        rm = self.RiskManager()
        rm.start()
        # Simulate a fill by calling the engine directly
        rm._engine.on_fill("AAPL", "BUY", 100, 150.0)
        rm._tracked_symbols.add("AAPL")
        # Feed a tick
        rm.on_tick("AAPL", 160.0)
        snap = rm.get_pnl_snapshot()
        assert snap["total_unrealized_pnl"] == pytest.approx(1000.0)
        rm.stop()

    def test_get_symbol_pnl_not_found(self):
        rm = self.RiskManager()
        rm.start()
        assert rm.get_symbol_pnl("ZZZZ") is None
        rm.stop()

    def test_tracked_symbols_after_fill(self):
        from OrderManagement.models.event_models import ExecutionReceivedEvent
        rm = self.RiskManager()
        rm.start()
        event = ExecutionReceivedEvent(
            exec_id="EX001",
            order_id=1,
            symbol="TSLA",
            side="BOT",
            shares=10,
            price=700.0,
            exchange="SMART",
            time="2025-01-01 10:00:00",
        )
        rm._on_execution(event)
        assert "TSLA" in rm.tracked_symbols
        rm.stop()

    def test_pnl_snapshot_structure(self):
        rm = self.RiskManager()
        rm.start()
        rm._engine.on_fill("AAPL", "BUY", 100, 150.0)
        rm._tracked_symbols.add("AAPL")
        rm._engine.update_price("AAPL", 155.0)
        snap = rm.get_pnl_snapshot()
        assert "timestamp" in snap
        assert "total_unrealized_pnl" in snap
        assert "total_realized_pnl" in snap
        assert "positions" in snap
        assert len(snap["positions"]) == 1
        pos = snap["positions"][0]
        assert pos["symbol"] == "AAPL"
        assert pos["quantity"] == 100
        assert pos["unrealized_pnl"] == pytest.approx(500.0)
        rm.stop()
