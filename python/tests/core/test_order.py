"""Tests for domain/order/order.py.

Covers:
  - OrderSide, OrderType, OrderStatus, TimeInForce enums
  - Order construction and __post_init__ validation
  - Order properties (is_long, is_short, is_market, is_limit, can_cancel)
  - Order immutability (frozen=True)
  - Fill construction, validation, properties (value, cost_basis)
  - OrderState construction and validation
  - OrderState status machine (apply_fill, mark_submitted, mark_cancelled, mark_rejected)
  - OrderState properties (is_complete, is_active, total_commission, total_value)
  - Edge cases: zero values, large quantities, multiple fills
"""

from __future__ import annotations

import pytest

from jerry_trader.domain.order.order import (
    Fill,
    Order,
    OrderSide,
    OrderState,
    OrderStatus,
    OrderType,
    TimeInForce,
)

# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════


def _order(**overrides) -> Order:
    defaults = dict(
        order_id="ORD-001",
        symbol="AAPL",
        side="buy",
        quantity=100,
        order_type="limit",
        limit_price=150.0,
    )
    defaults.update(overrides)
    return Order(**defaults)


def _fill(**overrides) -> Fill:
    defaults = dict(
        fill_id="FILL-001",
        order_id="ORD-001",
        timestamp_ns=1_715_000_000_000_000_000,
        quantity=50,
        price=150.0,
    )
    defaults.update(overrides)
    return Fill(**defaults)


def _state(**overrides) -> OrderState:
    defaults = dict(
        order_id="ORD-001",
        status=OrderStatus.PENDING,
        filled_qty=0,
        remaining_qty=100,
    )
    defaults.update(overrides)
    return OrderState(**defaults)


# ══════════════════════════════════════════════════════════════════════
# Enums
# ══════════════════════════════════════════════════════════════════════


class TestOrderSide:
    def test_values(self):
        assert OrderSide.BUY.value == "buy"
        assert OrderSide.SELL.value == "sell"

    def test_from_string(self):
        assert OrderSide("buy") == OrderSide.BUY
        assert OrderSide("sell") == OrderSide.SELL

    def test_invalid_value(self):
        with pytest.raises(ValueError):
            OrderSide("hold")


class TestOrderType:
    def test_values(self):
        assert OrderType.MARKET.value == "market"
        assert OrderType.LIMIT.value == "limit"
        assert OrderType.STOP.value == "stop"
        assert OrderType.STOP_LIMIT.value == "stop_limit"


class TestOrderStatus:
    def test_auto_values_are_ints(self):
        assert isinstance(OrderStatus.PENDING.value, int)
        assert isinstance(OrderStatus.FILLED.value, int)

    def test_distinct_values(self):
        values = [s.value for s in OrderStatus]
        assert len(values) == len(set(values))

    def test_terminal_states(self):
        terminal = {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.ERROR,
        }
        for status in terminal:
            state = _state(status=status)
            assert state.is_complete is True

    def test_active_states(self):
        for status in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILL):
            state = _state(status=status)
            assert state.is_active is True

    def test_non_active_states(self):
        for status in (
            OrderStatus.PENDING,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.ERROR,
        ):
            state = _state(status=status)
            assert state.is_active is False


class TestTimeInForce:
    def test_values(self):
        assert TimeInForce.DAY.value == "day"
        assert TimeInForce.GTC.value == "gtc"
        assert TimeInForce.IOC.value == "ioc"
        assert TimeInForce.FOK.value == "fok"


# ══════════════════════════════════════════════════════════════════════
# Order construction and validation
# ══════════════════════════════════════════════════════════════════════


class TestOrderConstruction:
    def test_minimal_limit_order(self):
        order = _order()
        assert order.order_id == "ORD-001"
        assert order.symbol == "AAPL"
        assert order.side == "buy"
        assert order.quantity == 100
        assert order.order_type == "limit"
        assert order.limit_price == 150.0

    def test_market_order_no_prices(self):
        order = _order(order_type="market", limit_price=None, stop_price=None)
        assert order.is_market is True
        assert order.limit_price is None
        assert order.stop_price is None

    def test_default_time_in_force(self):
        order = _order()
        assert order.time_in_force == "day"

    def test_optional_fields_default_none(self):
        order = _order()
        assert order.max_slippage is None
        assert order.strategy_id is None
        assert order.signal_id is None

    def test_with_metadata(self):
        order = _order(
            strategy_id="MOMENTUM_V1", signal_id="SIG-123", max_slippage=0.02
        )
        assert order.strategy_id == "MOMENTUM_V1"
        assert order.signal_id == "SIG-123"
        assert order.max_slippage == 0.02

    def test_stop_limit_order(self):
        order = _order(order_type="stop_limit", limit_price=150.0, stop_price=149.0)
        assert order.limit_price == 150.0
        assert order.stop_price == 149.0

    def test_stop_order(self):
        order = _order(order_type="stop", limit_price=None, stop_price=149.0)
        assert order.stop_price == 149.0
        assert order.limit_price is None

    def test_generated_order_id(self):
        order = _order(order_id="ORD-ABC-12345")
        assert order.order_id == "ORD-ABC-12345"


class TestOrderValidation:
    def test_zero_quantity_raises(self):
        with pytest.raises(ValueError, match="Quantity must be positive"):
            _order(quantity=0)

    def test_negative_quantity_raises(self):
        with pytest.raises(ValueError, match="Quantity must be positive"):
            _order(quantity=-10)

    def test_limit_order_missing_price_raises(self):
        with pytest.raises(ValueError, match="limit order requires limit_price"):
            _order(order_type="limit", limit_price=None)

    def test_stop_limit_missing_limit_raises(self):
        with pytest.raises(ValueError, match="stop_limit order requires limit_price"):
            _order(order_type="stop_limit", limit_price=None, stop_price=149.0)

    def test_stop_order_missing_stop_price_raises(self):
        with pytest.raises(ValueError, match="stop order requires stop_price"):
            _order(order_type="stop", limit_price=None, stop_price=None)

    def test_stop_limit_missing_stop_price_raises(self):
        with pytest.raises(ValueError, match="stop_limit order requires stop_price"):
            _order(order_type="stop_limit", limit_price=150.0, stop_price=None)

    def test_negative_limit_price_raises(self):
        with pytest.raises(ValueError, match="Limit price must be positive"):
            _order(limit_price=-1.0)

    def test_zero_limit_price_raises(self):
        with pytest.raises(ValueError, match="Limit price must be positive"):
            _order(limit_price=0.0)

    def test_negative_stop_price_raises(self):
        with pytest.raises(ValueError, match="Stop price must be positive"):
            _order(order_type="stop", limit_price=None, stop_price=-0.01)

    def test_max_slippage_negative_raises(self):
        with pytest.raises(ValueError, match="Max slippage must be between 0 and 1"):
            _order(max_slippage=-0.1)

    def test_max_slippage_over_one_raises(self):
        with pytest.raises(ValueError, match="Max slippage must be between 0 and 1"):
            _order(max_slippage=1.5)

    def test_max_slippage_zero_valid(self):
        order = _order(max_slippage=0.0)
        assert order.max_slippage == 0.0

    def test_max_slippage_one_valid(self):
        order = _order(max_slippage=1.0)
        assert order.max_slippage == 1.0


# ══════════════════════════════════════════════════════════════════════
# Order properties
# ══════════════════════════════════════════════════════════════════════


class TestOrderProperties:
    def test_is_long(self):
        assert _order(side="buy").is_long is True
        assert _order(side="sell").is_long is False

    def test_is_short(self):
        assert _order(side="sell").is_short is True
        assert _order(side="buy").is_short is False

    def test_is_market(self):
        assert _order(order_type="market", limit_price=None).is_market is True
        assert _order(order_type="limit").is_market is False

    def test_is_limit(self):
        assert _order(order_type="limit").is_limit is True
        assert _order(order_type="market", limit_price=None).is_limit is False

    def test_can_cancel_limit_order(self):
        """Limit orders can be cancelled before execution."""
        assert _order(order_type="limit").can_cancel() is True

    def test_can_cancel_market_order(self):
        """Market orders typically can't be cancelled."""
        assert _order(order_type="market", limit_price=None).can_cancel() is False

    def test_can_cancel_stop_order(self):
        assert (
            _order(order_type="stop", limit_price=None, stop_price=149.0).can_cancel()
            is True
        )


# ══════════════════════════════════════════════════════════════════════
# Order immutability
# ══════════════════════════════════════════════════════════════════════


class TestOrderImmutability:
    def test_cannot_set_attribute(self):
        order = _order()
        with pytest.raises(Exception):
            order.quantity = 999  # type: ignore[misc]

    def test_equal_orders(self):
        a = _order()
        b = _order()
        assert a == b
        assert hash(a) == hash(b)

    def test_hashable(self):
        order = _order()
        assert hash(order) is not None


# ══════════════════════════════════════════════════════════════════════
# Fill construction and validation
# ══════════════════════════════════════════════════════════════════════


class TestFillConstruction:
    def test_valid_fill(self):
        fill = _fill()
        assert fill.fill_id == "FILL-001"
        assert fill.order_id == "ORD-001"
        assert fill.quantity == 50
        assert fill.price == 150.0

    def test_default_commission_zero(self):
        fill = _fill()
        assert fill.commission == 0.0

    def test_default_exchange_none(self):
        fill = _fill()
        assert fill.exchange is None

    def test_with_commission(self):
        fill = _fill(commission=2.50)
        assert fill.commission == 2.50

    def test_with_exchange(self):
        fill = _fill(exchange="NASDAQ")
        assert fill.exchange == "NASDAQ"


class TestFillValidation:
    def test_zero_quantity_raises(self):
        with pytest.raises(ValueError, match="Fill quantity must be positive"):
            _fill(quantity=0)

    def test_negative_quantity_raises(self):
        with pytest.raises(ValueError, match="Fill quantity must be positive"):
            _fill(quantity=-10)

    def test_zero_price_raises(self):
        with pytest.raises(ValueError, match="Fill price must be positive"):
            _fill(price=0.0)

    def test_negative_price_raises(self):
        with pytest.raises(ValueError, match="Fill price must be positive"):
            _fill(price=-150.0)

    def test_negative_commission_raises(self):
        with pytest.raises(ValueError, match="Commission must be non-negative"):
            _fill(commission=-0.01)

    def test_zero_commission_valid(self):
        fill = _fill(commission=0.0)
        assert fill.commission == 0.0


class TestFillProperties:
    def test_value(self):
        fill = _fill(quantity=100, price=152.50)
        assert fill.value == 15250.0

    def test_value_single_share(self):
        fill = _fill(quantity=1, price=999.99)
        assert fill.value == 999.99

    def test_cost_basis_no_commission(self):
        fill = _fill(quantity=100, price=150.0, commission=0.0)
        assert fill.cost_basis == 15000.0

    def test_cost_basis_with_commission(self):
        fill = _fill(quantity=100, price=150.0, commission=3.50)
        assert fill.cost_basis == 15003.50


# ══════════════════════════════════════════════════════════════════════
# OrderState construction and validation
# ══════════════════════════════════════════════════════════════════════


class TestOrderStateConstruction:
    def test_default_state(self):
        state = _state()
        assert state.order_id == "ORD-001"
        assert state.status == OrderStatus.PENDING
        assert state.filled_qty == 0
        assert state.remaining_qty == 100
        assert state.fills == []
        assert state.avg_fill_price == 0.0

    def test_default_timestamps(self):
        state = _state()
        assert state.created_at_ns == 0
        assert state.submitted_at_ns is None
        assert state.filled_at_ns is None
        assert state.cancelled_at_ns is None

    def test_error_message_default_none(self):
        state = _state()
        assert state.error_message is None


class TestOrderStateValidation:
    def test_negative_filled_qty_raises(self):
        with pytest.raises(ValueError):
            _state(filled_qty=-1)

    def test_negative_remaining_qty_raises(self):
        with pytest.raises(ValueError):
            _state(remaining_qty=-1)

    def test_negative_avg_fill_price_raises(self):
        with pytest.raises(ValueError):
            _state(avg_fill_price=-0.01)

    def test_zero_filled_qty_valid(self):
        state = _state(filled_qty=0)
        assert state.filled_qty == 0

    def test_zero_avg_fill_price_valid(self):
        state = _state(avg_fill_price=0.0)
        assert state.avg_fill_price == 0.0


# ══════════════════════════════════════════════════════════════════════
# OrderState properties
# ══════════════════════════════════════════════════════════════════════


class TestOrderStateProperties:
    def test_is_complete_terminal_states(self):
        for status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.ERROR,
        ):
            state = _state(status=status)
            assert state.is_complete is True

    def test_is_complete_non_terminal(self):
        for status in (
            OrderStatus.PENDING,
            OrderStatus.SUBMITTED,
            OrderStatus.PARTIAL_FILL,
        ):
            state = _state(status=status)
            assert state.is_complete is False

    def test_total_commission_empty(self):
        state = _state()
        assert state.total_commission == 0.0

    def test_total_value_empty(self):
        state = _state()
        assert state.total_value == 0.0

    def test_is_active(self):
        assert _state(status=OrderStatus.PENDING).is_active is False


# ══════════════════════════════════════════════════════════════════════
# OrderState.can_cancel()
# ══════════════════════════════════════════════════════════════════════


class TestOrderStateCanCancel:
    def test_pending_can_cancel(self):
        assert _state(status=OrderStatus.PENDING).can_cancel() is True

    def test_submitted_can_cancel(self):
        assert _state(status=OrderStatus.SUBMITTED).can_cancel() is True

    def test_partial_fill_can_cancel(self):
        assert (
            _state(status=OrderStatus.PARTIAL_FILL, filled_qty=30).can_cancel() is True
        )

    def test_filled_cannot_cancel(self):
        assert _state(status=OrderStatus.FILLED, filled_qty=100).can_cancel() is False

    def test_cancelled_cannot_cancel(self):
        assert _state(status=OrderStatus.CANCELLED).can_cancel() is False

    def test_rejected_cannot_cancel(self):
        assert _state(status=OrderStatus.REJECTED).can_cancel() is False

    def test_error_cannot_cancel(self):
        assert _state(status=OrderStatus.ERROR).can_cancel() is False


# ══════════════════════════════════════════════════════════════════════
# OrderState state machine transitions
# ══════════════════════════════════════════════════════════════════════


class TestOrderStateMarkSubmitted:
    def test_pending_to_submitted(self):
        state = _state(status=OrderStatus.PENDING)
        state.mark_submitted(1_715_000_000_000_000_000)
        assert state.status == OrderStatus.SUBMITTED
        assert state.submitted_at_ns == 1_715_000_000_000_000_000

    def test_non_pending_skips(self):
        """Only PENDING → SUBMITTED transition is allowed."""
        state = _state(status=OrderStatus.SUBMITTED)
        state.mark_submitted(1_715_000_000_000_000_000)
        assert state.status == OrderStatus.SUBMITTED  # Unchanged


class TestOrderStateMarkCancelled:
    def test_pending_can_cancel(self):
        state = _state(status=OrderStatus.PENDING)
        state.mark_cancelled(1_715_000_000_000_000_000)
        assert state.status == OrderStatus.CANCELLED
        assert state.cancelled_at_ns == 1_715_000_000_000_000_000

    def test_submitted_can_cancel(self):
        state = _state(status=OrderStatus.SUBMITTED)
        state.mark_cancelled(1_715_000_000_000_000_000)
        assert state.status == OrderStatus.CANCELLED

    def test_partial_fill_can_cancel(self):
        state = _state(status=OrderStatus.PARTIAL_FILL, filled_qty=30)
        state.mark_cancelled(1_715_000_000_000_000_000)
        assert state.status == OrderStatus.CANCELLED

    def test_filled_cannot_cancel(self):
        state = _state(status=OrderStatus.FILLED, filled_qty=100)
        state.mark_cancelled(1_715_000_000_000_000_000)
        assert state.status == OrderStatus.FILLED  # Unchanged


class TestOrderStateMarkRejected:
    def test_rejected_from_pending(self):
        state = _state(status=OrderStatus.PENDING)
        state.mark_rejected("Insufficient margin", 1_715_000_000_000_000_000)
        assert state.status == OrderStatus.REJECTED
        assert state.error_message == "Insufficient margin"
        assert state.cancelled_at_ns == 1_715_000_000_000_000_000

    def test_rejected_from_submitted(self):
        state = _state(status=OrderStatus.SUBMITTED)
        state.mark_rejected("Exchange unavailable", 1_715_000_000_000_000_000)
        assert state.status == OrderStatus.REJECTED
        assert state.error_message == "Exchange unavailable"


# ══════════════════════════════════════════════════════════════════════
# OrderState.apply_fill()
# ══════════════════════════════════════════════════════════════════════


class TestOrderStateApplyFill:
    def test_single_fill_updates_qty(self):
        state = _state(status=OrderStatus.SUBMITTED, remaining_qty=100)
        fill = _fill(quantity=50, price=150.0)

        state.apply_fill(fill, total_qty=100)
        assert state.filled_qty == 50
        assert state.remaining_qty == 50
        assert state.status == OrderStatus.PARTIAL_FILL
        assert len(state.fills) == 1

    def test_single_fill_full_fill(self):
        state = _state(status=OrderStatus.SUBMITTED, remaining_qty=100)
        fill = _fill(quantity=100, price=150.0, timestamp_ns=1_715_000_000_000_000_000)

        state.apply_fill(fill, total_qty=100)
        assert state.filled_qty == 100
        assert state.remaining_qty == 0
        assert state.status == OrderStatus.FILLED
        assert state.filled_at_ns == 1_715_000_000_000_000_000

    def test_multiple_fills_avg_price(self):
        state = _state(status=OrderStatus.SUBMITTED, remaining_qty=100)

        fill1 = _fill(fill_id="FILL-001", quantity=60, price=150.0)
        state.apply_fill(fill1, total_qty=100)

        fill2 = _fill(fill_id="FILL-002", quantity=40, price=152.0)
        state.apply_fill(fill2, total_qty=100)

        assert state.filled_qty == 100
        # VWAP: (60*150 + 40*152) / 100 = (9000 + 6080) / 100 = 150.8
        assert state.avg_fill_price == pytest.approx(150.8)

    def test_multiple_fills_status_transitions(self):
        state = _state(status=OrderStatus.SUBMITTED, remaining_qty=100)

        fill1 = _fill(fill_id="FILL-001", quantity=30, price=150.0)
        state.apply_fill(fill1, total_qty=100)
        assert state.status == OrderStatus.PARTIAL_FILL

        fill2 = _fill(fill_id="FILL-002", quantity=70, price=151.0)
        state.apply_fill(fill2, total_qty=100)
        assert state.status == OrderStatus.FILLED

    def test_overfill_sets_filled(self):
        """Overfill (more filled than total) → FILLED."""
        state = _state(status=OrderStatus.SUBMITTED, remaining_qty=100)
        fill = _fill(quantity=150, price=150.0)

        state.apply_fill(fill, total_qty=100)
        assert state.filled_qty == 150
        assert state.remaining_qty == -50
        assert state.status == OrderStatus.FILLED

    def test_fills_accumulate_in_list(self):
        state = _state(status=OrderStatus.SUBMITTED, remaining_qty=100)

        state.apply_fill(
            _fill(fill_id="FILL-001", quantity=20, price=150.0), total_qty=100
        )
        state.apply_fill(
            _fill(fill_id="FILL-002", quantity=30, price=151.0), total_qty=100
        )
        state.apply_fill(
            _fill(fill_id="FILL-003", quantity=50, price=149.0), total_qty=100
        )

        assert len(state.fills) == 3
        assert [f.fill_id for f in state.fills] == ["FILL-001", "FILL-002", "FILL-003"]

    def test_avg_fill_price_with_existing(self):
        """avg_fill_price updates correctly when starting with existing filled qty."""
        state = _state(
            status=OrderStatus.PARTIAL_FILL,
            filled_qty=40,
            remaining_qty=60,
            avg_fill_price=100.0,
        )
        fill = _fill(quantity=60, price=110.0)
        state.apply_fill(fill, total_qty=100)

        # (40*100 + 60*110) / 100 = (4000 + 6600) / 100 = 106.0
        assert state.avg_fill_price == pytest.approx(106.0)
        assert state.filled_qty == 100

    def test_properties_with_fills(self):
        state = _state(status=OrderStatus.SUBMITTED, remaining_qty=100)

        state.apply_fill(_fill(quantity=50, price=100.0, commission=1.5), total_qty=100)
        state.apply_fill(_fill(quantity=50, price=102.0, commission=1.5), total_qty=100)

        # Total commission: 1.5 + 1.5 = 3.0
        assert state.total_commission == 3.0
        # Total value: 50*100 + 50*102 = 5000 + 5100 = 10100
        assert state.total_value == 10100.0
