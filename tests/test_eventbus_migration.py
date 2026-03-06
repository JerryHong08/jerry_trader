"""
测试新的类型安全 EventBus 架构
"""

import sys

sys.path.insert(0, "/home/jerryhong/code-projects/jerry_trader/src")

from OrderManagement.adapter.event_bus import get_event_bus
from OrderManagement.models.event_models import (
    AccountUpdatedEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    OrderStatusEvent,
    PositionUpdatedEvent,
)


def test_order_events():
    """测试订单事件"""
    print("🧪 Testing Order Events...")

    event_bus = get_event_bus()
    received_events = []

    def on_order_status(event: OrderStatusEvent):
        received_events.append(event)
        print(
            f"  ✅ Received OrderStatusEvent: order_id={event.order_id}, status={event.status}"
        )

    def on_order_placed(event: OrderPlacedEvent):
        received_events.append(event)
        print(f"  ✅ Received OrderPlacedEvent: order_id={event.order_id}")

    # 订阅事件
    event_bus.subscribe(OrderStatusEvent, on_order_status)
    event_bus.subscribe(OrderPlacedEvent, on_order_placed)

    # 发布事件
    event_bus.publish_event(
        OrderStatusEvent(
            order_id=123,
            status="Filled",
            filled=100,
            remaining=0,
            avg_fill_price=150.25,
            commission=1.0,
        )
    )

    # 验证
    assert len(received_events) == 1
    assert isinstance(received_events[0], OrderStatusEvent)
    assert received_events[0].order_id == 123
    print("✅ Order events test passed!\n")


def test_position_events():
    """测试持仓事件"""
    print("🧪 Testing Position Events...")

    event_bus = get_event_bus()
    received_events = []

    def on_position(event: PositionUpdatedEvent):
        received_events.append(event)
        print(
            f"  ✅ Received PositionUpdatedEvent: symbol={event.symbol}, position={event.position}"
        )

    event_bus.subscribe(PositionUpdatedEvent, on_position)

    event_bus.publish_event(
        PositionUpdatedEvent(
            symbol="AAPL",
            position=100,
            average_cost=150.0,
            market_price=155.0,
            market_value=15500.0,
            unrealized_pnl=500.0,
            realized_pnl=0.0,
        )
    )

    assert len(received_events) == 1
    assert received_events[0].symbol == "AAPL"
    print("✅ Position events test passed!\n")


def test_account_events():
    """测试账户事件"""
    print("🧪 Testing Account Events...")

    event_bus = get_event_bus()
    received_events = []

    def on_account(event: AccountUpdatedEvent):
        received_events.append(event)
        print(
            f"  ✅ Received AccountUpdatedEvent: tag={event.tag}, value={event.value}"
        )

    event_bus.subscribe(AccountUpdatedEvent, on_account)

    event_bus.publish_event(
        AccountUpdatedEvent(tag="TotalCashValue", value="100000.0", currency="USD")
    )

    assert len(received_events) == 1
    assert received_events[0].tag == "TotalCashValue"
    print("✅ Account events test passed!\n")


def test_type_safety():
    """测试类型安全"""
    print("🧪 Testing Type Safety...")

    # 正确的类型
    event = OrderStatusEvent(order_id=123, status="Filled", filled=100, remaining=0)

    assert event.order_id == 123
    assert event.status == "Filled"
    assert hasattr(event, "timestamp")  # 自动添加的时间戳
    print("  ✅ Event created with correct types")

    # 测试属性访问（不再使用字典）
    try:
        _ = event.order_id  # ✅ 正确
        print("  ✅ Attribute access works")
    except AttributeError:
        print("  ❌ Attribute access failed")
        raise

    print("✅ Type safety test passed!\n")


def test_event_history():
    """测试事件历史记录"""
    print("🧪 Testing Event History...")

    event_bus = get_event_bus()

    # 发布多个事件
    for i in range(5):
        event_bus.publish_event(
            OrderStatusEvent(
                order_id=100 + i, status="Submitted", filled=0, remaining=10
            )
        )

    # 获取历史记录
    history = event_bus.get_history(OrderStatusEvent, limit=3)

    assert len(history) >= 3
    print(f"  ✅ Retrieved {len(history)} historical events")
    print("✅ Event history test passed!\n")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 EventBus Type-Safe Architecture Test Suite")
    print("=" * 60)
    print()

    try:
        test_order_events()
        test_position_events()
        test_account_events()
        test_type_safety()
        test_event_history()

        print("=" * 60)
        print("✅ All tests passed! Migration successful! 🎉")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
