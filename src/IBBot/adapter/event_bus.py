"""
Event Bus for IBBot - 类型安全的事件驱动架构
Provides a publish-subscribe pattern for decoupling components

核心理念：
- 各模块不直接调用彼此，只和 event_bus 交互
- event_bus 是系统的消息中枢神经
- 直接使用事件类型订阅，无需额外的 Enum
"""

import logging
import threading
from datetime import datetime
from typing import Callable, Dict, List, Type

# 导入所有事件类型
from IBBot.models.event_models import (
    AccountUpdatedEvent,
    BaseEvent,
    ConnectionEvent,
    ErrorEvent,
    ExecutionReceivedEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    OrderStatusEvent,
    OrderSubmittedEvent,
    PositionUpdatedEvent,
)
from utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class EventBus:
    """
    Event Bus implementation using publish-subscribe pattern
    Thread-safe event bus for decoupling components

    直接使用事件类型进行订阅：
    event_bus.subscribe(OrderStatusEvent, handler)
    event_bus.publish_event(OrderStatusEvent(...))
    """

    def __init__(self):
        """Initialize the event bus"""
        self.subscribers: Dict[Type[BaseEvent], List[Callable]] = {}
        self.lock = threading.RLock()
        self.event_history: List[BaseEvent] = []
        self.max_history = 1000  # Keep last 1000 events
        logger.info("🚌 EventBus initialized")

    def publish_event(self, event: BaseEvent):
        """
        发布类型安全的事件对象

        Args:
            event: BaseEvent 的子类实例（如 OrderStatusEvent）

        Example:
            event_bus.publish_event(OrderStatusEvent(
                order_id=123,
                status="Filled",
                filled=10,
                remaining=0
            ))
        """
        event_type = type(event)

        # Store in history
        with self.lock:
            self.event_history.append(event)
            if len(self.event_history) > self.max_history:
                self.event_history.pop(0)

            # Get subscribers for this event type
            subscribers = self.subscribers.get(event_type, []).copy()

        # Call subscribers (outside lock to prevent deadlocks)
        for callback in subscribers:
            try:
                callback(event)
            except Exception as e:
                logger.error(
                    f"publish_event - ❌ Error in event callback for {event_type.__name__}: {e}"
                )

        logger.debug(f"publish_event - 📤 Published {event_type.__name__}")

    def subscribe(self, event_type: Type[BaseEvent], callback: Callable):
        """
        订阅事件类型

        Args:
            event_type: 事件类型（如 OrderStatusEvent）
            callback: 回调函数 callback(event: OrderStatusEvent)

        Example:
            def on_order_status(event: OrderStatusEvent):
                print(f"Order {event.order_id}: {event.status}")

            event_bus.subscribe(OrderStatusEvent, on_order_status)
        """
        with self.lock:
            if event_type not in self.subscribers:
                self.subscribers[event_type] = []

            if callback not in self.subscribers[event_type]:
                self.subscribers[event_type].append(callback)
                logger.debug(
                    f"subscribe - 📝 Subscribed to {event_type.__name__}: {callback.__name__}"
                )

    def unsubscribe(self, event_type: Type[BaseEvent], callback: Callable):
        """
        取消订阅事件类型

        Args:
            event_type: 事件类型
            callback: 回调函数
        """
        with self.lock:
            if event_type in self.subscribers:
                if callback in self.subscribers[event_type]:
                    self.subscribers[event_type].remove(callback)
                    logger.debug(
                        f"unsubscribe - ❌ Unsubscribed from {event_type.__name__}: {callback.__name__}"
                    )

    def get_history(
        self, event_type: Type[BaseEvent] = None, limit: int = 100
    ) -> List[BaseEvent]:
        """
        获取事件历史

        Args:
            event_type: 过滤事件类型（可选）
            limit: 最大返回数量

        Returns:
            事件列表
        """
        with self.lock:
            if event_type:
                events = [e for e in self.event_history if isinstance(e, event_type)]
            else:
                events = self.event_history.copy()

            return events[-limit:]

    def clear_history(self):
        """清空事件历史"""
        with self.lock:
            self.event_history.clear()

    def get_subscriber_count(self, event_type: Type[BaseEvent] = None) -> int:
        """
        获取订阅者数量

        Args:
            event_type: 特定事件类型（可选）

        Returns:
            订阅者数量
        """
        with self.lock:
            if event_type:
                return len(self.subscribers.get(event_type, []))
            else:
                return sum(len(subs) for subs in self.subscribers.values())

    def clear_subscribers(self, event_type: Type[BaseEvent] = None):
        """
        清空订阅者

        Args:
            event_type: 特定事件类型（可选）
        """
        with self.lock:
            if event_type:
                self.subscribers[event_type] = []
            else:
                self.subscribers.clear()


# Global event bus instance
_global_event_bus = None
_event_bus_lock = threading.Lock()


def get_event_bus() -> EventBus:
    """
    获取全局 EventBus 实例（单例）

    Returns:
        EventBus 实例
    """
    global _global_event_bus

    if _global_event_bus is None:
        with _event_bus_lock:
            if _global_event_bus is None:
                _global_event_bus = EventBus()

    return _global_event_bus


# Example usage:
"""
from IBBot.adapter.event_bus import get_event_bus
from IBBot.models.event_models import OrderPlacedEvent, OrderStatusEvent

# Get the event bus
event_bus = get_event_bus()

# Subscribe to events
def on_order_placed(event: OrderPlacedEvent):
    print(f"Order placed: {event.order_id}, {event.symbol}")

event_bus.subscribe(OrderPlacedEvent, on_order_placed)

# Publish type-safe events
event_bus.publish_event(OrderPlacedEvent(
    order_id=123,
    symbol='AAPL',
    action='BUY',
    quantity=100
))

# Unsubscribe
event_bus.unsubscribe(OrderPlacedEvent, on_order_placed)

# Get event history
recent_orders = event_bus.get_history(OrderPlacedEvent, limit=10)
"""
