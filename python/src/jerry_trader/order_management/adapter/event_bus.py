"""
Event Bus for order_management - 类型安全的事件驱动架构
Provides a publish-subscribe pattern for decoupling components

核心理念：
- 各模块不直接调用彼此，只和 event_bus 交互
- event_bus 是系统的消息中枢神经
- 直接使用事件类型订阅，无需额外的 Enum
"""

import asyncio
import inspect
import logging
import threading
from datetime import datetime
from typing import Awaitable, Callable, Dict, List, Optional, Type

# 导入所有事件类型
from jerry_trader.order_management.models.event_models import (
    AccountUpdatedEvent,
    BaseEvent,
    CompletedOrdersSyncEndEvent,
    ConnectionEvent,
    ErrorEvent,
    ExecutionReceivedEvent,
    OpenOrdersSyncEndEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    OrderStatusEvent,
    OrderSubmittedEvent,
    PositionUpdatedEvent,
)
from jerry_trader.utils.logger import setup_logger

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
        self._sync_subscribers: Dict[Type[BaseEvent], List[Callable]] = {}
        self._async_subscribers: Dict[
            Type[BaseEvent], List[Callable[[BaseEvent], Awaitable[None]]]
        ] = {}
        self.lock = threading.RLock()
        self.event_history: List[BaseEvent] = []
        self.max_history = 1000  # Keep last 1000 events
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        logger.info("🚌 EventBus initialized")

    def set_loop(self, loop: asyncio.AbstractEventLoop):
        """Bind an asyncio event loop for scheduling async subscribers.

        This is important when events are published from non-async threads
        (e.g., IB API callback thread) but async subscribers (e.g. WebSocket
        broadcast) need to run on the FastAPI event loop.
        """
        self._loop = loop

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
            sync_subs = self._sync_subscribers.get(event_type, []).copy()
            async_subs = self._async_subscribers.get(event_type, []).copy()

        # Call sync subscribers (outside lock to prevent deadlocks)
        for callback in sync_subs:
            try:
                callback(event)
            except Exception as e:
                logger.error(
                    f"publish_event - ❌ Error in sync callback for {event_type.__name__}: {e}"
                )

        # Schedule async subscribers on bound loop
        if async_subs:
            loop = self._loop
            if loop is None or not loop.is_running():
                logger.warning(
                    f"publish_event - ⚠️  Async subscribers exist for {event_type.__name__} but no running asyncio loop is bound; skipping async callbacks"
                )
            else:
                for callback in async_subs:

                    def _schedule(cb=callback):
                        task = loop.create_task(cb(event))

                        def _done(t: asyncio.Task):
                            exc = t.exception()
                            if exc:
                                logger.error(
                                    f"publish_event - ❌ Error in async callback for {event_type.__name__}: {exc}"
                                )

                        task.add_done_callback(_done)

                    loop.call_soon_threadsafe(_schedule)

        if (
            event_type.__name__ != "AccountUpdatedEvent"
        ):  # it will fload into tons of msg in a sec, don't display.
            logger.debug(f"publish_event - 📤 Published {event_type.__name__}")

    async def publish_event_async(self, event: BaseEvent):
        """Async publish that awaits async subscribers.

        Sync subscribers are executed via `asyncio.to_thread` to avoid blocking
        the event loop.
        """
        event_type = type(event)

        with self.lock:
            self.event_history.append(event)
            if len(self.event_history) > self.max_history:
                self.event_history.pop(0)

            sync_subs = self._sync_subscribers.get(event_type, []).copy()
            async_subs = self._async_subscribers.get(event_type, []).copy()

        async def _run_async(cb):
            try:
                await cb(event)
            except Exception as e:
                logger.error(
                    f"publish_event_async - ❌ Error in async callback for {event_type.__name__}: {e}"
                )

        def _run_sync(cb):
            try:
                cb(event)
            except Exception as e:
                logger.error(
                    f"publish_event_async - ❌ Error in sync callback for {event_type.__name__}: {e}"
                )

        await asyncio.gather(
            *(asyncio.to_thread(_run_sync, cb) for cb in sync_subs),
            *(_run_async(cb) for cb in async_subs),
        )

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
            if inspect.iscoroutinefunction(callback):
                if event_type not in self._async_subscribers:
                    self._async_subscribers[event_type] = []
                if callback not in self._async_subscribers[event_type]:
                    self._async_subscribers[event_type].append(callback)
                    logger.debug(
                        f"subscribe - 📝 Subscribed (async) to {event_type.__name__}: {callback.__name__}"
                    )
            else:
                if event_type not in self._sync_subscribers:
                    self._sync_subscribers[event_type] = []
                if callback not in self._sync_subscribers[event_type]:
                    self._sync_subscribers[event_type].append(callback)
                    logger.debug(
                        f"subscribe - 📝 Subscribed (sync) to {event_type.__name__}: {callback.__name__}"
                    )

    def unsubscribe(self, event_type: Type[BaseEvent], callback: Callable):
        """
        取消订阅事件类型

        Args:
            event_type: 事件类型
            callback: 回调函数
        """
        with self.lock:
            if (
                event_type in self._sync_subscribers
                and callback in self._sync_subscribers[event_type]
            ):
                self._sync_subscribers[event_type].remove(callback)
                logger.debug(
                    f"unsubscribe - ❌ Unsubscribed (sync) from {event_type.__name__}: {callback.__name__}"
                )

            if (
                event_type in self._async_subscribers
                and callback in self._async_subscribers[event_type]
            ):
                self._async_subscribers[event_type].remove(callback)
                logger.debug(
                    f"unsubscribe - ❌ Unsubscribed (async) from {event_type.__name__}: {callback.__name__}"
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
                return len(self._sync_subscribers.get(event_type, [])) + len(
                    self._async_subscribers.get(event_type, [])
                )
            else:
                return sum(len(subs) for subs in self._sync_subscribers.values()) + sum(
                    len(subs) for subs in self._async_subscribers.values()
                )

    def clear_subscribers(self, event_type: Type[BaseEvent] = None):
        """
        清空订阅者

        Args:
            event_type: 特定事件类型（可选）
        """
        with self.lock:
            if event_type:
                self._sync_subscribers[event_type] = []
                self._async_subscribers[event_type] = []
            else:
                self._sync_subscribers.clear()
                self._async_subscribers.clear()


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
from jerry_trader.order_management.adapter.event_bus import get_event_bus
from jerry_trader.order_management.models.event_models import OrderPlacedEvent, OrderStatusEvent

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
