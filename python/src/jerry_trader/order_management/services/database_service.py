from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy.exc import IntegrityError

from jerry_trader.order_management.adapter.event_bus import get_event_bus
from jerry_trader.order_management.models.event_models import (
    AccountUpdatedEvent,
    ConnectionEvent,
    ErrorEvent,
    ExecutionReceivedEvent,
    OrderCancelledEvent,
    OrderPlacedEvent,
    OrderStatusEvent,
    PositionUpdatedEvent,
)
from jerry_trader.order_management.persistence.db import (
    DbConfig,
    create_db_engine,
    create_session_factory,
)
from jerry_trader.order_management.persistence.models import (
    AccountSnapshot,
    EventLog,
    Execution,
    Order,
    OrderStatusLog,
    PositionSnapshot,
)
from jerry_trader.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.INFO)


def _json_sanitize(value: Any) -> Any:
    """Convert values into JSON-serializable primitives.

    Postgres JSON/JSONB requires strict JSON (no NaN/Infinity). IBKR values may
    include Decimal or other non-JSON-native types.
    """

    if value is None:
        return None

    if isinstance(value, (str, int, bool)):
        return value

    if isinstance(value, float):
        return value if math.isfinite(value) else None

    if isinstance(value, Decimal):
        # Keep precision by storing as string.
        return str(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [_json_sanitize(v) for v in value]

    # Fallback: represent as string to avoid failing the entire write.
    return str(value)


class DatabaseService:
    """Persist key events to Postgres via SQLAlchemy.

    Design goals:
    - Keep trading/event-handling logic decoupled from persistence.
    - Never block IBKR callback threads: DB writes are executed via `asyncio.to_thread`.
    - Be best-effort: DB failures should not break trading.
    """

    def __init__(self, database_url: str):
        self.cfg = DbConfig(url=database_url)
        self.engine = create_db_engine(self.cfg)
        self.Session = create_session_factory(self.engine)

        self._enabled = True
        self._event_bus = get_event_bus()

    def create_tables(self):
        """Create tables if they don't exist.

        For production, prefer Alembic migrations; this is a pragmatic bootstrap.
        """
        from jerry_trader.order_management.persistence.db import Base

        Base.metadata.create_all(self.engine)

    def subscribe(self):
        # Orders
        self._event_bus.subscribe(OrderPlacedEvent, self._on_order_placed)
        self._event_bus.subscribe(OrderStatusEvent, self._on_order_status)
        self._event_bus.subscribe(OrderCancelledEvent, self._on_order_cancelled)
        self._event_bus.subscribe(ExecutionReceivedEvent, self._on_execution)

        # Portfolio
        self._event_bus.subscribe(PositionUpdatedEvent, self._on_position)
        self._event_bus.subscribe(AccountUpdatedEvent, self._on_account)

        # System
        self._event_bus.subscribe(ConnectionEvent, self._on_system_event)
        self._event_bus.subscribe(ErrorEvent, self._on_system_event)

        logger.info("DatabaseService - ✅ subscribed to events")

    def unsubscribe(self):
        self._event_bus.unsubscribe(OrderPlacedEvent, self._on_order_placed)
        self._event_bus.unsubscribe(OrderStatusEvent, self._on_order_status)
        self._event_bus.unsubscribe(OrderCancelledEvent, self._on_order_cancelled)
        self._event_bus.unsubscribe(ExecutionReceivedEvent, self._on_execution)
        self._event_bus.unsubscribe(PositionUpdatedEvent, self._on_position)
        self._event_bus.unsubscribe(AccountUpdatedEvent, self._on_account)
        self._event_bus.unsubscribe(ConnectionEvent, self._on_system_event)
        self._event_bus.unsubscribe(ErrorEvent, self._on_system_event)

    async def _on_order_placed(self, event: OrderPlacedEvent):
        await self._write_async(self._write_order_placed, event)

    async def _on_order_status(self, event: OrderStatusEvent):
        await self._write_async(self._write_order_status, event)

    async def _on_order_cancelled(self, event: OrderCancelledEvent):
        await self._write_async(self._write_order_cancelled, event)

    async def _on_execution(self, event: ExecutionReceivedEvent):
        await self._write_async(self._write_execution, event)

    async def _on_position(self, event: PositionUpdatedEvent):
        await self._write_async(self._write_position, event)

    async def _on_account(self, event: AccountUpdatedEvent):
        await self._write_async(self._write_account, event)

    async def _on_system_event(self, event: Any):
        await self._write_async(self._write_event_log, event)

    async def _write_async(self, fn, event):
        if not self._enabled:
            return
        try:
            await asyncio.to_thread(fn, event)
        except Exception as e:
            # best-effort: disable after repeated errors to avoid spamming
            logger.error(f"DatabaseService - ✗ DB write failed: {e}")

    def _upsert_order_row(
        self,
        *,
        account: str,
        order_id: int,
        symbol: Optional[str] = None,
        action: Optional[str] = None,
        quantity: Optional[int] = None,
        order_type: Optional[str] = None,
        limit_price: Optional[float] = None,
        tif: Optional[str] = None,
        outside_rth: Optional[bool] = None,
        sec_type: Optional[str] = None,
        reason: Optional[str] = None,
        last_status: Optional[str] = None,
        filled: Optional[int] = None,
        remaining: Optional[int] = None,
        avg_fill_price: Optional[float] = None,
        commission: Optional[float] = None,
    ):
        """Insert or update an order row.

        Handles race condition between simultaneous events for the SAME new order
        by catching IntegrityError and retrying as update. Will still raise error
        for historical conflicts (different orders reusing the same order_id).
        """
        from jerry_trader.order_management.persistence.models import now_et

        account_norm = (account or "").strip()

        def _do_update(session):
            row = (
                session.query(Order)
                .filter(Order.account == account_norm, Order.order_id == int(order_id))
                .one_or_none()
            )
            if row is None:
                # Historical record was deleted between our insert attempt and now
                raise IntegrityError("Row not found for update", None, None)

            if symbol is not None:
                row.symbol = symbol
            if action is not None:
                row.action = action
            if quantity is not None:
                row.quantity = int(quantity)
            if order_type is not None:
                row.order_type = order_type
            if limit_price is not None:
                row.limit_price = float(limit_price)
            if tif is not None:
                row.tif = tif
            if outside_rth is not None:
                row.outside_rth = 1 if bool(outside_rth) else 0
            if sec_type is not None:
                row.sec_type = sec_type
            if reason is not None:
                row.reason = reason
            if last_status is not None:
                row.last_status = last_status
            if filled is not None:
                row.filled = int(filled)
            if remaining is not None:
                row.remaining = int(remaining)
            if avg_fill_price is not None:
                row.avg_fill_price = float(avg_fill_price)
            if commission is not None:
                row.commission = float(commission)

            row.updated_at = now_et()
            session.commit()

        with self.Session() as session:
            # First check if row exists
            existing = (
                session.query(Order)
                .filter(Order.account == account_norm, Order.order_id == int(order_id))
                .one_or_none()
            )

            if existing is not None:
                # Row exists - update it
                _do_update(session)
                return

            # Row doesn't exist - try to insert
            try:
                row = Order(account=account_norm, order_id=int(order_id))
                if symbol is not None:
                    row.symbol = symbol
                if action is not None:
                    row.action = action
                if quantity is not None:
                    row.quantity = int(quantity)
                if order_type is not None:
                    row.order_type = order_type
                if limit_price is not None:
                    row.limit_price = float(limit_price)
                if tif is not None:
                    row.tif = tif
                if outside_rth is not None:
                    row.outside_rth = 1 if bool(outside_rth) else 0
                if sec_type is not None:
                    row.sec_type = sec_type
                if reason is not None:
                    row.reason = reason
                if last_status is not None:
                    row.last_status = last_status
                if filled is not None:
                    row.filled = int(filled)
                if remaining is not None:
                    row.remaining = int(remaining)
                if avg_fill_price is not None:
                    row.avg_fill_price = float(avg_fill_price)
                if commission is not None:
                    row.commission = float(commission)

                row.created_at = now_et()
                row.updated_at = now_et()
                session.add(row)
                session.commit()
            except IntegrityError:
                # Race condition: another event inserted the same order_id just now
                # Retry as update
                session.rollback()
                _do_update(session)

    def _write_order_placed(self, event: OrderPlacedEvent):
        self._upsert_order_row(
            account=(event.account or ""),
            order_id=event.order_id,
            symbol=event.symbol,
            action=event.action,
            quantity=event.quantity,
            order_type=event.order_type,
            limit_price=event.limit_price,
            outside_rth=event.outsideRth,
            reason=event.reason,
            last_status="Placed",
        )

        payload = _json_sanitize(asdict(event))
        payload.pop("timestamp", None)
        with self.Session() as session:
            session.add(
                EventLog(
                    account=(event.account or "").strip(),
                    event_name=type(event).__name__,
                    payload=payload,
                )
            )
            session.commit()

    def _write_order_status(self, event: OrderStatusEvent):
        payload = _json_sanitize(asdict(event))
        payload.pop("timestamp", None)
        account_norm = (event.account or "").strip()

        with self.Session() as session:
            session.add(
                OrderStatusLog(
                    account=account_norm,
                    order_id=int(event.order_id),
                    status=event.status,
                    filled=int(event.filled),
                    remaining=int(event.remaining),
                    avg_fill_price=float(event.avg_fill_price),
                    commission=(
                        float(event.commission)
                        if event.commission is not None
                        else None
                    ),
                    symbol=event.symbol,
                    action=event.action,
                    quantity=(
                        int(event.quantity) if event.quantity is not None else None
                    ),
                    order_type=event.order_type,
                    limit_price=(
                        float(event.limit_price)
                        if event.limit_price is not None
                        else None
                    ),
                    stop_price=(
                        float(event.stop_price)
                        if event.stop_price is not None
                        else None
                    ),
                    tif=event.tif,
                    outside_rth=(
                        1
                        if event.outsideRth
                        else (0 if event.outsideRth is not None else None)
                    ),
                    sec_type=event.sec_type,
                    exchange=event.exchange,
                    currency=event.currency,
                    perm_id=event.perm_id,
                    parent_id=event.parent_id,
                    client_id=event.client_id,
                    last_fill_price=(
                        float(event.last_fill_price)
                        if event.last_fill_price is not None
                        else None
                    ),
                )
            )
            session.add(
                EventLog(
                    account=account_norm,
                    event_name=type(event).__name__,
                    payload=payload,
                )
            )
            session.commit()

        self._upsert_order_row(
            account=account_norm,
            order_id=event.order_id,
            symbol=event.symbol,
            action=event.action,
            quantity=event.quantity,
            order_type=event.order_type,
            limit_price=event.limit_price,
            tif=event.tif,
            outside_rth=event.outsideRth,
            sec_type=event.sec_type,
            last_status=event.status,
            filled=event.filled,
            remaining=event.remaining,
            avg_fill_price=event.avg_fill_price,
            commission=event.commission,
        )

    def _write_order_cancelled(self, event: OrderCancelledEvent):
        payload = _json_sanitize(asdict(event))
        payload.pop("timestamp", None)
        account_norm = (getattr(event, "account", None) or "").strip()
        with self.Session() as session:
            session.add(
                EventLog(
                    account=account_norm,
                    event_name=type(event).__name__,
                    payload=payload,
                )
            )
            session.commit()

    def _write_execution(self, event: ExecutionReceivedEvent):
        payload = _json_sanitize(asdict(event))
        payload.pop("timestamp", None)
        account_norm = (event.account or "").strip()
        with self.Session() as session:
            session.add(
                EventLog(
                    account=account_norm,
                    event_name=type(event).__name__,
                    payload=payload,
                )
            )
            try:
                session.add(
                    Execution(
                        account=account_norm,
                        exec_id=str(event.exec_id),
                        order_id=int(event.order_id),
                        symbol=str(event.symbol),
                        side=str(event.side),
                        shares=float(event.shares),
                        price=float(event.price),
                        exchange=(
                            str(event.exchange) if event.exchange is not None else None
                        ),
                        time=str(event.time) if event.time is not None else None,
                    )
                )
                session.commit()
            except IntegrityError:
                session.rollback()

    def _write_position(self, event: PositionUpdatedEvent):
        payload = _json_sanitize(asdict(event))
        payload.pop("timestamp", None)
        account_norm = (event.account or "").strip()
        with self.Session() as session:
            session.add(
                EventLog(
                    account=account_norm,
                    event_name=type(event).__name__,
                    payload=payload,
                )
            )
            session.add(
                PositionSnapshot(
                    account=account_norm,
                    symbol=str(event.symbol),
                    position=float(event.position),
                    average_cost=float(event.average_cost),
                    market_price=float(event.market_price),
                    market_value=float(event.market_value),
                    unrealized_pnl=float(event.unrealized_pnl),
                    realized_pnl=float(event.realized_pnl),
                )
            )
            session.commit()

    def _write_account(self, event: AccountUpdatedEvent):
        payload = _json_sanitize(asdict(event))
        payload.pop("timestamp", None)
        account_norm = (event.account or "").strip()
        with self.Session() as session:
            session.add(
                EventLog(
                    account=account_norm,
                    event_name=type(event).__name__,
                    payload=payload,
                )
            )
            session.add(
                AccountSnapshot(
                    account=account_norm,
                    tag=str(event.tag),
                    value=_json_sanitize(event.value),
                    currency=(
                        str(event.currency) if event.currency is not None else None
                    ),
                )
            )
            session.commit()

    def _write_event_log(self, event: Any):
        try:
            payload = _json_sanitize(asdict(event))
        except Exception:
            payload = {"repr": repr(event)}
        payload.pop("timestamp", None)

        account_norm = ""
        try:
            account_norm = (payload.get("account") or "").strip()
        except Exception:
            account_norm = ""

        with self.Session() as session:
            session.add(
                EventLog(
                    account=account_norm,
                    event_name=type(event).__name__,
                    payload=payload,
                )
            )
            session.commit()
