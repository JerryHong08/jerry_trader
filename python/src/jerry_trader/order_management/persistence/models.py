from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from jerry_trader.order_management.persistence.db import Base

NY_TZ = ZoneInfo("America/New_York")


def now_et() -> datetime:
    """Return timezone-aware current time in America/New_York."""
    return datetime.now(NY_TZ)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account: Mapped[str] = mapped_column(
        String(32), nullable=False, default="", index=True
    )
    order_id: Mapped[int] = mapped_column(Integer, index=True)

    symbol: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    action: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    quantity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    order_type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    limit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tif: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    outside_rth: Mapped[Optional[bool]] = mapped_column(
        Integer, nullable=True
    )  # 0/1 to keep it portable
    sec_type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    last_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    filled: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    remaining: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    avg_fill_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    commission: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=now_et
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=now_et
    )

    __table_args__ = (
        UniqueConstraint("account", "order_id", name="uq_orders_account_order_id"),
    )


class OrderStatusLog(Base):
    __tablename__ = "order_status"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account: Mapped[str] = mapped_column(
        String(32), nullable=False, default="", index=True
    )
    order_id: Mapped[int] = mapped_column(Integer, index=True)
    status: Mapped[str] = mapped_column(String(32))

    filled: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    remaining: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    avg_fill_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    commission: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    symbol: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    action: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    quantity: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    order_type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    limit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    stop_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tif: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    outside_rth: Mapped[Optional[bool]] = mapped_column(Integer, nullable=True)

    sec_type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    perm_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    parent_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    client_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_fill_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=now_et, index=True
    )


class Execution(Base):
    __tablename__ = "executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account: Mapped[str] = mapped_column(
        String(32), nullable=False, default="", index=True
    )
    exec_id: Mapped[str] = mapped_column(String(128), index=True)

    order_id: Mapped[int] = mapped_column(Integer, index=True)
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(8))
    shares: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    exchange: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    time: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=now_et, index=True
    )

    __table_args__ = (
        UniqueConstraint("account", "exec_id", name="uq_exec_account_exec_id"),
    )


class PositionSnapshot(Base):
    __tablename__ = "positions_snapshot"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account: Mapped[str] = mapped_column(
        String(32), nullable=False, default="", index=True
    )
    symbol: Mapped[str] = mapped_column(String(32), index=True)

    position: Mapped[float] = mapped_column(Float)
    average_cost: Mapped[float] = mapped_column(Float)
    market_price: Mapped[float] = mapped_column(Float)
    market_value: Mapped[float] = mapped_column(Float)
    unrealized_pnl: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float)

    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=now_et, index=True
    )


class AccountSnapshot(Base):
    __tablename__ = "account_snapshot"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account: Mapped[str] = mapped_column(
        String(32), nullable=False, default="", index=True
    )
    tag: Mapped[str] = mapped_column(String(128), index=True)
    value: Mapped[Any] = mapped_column(JSON)
    currency: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=now_et, index=True
    )


class EventLog(Base):
    __tablename__ = "event_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account: Mapped[str] = mapped_column(
        String(32), nullable=False, default="", index=True
    )
    event_name: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[Any] = mapped_column(JSON)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=now_et, index=True
    )
