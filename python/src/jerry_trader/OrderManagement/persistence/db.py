from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


@dataclass(frozen=True)
class DbConfig:
    url: str


def create_db_engine(cfg: DbConfig) -> Engine:
    # pool_pre_ping helps avoid stale connections
    return create_engine(cfg.url, pool_pre_ping=True)


def create_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(
        bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
    )
