"""Bootstrap IBBot persistence schema

This migration is intentionally defensive:
- If tables do not exist, it creates them.
- If tables exist from a previous bootstrap (create_all), it adds the `account` discriminator
  and upgrades timestamp columns to timestamptz where applicable.

Revision ID: 7d6b0b6a0a21
Revises:
Create Date: 2025-12-15

"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import inspect

from alembic import op

revision = "7d6b0b6a0a21"
down_revision = None
branch_labels = None
depends_on = None


def _has_table(bind, name: str) -> bool:
    return inspect(bind).has_table(name)


def _has_column(bind, table: str, column: str) -> bool:
    cols = {c["name"] for c in inspect(bind).get_columns(table)}
    return column in cols


def _unique_constraints(bind, table: str):
    return inspect(bind).get_unique_constraints(table)


def _drop_unique_on_columns(bind, table: str, column_names: list[str]):
    for uc in _unique_constraints(bind, table):
        if uc.get("column_names") == column_names and uc.get("name"):
            op.drop_constraint(uc["name"], table, type_="unique")


def _upgrade_timestamp_to_timestamptz(table: str, column: str):
    """Convert legacy UTC-naive timestamps to timestamptz.

    We treat existing values as UTC timestamps and convert to timestamptz.
    """
    op.alter_column(
        table,
        column,
        type_=sa.DateTime(timezone=True),
        postgresql_using=f"{column} AT TIME ZONE 'UTC'",
        existing_nullable=False,
    )


def upgrade() -> None:
    bind = op.get_bind()
    dialect = bind.dialect.name

    # -----------------------
    # orders
    # -----------------------
    if not _has_table(bind, "orders"):
        op.create_table(
            "orders",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "account", sa.String(length=32), nullable=False, server_default=""
            ),
            sa.Column("order_id", sa.Integer(), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=True),
            sa.Column("action", sa.String(length=8), nullable=True),
            sa.Column("quantity", sa.Integer(), nullable=True),
            sa.Column("order_type", sa.String(length=16), nullable=True),
            sa.Column("limit_price", sa.Float(), nullable=True),
            sa.Column("tif", sa.String(length=16), nullable=True),
            sa.Column("outside_rth", sa.Integer(), nullable=True),
            sa.Column("sec_type", sa.String(length=16), nullable=True),
            sa.Column("reason", sa.Text(), nullable=True),
            sa.Column("last_status", sa.String(length=32), nullable=True),
            sa.Column("filled", sa.Integer(), nullable=True),
            sa.Column("remaining", sa.Integer(), nullable=True),
            sa.Column("avg_fill_price", sa.Float(), nullable=True),
            sa.Column("commission", sa.Float(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint(
                "account", "order_id", name="uq_orders_account_order_id"
            ),
        )
        op.create_index("ix_orders_account", "orders", ["account"], unique=False)
        op.create_index("ix_orders_order_id", "orders", ["order_id"], unique=False)
    else:
        if not _has_column(bind, "orders", "account"):
            op.add_column(
                "orders",
                sa.Column(
                    "account", sa.String(length=32), nullable=False, server_default=""
                ),
            )
        # Convert timestamps (Postgres only)
        if dialect == "postgresql":
            if _has_column(bind, "orders", "created_at"):
                _upgrade_timestamp_to_timestamptz("orders", "created_at")
            if _has_column(bind, "orders", "updated_at"):
                _upgrade_timestamp_to_timestamptz("orders", "updated_at")

        # Replace legacy unique(order_id) with unique(account, order_id)
        for uc in _unique_constraints(bind, "orders"):
            if uc.get("name") == "uq_orders_order_id":
                op.drop_constraint("uq_orders_order_id", "orders", type_="unique")
        if not any(
            uc.get("name") == "uq_orders_account_order_id"
            for uc in _unique_constraints(bind, "orders")
        ):
            op.create_unique_constraint(
                "uq_orders_account_order_id", "orders", ["account", "order_id"]
            )

    # -----------------------
    # order_status
    # -----------------------
    if not _has_table(bind, "order_status"):
        op.create_table(
            "order_status",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column(
                "account", sa.String(length=32), nullable=False, server_default=""
            ),
            sa.Column("order_id", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("filled", sa.Integer(), nullable=True),
            sa.Column("remaining", sa.Integer(), nullable=True),
            sa.Column("avg_fill_price", sa.Float(), nullable=True),
            sa.Column("commission", sa.Float(), nullable=True),
            sa.Column("symbol", sa.String(length=32), nullable=True),
            sa.Column("action", sa.String(length=8), nullable=True),
            sa.Column("quantity", sa.Integer(), nullable=True),
            sa.Column("order_type", sa.String(length=16), nullable=True),
            sa.Column("limit_price", sa.Float(), nullable=True),
            sa.Column("stop_price", sa.Float(), nullable=True),
            sa.Column("tif", sa.String(length=16), nullable=True),
            sa.Column("outside_rth", sa.Integer(), nullable=True),
            sa.Column("sec_type", sa.String(length=16), nullable=True),
            sa.Column("exchange", sa.String(length=32), nullable=True),
            sa.Column("currency", sa.String(length=16), nullable=True),
            sa.Column("perm_id", sa.Integer(), nullable=True),
            sa.Column("parent_id", sa.Integer(), nullable=True),
            sa.Column("client_id", sa.Integer(), nullable=True),
            sa.Column("last_fill_price", sa.Float(), nullable=True),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index(
            "ix_order_status_account", "order_status", ["account"], unique=False
        )
        op.create_index(
            "ix_order_status_order_id", "order_status", ["order_id"], unique=False
        )
        op.create_index(
            "ix_order_status_recorded_at", "order_status", ["recorded_at"], unique=False
        )
    else:
        if not _has_column(bind, "order_status", "account"):
            op.add_column(
                "order_status",
                sa.Column(
                    "account", sa.String(length=32), nullable=False, server_default=""
                ),
            )
        if dialect == "postgresql" and _has_column(bind, "order_status", "recorded_at"):
            _upgrade_timestamp_to_timestamptz("order_status", "recorded_at")

    # -----------------------
    # executions
    # -----------------------
    if not _has_table(bind, "executions"):
        op.create_table(
            "executions",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column(
                "account", sa.String(length=32), nullable=False, server_default=""
            ),
            sa.Column("exec_id", sa.String(length=128), nullable=False),
            sa.Column("order_id", sa.Integer(), nullable=False),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("side", sa.String(length=8), nullable=False),
            sa.Column("shares", sa.Float(), nullable=False),
            sa.Column("price", sa.Float(), nullable=False),
            sa.Column("exchange", sa.String(length=32), nullable=True),
            sa.Column("time", sa.String(length=64), nullable=True),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("account", "exec_id", name="uq_exec_account_exec_id"),
        )
        op.create_index(
            "ix_executions_account", "executions", ["account"], unique=False
        )
        op.create_index(
            "ix_executions_exec_id", "executions", ["exec_id"], unique=False
        )
        op.create_index(
            "ix_executions_order_id", "executions", ["order_id"], unique=False
        )
        op.create_index(
            "ix_executions_recorded_at", "executions", ["recorded_at"], unique=False
        )
    else:
        if not _has_column(bind, "executions", "account"):
            op.add_column(
                "executions",
                sa.Column(
                    "account", sa.String(length=32), nullable=False, server_default=""
                ),
            )
        if dialect == "postgresql" and _has_column(bind, "executions", "recorded_at"):
            _upgrade_timestamp_to_timestamptz("executions", "recorded_at")

        # Drop legacy unique(exec_id) if present (name may vary)
        _drop_unique_on_columns(bind, "executions", ["exec_id"])

        if not any(
            uc.get("name") == "uq_exec_account_exec_id"
            for uc in _unique_constraints(bind, "executions")
        ):
            op.create_unique_constraint(
                "uq_exec_account_exec_id", "executions", ["account", "exec_id"]
            )

    # -----------------------
    # positions_snapshot
    # -----------------------
    if not _has_table(bind, "positions_snapshot"):
        op.create_table(
            "positions_snapshot",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column(
                "account", sa.String(length=32), nullable=False, server_default=""
            ),
            sa.Column("symbol", sa.String(length=32), nullable=False),
            sa.Column("position", sa.Float(), nullable=False),
            sa.Column("average_cost", sa.Float(), nullable=False),
            sa.Column("market_price", sa.Float(), nullable=False),
            sa.Column("market_value", sa.Float(), nullable=False),
            sa.Column("unrealized_pnl", sa.Float(), nullable=False),
            sa.Column("realized_pnl", sa.Float(), nullable=False),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index(
            "ix_positions_snapshot_account",
            "positions_snapshot",
            ["account"],
            unique=False,
        )
        op.create_index(
            "ix_positions_snapshot_symbol",
            "positions_snapshot",
            ["symbol"],
            unique=False,
        )
        op.create_index(
            "ix_positions_snapshot_recorded_at",
            "positions_snapshot",
            ["recorded_at"],
            unique=False,
        )
    else:
        if not _has_column(bind, "positions_snapshot", "account"):
            op.add_column(
                "positions_snapshot",
                sa.Column(
                    "account", sa.String(length=32), nullable=False, server_default=""
                ),
            )
        if dialect == "postgresql" and _has_column(
            bind, "positions_snapshot", "recorded_at"
        ):
            _upgrade_timestamp_to_timestamptz("positions_snapshot", "recorded_at")

    # -----------------------
    # account_snapshot
    # -----------------------
    if not _has_table(bind, "account_snapshot"):
        op.create_table(
            "account_snapshot",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column(
                "account", sa.String(length=32), nullable=False, server_default=""
            ),
            sa.Column("tag", sa.String(length=128), nullable=False),
            sa.Column("value", sa.JSON(), nullable=False),
            sa.Column("currency", sa.String(length=16), nullable=True),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index(
            "ix_account_snapshot_account", "account_snapshot", ["account"], unique=False
        )
        op.create_index(
            "ix_account_snapshot_tag", "account_snapshot", ["tag"], unique=False
        )
        op.create_index(
            "ix_account_snapshot_recorded_at",
            "account_snapshot",
            ["recorded_at"],
            unique=False,
        )
    else:
        if not _has_column(bind, "account_snapshot", "account"):
            op.add_column(
                "account_snapshot",
                sa.Column(
                    "account", sa.String(length=32), nullable=False, server_default=""
                ),
            )
        if dialect == "postgresql" and _has_column(
            bind, "account_snapshot", "recorded_at"
        ):
            _upgrade_timestamp_to_timestamptz("account_snapshot", "recorded_at")

    # -----------------------
    # event_log
    # -----------------------
    if not _has_table(bind, "event_log"):
        op.create_table(
            "event_log",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
            sa.Column(
                "account", sa.String(length=32), nullable=False, server_default=""
            ),
            sa.Column("event_name", sa.String(length=128), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        )
        op.create_index("ix_event_log_account", "event_log", ["account"], unique=False)
        op.create_index(
            "ix_event_log_event_name", "event_log", ["event_name"], unique=False
        )
        op.create_index(
            "ix_event_log_recorded_at", "event_log", ["recorded_at"], unique=False
        )
    else:
        if not _has_column(bind, "event_log", "account"):
            op.add_column(
                "event_log",
                sa.Column(
                    "account", sa.String(length=32), nullable=False, server_default=""
                ),
            )
        if dialect == "postgresql" and _has_column(bind, "event_log", "recorded_at"):
            _upgrade_timestamp_to_timestamptz("event_log", "recorded_at")


def downgrade() -> None:
    # Non-destructive downgrade: drop created tables if they exist.
    bind = op.get_bind()

    for table in [
        "event_log",
        "account_snapshot",
        "positions_snapshot",
        "executions",
        "order_status",
        "orders",
    ]:
        if _has_table(bind, table):
            op.drop_table(table)
