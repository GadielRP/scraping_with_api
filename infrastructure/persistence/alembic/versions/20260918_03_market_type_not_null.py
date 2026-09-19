"""Require canonical market type ids after the Phase 2 backfill.

Revision ID: 20260918_03
Revises: 20260918_02
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260918_03"
down_revision: Union[str, None] = "20260918_02"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_names(table_name: str) -> set[str]:
    return {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }


def upgrade() -> None:
    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if "markets" not in tables:
        return
    if "market_type_id" not in _column_names("markets"):
        raise RuntimeError("markets.market_type_id must exist before Phase 2 constraint")

    unresolved = bind.execute(
        sa.text("SELECT COUNT(*) FROM markets WHERE market_type_id IS NULL")
    ).scalar_one()
    if unresolved:
        raise RuntimeError(
            f"Cannot make markets.market_type_id NOT NULL: {unresolved} rows remain"
        )

    # PostgreSQL is the production target. SQLite create_all() test databases
    # keep the additive nullable metadata until their fixtures are canonical.
    if bind.dialect.name == "postgresql":
        op.alter_column(
            "markets",
            "market_type_id",
            existing_type=sa.SmallInteger(),
            nullable=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.alter_column(
            "markets",
            "market_type_id",
            existing_type=sa.SmallInteger(),
            nullable=True,
        )
