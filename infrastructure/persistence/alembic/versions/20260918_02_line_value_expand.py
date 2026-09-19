"""Add numeric market line identity and retain choice_group as a mirror.

Revision ID: 20260918_02
Revises: 20260918_01
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260918_02"
down_revision: Union[str, None] = "20260918_01"
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

    if "line_value" not in _column_names("markets"):
        op.add_column(
            "markets",
            sa.Column("line_value", sa.Numeric(18, 6), nullable=True),
        )

    # The previous correction pass guarantees that non-empty choice_group
    # values are signed numeric strings. Empty strings represent no line.
    if "choice_group" in _column_names("markets"):
        op.execute(
            "UPDATE markets SET line_value = "
            "CAST(NULLIF(TRIM(choice_group), '') AS NUMERIC) "
            "WHERE line_value IS NULL AND choice_group IS NOT NULL "
            "AND TRIM(choice_group) <> ''"
        )

    op.execute("DROP INDEX IF EXISTS uq_markets_canonical_without_line")
    op.execute("DROP INDEX IF EXISTS uq_markets_canonical_with_line")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_canonical_without_line "
        "ON markets (event_id, bookie_id, market_type_id, is_live) "
        "WHERE market_type_id IS NOT NULL AND line_value IS NULL"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_canonical_with_line "
        "ON markets (event_id, bookie_id, market_type_id, line_value, is_live) "
        "WHERE market_type_id IS NOT NULL AND line_value IS NOT NULL"
    )


def downgrade() -> None:
    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if "markets" not in tables or "line_value" not in _column_names("markets"):
        return

    op.execute("DROP INDEX IF EXISTS uq_markets_canonical_without_line")
    op.execute("DROP INDEX IF EXISTS uq_markets_canonical_with_line")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_canonical_without_line "
        "ON markets (event_id, bookie_id, market_type_id, is_live) "
        "WHERE market_type_id IS NOT NULL AND choice_group IS NULL"
    )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_canonical_with_line "
        "ON markets (event_id, bookie_id, market_type_id, choice_group, is_live) "
        "WHERE market_type_id IS NOT NULL AND choice_group IS NOT NULL"
    )
    op.drop_column("markets", "line_value")
