"""Migrate line_value to unconstrained NUMERIC and strip trailing zeroes.

Revision ID: 20260923_01
Revises: 20260921_02
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260923_01"
down_revision: Union[str, None] = "20260921_02"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _columns(table_name: str) -> set[str]:
    return {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }


def upgrade() -> None:
    bind = op.get_bind()
    tables = _tables()

    if "markets" in tables and "line_value" in _columns("markets"):
        if bind.dialect.name == "postgresql":
            bind.execute(
                sa.text(
                    "ALTER TABLE markets ALTER COLUMN line_value TYPE NUMERIC USING (line_value::float8)::numeric;"
                )
            )
        else:
            with op.batch_alter_table("markets") as batch_op:
                batch_op.alter_column(
                    "line_value",
                    existing_type=sa.Numeric(18, 6),
                    type_=sa.Numeric(),
                    nullable=True,
                )

    if "pillar_mining_units" in tables and "line_value" in _columns("pillar_mining_units"):
        if bind.dialect.name == "postgresql":
            bind.execute(
                sa.text(
                    "ALTER TABLE pillar_mining_units ALTER COLUMN line_value TYPE NUMERIC USING (line_value::float8)::numeric;"
                )
            )
        else:
            with op.batch_alter_table("pillar_mining_units") as batch_op:
                batch_op.alter_column(
                    "line_value",
                    existing_type=sa.Numeric(18, 6),
                    type_=sa.Numeric(),
                    nullable=True,
                )


def downgrade() -> None:
    bind = op.get_bind()
    tables = _tables()

    if "markets" in tables and "line_value" in _columns("markets"):
        if bind.dialect.name == "postgresql":
            bind.execute(
                sa.text(
                    "ALTER TABLE markets ALTER COLUMN line_value TYPE NUMERIC(18, 6);"
                )
            )
        else:
            with op.batch_alter_table("markets") as batch_op:
                batch_op.alter_column(
                    "line_value",
                    existing_type=sa.Numeric(),
                    type_=sa.Numeric(18, 6),
                    nullable=True,
                )

    if "pillar_mining_units" in tables and "line_value" in _columns("pillar_mining_units"):
        if bind.dialect.name == "postgresql":
            bind.execute(
                sa.text(
                    "ALTER TABLE pillar_mining_units ALTER COLUMN line_value TYPE NUMERIC(18, 6);"
                )
            )
        else:
            with op.batch_alter_table("pillar_mining_units") as batch_op:
                batch_op.alter_column(
                    "line_value",
                    existing_type=sa.Numeric(),
                    type_=sa.Numeric(18, 6),
                    nullable=True,
                )
