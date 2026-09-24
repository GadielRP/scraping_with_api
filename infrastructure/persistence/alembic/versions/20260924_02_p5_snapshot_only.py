"""Remove cache fallbacks from P5 historical prices.

Revision ID: 20260924_02
Revises: 20260924_01
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from infrastructure.persistence.views.p5_price_memory_view import (
    MV_P5_PRICE_MEMORY_INDEXES_SQL,
    build_p5_price_memory_view_sql,
)

revision: str = "20260924_02"
down_revision: Union[str, None] = "20260924_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # Rebuild after the diagnostics-only migration so teams can first capture
    # a COALESCE baseline, then upgrade to snapshot-only historical prices.
    bind.execute(
        sa.text("DROP MATERIALIZED VIEW IF EXISTS mv_p5_price_memory CASCADE")
    )
    bind.execute(
        sa.text(build_p5_price_memory_view_sql(include_quote_fallbacks=False))
    )
    for index_sql in MV_P5_PRICE_MEMORY_INDEXES_SQL:
        bind.execute(sa.text(index_sql))


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    bind.execute(
        sa.text("DROP MATERIALIZED VIEW IF EXISTS mv_p5_price_memory CASCADE")
    )
    bind.execute(sa.text(build_p5_price_memory_view_sql(include_quote_fallbacks=True)))
    for index_sql in MV_P5_PRICE_MEMORY_INDEXES_SQL:
        bind.execute(sa.text(index_sql))
