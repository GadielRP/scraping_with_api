"""Refine Pillar 5 price memory materialized view and composite indexes.

Revision ID: 20260920_01
Revises: 20260919_03

This migration recreates the mv_p5_price_memory materialized view with:
1. Atomic single-market vector guarantee (avoiding cross-market Frankenstein vectors).
2. Pre-match snapshot temporal cutoff (mcs.collected_at <= e.starts_at).
3. Optimized composite B-Tree indexes including sport and starts_at DESC for sub-millisecond lookups.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from infrastructure.persistence.views.p5_price_memory_view import (
    MV_P5_PRICE_MEMORY_INDEXES_SQL,
    build_p5_price_memory_view_sql,
)

revision: str = "20260920_01"
down_revision: Union[str, None] = "20260919_03"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS mv_p5_price_memory CASCADE;"))
    bind.execute(sa.text(build_p5_price_memory_view_sql(include_quote_fallbacks=True)))
    for index_sql in MV_P5_PRICE_MEMORY_INDEXES_SQL:
        bind.execute(sa.text(index_sql))


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS mv_p5_price_memory CASCADE;"))
