"""Create Pillar 5 price memory materialized view and indexes.

Revision ID: 20260919_01
Revises: 20260918_04

This migration creates the mv_p5_price_memory materialized view, which aggregates
finished events with official results and exact market odds (1X2, Home/Away in Full Time)
across bookmakers (Pinnacle, Bet365, SofaScore, etc.) with dedicated composite B-Tree indexes
for sub-millisecond price memory lookups.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from infrastructure.persistence.views.p5_price_memory_view import (
    MV_P5_PRICE_MEMORY_INDEXES_SQL,
    build_p5_price_memory_view_sql,
)

revision: str = "20260919_01"
down_revision: Union[str, None] = "20260918_04"
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
