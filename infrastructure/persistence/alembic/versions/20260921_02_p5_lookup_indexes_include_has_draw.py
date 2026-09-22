"""Include has_draw in the Pillar 5 exact-price lookup indexes.

Revision ID: 20260921_02
Revises: 20260921_01

The preceding migration recreates the materialized view and uses the index
definitions available at migration execution time.  This incremental migration
also upgrades databases where 20260921_01 was applied before ``has_draw`` was
added to the composite lookup indexes.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260921_02"
down_revision: Union[str, None] = "20260921_01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("DROP INDEX IF EXISTS idx_mv_p5_lookup_1x2;"))
    bind.execute(sa.text("DROP INDEX IF EXISTS idx_mv_p5_lookup_2way;"))
    bind.execute(
        sa.text(
            "CREATE INDEX idx_mv_p5_lookup_1x2 "
            "ON mv_p5_price_memory "
            "(sport, bookie_id, market_group, market_period, has_draw, "
            "odds_home, odds_draw, odds_away, starts_at DESC) "
            "WHERE odds_draw IS NOT NULL;"
        )
    )
    bind.execute(
        sa.text(
            "CREATE INDEX idx_mv_p5_lookup_2way "
            "ON mv_p5_price_memory "
            "(sport, bookie_id, market_group, market_period, has_draw, "
            "odds_home, odds_away, starts_at DESC) "
            "WHERE odds_draw IS NULL;"
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("DROP INDEX IF EXISTS idx_mv_p5_lookup_1x2;"))
    bind.execute(sa.text("DROP INDEX IF EXISTS idx_mv_p5_lookup_2way;"))
    bind.execute(
        sa.text(
            "CREATE INDEX idx_mv_p5_lookup_1x2 "
            "ON mv_p5_price_memory "
            "(sport, bookie_id, market_group, market_period, "
            "odds_home, odds_draw, odds_away, starts_at DESC) "
            "WHERE odds_draw IS NOT NULL;"
        )
    )
    bind.execute(
        sa.text(
            "CREATE INDEX idx_mv_p5_lookup_2way "
            "ON mv_p5_price_memory "
            "(sport, bookie_id, market_group, market_period, "
            "odds_home, odds_away, starts_at DESC) "
            "WHERE odds_draw IS NULL;"
        )
    )
