"""Rename persisted daily-discovery slots to describe their UTC target date."""

from alembic import op
import sqlalchemy as sa

revision = "20261005_01"
down_revision = "20261004_01"
branch_labels = depends_on = None


def upgrade():
    op.execute(
        sa.text(
            "UPDATE daily_discovery_log "
            "SET run_slot = 'next_utc_day' WHERE run_slot = 'AM'"
        )
    )
    op.execute(
        sa.text(
            "UPDATE daily_discovery_log "
            "SET run_slot = 'current_utc_day' WHERE run_slot = 'PM'"
        )
    )


def downgrade():
    op.execute(
        sa.text(
            "UPDATE daily_discovery_log "
            "SET run_slot = 'AM' WHERE run_slot = 'next_utc_day'"
        )
    )
    op.execute(
        sa.text(
            "UPDATE daily_discovery_log "
            "SET run_slot = 'PM' WHERE run_slot = 'current_utc_day'"
        )
    )
