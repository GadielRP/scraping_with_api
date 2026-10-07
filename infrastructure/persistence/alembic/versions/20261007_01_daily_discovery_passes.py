"""Name discovery passes independently of the retry's current UTC date."""

from alembic import op
import sqlalchemy as sa

revision = "20261007_01"
down_revision = "20261005_01"
branch_labels = depends_on = None


def upgrade():
    op.execute(sa.text(
        "UPDATE daily_discovery_log SET run_slot = CASE run_slot "
        "WHEN 'next_utc_day' THEN 'anticipada' "
        "WHEN 'current_utc_day' THEN 'actualizacion' END "
        "WHERE run_slot IN ('next_utc_day', 'current_utc_day')"
    ))


def downgrade():
    op.execute(sa.text(
        "UPDATE daily_discovery_log SET run_slot = CASE run_slot "
        "WHEN 'anticipada' THEN 'next_utc_day' "
        "WHEN 'actualizacion' THEN 'current_utc_day' END "
        "WHERE run_slot IN ('anticipada', 'actualizacion')"
    ))
