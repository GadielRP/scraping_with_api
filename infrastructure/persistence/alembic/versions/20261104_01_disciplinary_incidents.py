"""Store individual discipline incidents without retaining comment payloads.

Downgrade permanently removes acquired discipline data; no automatic backfill.
"""
from alembic import op
import sqlalchemy as sa

from infrastructure.persistence.types import UTCDateTime

revision = "20261104_01"
down_revision = "20261029_01"
branch_labels = depends_on = None


def upgrade():
    op.create_table(
        "disciplinary_incidents",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("event_source_mapping_id", sa.Integer(), sa.ForeignKey(
            "event_source_mappings.mapping_id", ondelete="CASCADE"), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("origin", sa.Text(), nullable=False),
        sa.Column("source_player_id", sa.Text()),
        sa.Column("player_name", sa.Text()),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("updated_at", UTCDateTime(), nullable=False),
        sa.CheckConstraint("type IN ('yellow_card', 'red_card', 'ejection', 'foul_out')", name="ck_discipline_type"),
        sa.CheckConstraint("origin IN ('reported', 'derived')", name="ck_discipline_origin"),
        sa.CheckConstraint("side IN ('home', 'away')", name="ck_discipline_side"),
        sa.CheckConstraint("source_player_id IS NULL OR length(trim(source_player_id)) > 0", name="ck_discipline_player_id"),
        sa.CheckConstraint("player_name IS NULL OR length(trim(player_name)) > 0", name="ck_discipline_player_name"),
    )
    op.create_index("ix_disciplinary_incidents_mapping_type", "disciplinary_incidents",
                    ["event_source_mapping_id", "type"])


def downgrade():
    op.drop_index("ix_disciplinary_incidents_mapping_type", table_name="disciplinary_incidents")
    op.drop_table("disciplinary_incidents")
