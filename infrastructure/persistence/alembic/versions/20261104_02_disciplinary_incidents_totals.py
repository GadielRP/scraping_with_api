"""Keep discipline aggregates separate from individual recipients.

Downgrade drops acquired totals; individual incidents remain intact.
"""
from alembic import op
import sqlalchemy as sa
from infrastructure.persistence.types import UTCDateTime

revision = "20261104_02"
down_revision = "20261104_01"
branch_labels = depends_on = None


def upgrade():
    op.create_table(
        "disciplinary_incidents_totals",
        sa.Column("event_source_mapping_id", sa.Integer(), sa.ForeignKey(
            "event_source_mappings.mapping_id", ondelete="CASCADE"), primary_key=True),
        sa.Column("type", sa.Text(), primary_key=True),
        sa.Column("side", sa.Text(), primary_key=True),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("created_at", UTCDateTime(), nullable=False),
        sa.Column("updated_at", UTCDateTime(), nullable=False),
        sa.CheckConstraint("type IN ('yellow_card', 'red_card', 'ejection', 'foul_out')", name="ck_discipline_totals_type"),
        sa.CheckConstraint("side IN ('home', 'away')", name="ck_discipline_totals_side"),
        sa.CheckConstraint("count >= 0", name="ck_discipline_totals_count"),
        sa.CheckConstraint("endpoint IN ('comments', 'statistics')", name="ck_discipline_totals_endpoint"),
    )


def downgrade():
    op.drop_table("disciplinary_incidents_totals")
