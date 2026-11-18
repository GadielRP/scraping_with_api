"""Record when a result became final and expose it through the season results view.

Existing rows keep NULL: the instant is unknown for them, and readers fall back to a per-sport estimate.
Downgrade drops the recorded instants.
"""
from alembic import op
import sqlalchemy as sa

from infrastructure.persistence.types import UTCDateTime
from infrastructure.persistence.views.season_views import build_season_events_with_results_view_sql

revision = "20261114_01"
down_revision = "20261110_01"
branch_labels = depends_on = None


def upgrade():
    op.add_column("results", sa.Column("finished_at", UTCDateTime(), nullable=True))
    if op.get_bind().dialect.name == "postgresql":
        # The new column is appended last, so the existing view is replaced in place and keeps its grants.
        op.execute(build_season_events_with_results_view_sql(finished_at=True))


def downgrade():
    if op.get_bind().dialect.name == "postgresql":
        # CREATE OR REPLACE VIEW cannot remove a column, and the view depends on the one being dropped.
        op.execute("DROP VIEW IF EXISTS season_events_with_results")
        op.execute(build_season_events_with_results_view_sql(finished_at=False))
    op.drop_column("results", "finished_at")
