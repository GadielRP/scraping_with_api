"""Rename competition configuration provenance without copying or rewriting data."""
from alembic import op

revision = "20261029_01"
down_revision = "20261012_01"
branch_labels = depends_on = None


def upgrade():
    op.alter_column("competitions", "league_config_source", new_column_name="competition_config_source")


def downgrade():
    op.alter_column("competitions", "competition_config_source", new_column_name="league_config_source")
