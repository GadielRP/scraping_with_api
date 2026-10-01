"""Persist source-scoped event discard evidence independently of events."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = '20261001_01'
down_revision = '20260925_01'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('event_discard_memory',
        sa.Column('source', sa.Text(), primary_key=True),
        sa.Column('source_event_id', sa.Text(), primary_key=True),
        sa.Column('original_event_id', sa.Integer(), nullable=False),
        sa.Column('parser_kind', sa.Text(), nullable=False),
        sa.Column('deletion_reason', sa.Text(), nullable=False),
        sa.Column('snapshot', sa.JSON().with_variant(JSONB(), 'postgresql'), nullable=False),
        sa.Column('observed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('discarded_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('origin', sa.Text(), nullable=False),
        sa.Column('policy_version', sa.Integer(), nullable=False),
    )
    op.create_index('ix_event_discard_memory_discarded_at', 'event_discard_memory', ['discarded_at'])


def downgrade():
    op.drop_table('event_discard_memory')
