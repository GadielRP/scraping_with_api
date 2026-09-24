"""Clarify that event source mapping participant references are source-scoped.

Revision ID: 20260924_06
Revises: 20260924_05
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "20260924_06"
down_revision: Union[str, None] = "20260924_05"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return

    op.alter_column(
        "event_source_mappings",
        "participant_home_id",
        new_column_name="source_participant_home_id",
        existing_type=sa.Integer(),
    )
    op.alter_column(
        "event_source_mappings",
        "participant_away_id",
        new_column_name="source_participant_away_id",
        existing_type=sa.Integer(),
    )

    op.execute(
        sa.text(
            "ALTER TABLE event_source_mappings "
            "RENAME CONSTRAINT fk_event_source_mappings_participant_home_id "
            "TO fk_event_source_mappings_source_participant_home_id"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE event_source_mappings "
            "RENAME CONSTRAINT fk_event_source_mappings_participant_away_id "
            "TO fk_event_source_mappings_source_participant_away_id"
        )
    )
    op.execute(
        sa.text(
            "ALTER INDEX public.idx_event_source_mappings_participant_home_id "
            "RENAME TO idx_event_source_mappings_source_participant_home_id"
        )
    )
    op.execute(
        sa.text(
            "ALTER INDEX public.idx_event_source_mappings_participant_away_id "
            "RENAME TO idx_event_source_mappings_source_participant_away_id"
        )
    )


def downgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return

    op.execute(
        sa.text(
            "ALTER INDEX public.idx_event_source_mappings_source_participant_home_id "
            "RENAME TO idx_event_source_mappings_participant_home_id"
        )
    )
    op.execute(
        sa.text(
            "ALTER INDEX public.idx_event_source_mappings_source_participant_away_id "
            "RENAME TO idx_event_source_mappings_participant_away_id"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE event_source_mappings "
            "RENAME CONSTRAINT fk_event_source_mappings_source_participant_home_id "
            "TO fk_event_source_mappings_participant_home_id"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE event_source_mappings "
            "RENAME CONSTRAINT fk_event_source_mappings_source_participant_away_id "
            "TO fk_event_source_mappings_participant_away_id"
        )
    )

    op.alter_column(
        "event_source_mappings",
        "source_participant_home_id",
        new_column_name="participant_home_id",
        existing_type=sa.Integer(),
    )
    op.alter_column(
        "event_source_mappings",
        "source_participant_away_id",
        new_column_name="participant_away_id",
        existing_type=sa.Integer(),
    )
