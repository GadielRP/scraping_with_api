"""Enforce canonical event ownership of provider mappings.

Revision ID: 20260919_03
Revises: 20260919_02
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import inspect, text

revision = "20260919_03"
down_revision = "20260919_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    foreign_keys = inspect(connection).get_foreign_keys("event_source_mappings")
    event_fk = next(
        (
            fk for fk in foreign_keys
            if fk["constrained_columns"] == ["event_id"]
        ),
        None,
    )
    if event_fk and (
        event_fk["referred_table"] == "events"
        and event_fk.get("options", {}).get("ondelete", "").upper() == "CASCADE"
    ):
        return

    orphan_count = connection.scalar(
        text(
            "SELECT count(*) FROM event_source_mappings AS mapping "
            "LEFT JOIN events ON events.id = mapping.event_id "
            "WHERE events.id IS NULL"
        )
    )
    if orphan_count:
        raise RuntimeError(
            f"Cannot enforce event_source_mappings.event_id FK: "
            f"{orphan_count} orphan mapping(s) need review first"
        )

    if event_fk:
        op.drop_constraint(event_fk["name"], "event_source_mappings", type_="foreignkey")
    connection.exec_driver_sql(
        "ALTER TABLE event_source_mappings "
        "ADD CONSTRAINT fk_event_source_mappings_event_id "
        "FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE NOT VALID"
    )
    connection.exec_driver_sql(
        "ALTER TABLE event_source_mappings "
        "VALIDATE CONSTRAINT fk_event_source_mappings_event_id"
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_event_source_mappings_event_id",
        "event_source_mappings",
        type_="foreignkey",
    )
