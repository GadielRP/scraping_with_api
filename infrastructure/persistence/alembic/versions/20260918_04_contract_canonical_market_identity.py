"""Contract market persistence around canonical ids and numeric lines.

Revision ID: 20260918_04
Revises: 20260918_03

This is the destructive half of the expand/migrate/contract rollout.  The
historical text identity is no longer needed after revision 03 proved that all
market rows have a canonical numeric type and a normalized nullable line.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260918_04"
down_revision: Union[str, None] = "20260918_03"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _tables() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _columns(table_name: str) -> set[str]:
    return {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }


def _drop_constraint_if_present(table_name: str, constraint_name: str) -> None:
    inspector = sa.inspect(op.get_bind())
    for constraint in inspector.get_unique_constraints(table_name):
        if constraint.get("name") == constraint_name:
            op.drop_constraint(constraint_name, table_name, type_="unique")
            return


def _drop_indexes(names: Sequence[str]) -> None:
    bind = op.get_bind()
    for name in names:
        bind.execute(sa.text(f"DROP INDEX IF EXISTS {name}"))


def _drop_dependent_views() -> None:
    # These reporting objects are recreated by create_or_replace_views() after
    # the migration. CASCADE removes only dependent view definitions/indexes;
    # it never deletes base odds rows.
    bind = op.get_bind()
    for object_name in (
        "event_all_odds",
        "basketball_results",
        "season_events_with_results",
        "v_dual_process_event_odds",
    ):
        bind.execute(sa.text(f"DROP VIEW IF EXISTS {object_name} CASCADE"))
    bind.execute(sa.text("DROP MATERIALIZED VIEW IF EXISTS mv_alert_events CASCADE"))


def upgrade() -> None:
    bind = op.get_bind()
    tables = _tables()
    _drop_dependent_views()

    if "canonical_market_types" in tables:
        columns = _columns("canonical_market_types")
        if "requires_choice_group" in columns and "requires_line_value" not in columns:
            op.alter_column(
                "canonical_market_types",
                "requires_choice_group",
                new_column_name="requires_line_value",
                existing_type=sa.Boolean(),
            )

    if "markets" in tables:
        _drop_constraint_if_present("markets", "unique_market_per_event_bookie")
        _drop_indexes(
            (
                "idx_markets_event_bookie_live_name_period",
                "idx_markets_event_bookie_live_group_period",
                "unique_market_per_event_bookie_period_line",
            )
        )
        for column_name in (
            "market_name",
            "market_group",
            "market_period",
            "choice_group",
        ):
            if column_name in _columns("markets"):
                op.drop_column("markets", column_name)
        bind.execute(
            sa.text(
                "CREATE INDEX IF NOT EXISTS "
                "idx_markets_event_bookie_live_type_line "
                "ON markets (event_id, bookie_id, is_live, market_type_id, line_value)"
            )
        )

    if "market_source_mappings" in tables:
        _drop_indexes(("idx_market_source_mappings_group_period",))
        for column_name in (
            "canonical_market_name",
            "canonical_market_group",
            "canonical_market_period",
        ):
            if column_name in _columns("market_source_mappings"):
                op.drop_column("market_source_mappings", column_name)

    if "pillar_mining_units" in tables:
        columns = _columns("pillar_mining_units")
        if "market_type_id" not in columns:
            op.add_column(
                "pillar_mining_units",
                sa.Column("market_type_id", sa.SmallInteger(), nullable=True),
            )
        if "line_value" not in columns:
            op.add_column(
                "pillar_mining_units",
                sa.Column("line_value", sa.Numeric(18, 6), nullable=True),
            )
        columns = _columns("pillar_mining_units")
        if {"market_name", "market_group", "market_period"}.issubset(columns):
            bind.execute(
                sa.text(
                    """
                    UPDATE pillar_mining_units unit
                    SET market_type_id = canonical.market_type_id
                    FROM canonical_market_types canonical
                    WHERE LOWER(TRIM(unit.market_name)) =
                          LOWER(TRIM(canonical.canonical_market_name))
                      AND LOWER(TRIM(COALESCE(unit.market_group, ''))) =
                          LOWER(TRIM(COALESCE(canonical.canonical_market_group, '')))
                      AND LOWER(TRIM(unit.market_period)) =
                          LOWER(TRIM(canonical.canonical_market_period))
                    """
                )
            )
        if "choice_group" in columns:
            bind.execute(
                sa.text(
                    """
                    UPDATE pillar_mining_units
                    SET line_value = CAST(NULLIF(TRIM(choice_group), '') AS NUMERIC)
                    WHERE NULLIF(TRIM(choice_group), '') IS NOT NULL
                      AND TRIM(choice_group) ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                    """
                )
            )
        op.create_foreign_key(
            "fk_pillar_mining_units_market_type_id",
            "pillar_mining_units",
            "canonical_market_types",
            ["market_type_id"],
            ["market_type_id"],
            ondelete="RESTRICT",
        )
        _drop_indexes(("idx_pillar_mining_unit_market",))
        bind.execute(
            sa.text(
                "CREATE INDEX IF NOT EXISTS idx_pillar_mining_unit_market_type "
                "ON pillar_mining_units (market_type_id, line_value)"
            )
        )
        for column_name in (
            "market_name",
            "market_group",
            "market_period",
            "choice_group",
        ):
            if column_name in _columns("pillar_mining_units"):
                op.drop_column("pillar_mining_units", column_name)


def downgrade() -> None:
    raise RuntimeError(
        "20260918_04 is an irreversible contraction: legacy market identity "
        "columns were intentionally removed after the Phase 2 backfill."
    )
