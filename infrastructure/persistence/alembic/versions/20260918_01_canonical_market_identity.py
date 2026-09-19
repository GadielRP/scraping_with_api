"""Add stable numeric canonical market identity.

Revision ID: 20260918_01
Revises: 20260918_00
Create Date: 2026-09-18

This is an additive expand migration.  Legacy textual columns remain in place
until the audit command reports complete coverage and a later contract
migration removes them.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260918_01"
down_revision: Union[str, None] = "20260918_00"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


MARKET_TYPE_IDS = {
    "1x2_full_time": 1,
    "1x2_1st_half": 2,
    "1x2_1st_quarter": 3,
    "1x2_first_to_fifth_inning": 4,
    "home_away_full_time": 5,
    "home_away_1st_half": 6,
    "home_away_1st_quarter": 7,
    "home_away_full_time_including_overtime": 8,
    "first_set_winner_1st_set": 9,
    "current_set_winner_current_set": 10,
    "home_away_first_to_fifth_inning": 11,
    "over_under_full_time": 12,
    "sets_over_under_full_time": 13,
    "over_under_full_time_including_overtime": 14,
    "over_under_1st_half": 15,
    "over_under_1st_quarter": 16,
    "over_under_1st_period": 17,
    "total_cards_full_time": 18,
    "total_corners_full_time": 19,
    "total_sets_games_extra_time": 20,
    "team_total_home_full_time": 21,
    "team_total_away_full_time": 22,
    "team_total_home_full_time_including_overtime": 23,
    "team_total_away_full_time_including_overtime": 24,
    "asian_handicap_full_time": 25,
    "asian_handicap_1st_half": 26,
    "asian_handicap_full_time_including_overtime": 27,
    "handicap_full_time_including_overtime": 28,
    "handicap_first_to_fifth_inning": 29,
    "european_handicap_full_time": 30,
    "draw_no_bet_full_time": 31,
    "double_chance_full_time": 32,
    "both_teams_to_score_full_time": 33,
    "both_teams_to_score_full_time_including_overtime": 34,
    "first_goal_full_time": 35,
    "last_goal_full_time": 36,
    "first_team_to_score_full_time": 37,
    "next_goal_full_time": 38,
    "tie_break_in_match_extra_time": 39,
}


def _column_names(table_name: str) -> set[str]:
    return {
        column["name"]
        for column in sa.inspect(op.get_bind()).get_columns(table_name)
    }


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    if "canonical_market_types" not in tables:
        raise RuntimeError(
            "canonical_market_types must exist before applying this baseline revision"
        )

    if "market_type_id" not in _column_names("canonical_market_types"):
        op.add_column(
            "canonical_market_types",
            sa.Column("market_type_id", sa.SmallInteger(), nullable=True),
        )
    update_catalog = sa.text(
        "UPDATE canonical_market_types SET market_type_id = :market_type_id "
        "WHERE canonical_market_key = :canonical_market_key"
    )
    bind.execute(
        update_catalog,
        [
            {
                "canonical_market_key": key,
                "market_type_id": value,
            }
            for key, value in MARKET_TYPE_IDS.items()
        ],
    )
    unresolved_catalog = bind.execute(
        sa.text(
            "SELECT COUNT(*) FROM canonical_market_types WHERE market_type_id IS NULL"
        )
    ).scalar_one()
    if unresolved_catalog:
        raise RuntimeError(
            f"{unresolved_catalog} canonical market types have no stable numeric id"
        )
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "uq_canonical_market_types_market_type_id "
        "ON canonical_market_types (market_type_id)"
    )
    if bind.dialect.name == "postgresql":
        op.alter_column(
            "canonical_market_types",
            "market_type_id",
            existing_type=sa.SmallInteger(),
            nullable=False,
        )

    if "market_source_mappings" in tables:
        if "market_type_id" not in _column_names("market_source_mappings"):
            op.add_column(
                "market_source_mappings",
                sa.Column("market_type_id", sa.SmallInteger(), nullable=True),
            )
        op.execute(
            "UPDATE market_source_mappings AS mapping "
            "SET market_type_id = canonical.market_type_id "
            "FROM canonical_market_types AS canonical "
            "WHERE mapping.canonical_market_key = canonical.canonical_market_key "
            "AND mapping.market_type_id IS NULL"
        )
        unresolved_mappings = bind.execute(
            sa.text(
                "SELECT COUNT(*) FROM market_source_mappings "
                "WHERE market_type_id IS NULL"
            )
        ).scalar_one()
        if unresolved_mappings:
            raise RuntimeError(
                f"{unresolved_mappings} market source mappings have no canonical id"
            )
        if bind.dialect.name == "postgresql":
            op.alter_column(
                "market_source_mappings",
                "market_type_id",
                existing_type=sa.SmallInteger(),
                nullable=False,
            )
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_source_mappings_market_type "
            "ON market_source_mappings (market_type_id)"
        )
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_market_source_mapping_scoped "
            "ON market_source_mappings (source, source_sport_id, source_market_id) "
            "WHERE source_sport_id IS NOT NULL"
        )
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_market_source_mapping_global "
            "ON market_source_mappings (source, source_market_id) "
            "WHERE source_sport_id IS NULL"
        )

    if "markets" in tables:
        if "market_type_id" not in _column_names("markets"):
            op.add_column(
                "markets",
                sa.Column("market_type_id", sa.SmallInteger(), nullable=True),
            )
        op.execute(
            "UPDATE markets AS market SET market_type_id = canonical.market_type_id "
            "FROM canonical_market_types AS canonical "
            "WHERE market.market_type_id IS NULL "
            "AND LOWER(TRIM(market.market_name)) = "
            "LOWER(TRIM(canonical.canonical_market_name)) "
            "AND LOWER(TRIM(COALESCE(market.market_group, ''))) = "
            "LOWER(TRIM(COALESCE(canonical.canonical_market_group, ''))) "
            "AND LOWER(TRIM(market.market_period)) = "
            "LOWER(TRIM(canonical.canonical_market_period))"
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS idx_markets_event_market_type "
            "ON markets (event_id, market_type_id)"
        )
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_canonical_without_line "
            "ON markets (event_id, bookie_id, market_type_id, is_live) "
            "WHERE market_type_id IS NOT NULL AND choice_group IS NULL"
        )
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_canonical_with_line "
            "ON markets "
            "(event_id, bookie_id, market_type_id, choice_group, is_live) "
            "WHERE market_type_id IS NOT NULL AND choice_group IS NOT NULL"
        )

    if bind.dialect.name == "postgresql":
        op.execute(
            "DO $$ BEGIN "
            "IF NOT EXISTS (SELECT 1 FROM pg_constraint "
            "WHERE conname = 'fk_market_source_mappings_market_type') THEN "
            "ALTER TABLE market_source_mappings "
            "ADD CONSTRAINT fk_market_source_mappings_market_type "
            "FOREIGN KEY (market_type_id) "
            "REFERENCES canonical_market_types(market_type_id) "
            "ON DELETE RESTRICT NOT VALID; END IF; END $$"
        )
        op.execute(
            "ALTER TABLE market_source_mappings "
            "VALIDATE CONSTRAINT fk_market_source_mappings_market_type"
        )
        op.execute(
            "DO $$ BEGIN "
            "IF NOT EXISTS (SELECT 1 FROM pg_constraint "
            "WHERE conname = 'fk_markets_market_type') THEN "
            "ALTER TABLE markets ADD CONSTRAINT fk_markets_market_type "
            "FOREIGN KEY (market_type_id) "
            "REFERENCES canonical_market_types(market_type_id) "
            "ON DELETE RESTRICT NOT VALID; END IF; END $$"
        )
        op.execute(
            "ALTER TABLE markets VALIDATE CONSTRAINT fk_markets_market_type"
        )


def downgrade() -> None:
    bind = op.get_bind()
    tables = set(sa.inspect(bind).get_table_names())
    if "markets" in tables and "market_type_id" in _column_names("markets"):
        if bind.dialect.name == "postgresql":
            op.drop_constraint("fk_markets_market_type", "markets", type_="foreignkey")
        op.drop_index("idx_markets_event_market_type", table_name="markets")
        op.drop_index("uq_markets_canonical_with_line", table_name="markets")
        op.drop_index("uq_markets_canonical_without_line", table_name="markets")
        op.drop_column("markets", "market_type_id")
    if (
        "market_source_mappings" in tables
        and "market_type_id" in _column_names("market_source_mappings")
    ):
        if bind.dialect.name == "postgresql":
            op.drop_constraint(
                "fk_market_source_mappings_market_type",
                "market_source_mappings",
                type_="foreignkey",
            )
        op.drop_index(
            "idx_market_source_mappings_market_type",
            table_name="market_source_mappings",
        )
        op.drop_index(
            "uq_market_source_mapping_scoped",
            table_name="market_source_mappings",
        )
        op.drop_index(
            "uq_market_source_mapping_global",
            table_name="market_source_mappings",
        )
        op.drop_column("market_source_mappings", "market_type_id")
    if "market_type_id" in _column_names("canonical_market_types"):
        op.drop_index(
            "uq_canonical_market_types_market_type_id",
            table_name="canonical_market_types",
        )
        op.drop_column("canonical_market_types", "market_type_id")
