"""PostgreSQL query for complete event-scoped pre-start odds histories.

The requested event and eligible quote sets are resolved before snapshot
history is joined. Target-minute projection belongs to the pillar formatter,
so this query returns every persisted snapshot for the selected quote lineage.
"""

from __future__ import annotations

from sqlalchemy import Integer, bindparam, text
from sqlalchemy.sql.elements import TextClause


def build_pre_start_trajectory_query() -> TextClause:
    """Build the event-scoped statement for complete eligible quote histories."""
    return text(
        """
        WITH requested_events AS (
            SELECT
                e.id AS event_id,
                e.start_time_utc
            FROM events e
            WHERE e.id IN :event_ids
        ),
        event_quotes AS (
            SELECT
                requested.event_id,
                requested.start_time_utc,
                m.market_id,
                m.market_name,
                m.market_group,
                m.market_period,
                m.choice_group,
                m.bookie_id,
                mc.choice_id,
                mc.choice_name,
                mcq.quote_id,
                mcq.initial_odds,
                mcq.source,
                mcq.source_market_id,
                mcq.source_outcome_id,
                mcq.bookmaker_outcome_id,
                mcq.main_line,
                mcq.exchange_side,
                mcq.exchange_level
            FROM requested_events requested
            JOIN markets m
              ON m.event_id = requested.event_id
             AND m.is_live = false
            JOIN market_choices mc
              ON mc.market_id = m.market_id
            JOIN market_choice_quotes mcq
              ON mcq.choice_id = mc.choice_id
            WHERE mcq.main_line IS TRUE
        ),
        eligible_quotes AS (
            SELECT ranked.*
            FROM (
                SELECT
                    event_quotes.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY choice_id, source, exchange_side
                        ORDER BY exchange_level, quote_id
                    ) AS depth_rank
                FROM event_quotes
                WHERE EXISTS (
                    SELECT 1
                    FROM market_choice_snapshots history
                    WHERE history.quote_id = event_quotes.quote_id
                )
                  AND NOT (
                    exchange_side IS NULL
                    AND EXISTS (
                        SELECT 1
                        FROM market_choice_quotes explicit_quote
                        WHERE explicit_quote.choice_id = event_quotes.choice_id
                          AND explicit_quote.source = event_quotes.source
                          AND explicit_quote.exchange_side IN ('back', 'lay')
                          AND EXISTS (
                              SELECT 1
                              FROM market_choice_snapshots explicit_history
                              WHERE explicit_history.quote_id = explicit_quote.quote_id
                          )
                    )
                  )
            ) ranked
            WHERE ranked.depth_rank = 1
        ),
        quote_context AS (
            SELECT
                eligible.*,
                b.name AS bookie_name,
                event_mapping.source_sport_id AS event_source_sport_id
            FROM eligible_quotes eligible
            JOIN bookies b
              ON b.bookie_id = eligible.bookie_id
            LEFT JOIN event_source_mappings event_mapping
              ON event_mapping.event_id = eligible.event_id
             AND event_mapping.source = eligible.source
        ),
        source_mapped AS (
            SELECT
                quote_context.*,
                COALESCE(
                    exact_mapping.mapping_id,
                    fallback_mapping.mapping_id
                ) AS market_source_mapping_id,
                COALESCE(
                    exact_mapping.canonical_market_key,
                    fallback_mapping.canonical_market_key
                ) AS mapped_canonical_market_key
            FROM quote_context
            LEFT JOIN market_source_mappings exact_mapping
              ON exact_mapping.source = quote_context.source
             AND exact_mapping.source_market_id = quote_context.source_market_id
             AND exact_mapping.source_sport_id = quote_context.event_source_sport_id
            LEFT JOIN market_source_mappings fallback_mapping
              ON fallback_mapping.source = quote_context.source
             AND fallback_mapping.source_market_id = quote_context.source_market_id
             AND fallback_mapping.source_sport_id IS NULL
        ),
        textual_canonical_match AS (
            SELECT
                source_mapped.*,
                textual_type.canonical_market_key AS textual_canonical_market_key,
                textual_type.canonical_market_name AS textual_market_name,
                textual_type.canonical_market_group AS textual_market_group,
                textual_type.canonical_market_period AS textual_market_period,
                textual_type.market_family AS textual_market_family,
                textual_type.requires_choice_group AS textual_requires_choice_group,
                textual_type.enabled_for_trajectory AS textual_enabled_for_trajectory,
                textual_type.display_order AS textual_market_display_order
            FROM source_mapped
            LEFT JOIN canonical_market_types textual_type
              ON LOWER(REPLACE(REPLACE(REPLACE(
                    COALESCE(source_mapped.market_name, ''), '-', ''
                 ), '_', ''), ' ', '')) =
                 LOWER(REPLACE(REPLACE(REPLACE(
                    textual_type.canonical_market_name, '-', ''
                 ), '_', ''), ' ', ''))
             AND LOWER(REPLACE(REPLACE(REPLACE(
                    COALESCE(source_mapped.market_group, ''), '-', ''
                 ), '_', ''), ' ', '')) =
                 LOWER(REPLACE(REPLACE(REPLACE(
                    textual_type.canonical_market_group, '-', ''
                 ), '_', ''), ' ', ''))
             AND LOWER(REPLACE(REPLACE(REPLACE(
                    COALESCE(source_mapped.market_period, ''), '-', ''
                 ), '_', ''), ' ', '')) =
                 LOWER(REPLACE(REPLACE(REPLACE(
                    textual_type.canonical_market_period, '-', ''
                 ), '_', ''), ' ', ''))
        ),
        canonical_quotes AS (
            SELECT
                textual.event_id,
                textual.start_time_utc,
                textual.market_id,
                COALESCE(
                    mapped_type.canonical_market_key,
                    textual.textual_canonical_market_key
                ) AS canonical_market_key,
                COALESCE(
                    mapped_type.market_family,
                    textual.textual_market_family
                ) AS market_family,
                COALESCE(
                    mapped_type.display_order,
                    textual.textual_market_display_order
                ) AS market_display_order,
                COALESCE(
                    mapped_type.canonical_market_name,
                    textual.textual_market_name,
                    textual.market_name
                ) AS market_name,
                COALESCE(
                    mapped_type.canonical_market_group,
                    textual.textual_market_group,
                    textual.market_group
                ) AS market_group,
                COALESCE(
                    mapped_type.canonical_market_period,
                    textual.textual_market_period,
                    textual.market_period
                ) AS market_period,
                textual.choice_group,
                textual.bookie_id,
                textual.bookie_name,
                textual.choice_id,
                textual.choice_name,
                textual.main_line,
                outcome_mapping.display_order AS choice_display_order,
                textual.initial_odds,
                textual.quote_id,
                textual.source,
                textual.exchange_side,
                textual.exchange_level
            FROM textual_canonical_match textual
            LEFT JOIN canonical_market_types mapped_type
              ON mapped_type.canonical_market_key =
                 textual.mapped_canonical_market_key
            LEFT JOIN market_outcome_source_mappings outcome_mapping
              ON outcome_mapping.market_source_mapping_id =
                 textual.market_source_mapping_id
             AND outcome_mapping.source_outcome_id = textual.source_outcome_id
            WHERE COALESCE(
                    mapped_type.enabled_for_trajectory,
                    textual.textual_enabled_for_trajectory,
                    false
                  ) = true
              AND (
                    COALESCE(
                        mapped_type.requires_choice_group,
                        textual.textual_requires_choice_group,
                        false
                    ) = false
                    OR textual.choice_group IS NOT NULL
                  )
        )
        SELECT
            canonical.event_id,
            canonical.market_id,
            canonical.canonical_market_key,
            canonical.market_family,
            canonical.market_display_order,
            canonical.market_name,
            canonical.market_group,
            canonical.market_period,
            canonical.choice_group,
            canonical.bookie_id,
            canonical.bookie_name,
            canonical.choice_id,
            canonical.choice_name,
            canonical.main_line,
            canonical.choice_display_order,
            canonical.quote_id,
            canonical.source,
            canonical.exchange_side,
            canonical.exchange_level,
            canonical.initial_odds,
            snapshots.odds_value,
            snapshots.source_limit,
            snapshots.exchange_size,
            snapshots.snapshot_id,
            snapshots.source_collected_at,
            snapshots.collected_at,
            ROUND(
                EXTRACT(
                    EPOCH FROM (canonical.start_time_utc - snapshots.collected_at)
                ) / 60
            )::int AS observed_minutes_before_start,
            ROUND(
                EXTRACT(
                    EPOCH FROM (
                        canonical.start_time_utc
                        - COALESCE(
                            snapshots.source_collected_at,
                            snapshots.collected_at
                        )
                    )
                ) / 60,
                6
            ) AS trajectory_minutes_before_start
        FROM canonical_quotes canonical
        JOIN market_choice_snapshots snapshots
          ON snapshots.quote_id = canonical.quote_id
        ORDER BY
            canonical.event_id,
            canonical.market_display_order NULLS LAST,
            canonical.market_group,
            canonical.market_period,
            canonical.choice_group NULLS FIRST,
            canonical.bookie_name,
            canonical.source,
            CASE canonical.exchange_side
                WHEN 'back' THEN 1
                WHEN 'lay' THEN 2
                ELSE 0
            END,
            canonical.exchange_level,
            canonical.quote_id,
            COALESCE(snapshots.source_collected_at, snapshots.collected_at),
            snapshots.collected_at,
            snapshots.snapshot_id,
            canonical.choice_display_order NULLS LAST,
            canonical.choice_name
        """
    ).bindparams(bindparam("event_ids", expanding=True, type_=Integer))


__all__ = ["build_pre_start_trajectory_query"]
