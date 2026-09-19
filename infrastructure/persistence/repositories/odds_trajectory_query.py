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
                e.starts_at
            FROM events e
            WHERE e.id IN :event_ids
        ),
        event_quotes AS (
            SELECT
                requested.event_id,
                requested.starts_at,
                m.market_id,
                m.market_type_id,
                cmt.canonical_market_key,
                cmt.canonical_market_name AS market_name,
                cmt.canonical_market_group AS market_group,
                cmt.canonical_market_period AS market_period,
                cmt.market_family,
                cmt.display_order AS market_display_order,
                cmt.requires_line_value,
                cmt.enabled_for_trajectory,
                m.line_value,
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
            JOIN canonical_market_types cmt
              ON cmt.market_type_id = m.market_type_id
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
        canonical_quotes AS (
            SELECT
                source_mapped.event_id,
                source_mapped.starts_at,
                source_mapped.market_id,
                source_mapped.canonical_market_key,
                source_mapped.market_family,
                source_mapped.market_display_order,
                source_mapped.market_name,
                source_mapped.market_group,
                source_mapped.market_period,
                source_mapped.line_value,
                source_mapped.bookie_id,
                source_mapped.bookie_name,
                source_mapped.choice_id,
                source_mapped.choice_name,
                source_mapped.main_line,
                outcome_mapping.display_order AS choice_display_order,
                source_mapped.initial_odds,
                source_mapped.quote_id,
                source_mapped.source,
                source_mapped.exchange_side,
                source_mapped.exchange_level
            FROM source_mapped
            LEFT JOIN market_outcome_source_mappings outcome_mapping
              ON outcome_mapping.market_source_mapping_id =
                 source_mapped.market_source_mapping_id
             AND outcome_mapping.source_outcome_id = source_mapped.source_outcome_id
            WHERE source_mapped.enabled_for_trajectory = true
              AND (
                    source_mapped.requires_line_value = false
                    OR source_mapped.line_value IS NOT NULL
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
            canonical.line_value,
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
                    EPOCH FROM (canonical.starts_at - snapshots.collected_at)
                ) / 60
            )::int AS observed_minutes_before_start,
            ROUND(
                EXTRACT(
                    EPOCH FROM (
                        canonical.starts_at
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
            canonical.line_value NULLS FIRST,
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
