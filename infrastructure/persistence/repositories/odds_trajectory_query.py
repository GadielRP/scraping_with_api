"""PostgreSQL query for bounded pre-start odds trajectory reads.

The request scope is introduced before quote eligibility and snapshot history.
This is essential: quote depth is meaningful per choice, while the scheduler
only needs choices owned by the requested events.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import bindparam, text
from sqlalchemy.sql.elements import TextClause


def build_pre_start_trajectory_query(target_minutes: Sequence[int]) -> TextClause:
    """Build the event-scoped trajectory statement for the requested moments."""
    if not target_minutes:
        raise ValueError("target_minutes must not be empty")

    target_value_rows = ", ".join(
        f"(:target_minute_{index})" for index, _ in enumerate(target_minutes)
    )

    return text(
        f"""
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
        snapshot_context AS (
            SELECT
                eligible.event_id,
                eligible.start_time_utc,
                eligible.market_id,
                eligible.market_name,
                eligible.market_group,
                eligible.market_period,
                eligible.choice_group,
                eligible.bookie_id,
                b.name AS bookie_name,
                eligible.choice_id,
                eligible.choice_name,
                eligible.initial_odds,
                snapshots.snapshot_id,
                eligible.source,
                snapshots.source_collected_at,
                eligible.source_market_id,
                eligible.source_outcome_id,
                eligible.bookmaker_outcome_id,
                eligible.main_line,
                snapshots.source_limit,
                snapshots.exchange_size,
                snapshots.odds_value,
                snapshots.collected_at,
                event_mapping.source_sport_id AS event_source_sport_id,
                ROUND(
                    EXTRACT(
                        EPOCH FROM (eligible.start_time_utc - snapshots.collected_at)
                    ) / 60
                )::int AS minutes_before_start,
                eligible.quote_id,
                eligible.exchange_side,
                eligible.exchange_level
            FROM eligible_quotes eligible
            JOIN market_choice_snapshots snapshots
              ON snapshots.quote_id = eligible.quote_id
            JOIN bookies b
              ON b.bookie_id = eligible.bookie_id
            LEFT JOIN event_source_mappings event_mapping
              ON event_mapping.event_id = eligible.event_id
             AND event_mapping.source = eligible.source
        ),
        source_mapped AS (
            SELECT
                snapshot_context.*,
                COALESCE(
                    exact_mapping.mapping_id,
                    fallback_mapping.mapping_id
                ) AS market_source_mapping_id,
                COALESCE(
                    exact_mapping.canonical_market_key,
                    fallback_mapping.canonical_market_key
                ) AS mapped_canonical_market_key
            FROM snapshot_context
            LEFT JOIN market_source_mappings exact_mapping
              ON exact_mapping.source = snapshot_context.source
             AND exact_mapping.source_market_id = snapshot_context.source_market_id
             AND exact_mapping.source_sport_id = snapshot_context.event_source_sport_id
            LEFT JOIN market_source_mappings fallback_mapping
              ON fallback_mapping.source = snapshot_context.source
             AND fallback_mapping.source_market_id = snapshot_context.source_market_id
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
        trajectory AS (
            SELECT
                textual.event_id,
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
                outcome_mapping.display_order AS choice_display_order,
                textual.initial_odds,
                textual.odds_value,
                textual.exchange_size,
                textual.snapshot_id,
                textual.source_collected_at,
                textual.collected_at,
                textual.minutes_before_start,
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
        ),
        target_moments AS (
            SELECT target_minute
            FROM (VALUES {target_value_rows}) AS moments(target_minute)
        ),
        candidate_rows AS (
            SELECT
                trajectory.*,
                target_moments.target_minute,
                ABS(
                    trajectory.minutes_before_start - target_moments.target_minute
                ) AS distance_from_target
            FROM trajectory
            CROSS JOIN target_moments
            WHERE ABS(
                    trajectory.minutes_before_start - target_moments.target_minute
                  ) <= :tolerance_minutes
              AND trajectory.quote_id IS NOT NULL
        ),
        ranked_trajectory AS (
            SELECT
                candidate_rows.*,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id, quote_id, target_minute
                    ORDER BY
                        distance_from_target,
                        collected_at DESC,
                        snapshot_id DESC
                ) AS trajectory_rank
            FROM candidate_rows
        )
        SELECT
            event_id,
            market_id,
            canonical_market_key,
            market_family,
            market_display_order,
            market_name,
            market_group,
            market_period,
            choice_group,
            bookie_id,
            bookie_name,
            choice_id,
            choice_name,
            choice_display_order,
            quote_id,
            source,
            exchange_side,
            exchange_level,
            initial_odds,
            odds_value,
            exchange_size,
            snapshot_id,
            source_collected_at,
            collected_at,
            minutes_before_start,
            target_minute,
            distance_from_target
        FROM ranked_trajectory
        WHERE trajectory_rank = 1
        ORDER BY
            event_id,
            market_display_order NULLS LAST,
            market_group,
            market_period,
            choice_group NULLS FIRST,
            bookie_name,
            source,
            CASE exchange_side
                WHEN 'back' THEN 1
                WHEN 'lay' THEN 2
                ELSE 0
            END,
            exchange_level,
            quote_id,
            target_minute DESC,
            choice_display_order NULLS LAST,
            choice_name
        """
    ).bindparams(bindparam("event_ids", expanding=True))


__all__ = ["build_pre_start_trajectory_query"]
