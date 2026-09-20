"""SQL definitions and builders for Dual Process (alerts) views and indexes."""

from __future__ import annotations


def _sql_string_list(values) -> str:
    cleaned = []
    for value in values:
        if value is None:
            continue
        escaped = str(value).replace("'", "''")
        cleaned.append(f"'{escaped}'")
    return ", ".join(cleaned) if cleaned else "''"


def build_dual_process_event_odds_view_sql(
    markets,
    periods,
) -> str:
    market_values = _sql_string_list(markets)
    period_values = _sql_string_list(periods)
    quote_join = """
    JOIN LATERAL (
        SELECT quote_candidate.*
        FROM market_choice_quotes quote_candidate
        WHERE quote_candidate.choice_id = mc.choice_id
          AND quote_candidate.source = 'sofascore'
          AND quote_candidate.exchange_side IS NULL
          AND quote_candidate.exchange_level = 0
        ORDER BY quote_candidate.quote_id
        LIMIT 1
    ) mcq ON TRUE
    """
    return f"""
    CREATE OR REPLACE VIEW v_dual_process_event_odds AS
    WITH choice_values AS (
        SELECT
            m.event_id,
            m.market_id,
            cmt.canonical_market_name AS market_name,
            cmt.canonical_market_group AS market_group,
            cmt.canonical_market_period AS market_period,
            m.bookie_id,
            m.collected_at,
            mc.choice_name,
            mcq.initial_odds AS initial_odds,
            COALESCE(latest.odds_value, mcq.current_odds) AS current_odds,
            COALESCE(
                latest.collected_at,
                mcq.current_updated_at,
                mcq.initial_captured_at,
                m.collected_at
            ) AS latest_snapshot_at
        FROM markets m
        JOIN canonical_market_types cmt ON cmt.market_type_id = m.market_type_id
        JOIN market_choices mc ON mc.market_id = m.market_id
        {quote_join}
        LEFT JOIN LATERAL (
            SELECT mcs.odds_value, mcs.collected_at
            FROM market_choice_snapshots mcs
            WHERE mcs.quote_id = mcq.quote_id
            ORDER BY mcs.collected_at DESC, mcs.snapshot_id DESC
            LIMIT 1
        ) latest ON TRUE
        WHERE m.bookie_id = 1
          AND m.is_live = false
          AND (
              cmt.canonical_market_name IN ({market_values})
              OR cmt.canonical_market_group IN ({market_values})
          )
          AND cmt.canonical_market_period IN ({period_values})
          AND mc.choice_name IN ('1', 'x', '2')
    ),
    pivoted AS (
        SELECT
            event_id,
            market_id,
            market_name,
            market_group,
            market_period,
            bookie_id,
            collected_at,
            MAX(CASE WHEN choice_name = '1' THEN initial_odds END) AS one_open,
            MAX(CASE WHEN choice_name = '1' THEN current_odds END) AS one_final,
            MAX(CASE WHEN choice_name = 'x' THEN initial_odds END) AS x_open,
            MAX(CASE WHEN choice_name = 'x' THEN current_odds END) AS x_final,
            MAX(CASE WHEN choice_name = '2' THEN initial_odds END) AS two_open,
            MAX(CASE WHEN choice_name = '2' THEN current_odds END) AS two_final,
            MAX(latest_snapshot_at) AS last_sync_at
        FROM choice_values
        GROUP BY event_id, market_id, market_name, market_group, market_period, bookie_id, collected_at
    ),
    valid_markets AS (
        SELECT
            *,
            (one_final - one_open)::numeric(8,3) AS var_one,
            CASE
                WHEN x_open IS NOT NULL AND x_final IS NOT NULL
                THEN (x_final - x_open)::numeric(8,3)
                ELSE NULL
            END AS var_x,
            (two_final - two_open)::numeric(8,3) AS var_two,
            (x_open IS NOT NULL AND x_final IS NOT NULL) AS var_shape,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY collected_at DESC, market_id DESC
            ) AS rn
        FROM pivoted
        WHERE one_open IS NOT NULL
          AND one_final IS NOT NULL
          AND two_open IS NOT NULL
          AND two_final IS NOT NULL
    )
    SELECT
        event_id,
        market_id,
        market_name,
        market_group,
        market_period,
        bookie_id,
        collected_at,
        one_open,
        one_final,
        x_open,
        x_final,
        two_open,
        two_final,
        var_one,
        var_x,
        var_two,
        var_shape,
        COALESCE(last_sync_at, collected_at) AS last_sync_at
    FROM valid_markets
    WHERE rn = 1;
    """


EVENT_ALL_ODDS_VIEW_SQL = """
CREATE OR REPLACE VIEW event_all_odds AS
SELECT
    e.starts_at AS starts_at,
    (hp.name || ' / ' || ap.name) AS participants,
    eo.one_open::numeric(6,2) AS odds1a,
    eo.one_final::numeric(6,2) AS odds1b,
    eo.x_open::numeric(6,2) AS momEa,
    eo.x_final::numeric(6,2) AS momeEb,
    eo.two_open::numeric(6,2) AS odds2a,
    eo.two_final::numeric(6,2) AS odds2b,
    eo.var_one::numeric(6,2) AS var_1,
    eo.var_x::numeric(6,2) AS var_x,
    eo.var_two::numeric(6,2) AS var_2,
    CASE
        WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL
        THEN (r.home_score::text || ' - ' || r.away_score::text)
        ELSE NULL
    END AS result,
    c.display_name AS competition,
    e.sport AS sport
FROM v_dual_process_event_odds eo
JOIN events e ON e.id = eo.event_id
JOIN participants hp ON hp.participant_id = e.home_participant_id
JOIN participants ap ON ap.participant_id = e.away_participant_id
JOIN competitions c ON c.competition_id = e.competition_id
LEFT JOIN results r ON r.event_id = eo.event_id
"""


MV_ALERT_EVENTS_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_alert_events AS
SELECT
    e.id AS event_id,
    e.sport,
    e.gender,
    e.discovery_source,
    e.starts_at,
    (hp.name || ' vs ' || ap.name) AS participants,
    hp.name AS home_team,
    ap.name AS away_team,
    c.display_name AS competition,
    eo.one_open,
    eo.one_final,
    eo.x_open,
    eo.x_final,
    eo.two_open,
    eo.two_final,
    eo.var_one,
    eo.var_x,
    eo.var_two,
    -- Computed fields for matching
    eo.var_shape,
    (COALESCE(eo.var_one, 0) + COALESCE(eo.var_x, 0) + COALESCE(eo.var_two, 0)) AS var_total,
    ROUND((COALESCE(eo.var_one, 0) + COALESCE(eo.var_x, 0) + COALESCE(eo.var_two, 0))::numeric, 2) AS var_total_rounded,
    -- Result fields
    r.home_score,
    r.away_score,
    r.winner AS winner_side,  -- '1', 'X', '2' or NULL
    CASE
        WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL
        THEN ABS(r.home_score - r.away_score)
        ELSE NULL
    END AS point_diff,
    CASE
        WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL
        THEN (r.home_score::text || '-' || r.away_score::text)
        ELSE NULL
    END AS result_text
FROM v_dual_process_event_odds eo
JOIN events e ON e.id = eo.event_id
JOIN participants hp ON hp.participant_id = e.home_participant_id
JOIN participants ap ON ap.participant_id = e.away_participant_id
JOIN competitions c ON c.competition_id = e.competition_id
LEFT JOIN results r ON r.event_id = eo.event_id
WHERE r.home_score IS NOT NULL AND r.away_score IS NOT NULL  -- Only finished events
"""

MV_ALERT_EVENTS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_sport_shape_total ON mv_alert_events (sport, var_shape, var_total);",
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_sport_winner_diff ON mv_alert_events (sport, winner_side, point_diff);",
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_start_time ON mv_alert_events (starts_at);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_alert_event_id ON mv_alert_events (event_id);",
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_sport_gender ON mv_alert_events (sport, gender);",
]

DUAL_PROCESS_MARKET_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_markets_event_bookie_live_type_line ON markets (event_id, bookie_id, is_live, market_type_id, line_value);",
    "CREATE INDEX IF NOT EXISTS idx_market_choices_market_choice_name ON market_choices (market_id, choice_name);",
    """CREATE INDEX IF NOT EXISTS idx_market_choice_quotes_dual_sofascore
       ON market_choice_quotes (choice_id, quote_id)
       INCLUDE (initial_odds, current_odds, current_updated_at, initial_captured_at)
       WHERE source = 'sofascore' AND exchange_side IS NULL AND exchange_level = 0;""",
]

EVENT_ODDS_HISTORY_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_events_starts_at ON events (starts_at);",
    "CREATE INDEX IF NOT EXISTS idx_events_sport_starts_at ON events (sport, starts_at);",
    "CREATE INDEX IF NOT EXISTS idx_events_season_starts_at ON events (season_id, starts_at);",
    "CREATE INDEX IF NOT EXISTS idx_market_choice_snapshots_quote_collected ON market_choice_snapshots (quote_id, collected_at DESC, snapshot_id DESC);",
]
