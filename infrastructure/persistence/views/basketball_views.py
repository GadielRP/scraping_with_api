"""SQL view definition for basketball quarter results."""

from __future__ import annotations

BASKETBALL_RESULTS_VIEW_SQL = """
CREATE OR REPLACE VIEW basketball_results AS
SELECT
    e.id AS event_id,
    hp.name AS home_team,
    ap.name AS away_team,
    e.round,
    e.season_id,
    e.starts_at AS start_time,
    r.home_score,
    r.away_score,
    r.winner,
    -- Parse home_sets string (format: '23-23-31-24' or '23-23-31-24-(16)' for overtime)
    -- Remove overtime (parentheses) and penalties (plus signs) before parsing
    CASE 
        WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 1) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 1)::INTEGER
        ELSE NULL
    END AS quarter_1_home,
    CASE 
        WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 2) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 2)::INTEGER
        ELSE NULL
    END AS quarter_2_home,
    CASE 
        WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 3) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 3)::INTEGER
        ELSE NULL
    END AS quarter_3_home,
    CASE 
        WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 4) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 4)::INTEGER
        ELSE NULL
    END AS quarter_4_home,
    -- Extract overtime score from home_sets (format: '23-23-31-24-(16)' where (16) is OT)
    CASE 
        WHEN r.home_sets IS NOT NULL AND r.home_sets ~ '\\([0-9]+'
        THEN (regexp_match(r.home_sets, '\\(([0-9]+)'))[1]::INTEGER
        ELSE NULL
    END AS ot_home,
    -- Parse away_sets string (format: '19-35-24-31' or '19-35-24-31-(16)' for overtime)
    CASE 
        WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 1) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 1)::INTEGER
        ELSE NULL
    END AS quarter_1_away,
    CASE 
        WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 2) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 2)::INTEGER
        ELSE NULL
    END AS quarter_2_away,
    CASE 
        WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 3) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 3)::INTEGER
        ELSE NULL
    END AS quarter_3_away,
    CASE 
        WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 4) ~ '^[0-9]+$'
        THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 4)::INTEGER
        ELSE NULL
    END AS quarter_4_away,
    -- Extract overtime score from away_sets (format: '19-35-24-31-(16)' where (16) is OT)
    CASE 
        WHEN r.away_sets IS NOT NULL AND r.away_sets ~ '\\([0-9]+'
        THEN (regexp_match(r.away_sets, '\\(([0-9]+)'))[1]::INTEGER
        ELSE NULL
    END AS ot_away
FROM events e
JOIN results r ON r.event_id = e.id
JOIN participants hp ON hp.participant_id = e.home_participant_id
JOIN participants ap ON ap.participant_id = e.away_participant_id
WHERE e.sport = 'Basketball'
  AND r.home_sets IS NOT NULL
  AND r.away_sets IS NOT NULL
"""
