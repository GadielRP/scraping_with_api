"""SQL view definition for season events with results (standings)."""

from __future__ import annotations

SEASON_EVENTS_WITH_RESULTS_VIEW_SQL = """
CREATE OR REPLACE VIEW season_events_with_results AS
SELECT
    e.id AS event_id,
    e.season_id,
    s.year AS season_year,
    e.starts_at,
    e.competition_id,
    hp.name AS home_team,
    ap.name AS away_team,
    e.sport,
    c.display_name AS competition,
    c.source_tournament_id,
    c.source_unique_tournament_id,
    r.home_score,
    r.away_score,
    r.winner,
    CASE
        WHEN r.winner = 'X' THEN 'DRAW'
        WHEN e.sport = 'Ice hockey'
             AND (POSITION('+' IN COALESCE(r.home_sets, '')) > 0 OR POSITION('+' IN COALESCE(r.away_sets, '')) > 0)
        THEN 'SO'
        WHEN e.sport = 'Ice hockey'
            AND (POSITION('(' IN COALESCE(r.home_sets, '')) > 0 OR POSITION('(' IN COALESCE(r.away_sets, '')) > 0)
        THEN 'OT'
        ELSE 'REG'
    END AS result_subtype,
    e.round
FROM events e
JOIN results r ON r.event_id = e.id
JOIN participants hp ON hp.participant_id = e.home_participant_id
JOIN participants ap ON ap.participant_id = e.away_participant_id
JOIN competitions c ON c.competition_id = e.competition_id
JOIN seasons s ON s.id = e.season_id
WHERE e.season_id IS NOT NULL
  AND r.home_score IS NOT NULL
  AND r.away_score IS NOT NULL;
"""
