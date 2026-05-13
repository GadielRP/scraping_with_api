"""Manual competition metadata defaults.

Keyed by SofaScore unique tournament id. Keep this intentionally small and
limited to known regular leagues; cups and playoff-only tournaments should not
be mapped here.
"""

from __future__ import annotations

from typing import Dict, Optional


MANUAL_COMPETITION_METADATA_BY_UNIQUE_TOURNAMENT_ID: Dict[int, Dict[str, object]] = {
    132: {
        "source": "sofascore",
        "source_unique_tournament_id": 132,
        "unique_slug": "nba",
        "number_of_teams": 30,
        "total_regular_season_games": 82,
        "standings_grouping": "split_tables",
        "league_config_source": "manual_config",
    },
    17: {
        "source": "sofascore",
        "source_unique_tournament_id": 17,
        "unique_slug": "premier-league",
        "number_of_teams": 20,
        "total_regular_season_games": 38,
        "standings_grouping": "single_table",
        "league_config_source": "manual_config",
    },
    8: {
        "source": "sofascore",
        "source_unique_tournament_id": 8,
        "unique_slug": "laliga",
        "number_of_teams": 20,
        "total_regular_season_games": 38,
        "standings_grouping": "single_table",
        "league_config_source": "manual_config",
    },
    23: {
        "source": "sofascore",
        "source_unique_tournament_id": 23,
        "unique_slug": "serie-a",
        "number_of_teams": 20,
        "total_regular_season_games": 38,
        "standings_grouping": "single_table",
        "league_config_source": "manual_config",
    },
    35: {
        "source": "sofascore",
        "source_unique_tournament_id": 35,
        "unique_slug": "bundesliga",
        "number_of_teams": 18,
        "total_regular_season_games": 34,
        "standings_grouping": "single_table",
        "league_config_source": "manual_config",
    },
}


def get_manual_competition_metadata(source_unique_tournament_id: Optional[int]) -> Optional[Dict[str, object]]:
    if source_unique_tournament_id is None:
        return None
    try:
        key = int(source_unique_tournament_id)
    except (TypeError, ValueError):
        return None
    return MANUAL_COMPETITION_METADATA_BY_UNIQUE_TOURNAMENT_ID.get(key)
