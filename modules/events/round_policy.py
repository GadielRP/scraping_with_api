"""Round classification shared by single and batch event persistence."""

NBA_SEASONS = [
    {"season_name": "NBA 2020/2021", "season_id": 34951, "year": 2020, "nba_cup_season_id": 0},
    {"season_name": "NBA 2021/2022", "season_id": 38191, "year": 2021, "nba_cup_season_id": 0},
    {"season_name": "NBA 2022/2023", "season_id": 45096, "year": 2022, "nba_cup_season_id": 0},
    {"season_name": "NBA 2023/2024", "season_id": 54105, "year": 2023, "nba_cup_season_id": 56094},
    {"season_name": "NBA 2024/2025", "season_id": 65360, "year": 2024, "nba_cup_season_id": 69143},
    {"season_name": "NBA 2025/2026", "season_id": 80229, "year": 2025, "nba_cup_season_id": 84238},
]

NBA_CUP_SEASON_IDS = frozenset(s['nba_cup_season_id'] for s in NBA_SEASONS if s['nba_cup_season_id'])


def resolve_event_round(season_id, competition, incoming, existing):
    if season_id in NBA_CUP_SEASON_IDS and 'nba cup' in (competition or '').lower():
        incoming = 'knockouts/playoffs'
    if not incoming or ((existing or '').lower() == 'knockouts/playoffs'
                        and str(incoming).lower() == 'regular_season'):
        return existing
    return incoming
