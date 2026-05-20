"""League configuration for collected competitions and standings scope.

This module is the single source of truth for league metadata that depends on
competition identity rather than season_id.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple


@dataclass(frozen=True, slots=True)
class CompetitionIdentity:
    """Stable SofaScore competition identity."""

    source_unique_tournament_id: int
    source_tournament_id: Optional[int] = None

    def matches(
        self,
        source_unique_tournament_id: Optional[int],
        source_tournament_id: Optional[int] = None,
    ) -> bool:
        if source_unique_tournament_id is None:
            return False
        if int(source_unique_tournament_id) != int(self.source_unique_tournament_id):
            return False
        if self.source_tournament_id is None:
            return True
        if source_tournament_id is None:
            return False
        return int(source_tournament_id) == int(self.source_tournament_id)


@dataclass(frozen=True, slots=True)
class CollectedSeasonBundle:
    """A collected season scope that may span multiple related season ids."""

    canonical_season_id: int
    included_season_ids: Tuple[int, ...]
    included_competition_identities: Tuple[CompetitionIdentity, ...]

    def includes_season(self, season_id: Optional[int]) -> bool:
        return season_id is not None and int(season_id) in self.included_season_ids

    def matches_competition(
        self,
        source_unique_tournament_id: Optional[int],
        source_tournament_id: Optional[int] = None,
    ) -> bool:
        if not self.included_competition_identities:
            return True
        return any(
            identity.matches(source_unique_tournament_id, source_tournament_id)
            for identity in self.included_competition_identities
        )


@dataclass(frozen=True, slots=True)
class LeagueConfig:
    """Stable league configuration keyed by competition identity."""

    code: str
    display_name: str
    primary_identity: CompetitionIdentity
    collected: bool
    standings_method: str
    grouping_method: str
    number_of_teams: Optional[int]
    total_regular_season_games: Optional[int]
    standings_grouping: str
    season_bundles: Tuple[CollectedSeasonBundle, ...] = ()


def _bundle(
    canonical_season_id: int,
    included_season_ids: Iterable[int],
    competition_identities: Tuple[CompetitionIdentity, ...] = (),
) -> CollectedSeasonBundle:
    return CollectedSeasonBundle(
        canonical_season_id=canonical_season_id,
        included_season_ids=tuple(int(season_id) for season_id in included_season_ids),
        included_competition_identities=competition_identities,
    )


# ---------------------------------------------------------------------------
# League configurations
# ---------------------------------------------------------------------------

# NBA special bundles use season ids only. The regular case falls back to the
# current event season_id dynamically.

NBA_CONFIG = LeagueConfig(
    code="nba",
    display_name="NBA",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=132),
    collected=True,
    standings_method="win_pct",
    grouping_method="nba_conference",
    number_of_teams=30,
    total_regular_season_games=82,
    standings_grouping="split_tables",
    season_bundles=(
        _bundle(
            80229,
            (80229, 84238),
        ),
        _bundle(
            65360,
            (65360, 69143),
        ),
        _bundle(
            54105,
            (54105, 56094),
        ),
    ),
)

LALIGA_CONFIG = LeagueConfig(
    code="laliga",
    display_name="LaLiga",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=8),
    collected=True,
    standings_method="football_3_1_0_h2h",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

PREMIER_LEAGUE_CONFIG = LeagueConfig(
    code="premier_league",
    display_name="Premier League",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=17),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

NFL_CONFIG = LeagueConfig(
    code="nfl",
    display_name="NFL",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=9464),
    collected=True,
    standings_method="win_pct_half_tie",
    grouping_method="nfl_conference",
    number_of_teams=32,
    total_regular_season_games=17,
    standings_grouping="split_tables",
)

MLB_CONFIG = LeagueConfig(
    code="mlb",
    display_name="MLB",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=11205),
    collected=True,
    standings_method="win_pct",
    grouping_method="mlb_league",
    number_of_teams=30,
    total_regular_season_games=162,
    standings_grouping="split_tables",
)

NHL_CONFIG = LeagueConfig(
    code="nhl",
    display_name="NHL",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=234),
    collected=True,
    standings_method="nhl_2_1_0_otl",
    grouping_method="nhl_conference",
    number_of_teams=32,
    total_regular_season_games=82,
    standings_grouping="split_tables",
)

# TODO: Final title/relegation playoff exceptions are not modeled yet.
SERIE_A_CONFIG = LeagueConfig(
    code="serie_a",
    display_name="Serie A",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=23),
    collected=True,
    standings_method="football_3_1_0_h2h",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

BUNDESLIGA_CONFIG = LeagueConfig(
    code="bundesliga",
    display_name="Bundesliga",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=35),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
)

LIGUE_1_CONFIG = LeagueConfig(
    code="ligue_1",
    display_name="Ligue 1",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=34),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
)

SAUDI_PRO_LEAGUE_CONFIG = LeagueConfig(
    code="saudi_pro_league",
    display_name="Saudi Pro League",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=955),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
)

SHL_CONFIG = LeagueConfig(
    code="shl",
    display_name="SHL",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=261),
    collected=True,
    standings_method="hockey_3_2_1_0",
    grouping_method="league_wide",
    number_of_teams=14,
    total_regular_season_games=52,
    standings_grouping="single_table",
)

PFL_CONFIG = LeagueConfig(
    code="pfl",
    display_name="PFL",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=1654),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=None,
    total_regular_season_games=None,
    standings_grouping="single_table",
)

CBA_CONFIG = LeagueConfig(
    code="cba",
    display_name="CBA",
    primary_identity=CompetitionIdentity(source_unique_tournament_id=1566),
    collected=True,
    standings_method="win_pct",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=None,
    standings_grouping="single_table",
)

LEAGUE_CONFIGS: Tuple[LeagueConfig, ...] = (
    NBA_CONFIG,
    LALIGA_CONFIG,
    PREMIER_LEAGUE_CONFIG,
    NFL_CONFIG,
    MLB_CONFIG,
    NHL_CONFIG,
    SERIE_A_CONFIG,
    BUNDESLIGA_CONFIG,
    LIGUE_1_CONFIG,
    SAUDI_PRO_LEAGUE_CONFIG,
    SHL_CONFIG,
    PFL_CONFIG,
    CBA_CONFIG,
)

_LEAGUE_CONFIGS_BY_UNIQUE_TOURNAMENT_ID: Dict[int, list[LeagueConfig]] = {}
for _config in LEAGUE_CONFIGS:
    _LEAGUE_CONFIGS_BY_UNIQUE_TOURNAMENT_ID.setdefault(
        int(_config.primary_identity.source_unique_tournament_id),
        [],
    ).append(_config)


def get_league_config(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int] = None,
) -> Optional[LeagueConfig]:
    if source_unique_tournament_id is None:
        return None

    configs = tuple(_LEAGUE_CONFIGS_BY_UNIQUE_TOURNAMENT_ID.get(int(source_unique_tournament_id), ()))
    if not configs:
        return None

    if source_tournament_id is not None:
        for config in configs:
            if config.primary_identity.source_tournament_id is not None and int(config.primary_identity.source_tournament_id) == int(source_tournament_id):
                return config

    for config in configs:
        if config.primary_identity.source_tournament_id is None:
            return config

    return configs[0]


def get_collected_season_bundle(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    season_id: Optional[int],
) -> Optional[CollectedSeasonBundle]:
    if season_id is None:
        return None

    league_config = get_league_config(source_unique_tournament_id, source_tournament_id)
    if league_config is None or not league_config.collected:
        return None

    # Special bundles only cover exceptional multi-season scopes.
    # The normal case falls back to the event's own season_id.
    for bundle in league_config.season_bundles:
        if bundle.includes_season(season_id) and bundle.matches_competition(
            source_unique_tournament_id,
            source_tournament_id,
        ):
            return bundle

    return CollectedSeasonBundle(
        canonical_season_id=int(season_id),
        included_season_ids=(int(season_id),),
        included_competition_identities=(),
    )


def get_included_season_ids(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    season_id: Optional[int],
) -> Tuple[int, ...]:
    bundle = get_collected_season_bundle(source_unique_tournament_id, source_tournament_id, season_id)
    if bundle is None:
        return (int(season_id),) if season_id is not None else ()
    return bundle.included_season_ids


def get_canonical_season_id(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    season_id: Optional[int],
) -> int:
    bundle = get_collected_season_bundle(source_unique_tournament_id, source_tournament_id, season_id)
    if bundle is None:
        return int(season_id) if season_id is not None else 0
    return bundle.canonical_season_id


def is_collected_competition_scope(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    season_id: Optional[int],
) -> bool:
    league_config = get_league_config(source_unique_tournament_id, source_tournament_id)
    return bool(league_config and league_config.collected and season_id is not None)


def get_standings_method(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int] = None,
    sport: Optional[str] = None,
) -> str:
    league_config = get_league_config(source_unique_tournament_id, source_tournament_id)
    if league_config is not None:
        return league_config.standings_method

    if sport:
        sport_lower = sport.lower()
        if "american football" in sport_lower:
            return "win_pct_half_tie"
        if "football" in sport_lower or "soccer" in sport_lower:
            return "football_3_1_0"
        if "ice hockey" in sport_lower or "hockey" in sport_lower:
            return "nhl_2_1_0_otl"
        if "basketball" in sport_lower or "baseball" in sport_lower:
            return "win_pct"

    return "win_pct"


def get_grouping_method(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int] = None,
) -> str:
    league_config = get_league_config(source_unique_tournament_id, source_tournament_id)
    if league_config is not None:
        return league_config.grouping_method
    return "league_wide"


__all__ = [
    "CollectedSeasonBundle",
    "CompetitionIdentity",
    "LeagueConfig",
    "get_canonical_season_id",
    "get_collected_season_bundle",
    "get_grouping_method",
    "get_included_season_ids",
    "get_league_config",
    "get_standings_method",
    "is_collected_competition_scope",
]
