"""Competition provider identities and historical collection scopes.

This catalogue owns provider identities and historical collection scopes.
Scoring, grouping and current sporting facts are configured separately
in competition_profiles.py; tracking remains in tracked_competitions.py.
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
class CompetitionHistoryConfig:
    """Provider identities and historical collection scopes only."""

    code: str
    display_name: str
    primary_identity: CompetitionIdentity
    collected: bool
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
# Competition history configurations
# ---------------------------------------------------------------------------

# NBA special bundles use season ids only. The regular case falls back to the
# current event season_id dynamically.

NBA_CONFIG = CompetitionHistoryConfig(
    code="nba",
    display_name="NBA",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=132,
        source_tournament_id=177,
    ),
    collected=True,
)

LALIGA_CONFIG = CompetitionHistoryConfig(
    code="laliga",
    display_name="LaLiga",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=8,
        source_tournament_id=36,
    ),
    collected=True,
)

PREMIER_LEAGUE_CONFIG = CompetitionHistoryConfig(
    code="premier_league",
    display_name="Premier League",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=17,
        source_tournament_id=1,
    ),
    collected=True,
)

NFL_CONFIG = CompetitionHistoryConfig(
    code="nfl",
    display_name="NFL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=9464,
        source_tournament_id=108947,
    ),
    collected=True,
)

MLB_CONFIG = CompetitionHistoryConfig(
    code="mlb",
    display_name="MLB",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=11205,
        source_tournament_id=108806,
    ),
    collected=True,
)

NHL_CONFIG = CompetitionHistoryConfig(
    code="nhl",
    display_name="NHL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=234,
        source_tournament_id=142,
    ),
    collected=True,
)

# TODO: Final title/relegation playoff exceptions are not modeled yet.
SERIE_A_CONFIG = CompetitionHistoryConfig(
    code="serie_a",
    display_name="Serie A",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=23,
        source_tournament_id=33,
    ),
    collected=True,
)

BUNDESLIGA_CONFIG = CompetitionHistoryConfig(
    code="bundesliga",
    display_name="Bundesliga",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=35,
        source_tournament_id=42,
    ),
    collected=True,
)

LIGUE_1_CONFIG = CompetitionHistoryConfig(
    code="ligue_1",
    display_name="Ligue 1",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=34,
        source_tournament_id=4,
    ),
    collected=True,
)

SAUDI_PRO_LEAGUE_CONFIG = CompetitionHistoryConfig(
    code="saudi_pro_league",
    display_name="Saudi Pro League",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=955,
        source_tournament_id=3708,
    ),
    collected=True,
)

SHL_CONFIG = CompetitionHistoryConfig(
    code="shl",
    display_name="SHL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=261,
        source_tournament_id=115,
    ),
    collected=True,
)

WNBA_CONFIG = CompetitionHistoryConfig(
    code="wnba",
    display_name="WNBA",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=486,
        source_tournament_id=591,
    ),
    collected=True,
)

PFL_CONFIG = CompetitionHistoryConfig(
    code="pfl",
    display_name="PFL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=1654,
        source_tournament_id=19822,
    ),
    collected=True,
)

CBA_CONFIG = CompetitionHistoryConfig(
    code="cba",
    display_name="CBA",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=1566,
        source_tournament_id=10233,
    ),
    collected=True,
)

LIGA_MX_APERTURA_CONFIG = CompetitionHistoryConfig(
    code="liga_mx_apertura",
    display_name="Liga MX Apertura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=11621,
        source_tournament_id=28,
    ),
    collected=True,
)

LIGA_MX_CLAUSURA_CONFIG = CompetitionHistoryConfig(
    code="liga_mx_clausura",
    display_name="Liga MX Clausura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=11620,
        source_tournament_id=16753,
    ),
    collected=True,
)

BRASILEIRAO_CONFIG = CompetitionHistoryConfig(
    code="brasileirao",
    display_name="Brasileirão",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=325,
        source_tournament_id=83,
    ),
    collected=True,
)

LIGA_PROFESIONAL_DE_ARGENTINA_APERTURA_CONFIG = CompetitionHistoryConfig(
    code="liga_profesional_de_argentina_apertura",
    display_name="Liga Profesional de Argentina Apertura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=155,
        source_tournament_id=143625,
    ),
    collected=True,
)

LIGA_PROFESIONAL_DE_ARGENTINA_CLAUSURA_CONFIG = CompetitionHistoryConfig(
    code="liga_profesional_de_argentina_clausura",
    display_name="Liga Profesional de Argentina Clausura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=155,
        source_tournament_id=156467,
    ),
    collected=True,
)

EREDIVISIE_CONFIG = CompetitionHistoryConfig(
    code="eredivisie",
    display_name="Eredivisie",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=37,
        source_tournament_id=39,
    ),
    collected=True,
)

# EuroLeague ranks by victories, then a head-to-head mini-table among teams
# tied on wins. Official overtime-point exclusion is not modeled.
EUROLEAGUE_CONFIG = CompetitionHistoryConfig(
    code="euroleague",
    display_name="Euroleague",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=138,
        source_tournament_id=42527,
    ),
    collected=True,
)

# Liiga uses 3-2-1-0. Official last-resort H2H / even-strength H2H goals
# after regulation wins, goal difference, and goals for are not modeled.
LIIGA_CONFIG = CompetitionHistoryConfig(
    code="liiga",
    display_name="Liiga",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=134,
        source_tournament_id=108,
    ),
    collected=True,
)

# Identity verified against canonical competitions.competition_id=261.
LIGA_PORTUGAL_CONFIG = CompetitionHistoryConfig(
    code="liga_portugal",
    display_name="Liga Portugal",
    primary_identity=CompetitionIdentity(238, 52),
    collected=True,
)


COMPETITION_HISTORY_CONFIGS: Tuple[CompetitionHistoryConfig, ...] = (
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
    WNBA_CONFIG,
    PFL_CONFIG,
    CBA_CONFIG,
    LIGA_MX_APERTURA_CONFIG,
    LIGA_MX_CLAUSURA_CONFIG,
    BRASILEIRAO_CONFIG,
    EREDIVISIE_CONFIG,
    EUROLEAGUE_CONFIG,
    LIIGA_CONFIG,
    LIGA_PORTUGAL_CONFIG,
    LIGA_PROFESIONAL_DE_ARGENTINA_APERTURA_CONFIG,
    LIGA_PROFESIONAL_DE_ARGENTINA_CLAUSURA_CONFIG,
)

_COMPETITION_HISTORY_CONFIGS_BY_UNIQUE_TOURNAMENT_ID: Dict[int, list[CompetitionHistoryConfig]] = {}
for _config in COMPETITION_HISTORY_CONFIGS:
    _COMPETITION_HISTORY_CONFIGS_BY_UNIQUE_TOURNAMENT_ID.setdefault(
        int(_config.primary_identity.source_unique_tournament_id),
        [],
    ).append(_config)


def get_competition_history_config(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int] = None,
) -> Optional[CompetitionHistoryConfig]:
    if source_unique_tournament_id is None:
        return None

    configs = tuple(_COMPETITION_HISTORY_CONFIGS_BY_UNIQUE_TOURNAMENT_ID.get(int(source_unique_tournament_id), ()))
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

    history_config = get_competition_history_config(source_unique_tournament_id, source_tournament_id)
    if history_config is None or not history_config.collected:
        return None

    # Special bundles only cover exceptional multi-season scopes.
    # The normal case falls back to the event's own season_id.
    for bundle in history_config.season_bundles:
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
    history_config = get_competition_history_config(source_unique_tournament_id, source_tournament_id)
    return bool(history_config and history_config.collected and season_id is not None)


__all__ = [
    "CollectedSeasonBundle",
    "CompetitionIdentity",
    "CompetitionHistoryConfig",
    "get_canonical_season_id",
    "get_collected_season_bundle",
    "get_included_season_ids",
    "get_competition_history_config",
    "is_collected_competition_scope",
]
