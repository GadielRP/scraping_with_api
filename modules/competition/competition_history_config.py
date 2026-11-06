"""Competition provider identities and collection policy.

This catalogue owns provider identities and collection policy.
Scoring, grouping and current sporting facts are configured separately
in competition_profiles.py; tracking remains in tracked_competitions.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


from .competition_identity import CompetitionIdentity
from . import competition_ids


@dataclass(frozen=True, slots=True)
class CompetitionHistoryConfig:
    """Provider identity and collection policy only."""

    code: str
    display_name: str
    primary_identity: CompetitionIdentity
    collected: bool


# ---------------------------------------------------------------------------
# Competition history configurations
# ---------------------------------------------------------------------------

# Collection uses the season_id supplied by each event.

NBA_CONFIG = CompetitionHistoryConfig(
    code="nba",
    display_name="NBA",
    primary_identity=competition_ids.NBA,
    collected=True,
)

LALIGA_CONFIG = CompetitionHistoryConfig(
    code="laliga",
    display_name="LaLiga",
    primary_identity=competition_ids.LALIGA,
    collected=True,
)

PREMIER_LEAGUE_CONFIG = CompetitionHistoryConfig(
    code="premier_league",
    display_name="Premier League",
    primary_identity=competition_ids.PREMIER_LEAGUE,
    collected=True,
)

NFL_CONFIG = CompetitionHistoryConfig(
    code="nfl",
    display_name="NFL",
    primary_identity=competition_ids.NFL,
    collected=True,
)

MLB_CONFIG = CompetitionHistoryConfig(
    code="mlb",
    display_name="MLB",
    primary_identity=competition_ids.MLB,
    collected=True,
)

NHL_CONFIG = CompetitionHistoryConfig(
    code="nhl",
    display_name="NHL",
    primary_identity=competition_ids.NHL,
    collected=True,
)

SERIE_A_CONFIG = CompetitionHistoryConfig(
    code="serie_a",
    display_name="Serie A",
    primary_identity=competition_ids.SERIE_A,
    collected=True,
)

BUNDESLIGA_CONFIG = CompetitionHistoryConfig(
    code="bundesliga",
    display_name="Bundesliga",
    primary_identity=competition_ids.BUNDESLIGA,
    collected=True,
)

LIGUE_1_CONFIG = CompetitionHistoryConfig(
    code="ligue_1",
    display_name="Ligue 1",
    primary_identity=competition_ids.LIGUE_1,
    collected=True,
)

SAUDI_PRO_LEAGUE_CONFIG = CompetitionHistoryConfig(
    code="saudi_pro_league",
    display_name="Saudi Pro League",
    primary_identity=competition_ids.SAUDI_PRO_LEAGUE,
    collected=True,
)

SHL_CONFIG = CompetitionHistoryConfig(
    code="shl",
    display_name="SHL",
    primary_identity=competition_ids.SHL,
    collected=True,
)

WNBA_CONFIG = CompetitionHistoryConfig(
    code="wnba",
    display_name="WNBA",
    primary_identity=competition_ids.WNBA,
    collected=True,
)

PFL_CONFIG = CompetitionHistoryConfig(
    code="pfl",
    display_name="PFL",
    primary_identity=competition_ids.PFL,
    collected=True,
)

CBA_CONFIG = CompetitionHistoryConfig(
    code="cba",
    display_name="CBA",
    primary_identity=competition_ids.CBA,
    collected=True,
)

LIGA_MX_APERTURA_CONFIG = CompetitionHistoryConfig(
    code="liga_mx_apertura",
    display_name="Liga MX Apertura",
    primary_identity=competition_ids.LIGA_MX_APERTURA,
    collected=True,
)

LIGA_MX_CLAUSURA_CONFIG = CompetitionHistoryConfig(
    code="liga_mx_clausura",
    display_name="Liga MX Clausura",
    primary_identity=competition_ids.LIGA_MX_CLAUSURA,
    collected=True,
)

BRASILEIRAO_CONFIG = CompetitionHistoryConfig(
    code="brasileirao",
    display_name="Brasileirão",
    primary_identity=competition_ids.BRASILEIRAO,
    collected=True,
)

LIGA_PROFESIONAL_DE_ARGENTINA_APERTURA_CONFIG = CompetitionHistoryConfig(
    code="liga_profesional_de_argentina_apertura",
    display_name="Liga Profesional de Argentina Apertura",
    primary_identity=competition_ids.ARGENTINA_APERTURA,
    collected=True,
)

LIGA_PROFESIONAL_DE_ARGENTINA_CLAUSURA_CONFIG = CompetitionHistoryConfig(
    code="liga_profesional_de_argentina_clausura",
    display_name="Liga Profesional de Argentina Clausura",
    primary_identity=competition_ids.ARGENTINA_CLAUSURA,
    collected=True,
)

EREDIVISIE_CONFIG = CompetitionHistoryConfig(
    code="eredivisie",
    display_name="Eredivisie",
    primary_identity=competition_ids.EREDIVISIE,
    collected=True,
)

# EuroLeague ranks by victories, then a head-to-head mini-table among teams
# tied on wins. Official overtime-point exclusion is not modeled.
EUROLEAGUE_CONFIG = CompetitionHistoryConfig(
    code="euroleague",
    display_name="Euroleague",
    primary_identity=competition_ids.EUROLEAGUE,
    collected=True,
)

LIIGA_CONFIG = CompetitionHistoryConfig(
    code="liiga",
    display_name="Liiga",
    primary_identity=competition_ids.LIIGA,
    collected=True,
)

# Identity verified against canonical competitions.competition_id=261.
LIGA_PORTUGAL_CONFIG = CompetitionHistoryConfig(
    code="liga_portugal",
    display_name="Liga Portugal",
    primary_identity=competition_ids.LIGA_PORTUGAL,
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


def is_collected_competition_scope(
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    season_id: Optional[int],
) -> bool:
    history_config = get_competition_history_config(source_unique_tournament_id, source_tournament_id)
    return bool(history_config and history_config.collected and season_id is not None)


__all__ = [
    "CompetitionIdentity",
    "CompetitionHistoryConfig",
    "get_competition_history_config",
    "is_collected_competition_scope",
]
