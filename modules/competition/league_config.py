"""League configuration for collected competitions and standings scope.

This catalogue associates competition identities with stable metadata, collected
season scopes and explicit season-specific sporting rules. Tracking remains a
separate operational policy in tracked_competitions.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

from modules.competition.competitive_rules import CompetitionRules, ObjectiveRule


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
    """Competition metadata and season profiles keyed by provider identity."""

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
    competitive_rules: Tuple[CompetitionRules, ...] = ()


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


def _qualification(start, end, target, path="DIRECT", scope="GLOBAL", group_size=None):
    return ObjectiveRule(
        "STAGE_QUALIFICATION", target, path, ranking_scope=scope,
        rank_start=start, rank_end=end, reference_rank=end, group_size=group_size,
    )


def _retention(start, end, safe_rank, path="DIRECT_RELEGATION"):
    return ObjectiveRule(
        "DIVISION_RETENTION", "CURRENT_DIVISION", path,
        rank_start=start, rank_end=end, reference_rank=safe_rank,
    )


def _external(*targets, target_season, aggregation_scope="TOURNAMENT",
              note="Effective places depend on other qualifiers and reallocations."):
    return tuple(ObjectiveRule(
        "COMPETITION_QUALIFICATION", target, "CONDITIONAL", status="PENDING",
        target_season=target_season, aggregation_scope=aggregation_scope, note=note,
    ) for target in targets)


def _rules(season, teams, games, objectives, *sources, points=3.0, metric="POINTS",
           grouping="single_table", draw=None, note=""):
    return (CompetitionRules(
        season, teams, games, points, metric, tuple(objectives), tuple(sources),
        standings_grouping=grouping, points_per_draw=draw, note=note,
    ),)


_UEFA_SOURCE = "https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/"
_UEFA_TARGETS = ("UEFA_CHAMPIONS_LEAGUE", "UEFA_EUROPA_LEAGUE", "UEFA_CONFERENCE_LEAGUE")
_LPF_SOURCE = "https://www.ligaprofesional.ar/wp-content/uploads/2026/01/Reglamento-Torneos-LPF-Primera-2026-1.pdf"
_MX_SOURCE = "https://www.rayados.com/es/noticias/21455/descubre-el-calendario-de-rayados-para-el-torneo-apertura-2026"
_CHL_SOURCE = "https://www.chl.hockey/en/about-us/chl-format"


def _football_rules(teams, games, first_relegated, source, *, playoff=None, note=""):
    safe_rank = (playoff or first_relegated) - 1
    objectives = [_retention(first_relegated, teams, safe_rank)]
    if playoff:
        objectives.append(_retention(playoff, playoff, safe_rank, "RELEGATION_PLAYOFF"))
    objectives.extend(_external(*_UEFA_TARGETS, target_season="2027/28"))
    return _rules("2026/27", teams, games, objectives, source, _UEFA_SOURCE, draw=1, note=note)


def _argentina_rules():
    return _rules("2026", 30, 16, (
        _qualification(1, 8, "ROUND_OF_16", scope="GROUP", group_size=15),
        ObjectiveRule("DIVISION_RETENTION", "CURRENT_DIVISION", "DIRECT_RELEGATION",
                      aggregation_scope="SEASON_AGGREGATE", status="PENDING",
                      rank_start=30, rank_end=30, reference_rank=29,
                      note="Worst annual team excluding the club relegated by rolling average."),
        ObjectiveRule("DIVISION_RETENTION", "CURRENT_DIVISION", "DIRECT_RELEGATION",
                      aggregation_scope="MULTISEASON_RECORD", status="PENDING",
                      selection_method="ROLLING_AVERAGE", places=1,
                      note="Worst points-per-game average; requires three seasons and exclusions."),
        *_external("CONMEBOL_LIBERTADORES", "CONMEBOL_SUDAMERICANA", target_season="2027",
                   aggregation_scope="SEASON_AGGREGATE",
                   note="Annual table, tournament champions and exclusions determine eligible qualifiers."),
    ), _LPF_SOURCE, grouping="split_tables", draw=1,
        note="Two groups of 15; annual table has 32 games per team. Never use a group for relegation.")


# Current-season rules are separate from the legacy metadata defaults below.
# An explicit season match is required before overriding cached metadata.
_COMPETITIVE_RULES = {
    "laliga": _football_rules(20, 38, 18, "https://www.laliga.com/noticias/equipos-que-bajan-a-segunda"),
    "premier_league": _football_rules(20, 38, 18, "https://www.premierleague.com/en/news/4657245/202526-premier-league-relegation-faq"),
    "serie_a": _football_rules(20, 38, 18, "https://en.legaseriea.it/serie-a/news/180-minutes-left-in-fight-for-survival",
                              note="Exceptional tied-position deciders are not a fixed playoff zone."),
    "bundesliga": _football_rules(18, 34, 17, "https://www.bundesliga.com/en/faq/what-are-the-rules-and-regulations-of-soccer/how-does-promotion-and-relegation-work-in-the-bundesliga-10645", playoff=16),
    "ligue_1": _football_rules(18, 34, 17, "https://ligue1.com/en/articles/l1_article_3812-", playoff=16),
    "liga_portugal": _football_rules(18, 34, 17, "https://www.ligaportugal.pt/pages/sobre-a-liga", playoff=16),
    "eredivisie": _football_rules(18, 34, 17, "https://www.knvb.nl/downloads/bestand/30062/definitiefcompetitieprogrammavriendenloterijeredivisie", playoff=16,
                                 note="Domestic playoff participants for international qualification remain pending."),
    "saudi_pro_league": _rules("2026/27", 18, 34, (
        _retention(16, 18, 15),
        *_external("AFC_CHAMPIONS_LEAGUE_ELITE", "AFC_CHAMPIONS_LEAGUE_TWO", target_season="2027/28"),
    ), "https://www.sportmonks.com/glossary/saudi-pro-league/", "https://www.saff.com.sa/news.php?id=4184", draw=1),
    "brasileirao": _rules("2026", 20, 38, (
        _retention(17, 20, 16),
        ObjectiveRule("COMPETITION_QUALIFICATION", "CONMEBOL_LIBERTADORES", "DIRECT",
                      status="PENDING", rank_start=1, rank_end=4, reference_rank=4, target_season="2027",
                      note="Base allocation; effective boundary depends on cup/continental reallocations."),
        ObjectiveRule("COMPETITION_QUALIFICATION", "CONMEBOL_LIBERTADORES", "QUALIFYING_ROUND",
                      status="PENDING", rank_start=5, rank_end=5, reference_rank=5, target_season="2027"),
        ObjectiveRule("COMPETITION_QUALIFICATION", "CONMEBOL_SUDAMERICANA", "DIRECT",
                      status="PENDING", selection_method="BEST_ELIGIBLE", places=6, target_season="2027",
                      note="Six best eligible teams, not a fixed range of table positions."),
    ), "https://stcbfsiteprdimgbrs.blob.core.windows.net/img-site/cdn/REC_Brasileiro_Serie_A_2026_c984f8cf05.pdf", draw=1),
    "liga_mx_apertura": _rules("2026", 18, 17, (
        _qualification(1, 8, "QUARTERFINALS"),
        *_external("CONCACAF_CHAMPIONS_CUP", target_season="2027",
                   aggregation_scope="SEASON_AGGREGATE",
                   note="Requires season aggregate, finalists and other qualifying paths."),
    ), _MX_SOURCE, "https://www.si.com/mx/liga-mx/oficial-la-liga-mx-se-queda-sin-ascenso-descenso-y-multas-2026-2027", draw=1),
    "liga_mx_clausura": _rules("2026", 18, 17, (
        _qualification(1, 8, "QUARTERFINALS"),
        *_external("CONCACAF_CHAMPIONS_CUP", target_season="2027",
                   aggregation_scope="SEASON_AGGREGATE",
                   note="Requires season aggregate, finalists and other qualifying paths."),
    ), _MX_SOURCE, "https://fmf.mx/docs/reglamentos/458.pdf", draw=1,
        note="Clausura 2026 only; do not extrapolate to Clausura 2027."),
    "liga_profesional_de_argentina_apertura": _argentina_rules(),
    "liga_profesional_de_argentina_clausura": _argentina_rules(),
    "nba": _rules("2026/27", 30, 82, (
        _qualification(1, 6, "PLAYOFFS", scope="CONFERENCE", group_size=15),
        _qualification(7, 10, "PLAY_IN", "PLAY_IN", "CONFERENCE", 15),
    ), "https://www.nba.com/news/nba-play-in-tournament", "https://www.nba.com/news/2026-27-nba-regular-season-schedule",
        points=1, metric="WIN_PERCENTAGE", grouping="split_tables"),
    "euroleague": _rules("2026/27", 20, 38, (
        _qualification(1, 6, "PLAYOFFS"), _qualification(7, 10, "PLAY_IN", "PLAY_IN"),
    ), "https://mediacentre.euroleague.net/en/app/2/communication/communication/preview/24407", points=1, metric="WINS"),
    "mlb": _rules("2026", 30, 162, (
        ObjectiveRule("STAGE_QUALIFICATION", "POSTSEASON", "DIRECT", ranking_scope="LEAGUE",
                      selection_method="DIVISION_WINNERS", places=3),
        ObjectiveRule("STAGE_QUALIFICATION", "POSTSEASON", "WILD_CARD", ranking_scope="LEAGUE",
                      selection_method="BEST_REMAINING", places=3),
    ), "https://www.mlb.com/news/mlb-playoff-format-faq", points=1, metric="WIN_PERCENTAGE", grouping="split_tables"),
    "nfl": _rules("2026", 32, 17, (
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "DIRECT", ranking_scope="CONFERENCE",
                      selection_method="DIVISION_WINNERS", places=4),
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "WILD_CARD", ranking_scope="CONFERENCE",
                      selection_method="BEST_REMAINING", places=3),
    ), "https://operations.nfl.com/media/nppjkdp1/2023-record-and-fact-book.pdf",
        points=1, metric="WIN_PERCENTAGE", grouping="split_tables", draw=0.5),
    "shl": _rules("2026/27", 14, 52, (
        _qualification(1, 6, "QUARTERFINALS"), _qualification(7, 10, "PRELIMINARY_ROUND", "QUALIFYING_PLAYOFF"),
        _retention(13, 14, 12, "RELEGATION_PLAYOFF"),
        *_external("CHAMPIONS_HOCKEY_LEAGUE", target_season="2027/28"),
    ), "https://www.swehockey.se/toppnyheter/het-premiaerloerdag-foer-shl/", _CHL_SOURCE),
    "liiga": _rules("2026/27", 17, 64, (
        _qualification(1, 12, "PLAYOFFS"), _retention(13, 16, 12, "RELEGATION_PLAYOFF"),
        _retention(17, 17, 12),
        *_external("CHAMPIONS_HOCKEY_LEAGUE", target_season="2027/28"),
    ), "https://hc.tps.fi/fi-fi/article/uutiset/liiga-tiedottaa-uusi-1410-mallinen-sarjamuoto-kaynnistyy-kaudella-202728/1573/", _CHL_SOURCE,
        note="Transitional season; internal playoff entry paths not yet resolved."),
    "nhl": _rules("2026/27", 32, 84, (
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "DIRECT", ranking_scope="DIVISION",
                      rank_start=1, rank_end=3, reference_rank=3, group_size=8),
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "WILD_CARD", ranking_scope="CONFERENCE",
                      selection_method="BEST_REMAINING", places=2),
    ), "https://www.nhl.com/news/nhl-stats-pack-2026-27-regular-season-schedule",
       "https://www.nhl.com/info/standings-info/playoff-format", points=2, grouping="split_tables"),
}


# ---------------------------------------------------------------------------
# League configurations
# ---------------------------------------------------------------------------

# NBA special bundles use season ids only. The regular case falls back to the
# current event season_id dynamically.

NBA_CONFIG = LeagueConfig(
    code="nba",
    competitive_rules=_COMPETITIVE_RULES["nba"],
    display_name="NBA",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=132,
        source_tournament_id=177,
    ),
    collected=True,
    standings_method="win_pct",
    grouping_method="nba_conference",
    number_of_teams=30,
    total_regular_season_games=82,
    standings_grouping="split_tables",
)

LALIGA_CONFIG = LeagueConfig(
    code="laliga",
    competitive_rules=_COMPETITIVE_RULES["laliga"],
    display_name="LaLiga",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=8,
        source_tournament_id=36,
    ),
    collected=True,
    standings_method="football_3_1_0_h2h",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

PREMIER_LEAGUE_CONFIG = LeagueConfig(
    code="premier_league",
    competitive_rules=_COMPETITIVE_RULES["premier_league"],
    display_name="Premier League",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=17,
        source_tournament_id=1,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

NFL_CONFIG = LeagueConfig(
    code="nfl",
    competitive_rules=_COMPETITIVE_RULES["nfl"],
    display_name="NFL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=9464,
        source_tournament_id=108947,
    ),
    collected=True,
    standings_method="win_pct_half_tie",
    grouping_method="nfl_conference",
    number_of_teams=32,
    total_regular_season_games=17,
    standings_grouping="split_tables",
)

MLB_CONFIG = LeagueConfig(
    code="mlb",
    competitive_rules=_COMPETITIVE_RULES["mlb"],
    display_name="MLB",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=11205,
        source_tournament_id=108806,
    ),
    collected=True,
    standings_method="win_pct",
    grouping_method="mlb_league",
    number_of_teams=30,
    total_regular_season_games=162,
    standings_grouping="split_tables",
)

NHL_CONFIG = LeagueConfig(
    code="nhl",
    competitive_rules=_COMPETITIVE_RULES["nhl"],
    display_name="NHL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=234,
        source_tournament_id=142,
    ),
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
    competitive_rules=_COMPETITIVE_RULES["serie_a"],
    display_name="Serie A",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=23,
        source_tournament_id=33,
    ),
    collected=True,
    standings_method="football_3_1_0_h2h",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

BUNDESLIGA_CONFIG = LeagueConfig(
    code="bundesliga",
    competitive_rules=_COMPETITIVE_RULES["bundesliga"],
    display_name="Bundesliga",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=35,
        source_tournament_id=42,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
)

LIGUE_1_CONFIG = LeagueConfig(
    code="ligue_1",
    competitive_rules=_COMPETITIVE_RULES["ligue_1"],
    display_name="Ligue 1",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=34,
        source_tournament_id=4,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
)

SAUDI_PRO_LEAGUE_CONFIG = LeagueConfig(
    code="saudi_pro_league",
    competitive_rules=_COMPETITIVE_RULES["saudi_pro_league"],
    display_name="Saudi Pro League",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=955,
        source_tournament_id=3708,
    ),
    collected=True,
    standings_method="football_3_1_0_h2h",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
)

SHL_CONFIG = LeagueConfig(
    code="shl",
    competitive_rules=_COMPETITIVE_RULES["shl"],
    display_name="SHL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=261,
        source_tournament_id=115,
    ),
    collected=True,
    standings_method="hockey_3_2_1_0",
    grouping_method="league_wide",
    number_of_teams=14,
    total_regular_season_games=52,
    standings_grouping="single_table",
)

WNBA_CONFIG = LeagueConfig(
    code="wnba",
    display_name="WNBA",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=486,
        source_tournament_id=591,
    ),
    collected=True,
    standings_method="win_pct",
    grouping_method="league_wide",
    number_of_teams=15,
    total_regular_season_games=40,
    standings_grouping="split_tables",
)

PFL_CONFIG = LeagueConfig(
    code="pfl",
    display_name="PFL",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=1654,
        source_tournament_id=19822,
    ),
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
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=1566,
        source_tournament_id=10233,
    ),
    collected=True,
    standings_method="win_pct",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=None,
    standings_grouping="single_table",
)

LIGA_MX_APERTURA_CONFIG = LeagueConfig(
    code="liga_mx_apertura",
    competitive_rules=_COMPETITIVE_RULES["liga_mx_apertura"],
    display_name="Liga MX Apertura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=11621,
        source_tournament_id=28,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=17,
    standings_grouping="single_table",
)

LIGA_MX_CLAUSURA_CONFIG = LeagueConfig(
    code="liga_mx_clausura",
    competitive_rules=_COMPETITIVE_RULES["liga_mx_clausura"],
    display_name="Liga MX Clausura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=11620,
        source_tournament_id=16753,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=17,
    standings_grouping="single_table",
)

BRASILEIRAO_CONFIG = LeagueConfig(
    code="brasileirao",
    competitive_rules=_COMPETITIVE_RULES["brasileirao"],
    display_name="Brasileirão",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=325,
        source_tournament_id=83,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

LIGA_PROFESIONAL_DE_ARGENTINA_APERTURA_CONFIG = LeagueConfig(
    code="liga_profesional_de_argentina_apertura",
    competitive_rules=_COMPETITIVE_RULES["liga_profesional_de_argentina_apertura"],
    display_name="Liga Profesional de Argentina Apertura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=155,
        source_tournament_id=143625,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=28,
    total_regular_season_games=27,
    standings_grouping="single_table",
)

LIGA_PROFESIONAL_DE_ARGENTINA_CLAUSURA_CONFIG = LeagueConfig(
    code="liga_profesional_de_argentina_clausura",
    competitive_rules=_COMPETITIVE_RULES["liga_profesional_de_argentina_clausura"],
    display_name="Liga Profesional de Argentina Clausura",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=155,
        source_tournament_id=156467,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=28,
    total_regular_season_games=27,
    standings_grouping="single_table",
)

EREDIVISIE_CONFIG = LeagueConfig(
    code="eredivisie",
    competitive_rules=_COMPETITIVE_RULES["eredivisie"],
    display_name="Eredivisie",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=37,
        source_tournament_id=39,
    ),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
)

# EuroLeague ranks by victories, then a head-to-head mini-table among teams
# tied on wins. Official overtime-point exclusion is not modeled.
EUROLEAGUE_CONFIG = LeagueConfig(
    code="euroleague",
    competitive_rules=_COMPETITIVE_RULES["euroleague"],
    display_name="Euroleague",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=138,
        source_tournament_id=42527,
    ),
    collected=True,
    standings_method="wins_h2h",
    grouping_method="league_wide",
    number_of_teams=20,
    total_regular_season_games=38,
    standings_grouping="single_table",
)

# Liiga uses 3-2-1-0. Official last-resort H2H / even-strength H2H goals
# after regulation wins, goal difference, and goals for are not modeled.
LIIGA_CONFIG = LeagueConfig(
    code="liiga",
    competitive_rules=_COMPETITIVE_RULES["liiga"],
    display_name="Liiga",
    primary_identity=CompetitionIdentity(
        source_unique_tournament_id=134,
        source_tournament_id=108,
    ),
    collected=True,
    standings_method="hockey_3_2_1_0",
    grouping_method="league_wide",
    number_of_teams=17,
    total_regular_season_games=64,
    standings_grouping="single_table",
)

# Identity verified against canonical competitions.competition_id=261.
LIGA_PORTUGAL_CONFIG = LeagueConfig(
    code="liga_portugal",
    display_name="Liga Portugal",
    primary_identity=CompetitionIdentity(238, 52),
    collected=True,
    standings_method="football_3_1_0",
    grouping_method="league_wide",
    number_of_teams=18,
    total_regular_season_games=34,
    standings_grouping="single_table",
    competitive_rules=_COMPETITIVE_RULES["liga_portugal"],
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
