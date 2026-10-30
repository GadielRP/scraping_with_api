"""Current sporting profiles keyed by canonical competition ID.

This catalogue owns current format, scoring and objectives. Reference seasons
and source URLs document the facts; events are not filtered by season labels.
Tracking and historical collection routing remain independent.
"""

from types import MappingProxyType
from typing import Mapping, Literal

from modules.competition import standings_policies as standings
from modules.competition.competitive_rules import (
    CompetitionRules, ObjectiveRule, FOOTBALL_SCORING, WINS_SCORING,
    NFL_SCORING, NHL_SCORING, HOCKEY_SCORING,
)


def create_competition_profile(
    *,
    reference_season: str,
    number_of_teams: int | None,
    total_regular_season_games: int | None,
    objectives: tuple[ObjectiveRule, ...] | list[ObjectiveRule] | None,
    source_urls: tuple[str, ...] | list[str],
    ranking,
    scoring=FOOTBALL_SCORING,
    grouping: str = "league_wide",
    note: str = "",
) -> CompetitionRules:
    """Universal factory for canonical CompetitionRules with explicit parameter naming."""
    return CompetitionRules(
        reference_season=reference_season,
        number_of_teams=number_of_teams,
        total_regular_season_games=total_regular_season_games,
        scoring=scoring,
        standings=ranking,
        objectives=tuple(objectives) if objectives is not None else None,
        source_urls=tuple(source_urls),
        grouping_method=grouping,
        note=note,
    )


# Backward-compatible alias for any external caller expecting create_competition_rules
create_competition_rules = create_competition_profile


def create_stage_qualification_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    target: str,
    path: str = "DIRECT",
    scope: Literal["GLOBAL", "CONFERENCE", "DIVISION", "GROUP", "LEAGUE"] = "GLOBAL",
    status: Literal["CONFIGURED", "PENDING"] = "CONFIGURED",
    group_size: int | None = None,
    ranking_group: str | None = None,
    selection_method: str = "RANK_RANGE",
    places: int | None = None,
    note: str = "",
) -> ObjectiveRule:
    """Rule for stage qualification (knockout stage, playoffs, wild card, or group advance)."""
    return ObjectiveRule(
        objective_type="STAGE_QUALIFICATION",
        target=target,
        qualification_path=path,
        ranking_scope=scope,
        status=status,
        rank_start=start,
        rank_end=end,
        reference_rank=end,
        group_size=group_size,
        ranking_group=ranking_group,
        selection_method=selection_method,
        places=places,
        note=note,
    )


def create_division_retention_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    safe_rank: int | None = None,
    path: str = "DIRECT_RELEGATION",
    status: Literal["CONFIGURED", "PENDING"] = "CONFIGURED",
    aggregation_scope: Literal["TOURNAMENT", "SEASON_AGGREGATE", "MULTISEASON_RECORD"] = "TOURNAMENT",
    selection_method: str = "RANK_RANGE",
    places: int | None = None,
    note: str = "",
) -> ObjectiveRule:
    """Rule for division retention, direct relegation, or relegation playoff."""
    return ObjectiveRule(
        objective_type="DIVISION_RETENTION",
        target="CURRENT_DIVISION",
        qualification_path=path,
        status=status,
        aggregation_scope=aggregation_scope,
        rank_start=start,
        rank_end=end,
        reference_rank=safe_rank,
        selection_method=selection_method,
        places=places,
        note=note,
    )


def create_international_qualification_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    target: str,
    path: str = "DIRECT",
    status: Literal["CONFIGURED", "PENDING"] = "CONFIGURED",
    target_season: str = "2027/28",
    aggregation_scope: Literal["TOURNAMENT", "SEASON_AGGREGATE", "MULTISEASON_RECORD"] = "TOURNAMENT",
    selection_method: str = "RANK_RANGE",
    places: int | None = None,
    note: str = "Standard league table qualification boundary.",
) -> ObjectiveRule:
    """Confirmed or pending qualification zone for external/continental tournament."""
    return ObjectiveRule(
        objective_type="COMPETITION_QUALIFICATION",
        target=target,
        qualification_path=path,
        status=status,
        rank_start=start,
        rank_end=end,
        reference_rank=end,
        target_season=target_season,
        aggregation_scope=aggregation_scope,
        selection_method=selection_method,
        places=places,
        note=note,
    )


def create_pending_cup_winner_reallocation_rule(
    *targets: str,
    target_season: str,
    aggregation_scope: Literal["TOURNAMENT", "SEASON_AGGREGATE", "MULTISEASON_RECORD"] = "TOURNAMENT",
    note: str = "Domestic cup winner and continental champion reallocation pending resolution.",
) -> tuple[ObjectiveRule, ...]:
    """Secondary/contingent international slots that shift down if domestic cup winners already qualified."""
    return tuple(ObjectiveRule(
        objective_type="COMPETITION_QUALIFICATION",
        target=target,
        qualification_path="CONDITIONAL",
        status="PENDING",
        target_season=target_season,
        aggregation_scope=aggregation_scope,
        note=note,
    ) for target in targets)


_UEFA_SOURCE = "https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/"
_LPF_SOURCE = "https://www.ligaprofesional.ar/wp-content/uploads/2026/01/Reglamento-Torneos-LPF-Primera-2026-1.pdf"
_MX_SOURCE = "https://www.rayados.com/es/noticias/21455/descubre-el-calendario-de-rayados-para-el-torneo-apertura-2026"
_CHL_SOURCE = "https://www.chl.hockey/en/about-us/chl-format"


# One current profile per canonical competitions.competition_id.
# reference_season documents these facts; it is not a lookup key.
COMPETITION_PROFILES: Mapping[int, CompetitionRules] = MappingProxyType({
    # laliga (1-4 Champions League, 5 Europa League, 6 Conference League; 18-20 Descenso directo, seguro 17)
    167: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        objectives=(
            create_international_qualification_rule(start=1, end=4, path="DIRECT", target="UEFA_CHAMPIONS_LEAGUE"),
            create_international_qualification_rule(start=5, end=5, path="DIRECT", target="UEFA_EUROPA_LEAGUE"),
            create_international_qualification_rule(start=6, end=6, path="QUALIFYING_ROUND", target="UEFA_CONFERENCE_LEAGUE_QUALIFICATION"),
            create_division_retention_rule(start=18, end=20, safe_rank=17),
        ),
        source_urls=("https://www.laliga.com/noticias/equipos-que-bajan-a-segunda", _UEFA_SOURCE),
        ranking=standings.LALIGA,
    ),

    # premier_league (1-4 Champions League, 5 Europa League; Conference otorgada a campeón de EFL Cup; 18-20 Descenso directo)
    # PENDING IMPLEMENTATION TO SUPPORT EFL CUP WINNER RELOCATION TO CONFERENCE LEAGUE
    168: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        objectives=(
            create_international_qualification_rule(start=1, end=4, path="DIRECT", target="UEFA_CHAMPIONS_LEAGUE"),
            create_international_qualification_rule(start=5, end=5, path="DIRECT", target="UEFA_EUROPA_LEAGUE"),
            create_division_retention_rule(start=18, end=20, safe_rank=17),
        ),
        source_urls=("https://www.premierleague.com/en/news/4657245/202526-premier-league-relegation-faq", _UEFA_SOURCE),
        ranking=standings.PREMIER_LEAGUE,
        note="League finish directly qualifies through 5th. EFL Cup winner takes UECL; FA Cup winner takes UEL.",
    ),

    # serie_a (1-4 Champions League, 5 Europa League, 6 Conference League; 18-20 Descenso directo, seguro 17)
    88: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        objectives=(
            create_international_qualification_rule(start=1, end=4, path="DIRECT", target="UEFA_CHAMPIONS_LEAGUE"),
            create_international_qualification_rule(start=5, end=5, path="DIRECT", target="UEFA_EUROPA_LEAGUE"),
            create_international_qualification_rule(start=6, end=6, path="QUALIFYING_ROUND", target="UEFA_CONFERENCE_LEAGUE_QUALIFICATION"),
            create_division_retention_rule(start=18, end=20, safe_rank=17),
        ),
        source_urls=("https://en.legaseriea.it/serie-a/news/180-minutes-left-in-fight-for-survival", _UEFA_SOURCE),
        ranking=standings.SERIE_A,
        note="Exceptional tied-position deciders are not a fixed playoff zone.",
    ),

    # bundesliga (1-4 Champions League, 5 Europa League, 6 Conference League; 16 Playoff, 17-18 Descenso directo)
    171: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        objectives=(
            create_international_qualification_rule(start=1, end=4, path="DIRECT", target="UEFA_CHAMPIONS_LEAGUE"),
            create_international_qualification_rule(start=5, end=5, path="DIRECT", target="UEFA_EUROPA_LEAGUE"),
            create_international_qualification_rule(start=6, end=6, path="QUALIFYING_ROUND", target="UEFA_CONFERENCE_LEAGUE_QUALIFICATION"),
            create_division_retention_rule(start=16, end=16, safe_rank=15, path="RELEGATION_PLAYOFF"),
            create_division_retention_rule(start=17, end=18, safe_rank=15),
        ),
        source_urls=(
            "https://www.bundesliga.com/en/faq/what-are-the-rules-and-regulations-of-soccer/how-does-promotion-and-relegation-work-in-the-bundesliga-10645",
            _UEFA_SOURCE,
        ),
        ranking=standings.BUNDESLIGA,
    ),

    # ligue_1 (1-3 Champions directa, 4-5 Europa League, 6 Conference League; 16 Playoff, 17-18 Descenso directo)
    172: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        objectives=(
            create_international_qualification_rule(start=1, end=3, path="DIRECT", target="UEFA_CHAMPIONS_LEAGUE"),
            create_international_qualification_rule(start=4, end=4, path="QUALIFYING_ROUND", target="UEFA_CHAMPIONS_LEAGUE_QUALIFICATION"),
            create_international_qualification_rule(start=5, end=5, path="QUALIFYING_ROUND", target="UEFA_EUROPA_LEAGUE_QUALIFICATION"),
            create_international_qualification_rule(start=6, end=6, path="QUALIFYING_ROUND", target="UEFA_CONFERENCE_LEAGUE_QUALIFICATION"),
            create_division_retention_rule(start=16, end=16, safe_rank=15, path="RELEGATION_PLAYOFF"),
            create_division_retention_rule(start=17, end=18, safe_rank=15),
            ),
            source_urls=("https://ligue1.com/en/articles/l1_article_3812-", _UEFA_SOURCE),
        ranking=standings.LIGUE_1,
    ),

    # liga portugal (1-2 Champions League, 3 Europa League, 4-5 Conference League; 16 Playoff, 17-18 Descenso directo)
    261: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        objectives=(
            create_international_qualification_rule(start=1, end=2, path="DIRECT", target="UEFA_CHAMPIONS_LEAGUE"),
            create_international_qualification_rule(start=3, end=3, path="QUALIFYING_ROUND", target="UEFA_CHAMPIONS_LEAGUE_QUALIFICATION"),
            create_international_qualification_rule(start=4, end=4, path="QUALIFYING_ROUND", target="UEFA_EUROPA_LEAGUE_QUALIFICATION"),
            create_international_qualification_rule(start=5, end=5, path="QUALIFYING_ROUND", target="UEFA_CONFERENCE_LEAGUE_QUALIFICATION"),
            create_division_retention_rule(start=16, end=16, safe_rank=15, path="RELEGATION_PLAYOFF"),
            create_division_retention_rule(start=17, end=18, safe_rank=15),
        ),
        source_urls=("https://www.ligaportugal.pt/pages/sobre-a-liga", _UEFA_SOURCE),
        ranking=standings.LIGA_PORTUGAL,
    ),

    # eredivisie (1 Champions League, 1 Champions league qualification,  3 Europa League Qualification, 4 Conference League; 16 Playoff, 17-18 Descenso directo)
    317: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        objectives=(
            create_international_qualification_rule(start=1, end=1, path="DIRECT", target="UEFA_CHAMPIONS_LEAGUE"),
            create_international_qualification_rule(start=2, end=2, path="QUALIFYING_ROUND", target="UEFA_CHAMPIONS_LEAGUE_QUALIFICATION"),
            create_international_qualification_rule(start=3, end=3, path="QUALIFYING_ROUND", target="UEFA_EUROPA_LEAGUE_QUALIFICATION"),
            create_international_qualification_rule(start=4, end=7, path="QUALIFYING_ROUND", target="PLAYOFF_UEFA_CONFERENCE_LEAGUE_QUALIFICATION", note="Playoff winner qualifies for UEFA Conference League. Not sure though if this is the right way to do it."),
            create_division_retention_rule(start=16, end=16, safe_rank=15, path="RELEGATION_PLAYOFF"),
            create_division_retention_rule(start=17, end=18, safe_rank=15),
        ),
        source_urls=(
            "https://www.knvb.nl/downloads/bestand/30062/definitiefcompetitieprogrammavriendenloterijeredivisie",
            _UEFA_SOURCE,
        ),
        ranking=standings.EREDIVISIE,
        note="Base league positions configured; domestic European playoffs remain separate.",
    ),

    # saudi_pro_league (1-3 AFC Champions League Elite, 4 AFC Champions League Two; 16-18 Descenso directo)
    50: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        objectives=(
            create_international_qualification_rule(start=1, end=5, path="DIRECT", target="AFC_CHAMPIONS_LEAGUE_ELITE", target_season="2027/28"),
            create_international_qualification_rule(start=4, end=4, path="DIRECT", target="AFC_CHAMPIONS_LEAGUE_TWO", target_season="2027/28"),
            create_division_retention_rule(start=16, end=18, safe_rank=15),
        ),
        source_urls=("https://www.saff.com.sa/news.php?id=4184",),
        ranking=standings.SAUDI_PRO_LEAGUE,
    ),

    # brasileirao (1-4 Libertadores grupos, 5-6 Libertadores previa, 7-12 Sudamericana; 17-20 Descenso directo)
    318: create_competition_profile(
        reference_season="2026",
        number_of_teams=20,
        total_regular_season_games=38,
        objectives=(
            create_international_qualification_rule(start=1, end=4, target="CONMEBOL_LIBERTADORES", path="DIRECT", target_season="2027"),
            create_international_qualification_rule(start=5, end=5, target="CONMEBOL_LIBERTADORES", path="QUALIFYING_ROUND", target_season="2027"),
            create_international_qualification_rule(start=6, end=12, target="CONMEBOL_SUDAMERICANA", path="DIRECT", target_season="2027"),
            create_division_retention_rule(start=17, end=20, safe_rank=16),
        ),
        source_urls=("https://stcbfsiteprdimgbrs.blob.core.windows.net/img-site/cdn/REC_Brasileiro_Serie_A_2026_c984f8cf05.pdf",),
        ranking=standings.BRASILEIRAO,
        note="Base league allocations configured. Copa do Brasil / continental winner reallocations pending dynamic resolution.",
    ),

    # liga_mx_apertura (1-8 a cuartos de final de Liguilla; sin descenso; Concacaf pendiente por acumulado anual)
    328: create_competition_profile(
        reference_season="2026",
        number_of_teams=18,
        total_regular_season_games=17,
        objectives=(
            create_stage_qualification_rule(start=1, end=8, path="DIRECT", target="QUARTERFINALS"),
            *create_pending_cup_winner_reallocation_rule(
                "CONCACAF_CHAMPIONS_CUP",
                target_season="2027",
                aggregation_scope="SEASON_AGGREGATE",
                note="Requires season aggregate, finalists and other qualifying paths.",
            ),
        ),
        source_urls=(_MX_SOURCE,),
        ranking=standings.LIGA_MX_APERTURA,
        note="Standings criteria reference Clausura 2026; Apertura 2026 confirmation remains pending.",
    ),

    # liga_mx_clausura (1-8 a cuartos de final de Liguilla; sin descenso; Concacaf pendiente por acumulado anual)
    2192: create_competition_profile(
        reference_season="2026",
        number_of_teams=18,
        total_regular_season_games=17,
        objectives=(
            create_stage_qualification_rule(start=1, end=8, path="DIRECT", target="QUARTERFINALS"),
            *create_pending_cup_winner_reallocation_rule(
                "CONCACAF_CHAMPIONS_CUP",
                target_season="2027",
                aggregation_scope="SEASON_AGGREGATE",
                note="Requires season aggregate, finalists and other qualifying paths.",
            ),
        ),
        source_urls=(_MX_SOURCE, "https://fmf.mx/docs/reglamentos/483.pdf"),
        ranking=standings.LIGA_MX_CLAUSURA,
        note="Reference format verified for Clausura 2026; update this current profile when the format changes.",
    ),

    # liga_profesional_de_argentina_apertura (Dos grupos de 15; 1-8 de cada grupo a octavos; descenso anual y promedios)
    2476: create_competition_profile(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=16,
        objectives=(
            create_stage_qualification_rule(start=1, end=8, target="ROUND_OF_16", scope="GROUP", group_size=15),
            create_division_retention_rule(
                start=30, end=30, safe_rank=29, path="DIRECT_RELEGATION",
                aggregation_scope="SEASON_AGGREGATE", status="PENDING",
                note="Worst annual team excluding the club relegated by rolling average.",
            ),
            create_division_retention_rule(
                path="DIRECT_RELEGATION",
                aggregation_scope="MULTISEASON_RECORD", status="PENDING",
                selection_method="ROLLING_AVERAGE", places=1,
                note="Worst points-per-game average; requires three seasons and exclusions.",
            ),
            *create_pending_cup_winner_reallocation_rule(
                "CONMEBOL_LIBERTADORES", "CONMEBOL_SUDAMERICANA",
                target_season="2027",
                aggregation_scope="SEASON_AGGREGATE",
                note="Annual table, tournament champions and exclusions determine eligible qualifiers.",
            ),
        ),
        source_urls=(_LPF_SOURCE,),
        ranking=standings.ARGENTINA_ZONES,
        grouping="argentina_zones",
        note="Two groups of 15; annual table has 32 games per team. Never use a group for relegation.",
    ),

    # liga_profesional_de_argentina_clausura
    310: create_competition_profile(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=16,
        objectives=(
            create_stage_qualification_rule(start=1, end=8, target="ROUND_OF_16", scope="GROUP", group_size=15),
            create_division_retention_rule(
                start=30, end=30, safe_rank=29, path="DIRECT_RELEGATION",
                aggregation_scope="SEASON_AGGREGATE", status="PENDING",
                note="Worst annual team excluding the club relegated by rolling average.",
            ),
            create_division_retention_rule(
                path="DIRECT_RELEGATION",
                aggregation_scope="MULTISEASON_RECORD", status="PENDING",
                selection_method="ROLLING_AVERAGE", places=1,
                note="Worst points-per-game average; requires three seasons and exclusions.",
            ),
            *create_pending_cup_winner_reallocation_rule(
                "CONMEBOL_LIBERTADORES", "CONMEBOL_SUDAMERICANA",
                target_season="2027",
                aggregation_scope="SEASON_AGGREGATE",
                note="Annual table, tournament champions and exclusions determine eligible qualifiers.",
            ),
        ),
        source_urls=(_LPF_SOURCE,),
        ranking=standings.ARGENTINA_ZONES,
        grouping="argentina_zones",
        note="Two groups of 15; annual table has 32 games per team. Never use a group for relegation.",
    ),

    # nba (Conferencia: 1-6 Playoffs, 7-10 Play-In; sin descenso)
    176: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=30,
        total_regular_season_games=82,
        objectives=(
            create_stage_qualification_rule(start=1, end=6, path="DIRECT", target="PLAYOFFS", scope="CONFERENCE", group_size=15),
            create_stage_qualification_rule(start=7, end=10, path="PLAY_IN_QUALIFYING_ROUND", target="PLAY_IN", scope="CONFERENCE", group_size=15),
        ),
        source_urls=(
            "https://www.nba.com/news/nba-play-in-tournament",
            "https://www.nba.com/news/2026-27-nba-regular-season-schedule",
        ),
        ranking=standings.NBA,
        scoring=WINS_SCORING,
        grouping="nba_conference",
    ),

    # euroleague (1-6 Playoffs, 7-10 Play-In; sin descenso)
    429: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        objectives=(
            create_stage_qualification_rule(start=1, end=6, path="DIRECT", target="PLAYOFFS"),
            create_stage_qualification_rule(start=7, end=10, path="PLAY_IN_QUALIFYING_ROUND", target="PLAY_IN"),
        ),
        source_urls=("https://mediacentre.euroleague.net/en/app/2/communication/communication/preview/24407",),
        ranking=standings.EUROLEAGUE,
        scoring=WINS_SCORING,
    ),

    # mlb (Por liga: 3 campeones de división, 3 Wild Cards)
    129: create_competition_profile(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=162,
        objectives=(
            create_stage_qualification_rule(
                target="PLAYOFFS", path="DIRECT", scope="LEAGUE",
                selection_method="DIVISION_WINNERS", places=3,
            ),
            create_stage_qualification_rule(
                target="PLAYOFFS", path="WILD_CARD", scope="LEAGUE",
                selection_method="BEST_REMAINING", places=3,
            ),
        ),
        source_urls=("https://www.mlb.com/news/mlb-playoff-format-faq",),
        ranking=standings.MLB,
        scoring=WINS_SCORING,
        grouping="mlb_league",
    ),

    # nfl (Por conferencia: 4 campeones de división, 3 Wild Cards)
    5153: create_competition_profile(
        reference_season="2026",
        number_of_teams=32,
        total_regular_season_games=17,
        objectives=(
            create_stage_qualification_rule(
                target="PLAYOFFS", path="DIRECT", scope="CONFERENCE",
                selection_method="DIVISION_WINNERS", places=4,
            ),
            create_stage_qualification_rule(
                target="PLAYOFFS", path="WILD_CARD", scope="CONFERENCE",
                selection_method="BEST_REMAINING", places=3,
            ),
        ),
        source_urls=("https://operations.nfl.com/media/nppjkdp1/2023-record-and-fact-book.pdf",),
        ranking=standings.NFL,
        scoring=NFL_SCORING,
        grouping="nfl_conference",
    ),

    # shl (1-6 Cuartos, 7-10 Ronda previa; 13-14 Playoff de permanencia)
    180: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=14,
        total_regular_season_games=52,
        objectives=(
            create_stage_qualification_rule(start=1, end=6, path="DIRECT", target="QUARTERFINALS"),
            create_stage_qualification_rule(start=7, end=10, path="QUALIFYING_PLAYOFF", target="PRELIMINARY_ROUND"),
            create_division_retention_rule(start=13, end=14, safe_rank=12, path="RELEGATION_PLAYOFF"),
            *create_pending_cup_winner_reallocation_rule("CHAMPIONS_HOCKEY_LEAGUE", target_season="2027/28"),
        ),
        source_urls=("https://www.swehockey.se/toppnyheter/het-premiaerloerdag-foer-shl/", _CHL_SOURCE),
        ranking=standings.SHL,
        scoring=HOCKEY_SCORING,
    ),

    # liiga (1-12 Playoffs; 13-16 Playoff permanencia, 17 Descenso directo)
    525: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=17,
        total_regular_season_games=64,
        objectives=(
            create_stage_qualification_rule(start=1, end=12, target="PLAYOFFS"),
            create_division_retention_rule(start=13, end=16, safe_rank=12, path="RELEGATION_PLAYOFF"),
            create_division_retention_rule(start=17, end=17, safe_rank=12),
            *create_pending_cup_winner_reallocation_rule("CHAMPIONS_HOCKEY_LEAGUE", target_season="2027/28"),
        ),
        source_urls=(
            "https://hc.tps.fi/fi-fi/article/uutiset/liiga-tiedottaa-uusi-1410-mallinen-sarjamuoto-kaynnistyy-kaudella-202728/1573/",
            _CHL_SOURCE,
        ),
        ranking=standings.LIIGA,
        scoring=HOCKEY_SCORING,
        note="Transitional season; internal playoff entry paths not yet resolved.",
    ),

    # nhl (División: 1-3 Playoffs directos; Conferencia: 2 Wild Cards)
    5196: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=32,
        total_regular_season_games=84,
        objectives=(
            create_stage_qualification_rule(
                start=1, end=3, target="PLAYOFFS", path="DIRECT", scope="DIVISION", group_size=8,
            ),
            create_stage_qualification_rule(
                target="PLAYOFFS", path="WILD_CARD", scope="CONFERENCE", selection_method="BEST_REMAINING", places=2,
            ),
        ),
        source_urls=(
            "https://www.nhl.com/news/nhl-stats-pack-2026-27-regular-season-schedule",
            "https://www.nhl.com/info/standings-info/playoff-format",
        ),
        ranking=standings.NHL,
        scoring=NHL_SCORING,
        grouping="nhl_conference",
    ),

    # mls (Conferencia: 1-7 Round One Best-of-3, 8-9 Wild Card match; sin descenso)
    76: create_competition_profile(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=34,
        objectives=(
            create_stage_qualification_rule(start=1, end=7, target="ROUND_ONE_BEST_OF_THREE", scope="CONFERENCE", group_size=15, note="Playoffs first round"),
            create_stage_qualification_rule(start=8, end=9, target="WILD_CARD_MATCH", path="QUALIFYING_PLAYOFF", scope="CONFERENCE", group_size=15, note="Similar to play-in"),
            create_international_qualification_rule(
                target="CONCACAF_CHAMPIONS_CUP", path="CONDITIONAL", status="PENDING",
                selection_method="MULTIPLE_QUALIFICATION_PATHS", places=5, target_season="2027",
                note="MLS Cup winner, Shield winner, opposite conference winner and next two eligible Shield clubs; resolve other qualifiers/reallocations.",
            ),
        ),
        source_urls=(
            "https://www.mlssoccer.com/league-reports/competition-guidelines/",
            "https://www.concacaf.com/competitions/champions-cup/news/2027-concacaf-champions-cup-qualified-clubs",
            "https://www.mlssoccer.com/2027mlssprintseason/faq",
        ),
        ranking=standings.MLS,
        grouping="mls_conference",
        note="2026 format: no relegation. Cancelled games may require official PPG ruling. Update current profile before 2027 Sprint (14 games, top eight per conference); 2028 Leagues Cup is decided by Sprint, not this table.",
    ),

    # ahl (Calder Cup Playoffs según cuotas por división; sin descenso)
    554: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=32,
        total_regular_season_games=72,
        objectives=tuple(
            create_stage_qualification_rule(
                start=1, end=places, target="CALDER_CUP_PLAYOFFS", path="DIRECT",
                scope="DIVISION", ranking_group=division, group_size=size,
            )
            for division, size, places in (
                ("Atlantic", 7, 5), ("North", 8, 6), ("Central", 7, 5), ("Pacific", 10, 7),
            )
        ),
        source_urls=("https://theahl.com/news/ahl-unveils-2026-27-schedule",),
        ranking=standings.AHL,
        scoring=NHL_SCORING,
        grouping="ahl_division",
        note="No relegation or table-based international qualification. Division quotas verified by 2026/27 season-opening announcement; no NHL wildcard substitution. Internal round byes are outside this objective.",
    ),

    # wnba (15 equipos, 40 partidos; sin objetivos configurados)
    145: create_competition_profile(
        reference_season="",
        number_of_teams=15,
        total_regular_season_games=40,
        objectives=(
            create_stage_qualification_rule(start=1, end=8, target="PLAYOFFS", path="DIRECT", scope="CONFERENCE", group_size=15, note="Playoffs first round"),
        ),
        source_urls=("repository:previous-competition-configuration",),
        ranking=standings.WNBA,
        scoring=WINS_SCORING,
    ),

    # cba chinese basketball association (19-20 Playoff de permanencia)
    174: create_competition_profile(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=46,
        objectives=(
            create_division_retention_rule(start=19, end=20, safe_rank=18, path="RELEGATION_PLAYOFF"),
            create_stage_qualification_rule(
                start=1, end=4, target="QUARTERFINALS", path="DIRECT", status="PENDING",
                note="Previous primary structure; 2026/27 continuity unconfirmed.",
            ),
            create_stage_qualification_rule(
                start=5, end=12, target="PLAYOFFS", path="FIRST_ROUND", status="PENDING",
                note="Previous primary structure; 2026/27 continuity unconfirmed.",
            ),
            create_international_qualification_rule(
                target="FIBA_INTERCONTINENTAL_CUP", path="CONDITIONAL", status="PENDING",
                selection_method="CHAMPION", places=1, target_season="2027",
                note="Champion path only; not a regular season table finish.",
            ),
        ),
        source_urls=(
            "https://www.sport.gov.cn/n20001280/n20067662/n20067613/c30106244/content.html",
            "https://cbanetcdn.cba.net.cn/u/cms/www/202310/14124151vnl4.pdf",
            "https://www.fiba.basketball/en/news/meet-the-team-shanghai-sharks",
        ),
        ranking=standings.CBA,
        scoring=WINS_SCORING,
    ),

    # pfl philippines football league (histórico sin reglas configuradas)
    5151: create_competition_profile(
        reference_season="",
        number_of_teams=None,
        total_regular_season_games=None,
        objectives=None,
        source_urls=("repository:previous-competition-configuration",),
        ranking=standings.PFL,
        scoring=FOOTBALL_SCORING,
    ),
})


def get_competition_profile(competition_id: int | None) -> CompetitionRules | None:
    """Return the current profile by canonical ID, independent of provider or season labels."""
    return COMPETITION_PROFILES.get(competition_id)
