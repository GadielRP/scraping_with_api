"""Current sporting profiles keyed by canonical competition ID.

This catalogue owns current format, scoring and objectives. Reference seasons
and source URLs document the facts; events are not filtered by season labels.
Tracking and historical collection routing remain independent.
"""

from types import MappingProxyType
from typing import Mapping

from modules.competition import standings_policies as standings
from modules.competition.competitive_rules import (
    CompetitionRules, ObjectiveRule, FOOTBALL_SCORING, WINS_SCORING,
    NFL_SCORING, NHL_SCORING, HOCKEY_SCORING,
)


def create_competition_rules(
    reference_season: str,
    number_of_teams: int | None,
    total_regular_season_games: int | None,
    objectives: tuple[ObjectiveRule, ...] | list[ObjectiveRule] | None,
    *source_urls: str,
    ranking,
    scoring=FOOTBALL_SCORING,
    grouping: str = "league_wide",
    note: str = "",
) -> CompetitionRules:
    """Factory for canonical CompetitionRules with explicit parameter naming."""
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


_rules = create_competition_rules


def create_playoff_qualification_rule(
    start: int,
    end: int,
    target: str,
    path: str = "DIRECT",
    scope: str = "GLOBAL",
    group_size: int | None = None,
) -> ObjectiveRule:
    """Rule for internal league playoff or knockout stage qualification."""
    return ObjectiveRule(
        "STAGE_QUALIFICATION", target, path, ranking_scope=scope,
        rank_start=start, rank_end=end, reference_rank=end, group_size=group_size,
    )


def create_division_retention_rule(
    start: int,
    end: int,
    safe_rank: int,
    path: str = "DIRECT_RELEGATION",
) -> ObjectiveRule:
    """Rule for division retention, direct relegation, or relegation playoff."""
    return ObjectiveRule(
        "DIVISION_RETENTION", "CURRENT_DIVISION", path,
        rank_start=start, rank_end=end, reference_rank=safe_rank,
    )


def create_international_qualification_rule(
    start: int,
    end: int,
    target: str,
    path: str = "DIRECT",
    target_season: str = "2027/28",
    note: str = "Standard league table qualification boundary.",
) -> ObjectiveRule:
    """Confirmed regular season qualification zone for external/continental tournament."""
    return ObjectiveRule(
        "COMPETITION_QUALIFICATION", target, path, status="CONFIGURED",
        rank_start=start, rank_end=end, reference_rank=end, target_season=target_season,
        note=note,
    )


def create_pending_cup_winner_reallocation_rule(
    *targets: str,
    target_season: str,
    aggregation_scope: str = "TOURNAMENT",
    note: str = "Domestic cup winner and continental champion reallocation pending resolution.",
) -> tuple[ObjectiveRule, ...]:
    """Secondary/contingent international slots that shift down if domestic cup winners already qualified."""
    return tuple(ObjectiveRule(
        "COMPETITION_QUALIFICATION", target, "CONDITIONAL", status="PENDING",
        target_season=target_season, aggregation_scope=aggregation_scope, note=note,
    ) for target in targets)


# Aliases for backwards compatibility within this module
_qualification = create_playoff_qualification_rule
_retention = create_division_retention_rule
_external = create_pending_cup_winner_reallocation_rule


_UEFA_SOURCE = "https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/"
_UEFA_TARGETS = ("UEFA_CHAMPIONS_LEAGUE", "UEFA_EUROPA_LEAGUE", "UEFA_CONFERENCE_LEAGUE")
_LPF_SOURCE = "https://www.ligaprofesional.ar/wp-content/uploads/2026/01/Reglamento-Torneos-LPF-Primera-2026-1.pdf"
_MX_SOURCE = "https://www.rayados.com/es/noticias/21455/descubre-el-calendario-de-rayados-para-el-torneo-apertura-2026"
_CHL_SOURCE = "https://www.chl.hockey/en/about-us/chl-format"

def configure_european_football_profile(
    teams: int,
    games: int,
    first_relegated: int,
    source: str,
    *,
    ranking,
    ucl_end: int,
    uel_end: int,
    uecl_end: int | None = None,
    playoff: int | None = None,
    note: str = "",
) -> CompetitionRules:
    """Factory for European football profiles with explicit continental and relegation boundaries."""
    safe_rank = (playoff or first_relegated) - 1
    objectives = [
        create_international_qualification_rule(1, ucl_end, "UEFA_CHAMPIONS_LEAGUE"),
        create_international_qualification_rule(ucl_end + 1, uel_end, "UEFA_EUROPA_LEAGUE"),
    ]
    if uecl_end is not None and uecl_end >= uel_end + 1:
        objectives.append(
            create_international_qualification_rule(uel_end + 1, uecl_end, "UEFA_CONFERENCE_LEAGUE")
        )
    objectives.append(create_division_retention_rule(first_relegated, teams, safe_rank))
    if playoff:
        objectives.append(create_division_retention_rule(playoff, playoff, safe_rank, "RELEGATION_PLAYOFF"))
    return create_competition_rules("2026/27", teams, games, objectives, source, _UEFA_SOURCE, ranking=ranking, note=note)


_football_rules = configure_european_football_profile



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
    ), _LPF_SOURCE, ranking=standings.ARGENTINA_ZONES, grouping="argentina_zones",
        note="Two groups of 15; annual table has 32 games per team. Never use a group for relegation.")


# One current profile per canonical competitions.competition_id.
# reference_season documents these facts; it is not a lookup key.
COMPETITION_PROFILES: Mapping[int, CompetitionRules] = MappingProxyType({
    # laliga (1-4 Champions League, 5 Europa League, 6 Conference League; 18-20 Descenso directo)
    167: _football_rules(
        teams=20, games=38, first_relegated=18,
        ucl_end=4, uel_end=5, uecl_end=6,
        source="https://www.laliga.com/noticias/equipos-que-bajan-a-segunda",
        ranking=standings.LALIGA,
    ),
    # premier_league (1-4 Champions League, 5 Europa League; Conference otorgada a campeón de EFL Cup; 18-20 Descenso directo)
    168: _football_rules(
        teams=20, games=38, first_relegated=18,
        ucl_end=4, uel_end=5, uecl_end=None,
        source="https://www.premierleague.com/en/news/4657245/202526-premier-league-relegation-faq",
        ranking=standings.PREMIER_LEAGUE,
        note="League finish directly qualifies through 5th. EFL Cup winner takes UECL; FA Cup winner takes UEL.",
    ),
    # serie_a (1-4 Champions League, 5 Europa League, 6 Conference League; 18-20 Descenso directo)
    88: _football_rules(
        teams=20, games=38, first_relegated=18,
        ucl_end=4, uel_end=5, uecl_end=6,
        source="https://en.legaseriea.it/serie-a/news/180-minutes-left-in-fight-for-survival",
        ranking=standings.SERIE_A,
        note="Exceptional tied-position deciders are not a fixed playoff zone.",
    ),
    # bundesliga (1-4 Champions League, 5 Europa League, 6 Conference League; 16 Playoff, 17-18 Descenso directo)
    171: _football_rules(
        teams=18, games=34, first_relegated=17, playoff=16,
        ucl_end=4, uel_end=5, uecl_end=6,
        source="https://www.bundesliga.com/en/faq/what-are-the-rules-and-regulations-of-soccer/how-does-promotion-and-relegation-work-in-the-bundesliga-10645",
        ranking=standings.BUNDESLIGA,
    ),
    # ligue_1 (1-3 Champions directa, 4-5 Europa League, 6 Conference League; 16 Playoff, 17-18 Descenso directo)
    172: _football_rules(
        teams=18, games=34, first_relegated=17, playoff=16,
        ucl_end=3, uel_end=5, uecl_end=6,
        source="https://ligue1.com/en/articles/l1_article_3812-",
        ranking=standings.LIGUE_1,
    ),
    # liga_portugal (1-2 Champions League, 3 Europa League, 4-5 Conference League; 16 Playoff, 17-18 Descenso directo)
    261: _football_rules(
        teams=18, games=34, first_relegated=17, playoff=16,
        ucl_end=2, uel_end=3, uecl_end=5,
        source="https://www.ligaportugal.pt/pages/sobre-a-liga",
        ranking=standings.LIGA_PORTUGAL,
    ),
    # eredivisie (1-2 Champions League, 3 Europa League, 4 Conference League; 16 Playoff, 17-18 Descenso directo)
    317: _football_rules(
        teams=18, games=34, first_relegated=17, playoff=16,
        ucl_end=2, uel_end=3, uecl_end=4,
        source="https://www.knvb.nl/downloads/bestand/30062/definitiefcompetitieprogrammavriendenloterijeredivisie",
        ranking=standings.EREDIVISIE,
        note="Base league positions configured; domestic European playoffs remain separate.",
    ),
    # saudi_pro_league (1-3 AFC Champions League Elite, 4 AFC Champions League Two)
    50: create_competition_rules(
        "2026/27", 18, 34, (
            create_international_qualification_rule(1, 3, "AFC_CHAMPIONS_LEAGUE_ELITE", target_season="2027/28"),
            create_international_qualification_rule(4, 4, "AFC_CHAMPIONS_LEAGUE_TWO", target_season="2027/28"),
            create_division_retention_rule(16, 18, safe_rank=15),
        ), "https://www.saff.com.sa/news.php?id=4184", ranking=standings.SAUDI_PRO_LEAGUE,
    ),
    # brasileirao (1-4 Libertadores group, 5-6 Libertadores qual, 7-12 Sudamericana)
    318: create_competition_rules(
        "2026", 20, 38, (
            create_international_qualification_rule(1, 4, "CONMEBOL_LIBERTADORES", path="DIRECT", target_season="2027"),
            create_international_qualification_rule(5, 6, "CONMEBOL_LIBERTADORES", path="QUALIFYING_ROUND", target_season="2027"),
            create_international_qualification_rule(7, 12, "CONMEBOL_SUDAMERICANA", path="DIRECT", target_season="2027"),
            create_division_retention_rule(17, 20, safe_rank=16),
        ), "https://stcbfsiteprdimgbrs.blob.core.windows.net/img-site/cdn/REC_Brasileiro_Serie_A_2026_c984f8cf05.pdf",
        ranking=standings.BRASILEIRAO,
        note="Base league allocations configured. Copa do Brasil / continental winner reallocations pending dynamic resolution.",
    ),
    # liga_mx_apertura
    328: _rules("2026", 18, 17, (
        _qualification(1, 8, "QUARTERFINALS"),
        *_external("CONCACAF_CHAMPIONS_CUP", target_season="2027",
                   aggregation_scope="SEASON_AGGREGATE",
                   note="Requires season aggregate, finalists and other qualifying paths."),
    ), _MX_SOURCE, ranking=standings.LIGA_MX_APERTURA,
        note="Standings criteria reference Clausura 2026; Apertura 2026 confirmation remains pending."),
    # liga_mx_clausura
    2192: _rules("2026", 18, 17, (
        _qualification(1, 8, "QUARTERFINALS"),
        *_external("CONCACAF_CHAMPIONS_CUP", target_season="2027",
                   aggregation_scope="SEASON_AGGREGATE",
                   note="Requires season aggregate, finalists and other qualifying paths."),
    ), _MX_SOURCE, "https://fmf.mx/docs/reglamentos/483.pdf", ranking=standings.LIGA_MX_CLAUSURA,
        note="Reference format verified for Clausura 2026; update this current profile when the format changes."),
    # liga_profesional_de_argentina_apertura
    2476: _argentina_rules(),
    # liga_profesional_de_argentina_clausura
    310: _argentina_rules(),
    # nba
    176: _rules("2026/27", 30, 82, (
        _qualification(1, 6, "PLAYOFFS", scope="CONFERENCE", group_size=15),
        _qualification(7, 10, "PLAY_IN", "PLAY_IN", "CONFERENCE", 15),
    ), "https://www.nba.com/news/nba-play-in-tournament", "https://www.nba.com/news/2026-27-nba-regular-season-schedule",
        scoring=WINS_SCORING, ranking=standings.NBA, grouping="nba_conference"),
    # euroleague
    429: _rules("2026/27", 20, 38, (
        _qualification(1, 6, "PLAYOFFS"), _qualification(7, 10, "PLAY_IN", "PLAY_IN"),
    ), "https://mediacentre.euroleague.net/en/app/2/communication/communication/preview/24407", scoring=WINS_SCORING, ranking=standings.EUROLEAGUE),
    # mlb
    129: _rules("2026", 30, 162, (
        ObjectiveRule("STAGE_QUALIFICATION", "POSTSEASON", "DIRECT", ranking_scope="LEAGUE",
                      selection_method="DIVISION_WINNERS", places=3),
        ObjectiveRule("STAGE_QUALIFICATION", "POSTSEASON", "WILD_CARD", ranking_scope="LEAGUE",
                      selection_method="BEST_REMAINING", places=3),
    ), "https://www.mlb.com/news/mlb-playoff-format-faq", scoring=WINS_SCORING, ranking=standings.MLB, grouping="mlb_league"),
    # nfl
    5153: _rules("2026", 32, 17, (
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "DIRECT", ranking_scope="CONFERENCE",
                      selection_method="DIVISION_WINNERS", places=4),
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "WILD_CARD", ranking_scope="CONFERENCE",
                      selection_method="BEST_REMAINING", places=3),
    ), "https://operations.nfl.com/media/nppjkdp1/2023-record-and-fact-book.pdf",
        scoring=NFL_SCORING, ranking=standings.NFL, grouping="nfl_conference"),
    # shl
    180: _rules("2026/27", 14, 52, (
        _qualification(1, 6, "QUARTERFINALS"), _qualification(7, 10, "PRELIMINARY_ROUND", "QUALIFYING_PLAYOFF"),
        _retention(13, 14, 12, "RELEGATION_PLAYOFF"),
        *_external("CHAMPIONS_HOCKEY_LEAGUE", target_season="2027/28"),
    ), "https://www.swehockey.se/toppnyheter/het-premiaerloerdag-foer-shl/", _CHL_SOURCE, scoring=HOCKEY_SCORING, ranking=standings.SHL),
    # liiga
    525: _rules("2026/27", 17, 64, (
        _qualification(1, 12, "PLAYOFFS"), _retention(13, 16, 12, "RELEGATION_PLAYOFF"),
        _retention(17, 17, 12),
        *_external("CHAMPIONS_HOCKEY_LEAGUE", target_season="2027/28"),
    ), "https://hc.tps.fi/fi-fi/article/uutiset/liiga-tiedottaa-uusi-1410-mallinen-sarjamuoto-kaynnistyy-kaudella-202728/1573/", _CHL_SOURCE,
        scoring=HOCKEY_SCORING, ranking=standings.LIIGA, note="Transitional season; internal playoff entry paths not yet resolved."),
    # nhl
    5196: _rules("2026/27", 32, 84, (
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "DIRECT", ranking_scope="DIVISION",
                      rank_start=1, rank_end=3, reference_rank=3, group_size=8),
        ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "WILD_CARD", ranking_scope="CONFERENCE",
                      selection_method="BEST_REMAINING", places=2),
    ), "https://www.nhl.com/news/nhl-stats-pack-2026-27-regular-season-schedule",
       "https://www.nhl.com/info/standings-info/playoff-format", scoring=NHL_SCORING, ranking=standings.NHL, grouping="nhl_conference"),

    # mls
    76: _rules("2026", 30, 34, (
        _qualification(1, 7, "ROUND_ONE_BEST_OF_THREE", scope="CONFERENCE", group_size=15),
        _qualification(8, 9, "WILD_CARD_MATCH", "QUALIFYING_PLAYOFF", "CONFERENCE", 15),
        ObjectiveRule("COMPETITION_QUALIFICATION", "CONCACAF_CHAMPIONS_CUP", "CONDITIONAL",
                      status="PENDING", selection_method="MULTIPLE_QUALIFICATION_PATHS",
                      places=5, target_season="2027",
                      note="MLS Cup winner, Shield winner, opposite conference winner and next two eligible Shield clubs; resolve other qualifiers/reallocations."),
    ),
        "https://www.mlssoccer.com/league-reports/competition-guidelines/",
        "https://www.concacaf.com/competitions/champions-cup/news/2027-concacaf-champions-cup-qualified-clubs",
        "https://www.mlssoccer.com/2027mlssprintseason/faq",
        ranking=standings.MLS, grouping="mls_conference",
        note="2026 format: no relegation. Cancelled games may require official PPG ruling. Update current profile before 2027 Sprint (14 games, top eight per conference); 2028 Leagues Cup is decided by Sprint, not this table."),

    # ahl    
    554: _rules("2026/27", 32, 72, tuple(
        ObjectiveRule("STAGE_QUALIFICATION", "CALDER_CUP_PLAYOFFS", "DIRECT",
                      ranking_scope="DIVISION", ranking_group=division,
                      rank_start=1, rank_end=places, reference_rank=places, group_size=size)
        for division, size, places in (
            ("Atlantic", 7, 5), ("North", 8, 6), ("Central", 7, 5), ("Pacific", 10, 7),
        )
    ),
        "https://theahl.com/news/ahl-unveils-2026-27-schedule", ranking=standings.AHL,
        # The generic standings/qualification pages still show the previous allocation.
        scoring=NHL_SCORING, grouping="ahl_division",
        note="No relegation or table-based international qualification. Division quotas verified by 2026/27 season-opening announcement; no NHL wildcard substitution. Internal round byes are outside this objective."),
    # Existing history consumers: missing facts remain explicit, never guessed.

    # wnba 
    145: CompetitionRules("", 15, 40, WINS_SCORING, standings.WNBA, None,
                          ("repository:previous-competition-configuration",)),

    # cba chinese basketball association                     
    174: create_competition_rules(
        "2026/27", 20, 46, (
            create_division_retention_rule(19, 20, safe_rank=18, path="RELEGATION_PLAYOFF"),
            ObjectiveRule("STAGE_QUALIFICATION", "QUARTERFINALS", "DIRECT",
                          status="PENDING", rank_start=1, rank_end=4, reference_rank=4,
                          note="Previous primary structure; 2026/27 continuity unconfirmed."),
            ObjectiveRule("STAGE_QUALIFICATION", "PLAYOFFS", "FIRST_ROUND",
                          status="PENDING", rank_start=5, rank_end=12, reference_rank=12,
                          note="Previous primary structure; 2026/27 continuity unconfirmed."),
            ObjectiveRule("COMPETITION_QUALIFICATION", "FIBA_INTERCONTINENTAL_CUP", "CONDITIONAL",
                          status="PENDING", selection_method="CHAMPION", places=1, target_season="2027",
                          note="Champion path only; not a regular season table finish."),
        ),
        "https://www.sport.gov.cn/n20001280/n20067662/n20067613/c30106244/content.html",
        "https://cbanetcdn.cba.net.cn/u/cms/www/202310/14124151vnl4.pdf",
        "https://www.fiba.basketball/en/news/meet-the-team-shanghai-sharks",
        ranking=standings.CBA, scoring=WINS_SCORING,
    ),

    # pfl philippines football league                      
    5151: CompetitionRules("", None, None, FOOTBALL_SCORING, standings.PFL, None,
                           ("repository:previous-competition-configuration",)),
})


def get_competition_profile(competition_id: int | None) -> CompetitionRules | None:
    """Return the current profile by canonical ID, independent of provider or season labels."""
    return COMPETITION_PROFILES.get(competition_id)
