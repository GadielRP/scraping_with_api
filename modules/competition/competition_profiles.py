"""Current sporting profiles keyed by canonical competition ID.

This catalogue owns current format, scoring and objectives. Reference seasons
document the current configuration; events are not filtered by season labels.
Tracking and historical collection routing remain independent.
Vocabulary is canonical; sporting boundaries remain unchanged pending review.
"""

from types import MappingProxyType
from typing import Mapping

from modules.competition import competition_ids, standings_policies
from modules.competition.competitive_rules import (
    CompetitionRules, DisciplineRules, ObjectiveRule, FOOTBALL_SCORING, WINS_SCORING,
    NFL_SCORING, NHL_SCORING, HOCKEY_SCORING,
)
from modules.competition.rule_terms import (
    AggregationScope,
    CompetitionStage,
    GroupingMethod,
    ObjectiveType,
    QualificationPath,
    RankingScope,
    RuleStatus,
    SelectionMethod,
)


def create_stage_qualification_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    destination_stage: CompetitionStage,
    path: QualificationPath | None = QualificationPath.DIRECT,
    scope: RankingScope = RankingScope.GLOBAL,
    status: RuleStatus = RuleStatus.CONFIGURED,
    group_size: int | None = None,
    ranking_group: str | None = None,
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE,
    places: int | None = None,
    note: str = "",
) -> ObjectiveRule:
    """Rule for stage qualification (knockout stage, playoffs, wild card, or group advance)."""
    return ObjectiveRule(
        objective_type=ObjectiveType.STAGE_QUALIFICATION,
        destination_stage=destination_stage,
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
    path: QualificationPath | None = QualificationPath.DIRECT_RELEGATION,
    status: RuleStatus = RuleStatus.CONFIGURED,
    aggregation_scope: AggregationScope = AggregationScope.TOURNAMENT,
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE,
    places: int | None = None,
    note: str = "",
) -> ObjectiveRule:
    """Rule for division retention, direct relegation, or relegation playoff."""
    return ObjectiveRule(
        objective_type=ObjectiveType.DIVISION_RETENTION,
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


def create_competition_qualification_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    destination_competition_id: int | None,
    destination_competition_name: str,
    path: QualificationPath | None = QualificationPath.DIRECT,
    status: RuleStatus = RuleStatus.CONFIGURED,
    aggregation_scope: AggregationScope = AggregationScope.TOURNAMENT,
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE,
    places: int | None = None,
    note: str = "Standard league table qualification boundary.",
) -> ObjectiveRule:
    """Configured or pending qualification to another competition or its qualifying record."""
    return ObjectiveRule(
        objective_type=ObjectiveType.COMPETITION_QUALIFICATION,
        destination_competition_id=destination_competition_id,
        destination_competition_name=destination_competition_name,
        qualification_path=path,
        status=status,
        rank_start=start,
        rank_end=end,
        reference_rank=end,
        aggregation_scope=aggregation_scope,
        selection_method=selection_method,
        places=places,
        note=note,
    )


# One current profile per canonical competitions.competition_id.
# reference_season documents these facts; it is not a lookup key.
COMPETITION_PROFILES: Mapping[int, CompetitionRules] = MappingProxyType({
    # laliga (1-4 Champions League, 5 Europa League, 6 Conference League; 18-20 Descenso directo, seguro 17)
    competition_ids.LALIGA: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=4,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=18, end=20,
                safe_rank=17,
            ),
        ),
        standings=standings_policies.LALIGA,
    ),

    # premier_league (1-4 Champions League, 5 Europa League; Conference otorgada a campeón de EFL Cup; 18-20 Descenso directo)
    # PENDING IMPLEMENTATION TO SUPPORT EFL CUP WINNER RELOCATION TO CONFERENCE LEAGUE
    competition_ids.PREMIER_LEAGUE: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=4,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_division_retention_rule(
                start=18, end=20,
                safe_rank=17,
            ),
        ),
        standings=standings_policies.PREMIER_LEAGUE,
        note="League finish directly qualifies through 5th. EFL Cup winner takes UECL; FA Cup winner takes UEL.",
    ),

    # serie_a (1-4 Champions League, 5 Europa League, 6 Conference League; 18-20 Descenso directo, seguro 17)
    competition_ids.SERIE_A: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=4,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=18, end=20,
                safe_rank=17,
            ),
        ),
        standings=standings_policies.SERIE_A,
        note="Exceptional tied-position deciders are not a fixed playoff zone.",
    ),

    # bundesliga (1-4 Champions League, 5 Europa League, 6 Conference League; 16 Playoff, 17-18 Descenso directo)
    competition_ids.BUNDESLIGA: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=4,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                path=QualificationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.BUNDESLIGA,
    ),

    # ligue_1 (1-3 Champions directa, 4-5 Europa League, 6 Conference League; 16 Playoff, 17-18 Descenso directo)
    competition_ids.LIGUE_1: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=3,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=4,
                end=4,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Champions League, Qualification',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                path=QualificationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.LIGUE_1,
    ),

    # liga portugal (1-2 Champions League, 3 Europa League, 4-5 Conference League; 16 Playoff, 17-18 Descenso directo)
    competition_ids.LIGA_PORTUGAL: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=2,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=3,
                end=3,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Champions League, Qualification',
            ),
            create_competition_qualification_rule(
                start=4,
                end=4,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_EUROPA_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Europa League, Qualification',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                path=QualificationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.LIGA_PORTUGAL,
    ),

    # eredivisie (1 Champions League, 1 Champions league qualification,  3 Europa League Qualification, 4 Conference League; 16 Playoff, 17-18 Descenso directo)
    competition_ids.EREDIVISIE: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=1,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=2,
                end=2,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_CHAMPIONS_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Champions League, Qualification',
            ),
            create_competition_qualification_rule(
                start=3,
                end=3,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.UEFA_EUROPA_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Europa League, Qualification',
            ),
            create_competition_qualification_rule(
                start=4,
                end=7,
                path=QualificationPath.QUALIFYING_PLAYOFF,
                note="Playoff winner qualifies for UEFA Conference League. Not sure though if this is the right way to do it.",
                destination_competition_id=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                path=QualificationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.EREDIVISIE,
        note="Base league positions configured; domestic European playoffs remain separate.",
    ),

    # saudi_pro_league (1-3 AFC Champions League Elite, 4 AFC Champions League Two; 16-18 Descenso directo)
    competition_ids.SAUDI_PRO_LEAGUE: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=5,
                path=QualificationPath.DIRECT,
                destination_competition_id=competition_ids.AFC_CHAMPIONS_LEAGUE_ELITE,
                destination_competition_name='AFC Champions League Elite',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.DIRECT,
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='AFC Champions League Two',
                status=RuleStatus.PENDING,
                note='AFC CHAMPIONS LEAGUE TWO: general competition_id not present in catalogue; destination pending. ',
            ),
            create_division_retention_rule(
                start=16, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.SAUDI_PRO_LEAGUE,
    ),

    # brasileirao (1-4 Libertadores grupos, 5-6 Libertadores previa, 7-12 Sudamericana; 17-20 Descenso directo)
    competition_ids.BRASILEIRAO: CompetitionRules(
        reference_season="2026",
        number_of_teams=20,
        total_regular_season_games=38,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=4,
                path=QualificationPath.DIRECT,
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='CONMEBOL Libertadores',
                status=RuleStatus.PENDING,
                note='CONMEBOL LIBERTADORES: general competition_id not present in catalogue; destination pending. ',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.QUALIFYING_ROUND,
                destination_competition_id=competition_ids.CONMEBOL_LIBERTADORES_QUALIFICATION,
                destination_competition_name='CONMEBOL Libertadores, Qualification',
            ),
            create_competition_qualification_rule(
                start=6,
                end=12,
                path=QualificationPath.DIRECT,
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='CONMEBOL Sudamericana',
                status=RuleStatus.PENDING,
                note='CONMEBOL SUDAMERICANA: general competition_id not present in catalogue; destination pending. ',
            ),
            create_division_retention_rule(
                start=17, end=20,
                safe_rank=16,
            ),
        ),
        standings=standings_policies.BRASILEIRAO,
        note="Base league allocations configured. Copa do Brasil / continental winner reallocations pending dynamic resolution.",
    ),

    # liga_mx_apertura (1-8 a cuartos de final de Liguilla; sin descenso; Concacaf pendiente por acumulado anual)
    competition_ids.LIGA_MX_APERTURA: CompetitionRules(
        reference_season="2026",
        number_of_teams=18,
        total_regular_season_games=17,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=8,
                path=QualificationPath.DIRECT,
                destination_stage=CompetitionStage.PLAYOFFS,
                note='Entry round: quarterfinals. ',
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note="Requires season aggregate, finalists and other qualifying paths.",
                destination_competition_id=competition_ids.CONCACAF_CHAMPIONS_CUP,
                destination_competition_name='CONCACAF Champions Cup',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.LIGA_MX_APERTURA,
        note="Standings criteria reference Clausura 2026; Apertura 2026 confirmation remains pending.",
    ),

    # liga_mx_clausura (1-8 a cuartos de final de Liguilla; sin descenso; Concacaf pendiente por acumulado anual)
    competition_ids.LIGA_MX_CLAUSURA: CompetitionRules(
        reference_season="2026",
        number_of_teams=18,
        total_regular_season_games=17,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=8,
                path=QualificationPath.DIRECT,
                destination_stage=CompetitionStage.PLAYOFFS,
                note='Entry round: quarterfinals. ',
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note="Requires season aggregate, finalists and other qualifying paths.",
                destination_competition_id=competition_ids.CONCACAF_CHAMPIONS_CUP,
                destination_competition_name='CONCACAF Champions Cup',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.LIGA_MX_CLAUSURA,
        note="Clausura 2026 configuration; sporting review pending.",
    ),

    # liga_profesional_de_argentina_apertura (Dos grupos de 15; 1-8 de cada grupo a octavos; descenso anual y promedios)
    competition_ids.ARGENTINA_APERTURA: CompetitionRules(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=16,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=8,
                scope=RankingScope.GROUP,
                group_size=15,
                destination_stage=CompetitionStage.PLAYOFFS,
                note='Entry round: round of 16. ',
            ),
            create_division_retention_rule(
                start=30, end=30,
                safe_rank=29,
                path=QualificationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                status=RuleStatus.PENDING,
                note="Worst annual team excluding the club relegated by rolling average.",
            ),
            create_division_retention_rule(
                path=QualificationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.MULTISEASON_RECORD,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.LOWEST_POINTS_AVERAGE,
                places=1,
                note="Worst points-per-game average; requires three seasons and exclusions.",
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note='CONMEBOL LIBERTADORES: general competition_id not present in catalogue; destination pending. Annual table, tournament champions and exclusions determine eligible qualifiers.',
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='CONMEBOL Libertadores',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note='CONMEBOL SUDAMERICANA: general competition_id not present in catalogue; destination pending. Annual table, tournament champions and exclusions determine eligible qualifiers.',
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='CONMEBOL Sudamericana',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.ARGENTINA_ZONES,
        grouping_method=GroupingMethod.ARGENTINA_ZONES,
        note="Two groups of 15; annual table has 32 games per team. Never use a group for relegation.",
    ),

    # liga_profesional_de_argentina_clausura
    competition_ids.ARGENTINA_CLAUSURA: CompetitionRules(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=16,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=8,
                scope=RankingScope.GROUP,
                group_size=15,
                destination_stage=CompetitionStage.PLAYOFFS,
                note='Entry round: round of 16. ',
            ),
            create_division_retention_rule(
                start=30, end=30,
                safe_rank=29,
                path=QualificationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                status=RuleStatus.PENDING,
                note="Worst annual team excluding the club relegated by rolling average.",
            ),
            create_division_retention_rule(
                path=QualificationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.MULTISEASON_RECORD,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.LOWEST_POINTS_AVERAGE,
                places=1,
                note="Worst points-per-game average; requires three seasons and exclusions.",
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note='CONMEBOL LIBERTADORES: general competition_id not present in catalogue; destination pending. Annual table, tournament champions and exclusions determine eligible qualifiers.',
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='CONMEBOL Libertadores',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note='CONMEBOL SUDAMERICANA: general competition_id not present in catalogue; destination pending. Annual table, tournament champions and exclusions determine eligible qualifiers.',
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='CONMEBOL Sudamericana',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.ARGENTINA_ZONES,
        grouping_method=GroupingMethod.ARGENTINA_ZONES,
        note="Two groups of 15; annual table has 32 games per team. Never use a group for relegation.",
    ),

    # nba (Conferencia: 1-6 Playoffs, 7-10 Play-In; sin descenso)
    competition_ids.NBA: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=30,
        total_regular_season_games=82,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=6,
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_stage_qualification_rule(
                start=7,
                end=10,
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                destination_stage=CompetitionStage.PLAY_IN,
            ),
        ),
        standings=standings_policies.NBA,
        scoring=WINS_SCORING,
        grouping_method=GroupingMethod.NBA_CONFERENCE,
        discipline=DisciplineRules(personal_foul_limit=6),
    ),

    # euroleague (1-6 Playoffs, 7-10 Play-In; sin descenso)
    competition_ids.EUROLEAGUE: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=38,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=6,
                path=QualificationPath.DIRECT,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_stage_qualification_rule(
                start=7,
                end=10,
                path=QualificationPath.DIRECT,
                destination_stage=CompetitionStage.PLAY_IN,
            ),
        ),
        standings=standings_policies.EUROLEAGUE,
        scoring=WINS_SCORING,
    ),

    # mlb (Por liga: 3 campeones de división, 3 Wild Cards)
    competition_ids.MLB: CompetitionRules(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=162,
        objectives=(
            create_stage_qualification_rule(
                path=QualificationPath.DIRECT,
                scope=RankingScope.LEAGUE,
                selection_method=SelectionMethod.DIVISION_WINNERS,
                places=3,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_stage_qualification_rule(
                path=QualificationPath.DIRECT,
                note="Wild Card places selected from the best remaining eligible teams.",
                scope=RankingScope.LEAGUE,
                selection_method=SelectionMethod.BEST_REMAINING,
                places=3,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
        ),
        standings=standings_policies.MLB,
        scoring=WINS_SCORING,
        grouping_method=GroupingMethod.MLB_LEAGUE,
    ),

    # nfl (Por conferencia: 4 campeones de división, 3 Wild Cards)
    competition_ids.NFL: CompetitionRules(
        reference_season="2026",
        number_of_teams=32,
        total_regular_season_games=17,
        objectives=(
            create_stage_qualification_rule(
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                selection_method=SelectionMethod.DIVISION_WINNERS,
                places=4,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_stage_qualification_rule(
                path=QualificationPath.DIRECT,
                note="Wild Card places selected from the best remaining eligible teams.",
                scope=RankingScope.CONFERENCE,
                selection_method=SelectionMethod.BEST_REMAINING,
                places=3,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
        ),
        standings=standings_policies.NFL,
        scoring=NFL_SCORING,
        grouping_method=GroupingMethod.NFL_CONFERENCE,
    ),

    # shl (1-6 Cuartos, 7-10 Ronda previa; 13-14 Playoff de permanencia)
    competition_ids.SHL: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=14,
        total_regular_season_games=52,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=6,
                path=QualificationPath.DIRECT,
                destination_stage=CompetitionStage.PLAYOFFS,
                note='Entry round: quarterfinals. ',
            ),
            create_stage_qualification_rule(
                start=7,
                end=10,
                path=QualificationPath.DIRECT,
                destination_stage=CompetitionStage.PLAYOFFS,
                note='Entry round: preliminary playoff round. ',
            ),
            create_division_retention_rule(
                start=13, end=14,
                safe_rank=12,
                path=QualificationPath.RELEGATION_PLAYOFF,
            ),
            create_competition_qualification_rule(
                destination_competition_id=competition_ids.CHAMPIONS_HOCKEY_LEAGUE,
                destination_competition_name='Champions Hockey League',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.SHL,
        scoring=HOCKEY_SCORING,
    ),

    # liiga (1-12 Playoffs; 13-16 Playoff permanencia, 17 Descenso directo)
    competition_ids.LIIGA: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=17,
        total_regular_season_games=64,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=12,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_division_retention_rule(
                start=13, end=16,
                safe_rank=12,
                path=QualificationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=17,
                safe_rank=12,
            ),
            create_competition_qualification_rule(
                destination_competition_id=competition_ids.CHAMPIONS_HOCKEY_LEAGUE,
                destination_competition_name='Champions Hockey League',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.LIIGA,
        scoring=HOCKEY_SCORING,
        note="Transitional season; internal playoff entry paths not yet resolved.",
    ),

    # nhl (División: 1-3 Playoffs directos; Conferencia: 2 Wild Cards)
    competition_ids.NHL: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=32,
        total_regular_season_games=84,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=3,
                path=QualificationPath.DIRECT,
                scope=RankingScope.DIVISION,
                group_size=8,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_stage_qualification_rule(
                path=QualificationPath.DIRECT,
                note="Wild Card places selected from the best remaining eligible teams.",
                scope=RankingScope.CONFERENCE,
                selection_method=SelectionMethod.BEST_REMAINING,
                places=2,
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
        ),
        standings=standings_policies.NHL,
        scoring=NHL_SCORING,
        grouping_method=GroupingMethod.NHL_CONFERENCE,
    ),

    # mls (Conferencia: 1-7 Round One Best-of-3, 8-9 Wild Card match; sin descenso)
    competition_ids.MLS: CompetitionRules(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=34,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=7,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                note='Entry round: first round. MLS Round One: best-of-three series.',
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_stage_qualification_rule(
                start=8,
                end=9,
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                note="MLS Wild Card match: single-game entry to Round One.",
                destination_stage=CompetitionStage.PLAY_IN,
            ),
            create_competition_qualification_rule(
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
                places=5,
                note="MLS Cup winner, Shield winner, opposite conference winner and next two eligible Shield clubs; resolve other qualifiers/reallocations.",
                destination_competition_id=competition_ids.CONCACAF_CHAMPIONS_CUP,
                destination_competition_name='CONCACAF Champions Cup',
            ),
        ),
        standings=standings_policies.MLS,
        grouping_method=GroupingMethod.MLS_CONFERENCE,
        note="2026 format: no relegation. Cancelled games may require official PPG ruling. Update current profile before 2027 Sprint (14 games, top eight per conference); 2028 Leagues Cup is decided by Sprint, not this table.",
    ),

    # ahl (Calder Cup Playoffs según cuotas por división; sin descenso)
    competition_ids.AHL: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=32,
        total_regular_season_games=72,
        objectives=tuple(
            create_stage_qualification_rule(
                start=1,
                end=places,
                path=QualificationPath.DIRECT,
                note="Calder Cup Playoffs.",
                scope=RankingScope.DIVISION,
                ranking_group=division,
                group_size=size,
                destination_stage=CompetitionStage.PLAYOFFS,
            )
            for division, size, places in (
                ("Atlantic", 7, 5), ("North", 8, 6), ("Central", 7, 5), ("Pacific", 10, 7),
            )
        ),
        standings=standings_policies.AHL,
        scoring=NHL_SCORING,
        grouping_method=GroupingMethod.AHL_DIVISION,
        note="No relegation or table-based international qualification. Division-specific quotas; no NHL wildcard substitution. Internal round byes are outside this objective.",
    ),

    # wnba (15 equipos, 40 partidos; sin objetivos configurados)
    competition_ids.WNBA: CompetitionRules(
        reference_season="",
        number_of_teams=15,
        total_regular_season_games=40,
        objectives=(
            create_stage_qualification_rule(
                start=1,
                end=8,
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                note="Playoffs first round",
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
        ),
        standings=standings_policies.WNBA,
        scoring=WINS_SCORING,
    ),

    # cba chinese basketball association (19-20 Playoff de permanencia)
    competition_ids.CBA: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=20,
        total_regular_season_games=46,
        objectives=(
            create_division_retention_rule(
                start=19, end=20,
                safe_rank=18,
                path=QualificationPath.RELEGATION_PLAYOFF,
            ),
            create_stage_qualification_rule(
                start=1,
                end=4,
                path=QualificationPath.DIRECT,
                status=RuleStatus.PENDING,
                note='Entry round: quarterfinals. Previous primary structure; 2026/27 continuity unconfirmed.',
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_stage_qualification_rule(
                start=5,
                end=12,
                path=QualificationPath.DIRECT,
                status=RuleStatus.PENDING,
                note='Entry round: first round. Previous primary structure; 2026/27 continuity unconfirmed.',
                destination_stage=CompetitionStage.PLAYOFFS,
            ),
            create_competition_qualification_rule(
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.CHAMPION,
                places=1,
                note='FIBA INTERCONTINENTAL CUP: general competition_id not present in catalogue; destination pending. Champion path only; not a regular season table finish.',
                # General record missing; do not substitute a group/knockout record.
                destination_competition_id=None,
                destination_competition_name='FIBA Intercontinental Cup',
            ),
        ),
        standings=standings_policies.CBA,
        scoring=WINS_SCORING,
    ),

    # pfl philippines football league (histórico sin reglas configuradas)
    competition_ids.PFL: CompetitionRules(
        reference_season="",
        number_of_teams=None,
        total_regular_season_games=None,
        objectives=None,
        standings=standings_policies.PFL,
        scoring=FOOTBALL_SCORING,
    ),
})


def get_competition_profile(competition_id: int | None) -> CompetitionRules | None:
    """Return the current profile by canonical ID, independent of provider or season labels."""
    return COMPETITION_PROFILES.get(competition_id)
