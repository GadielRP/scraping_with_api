"""Current sporting profiles keyed by provider competition identity.

This catalogue owns current format, scoring and objectives. Reference seasons
document the current configuration; events are not filtered by season labels.
Tracking and historical collection routing remain independent.
Sporting boundaries distinguish confirmed rules from pending allocations.
"""

from types import MappingProxyType
from typing import Mapping

from modules.competition import competition_ids, standings_policies
from .competition_identity import CompetitionIdentity
from .qualification_destination import QualificationDestination
from .qualification_reallocation import CupReallocationCase, CupReallocationPolicy
from .season_aggregate import SeasonAggregateProfile, SeasonAggregateSource
from modules.competition.competitive_rules import (
    CompetitionRules, CupQualificationRule, DisciplineRules, ObjectiveRule, RankSelection, FOOTBALL_SCORING, WINS_SCORING,
    NFL_SCORING, NHL_SCORING, HOCKEY_SCORING,
)
from modules.competition.rule_terms import (
    AggregationScope,
    DestinationPhase,
    DestinationRound,
    GroupingMethod,
    ObjectiveType,
    QualificationPath,
    RelegationPath,
    RankingScope,
    RuleStatus,
    SelectionMethod,
)

def create_phase_qualification_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    destination_phase: DestinationPhase,
    destination_round: DestinationRound | None = None,
    destination_competition_identity: CompetitionIdentity | None = None,
    destination_competition_name: str | None = None,
    path: QualificationPath | None = QualificationPath.DIRECT,
    scope: RankingScope = RankingScope.GLOBAL,
    status: RuleStatus = RuleStatus.CONFIGURED,
    group_size: int | None = None,
    ranking_group: str | None = None,
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE,
    places: int | None = None,
    note: str = "",
) -> ObjectiveRule:
    """Rule for qualification into a phase of the current competition."""
    return ObjectiveRule(
        objective_type=ObjectiveType.PHASE_QUALIFICATION,
        qualification_destination=QualificationDestination(
            destination_competition_identity=destination_competition_identity,
            destination_competition_name=destination_competition_name,
            destination_phase=destination_phase,
            destination_round=destination_round,
            qualification_path=path,
        ),
        rank_selection=RankSelection(
            rank_start=start, rank_end=end, reference_rank=end,
            selection_method=selection_method, ranking_scope=scope,
            places=places, group_size=group_size, ranking_group=ranking_group,
        ),
        status=status,
        note=note,
    )


def create_division_retention_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    safe_rank: int | None = None,
    relegation_path: RelegationPath | None = RelegationPath.DIRECT_RELEGATION,
    status: RuleStatus = RuleStatus.CONFIGURED,
    aggregation_scope: AggregationScope = AggregationScope.TOURNAMENT,
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE,
    places: int | None = None,
    note: str = "",
) -> ObjectiveRule:
    """Rule for division retention, direct relegation, or relegation playoff."""
    return ObjectiveRule(
        objective_type=ObjectiveType.DIVISION_RETENTION,
        rank_selection=RankSelection(
            rank_start=start, rank_end=end, reference_rank=safe_rank,
            selection_method=selection_method, aggregation_scope=aggregation_scope,
            places=places,
        ),
        relegation_path=relegation_path,
        status=status,
        note=note,
    )


def create_competition_qualification_rule(
    start: int | None = None,
    end: int | None = None,
    *,
    destination_competition_identity: CompetitionIdentity | None,
    destination_competition_name: str,
    destination_phase: DestinationPhase | None,
    destination_round: DestinationRound | None = None,
    path: QualificationPath | None = QualificationPath.DIRECT,
    status: RuleStatus = RuleStatus.CONFIGURED,
    aggregation_scope: AggregationScope = AggregationScope.TOURNAMENT,
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE,
    places: int | None = None,
    note: str = "Standard league table qualification boundary.",
) -> ObjectiveRule:
    """Configured or pending qualification to another competition and its phase."""
    return ObjectiveRule(
        objective_type=ObjectiveType.COMPETITION_QUALIFICATION,
        qualification_destination=QualificationDestination(
            destination_competition_identity=destination_competition_identity,
            destination_competition_name=destination_competition_name,
            destination_phase=destination_phase,
            destination_round=destination_round,
            qualification_path=path,
        ),
        rank_selection=RankSelection(
            rank_start=start, rank_end=end, reference_rank=end,
            selection_method=selection_method, aggregation_scope=aggregation_scope,
            places=places,
        ),
        status=status,
        note=note,
    )


# One current profile per provider tournament identity.
# reference_season documents these facts; it is not a lookup key.
COMPETITION_PROFILES: Mapping[CompetitionIdentity, CompetitionRules] = MappingProxyType({
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
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_competition_identity=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=18, end=20,
                safe_rank=17,
            ),
        ),
        standings=standings_policies.LALIGA,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.COPA_DEL_REY,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.COPA_DEL_REY],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(reallocate_if_champion_already_qualified=True),
                destination_season_offset=1,
                sources=(
                    "https://rfef.es/sites/default/files/2026-06/1._CNL_Primera_y_Segunda_Division.pdf#page=5",
                    "https://rfef.es/sites/default/files/2026-07/5._Torneos_Federacion.pdf#page=12",
                    "https://rankinguefa.pl/wp-content/uploads/2026/09/20260909_Circular_2026_54_en_enclosure_2_2027-28-access-list.pdf",
                    "https://documents.uefa.com/r/Regulations-of-the-UEFA-Europa-League-2026/27/Article-3-Entries-for-the-competition-Online",
                ),
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                    destination_competition_name='UEFA Europa League',
                    destination_phase=DestinationPhase.LEAGUE_PHASE,
                    qualification_path=QualificationPath.DIRECT,
                ),
            ),
        ),
    ),

    # premier league (1-4 Champions League, 5 Europa League; Conference otorgada a campeón de EFL Cup; 18-20 Descenso directo)
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
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_division_retention_rule(
                start=18, end=20,
                safe_rank=17,
            ),
        ),
        standings=standings_policies.PREMIER_LEAGUE,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.FA_CUP,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.FA_CUP],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(reallocate_if_champion_already_qualified=True),
                destination_season_offset=1,
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                    destination_competition_name='UEFA Europa League',
                    destination_phase=DestinationPhase.LEAGUE_PHASE,
                    qualification_path=QualificationPath.DIRECT,
                ),
            ),
            CupQualificationRule(
                competition_identity=competition_ids.EFL_CUP,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.EFL_CUP],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(reallocate_if_champion_already_qualified=True),
                destination_season_offset=1,
                sources=(
                    "https://www.premierleague.com/en/news/373663/european-qualification-for-uefa-competitions-explained",
                ),
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                    destination_competition_name='UEFA Conference League, Qualification',
                    destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                    destination_round=DestinationRound.QUALIFYING_PLAYOFF,
                    qualification_path=QualificationPath.DIRECT,
                ),
            )
        ),
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
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_competition_identity=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=18, end=20,
                safe_rank=17,
            ),
        ),
        standings=standings_policies.SERIE_A,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.COPPA_ITALIA,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.COPPA_ITALIA],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(reallocate_if_champion_already_qualified=True),
                destination_season_offset=1,
                sources=(
                    "https://documents.uefa.com/r/Regulations-of-the-UEFA-Europa-League-2026/27/Article-3-Entries-for-the-competition-Online",
                ),
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                    destination_competition_name='UEFA Europa League',
                    destination_phase=DestinationPhase.LEAGUE_PHASE,
                    qualification_path=QualificationPath.DIRECT,
                ),
            ),
        ),
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
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_competition_identity=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                relegation_path=RelegationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.BUNDESLIGA,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.DFB_POKAL,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.DFB_POKAL],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(reallocate_if_champion_already_qualified=True),
                destination_season_offset=1,
                sources=(
                    "https://www.bundesliga.com/en/bundesliga/news/germany-nine-teams-qualify-for-europe-2026-27-bayern-munich-freiburg-36936",
                    "https://documents.uefa.com/r/Regulations-of-the-UEFA-Europa-League-2026/27/Article-3-Entries-for-the-competition-Online",
                ),
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                    destination_competition_name='UEFA Europa League',
                    destination_phase=DestinationPhase.LEAGUE_PHASE,
                    qualification_path=QualificationPath.DIRECT,
                ),
            ),
        ),
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
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=4,
                end=4,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Champions League, Qualification',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                destination_competition_name='UEFA Europa League',
            ),
            create_competition_qualification_rule(
                start=6,
                end=6,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_competition_identity=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                relegation_path=RelegationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.LIGUE_1,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.COUPE_DE_FRANCE,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.COUPE_DE_FRANCE],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(reallocate_if_champion_already_qualified=True),
                destination_season_offset=1,
                sources=(
                    "https://media.fff.fr/uploads/documents/comex-20260217-annexe-5.-nombre-de-clubs-qualifies-par-ligue-au-terme-du-6e-tour-de-la-cdf-credit-agricole-26-27.pdf",
                    "https://documents.uefa.com/r/Regulations-of-the-UEFA-Europa-League-2026/27/Article-3-Entries-for-the-competition-Online",
                ),
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                    destination_competition_name='UEFA Europa League',
                    destination_phase=DestinationPhase.LEAGUE_PHASE,
                    qualification_path=QualificationPath.DIRECT,
                ),
            ),
        ),
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
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=3,
                end=3,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_round=DestinationRound.THIRD_QUALIFYING_ROUND,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Champions League, Qualification',
            ),
            create_competition_qualification_rule(
                start=4,
                end=4,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_round=DestinationRound.SECOND_QUALIFYING_ROUND,
                destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Europa League, Qualification',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_round=DestinationRound.SECOND_QUALIFYING_ROUND,
                destination_competition_identity=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                relegation_path=RelegationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.LIGA_PORTUGAL,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.TACA_DE_PORTUGAL,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.TACA_DE_PORTUGAL],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(
                    cases=(
                        # The top three keep their Champions League berths. Fourth takes
                        # the cup berth, so fifth and sixth take the two qualifying berths.
                        CupReallocationCase(
                            champion_rank_start=1,
                            champion_rank_end=3,
                            recipient_rank=4,
                        ),
                        # A champion in fourth or fifth takes the cup berth and
                        # frees its league berth for the next eligible club.
                        CupReallocationCase(champion_rank_start=4, champion_rank_end=5),
                    ),
                ),
                destination_season_offset=1,
                sources=(
                    "https://www.ligaportugal.pt/competition/854/liga-portugal-betclic/round/20262027?tab=standings",
                    "https://documents.uefa.com/r/Regulations-of-the-UEFA-Europa-League-2026/27/Article-3-Entries-for-the-competition-Online",
                ),
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                    destination_competition_name='UEFA Europa League',
                    destination_phase=DestinationPhase.LEAGUE_PHASE,
                    qualification_path=QualificationPath.DIRECT,
                ),
            ),
        ),
    ),

    # Eredivisie 2026/27. The domestic playoff decides one Conference League berth.
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
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE,
                destination_competition_name='UEFA Champions League',
            ),
            create_competition_qualification_rule(
                start=2,
                end=2,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_round=DestinationRound.THIRD_QUALIFYING_ROUND,
                destination_competition_identity=competition_ids.UEFA_CHAMPIONS_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Champions League, Qualification',
            ),
            create_competition_qualification_rule(
                start=3,
                end=3,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_round=DestinationRound.SECOND_QUALIFYING_ROUND,
                destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Europa League, Qualification',
            ),
            create_competition_qualification_rule(
                start=4,
                end=7,
                path=QualificationPath.VIA_DOMESTIC_PLAYOFF,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_round=DestinationRound.SECOND_QUALIFYING_ROUND,
                note="Positions 4-7 enter the domestic playoff for a UEFA Conference League qualifying place.",
                destination_competition_identity=competition_ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION,
                destination_competition_name='UEFA Conference League, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=16,
                safe_rank=15,
                relegation_path=RelegationPath.RELEGATION_PLAYOFF,
            ),
            create_division_retention_rule(
                start=17, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.EREDIVISIE,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.KNVB_BEKER,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.KNVB_BEKER],
                final_round="final",
                reallocation_policy=CupReallocationPolicy(
                    cases=(
                        # The champion keeps its Champions League berth. Third takes
                        # the cup berth, fourth takes Europa League Q2, and 5–8 enter the playoff.
                        CupReallocationCase(
                            champion_rank_start=1,
                            champion_rank_end=2,
                            recipient_rank=3,
                        ),
                        # Third keeps the cup berth. Fourth takes Europa League Q2
                        # and 5–8 enter the playoff.
                        CupReallocationCase(champion_rank_start=3, champion_rank_end=3),
                        # The champion keeps the cup berth. Eighth replaces that
                        # team in the playoff.
                        CupReallocationCase(champion_rank_start=4, champion_rank_end=7),
                    ),
                ),
                destination_season_offset=1,
                sources=(
                    "https://www.knvb.nl/downloads/bestand/30196/reglement-play-off-europees-ticket-seizoen-2026-27",
                ),
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.UEFA_EUROPA_LEAGUE,
                    destination_competition_name='UEFA Europa League',
                    destination_phase=DestinationPhase.LEAGUE_PHASE,
                    qualification_path=QualificationPath.DIRECT,
                ),
            ),
        ),
        note=(
            "KNVB articles 1–2. A cup champion outside the top seven takes the cup berth "
            "and the domestic playoff stays at 4–7."
        ),
    ),

    # saudi_pro_league (1-3 Elite League Phase, 4-5 Elite Qualification Round; 16-18 Descenso directo)
    competition_ids.SAUDI_PRO_LEAGUE: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=18,
        total_regular_season_games=34,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_competition_qualification_rule(
                start=1,
                end=3,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.LEAGUE_PHASE,
                destination_competition_identity=competition_ids.AFC_CHAMPIONS_LEAGUE_ELITE,
                destination_competition_name='AFC Champions League Elite',
            ),
            create_competition_qualification_rule(
                start=4,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_competition_identity=competition_ids.AFC_CHAMPIONS_LEAGUE_ELITE_QUALIFICATION,
                destination_competition_name='AFC Champions League Elite, Qualification',
            ),
            create_division_retention_rule(
                start=16, end=18,
                safe_rank=15,
            ),
        ),
        standings=standings_policies.SAUDI_PRO_LEAGUE,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.SAUDI_KINGS_CUP,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.SAUDI_KINGS_CUP],
                final_round="final",
                # The top five keep their Elite berths. The first unassigned team
                # is sixth when the cup champion qualifies for Elite through the table.
                reallocation_policy=CupReallocationPolicy(reallocate_if_champion_already_qualified=True),
                destination_season_offset=1,
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.AFC_CHAMPIONS_LEAGUE_TWO,
                    destination_competition_name="AFC Champions League Two",
                    destination_phase=DestinationPhase.GROUP_STAGE,
                    qualification_path=QualificationPath.DIRECT,
                ),
                sources=(
                    "https://www.saff.com.sa/news.php?id=4184",
                    "https://assets.the-afc.com/downloads/tournament-regulations/AFC-Club-Comps-2027-28-Slot-Allocation.pdf",
                ),
            ),
        ),
    ),

    # brasileirao (base: 1-4 Libertadores grupos, 5 previa, 6-11 Sudamericana; 17-20 descenso)
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
                destination_phase=DestinationPhase.GROUP_STAGE,
                destination_competition_identity=competition_ids.CONMEBOL_LIBERTADORES,
                destination_competition_name='CONMEBOL Libertadores',
            ),
            create_competition_qualification_rule(
                start=5,
                end=5,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                destination_competition_identity=competition_ids.CONMEBOL_LIBERTADORES_QUALIFICATION,
                destination_competition_name='CONMEBOL Libertadores, Qualification',
            ),
            create_competition_qualification_rule(
                start=6,
                end=11,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.GROUP_STAGE,
                destination_competition_identity=competition_ids.CONMEBOL_SUDAMERICANA,
                destination_competition_name='CONMEBOL Sudamericana',
            ),
            create_division_retention_rule(
                start=17, end=20,
                safe_rank=16,
            ),
        ),
        standings=standings_policies.BRASILEIRAO,
        cup_qualifications=(
            CupQualificationRule(
                competition_identity=competition_ids.COPA_DO_BRASIL,
                competition_name=competition_ids.COMPETITION_NAMES[competition_ids.COPA_DO_BRASIL],
                final_round="final",
                # CBF art. 4 and Série A art. 6 redistribute both places together.
                # The runner-up can inherit the group-stage place. Continental
                # winners and 2027 Série A eligibility can also change recipients.
                status=RuleStatus.PENDING,
                destination_season_offset=1,
                qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.CONMEBOL_LIBERTADORES,
                    destination_competition_name='CONMEBOL Libertadores',
                    destination_phase=DestinationPhase.GROUP_STAGE,
                    qualification_path=QualificationPath.DIRECT,
                ),
                runner_up_qualification_destination=QualificationDestination(
                    destination_competition_identity=competition_ids.CONMEBOL_LIBERTADORES_QUALIFICATION,
                    destination_competition_name="CONMEBOL Libertadores, Qualification",
                    destination_phase=DestinationPhase.QUALIFICATION_ROUND,
                    qualification_path=QualificationPath.DIRECT,
                ),
                sources=(
                    "https://stcbfsiteprdimgbrs.blob.core.windows.net/img-site/cdn/REC_Copa_do_Brasil_2026_66989a5426.pdf",
                    "https://stcbfsiteprdimgbrs.blob.core.windows.net/img-site/cdn/REC_Brasileiro_SA_rie_A_2026_v15_12_2025_final_02692c1077.pdf",
                ),
            ),
        ),
        note=(
            "The league cutoffs are the baseline. Cup and continental reallocations "
            "require both cup finalists, continental winners and 2027 Série A eligibility."
        ),
    ),

    # liga_mx_apertura (1-8 a cuartos de final de Liguilla; sin descenso; Concacaf pendiente por acumulado anual)
    competition_ids.LIGA_MX_APERTURA: CompetitionRules(
        reference_season="2026",
        number_of_teams=18,
        total_regular_season_games=17,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_phase_qualification_rule(
                start=1,
                end=8,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.QUARTER_FINALS,
                destination_competition_identity=competition_ids.LIGA_MX_APERTURA_PLAYOFFS,
                destination_competition_name="Liga MX, Apertura Playoffs",
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note=(
                    "Resolve together with Clausura using both finals and the season aggregate. "
                    "Concacaf 2027 used a Round of 16 bye for the best league champion "
                    "and reassigned overlaps with Leagues Cup."
                ),
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.CONCACAF_CHAMPIONS_CUP,
                destination_competition_name='CONCACAF Champions Cup',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.LIGA_MX_APERTURA,
        note="FMF 2026/27 regulation: Apertura 2026, top eight to quarterfinals; international allocations remain pending.",
    ),

    # liga_mx_clausura (1-8 a cuartos de final de Liguilla; sin descenso; Concacaf pendiente por acumulado anual)
    competition_ids.LIGA_MX_CLAUSURA: CompetitionRules(
        reference_season="2027",
        number_of_teams=18,
        total_regular_season_games=17,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_phase_qualification_rule(
                start=1,
                end=8,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.QUARTER_FINALS,
                destination_competition_identity=competition_ids.LIGA_MX_CLAUSURA_PLAYOFFS,
                destination_competition_name="Liga MX, Clausura Playoffs",
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note=(
                    "Resolve together with Apertura using both finals and the season aggregate. "
                    "Concacaf 2027 used a Round of 16 bye for the best league champion "
                    "and reassigned overlaps with Leagues Cup."
                ),
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.CONCACAF_CHAMPIONS_CUP,
                destination_competition_name='CONCACAF Champions Cup',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
        ),
        standings=standings_policies.LIGA_MX_CLAUSURA,
        note="FMF 2026/27 regulation: Clausura 2027, top eight to quarterfinals; international allocations remain pending.",
    ),

    # liga_profesional_de_argentina_apertura (Dos grupos de 15; 1-8 de cada grupo a octavos; descenso anual y promedios)
    # AFA 2026 Primera División regulations, arts. 24 and 27–28.
    competition_ids.ARGENTINA_APERTURA: CompetitionRules(
        reference_season="2026",
        number_of_teams=30,
        total_regular_season_games=16,
        scoring=FOOTBALL_SCORING,
        objectives=(
            create_phase_qualification_rule(
                start=1,
                end=8,
                scope=RankingScope.GROUP,
                group_size=15,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.ROUND_OF_16,
                destination_competition_identity=competition_ids.ARGENTINA_APERTURA_PLAYOFFS,
                destination_competition_name="Liga Profesional de Fútbol, Apertura Playoffs",
            ),
            create_division_retention_rule(
                start=30, end=30,
                safe_rank=29,
                relegation_path=RelegationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                status=RuleStatus.PENDING,
                note="Worst annual team excluding the club relegated by rolling average.",
            ),
            create_division_retention_rule(
                relegation_path=RelegationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.MULTISEASON_RECORD,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.LOWEST_POINTS_AVERAGE,
                places=1,
                note="Worst points-per-game average; requires three seasons and exclusions.",
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note=(
                    "AFA art. 27. Apertura and Clausura champions and Copa Argentina award separate berths. "
                    "If the cup champion already holds a league champion berth, select the next eligible "
                    "top-flight finisher in the cup and break ties using the annual table."
                ),
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.CONMEBOL_LIBERTADORES,
                destination_competition_name='CONMEBOL Libertadores',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note=(
                    "AFA art. 28. Take six from the annual table after excluding all Libertadores qualifiers. "
                    "Participants must be in the 2027 top flight."
                ),
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.CONMEBOL_SUDAMERICANA,
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
            create_phase_qualification_rule(
                start=1,
                end=8,
                scope=RankingScope.GROUP,
                group_size=15,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.ROUND_OF_16,
                destination_competition_identity=competition_ids.ARGENTINA_CLAUSURA_PLAYOFFS,
                destination_competition_name="Liga Profesional, Clausura Playoffs",
            ),
            create_division_retention_rule(
                start=30, end=30,
                safe_rank=29,
                relegation_path=RelegationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                status=RuleStatus.PENDING,
                note="Worst annual team excluding the club relegated by rolling average.",
            ),
            create_division_retention_rule(
                relegation_path=RelegationPath.DIRECT_RELEGATION,
                aggregation_scope=AggregationScope.MULTISEASON_RECORD,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.LOWEST_POINTS_AVERAGE,
                places=1,
                note="Worst points-per-game average; requires three seasons and exclusions.",
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note=(
                    "AFA art. 27. Apertura and Clausura champions and Copa Argentina award separate berths. "
                    "If the cup champion already holds a league champion berth, select the next eligible "
                    "top-flight finisher in the cup and break ties using the annual table."
                ),
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.CONMEBOL_LIBERTADORES,
                destination_competition_name='CONMEBOL Libertadores',
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
            ),
            create_competition_qualification_rule(
                aggregation_scope=AggregationScope.SEASON_AGGREGATE,
                note=(
                    "AFA art. 28. Take six from the annual table after excluding all Libertadores qualifiers. "
                    "Participants must be in the 2027 top flight."
                ),
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.CONMEBOL_SUDAMERICANA,
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
            create_phase_qualification_rule(
                start=1,
                end=6,
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.NBA_PLAYOFFS,
                destination_competition_name="NBA, Playoffs",
            ),
            create_phase_qualification_rule(
                start=7,
                end=10,
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                destination_phase=DestinationPhase.PLAY_IN,
                destination_competition_identity=competition_ids.NBA_PLAY_IN,
                destination_competition_name="NBA, Play-In Tournament",
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
            create_phase_qualification_rule(
                start=1,
                end=6,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.EUROLEAGUE_PLAYOFFS,
                destination_competition_name="Euroleague, Playoffs",
            ),
            create_phase_qualification_rule(
                start=7,
                end=10,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.PLAY_IN,
                destination_competition_identity=competition_ids.EUROLEAGUE_PLAY_IN,
                destination_competition_name="Euroleague, Play-In",
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
            create_phase_qualification_rule(
                path=QualificationPath.DIRECT,
                scope=RankingScope.LEAGUE,
                selection_method=SelectionMethod.DIVISION_WINNERS,
                places=3,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.MLB_POSTSEASON,
                destination_competition_name="MLB Postseason",
            ),
            create_phase_qualification_rule(
                path=QualificationPath.DIRECT,
                note="Wild Card places selected from the best remaining eligible teams.",
                scope=RankingScope.LEAGUE,
                selection_method=SelectionMethod.BEST_REMAINING,
                places=3,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.MLB_POSTSEASON,
                destination_competition_name="MLB Postseason",
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
            create_phase_qualification_rule(
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                selection_method=SelectionMethod.DIVISION_WINNERS,
                places=4,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.NFL_PLAYOFFS,
                destination_competition_name="NFL, Playoffs",
            ),
            create_phase_qualification_rule(
                path=QualificationPath.DIRECT,
                note="Wild Card places selected from the best remaining eligible teams.",
                scope=RankingScope.CONFERENCE,
                selection_method=SelectionMethod.BEST_REMAINING,
                places=3,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.NFL_PLAYOFFS,
                destination_competition_name="NFL, Playoffs",
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
            create_phase_qualification_rule(
                start=1,
                end=6,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.QUARTER_FINALS,
                destination_competition_identity=competition_ids.SHL_PLAYOFFS,
                destination_competition_name="SHL, Playoffs",
            ),
            create_phase_qualification_rule(
                start=7,
                end=10,
                path=QualificationPath.DIRECT,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.ROUND_OF_16,
                destination_competition_identity=competition_ids.SHL_PLAYOFFS,
                destination_competition_name="SHL, Playoffs",
            ),
            create_division_retention_rule(
                start=13, end=14,
                safe_rank=12,
                relegation_path=RelegationPath.RELEGATION_PLAYOFF,
            ),
        ),
        standings=standings_policies.SHL,
        scoring=HOCKEY_SCORING,
    ),

    # Liiga (1–12 playoffs; 15–17 descienden, 14 conserva la categoría)
    competition_ids.LIIGA: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=17,
        total_regular_season_games=64,
        objectives=(
            create_phase_qualification_rule(
                start=1,
                end=4,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.QUARTER_FINALS,
                destination_competition_identity=competition_ids.LIIGA_PLAYOFFS,
                destination_competition_name="Liiga Playoffs",
            ),
            create_phase_qualification_rule(
                start=5,
                end=12,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.FIRST_ROUND,
                destination_competition_identity=competition_ids.LIIGA_PLAYOFFS,
                destination_competition_name="Liiga Playoffs",
            ),
            create_division_retention_rule(
                start=15, end=17,
                safe_rank=14,
            ),
        ),
        standings=standings_policies.LIIGA,
        scoring=HOCKEY_SCORING,
        note="2026/27 transition: regular-season top 14 retain the upper tier; 15–17 move to the lower tier without playouts (Liiga decision, 13 March 2026).",
    ),

    # nhl (División: 1-3 Playoffs directos; Conferencia: 2 Wild Cards)
    competition_ids.NHL: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=32,
        total_regular_season_games=84,
        objectives=(
            create_phase_qualification_rule(
                start=1,
                end=3,
                path=QualificationPath.DIRECT,
                scope=RankingScope.DIVISION,
                group_size=8,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.NHL_PLAYOFFS,
                destination_competition_name="NHL, Playoffs",
            ),
            create_phase_qualification_rule(
                path=QualificationPath.DIRECT,
                note="Wild Card places selected from the best remaining eligible teams.",
                scope=RankingScope.CONFERENCE,
                selection_method=SelectionMethod.BEST_REMAINING,
                places=2,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.NHL_PLAYOFFS,
                destination_competition_name="NHL, Playoffs",
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
            create_phase_qualification_rule(
                start=1,
                end=7,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.FIRST_ROUND,
                destination_competition_identity=competition_ids.MLS_PLAYOFFS,
                destination_competition_name="MLS, Playoffs",
                note='MLS Round One: best-of-three series.',
            ),
            create_phase_qualification_rule(
                start=8,
                end=9,
                path=QualificationPath.DIRECT,
                scope=RankingScope.CONFERENCE,
                group_size=15,
                note="MLS Wild Card match: single-game entry to Round One.",
                destination_phase=DestinationPhase.PLAY_IN,
                destination_competition_identity=competition_ids.MLS_PLAYOFFS,
                destination_competition_name="MLS, Playoffs",
            ),
            create_competition_qualification_rule(
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.MULTIPLE_QUALIFICATION_PATHS,
                places=5,
                note=(
                    "For Concacaf 2027, the MLS Cup winner enters the Round of 16. "
                    "The Shield winner, other conference winner and two next eligible Shield clubs "
                    "enter Round One. Resolve overlaps with Leagues Cup and domestic cups."
                ),
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.CONCACAF_CHAMPIONS_CUP,
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
            create_phase_qualification_rule(
                start=1,
                end=places,
                path=QualificationPath.DIRECT,
                note="Calder Cup Playoffs.",
                scope=RankingScope.DIVISION,
                ranking_group=division,
                group_size=size,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_competition_identity=competition_ids.AHL_PLAYOFFS,
                destination_competition_name="AHL, Playoffs",
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

    # wnba (2026: 15 equipos, 44 partidos; ocho mejores globales a playoffs)
    competition_ids.WNBA: CompetitionRules(
        reference_season="2026",
        number_of_teams=15,
        total_regular_season_games=44,
        objectives=(
            create_phase_qualification_rule(
                start=1,
                end=8,
                path=QualificationPath.DIRECT,
                scope=RankingScope.GLOBAL,
                group_size=15,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.FIRST_ROUND,
                destination_competition_identity=competition_ids.WNBA_PLAYOFFS,
                destination_competition_name="WNBA, Playoffs",
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
                relegation_path=RelegationPath.RELEGATION_PLAYOFF,
            ),
            create_phase_qualification_rule(
                start=1,
                end=4,
                path=QualificationPath.DIRECT,
                status=RuleStatus.PENDING,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.QUARTER_FINALS,
                destination_competition_identity=competition_ids.CBA_PLAYOFFS,
                destination_competition_name="China CBA, Playoffs",
                note='Public reports indicate this entry round; the primary 2026/27 competition regulation remains to be checked.',
            ),
            create_phase_qualification_rule(
                start=5,
                end=12,
                path=QualificationPath.DIRECT,
                status=RuleStatus.PENDING,
                destination_phase=DestinationPhase.PLAYOFFS,
                destination_round=DestinationRound.FIRST_ROUND,
                destination_competition_identity=competition_ids.CBA_PLAYOFFS,
                destination_competition_name="China CBA, Playoffs",
                note='Public reports indicate a best-of-three format; the primary 2026/27 competition regulation remains to be checked.',
            ),
            create_competition_qualification_rule(
                path=None,
                status=RuleStatus.PENDING,
                selection_method=SelectionMethod.CHAMPION,
                places=1,
                note='Champion path only; destination phase unresolved; not a regular season table finish.',
                destination_phase=None,  # Pending: routes or destination phase unresolved.
                destination_competition_identity=competition_ids.FIBA_INTERCONTINENTAL_CUP,
                destination_competition_name='FIBA Intercontinental Cup',
            ),
        ),
        standings=standings_policies.CBA,
        scoring=WINS_SCORING,
    ),

    # pfl philippines football league (2026/27: formato y objetivos incompletos)
    competition_ids.PFL: CompetitionRules(
        reference_season="2026/27",
        number_of_teams=10,
        total_regular_season_games=None,
        objectives=None,
        standings=standings_policies.PFL,
        scoring=FOOTBALL_SCORING,
        note="PFF publishes ten participants for 2026/27; regular-season duration, phase boundaries and international allocations remain unconfirmed.",
    ),
})


def get_competition_profile(identity: CompetitionIdentity | None) -> CompetitionRules | None:
    """Return the current profile by exact provider identity, independent of local IDs."""
    return COMPETITION_PROFILES.get(identity)


_ARGENTINA_TOURNAMENTS = (competition_ids.ARGENTINA_APERTURA, competition_ids.ARGENTINA_CLAUSURA)
_ARGENTINA_AGGREGATE = SeasonAggregateProfile(
    number_of_teams=COMPETITION_PROFILES[competition_ids.ARGENTINA_CLAUSURA].number_of_teams,
    sources=tuple(
        SeasonAggregateSource(identity, COMPETITION_PROFILES[identity].total_regular_season_games)
        for identity in _ARGENTINA_TOURNAMENTS
    ),
    scoring=FOOTBALL_SCORING,
    standings=standings_policies.ARGENTINA_ANNUAL,
)

_SEASON_AGGREGATES = MappingProxyType({
    competition_ids.ARGENTINA_CLAUSURA: _ARGENTINA_AGGREGATE,
})


def get_season_aggregate_profile(identity: CompetitionIdentity | None) -> SeasonAggregateProfile | None:
    """Select the shared table configuration by tournament identity."""
    return _SEASON_AGGREGATES.get(identity)
