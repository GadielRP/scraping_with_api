"""Unit tests for the Universal Berth Allocation Engine and Relegation Engine."""

from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.berth_allocation_engine.berth_allocation_engine import allocate_berths
from modules.competition.berth_allocation_engine.models import (
    BerthResolution,
    QualificationDestination,
    QualificationRules,
    TableBerthRule,
    TournamentBerthRule,
)
from modules.competition.berth_allocation_engine.relegation_engine import resolve_relegation
from modules.competition.final_outcome import CupFinalEvidence, ParticipantIdentity
from modules.competition.rule_terms import DestinationPhase, QualificationPath, SelectionMethod
from modules.competition.competitive_rules import RankSelection
from modules.competition.season_aggregate import SeasonStanding


UCL_DEST = QualificationDestination(
    destination_competition_name="UEFA Champions League",
    destination_phase=DestinationPhase.GROUP_STAGE,
    qualification_path=QualificationPath.DIRECT,
)
UEL_DEST = QualificationDestination(
    destination_competition_name="UEFA Europa League",
    destination_phase=DestinationPhase.GROUP_STAGE,
    qualification_path=QualificationPath.DIRECT,
)
FA_CUP_ID = CompetitionIdentity(source="sofascore", source_unique_tournament_id=19)


def make_standing(club_id: int, rank: int, points: float) -> SeasonStanding:
    p = ParticipantIdentity(source="sofascore", source_participant_id=club_id, source_participant_name=f"Club {club_id}")
    return SeasonStanding(
        participant=p,
        name=f"Club {club_id}",
        rank=rank,
        points=points,
        games_played=38,
        wins=10,
        draws=5,
        losses=5,
        goals_for=30,
        goals_against=20,
        goal_diff=10,
        ranking_reason=None,
    )


def test_premier_league_standard_berths_without_cup_evidence():
    standings = [make_standing(i, rank=i, points=100 - i * 2) for i in range(1, 10)]
    rules = QualificationRules(
        destination_season_offset=1,
        tournament_berths=(
            TournamentBerthRule(
                destination=UEL_DEST,
                competition=FA_CUP_ID,
                reallocate_to_table=True,
            ),
        ),
        table_berths=(
            TableBerthRule(
                places=4,
                destination=UCL_DEST,
            ),
            TableBerthRule(
                places=1,
                destination=UEL_DEST,
            ),
        ),
    )

    outcome = allocate_berths(rules, standings, season_year=2026)

    # FA cup final is absent -> pending
    assert outcome.berths_by_source[FA_CUP_ID].resolution == BerthResolution.UNRESOLVED
    assert outcome.berths_by_source[FA_CUP_ID].participant is None

    # Table berths are projected from the current table
    assert outcome.berths[1].participant.source_participant_id == 1
    assert outcome.berths[1].resolution == BerthResolution.PROJECTED
    assert outcome.berths[4].participant.source_participant_id == 4
    assert outcome.berths[5].participant.source_participant_id == 5


def test_cup_winner_already_qualified_reallocates_to_table():
    standings = [make_standing(i, rank=i, points=100 - i * 2) for i in range(1, 10)]
    rules = QualificationRules(
        destination_season_offset=1,
        tournament_berths=(
            TournamentBerthRule(
                destination=UEL_DEST,
                competition=FA_CUP_ID,
                reallocate_to_table=True,
            ),
        ),
        table_berths=(
            TableBerthRule(
                places=4,
                destination=UCL_DEST,
            ),
            TableBerthRule(
                places=1,
                destination=UEL_DEST,
            ),
        ),
    )

    # Winner of FA Cup is Club 1.
    # In tournament allocation, Club 1 gets the tournament slot (first berth).
    cup_evidence = CupFinalEvidence(
        competition=FA_CUP_ID,
        season_year=2026,
        status="DECIDED",
        winner=ParticipantIdentity("sofascore", 1),
    )

    outcome = allocate_berths(
        rules,
        standings,
        season_year=2026,
        tournament_evidence=[cup_evidence],
    )

    # FA cup berth goes to Club 1
    assert outcome.berths_by_source[FA_CUP_ID].participant.source_participant_id == 1
    assert outcome.berths_by_source[FA_CUP_ID].resolution == BerthResolution.CONFIRMED

    # Because Club 1 already has a berth, the table berths advance down:
    # the first table berth goes to Club 2!
    assert outcome.berths[1].participant.source_participant_id == 2
    assert outcome.berths[4].participant.source_participant_id == 5
    assert outcome.berths[5].participant.source_participant_id == 6


def test_relegation_direct_and_average():
    standings = [make_standing(i, rank=i, points=50 - i) for i in range(1, 6)]

    direct_rule = RankSelection(
        rank_start=5,
        rank_end=5,
        places=1,
        selection_method=SelectionMethod.LOWEST_POINTS,
    )
    res = resolve_relegation([direct_rule], standings)
    assert len(res.participants) == 1
    assert res.participants[0].source_participant_id == 5
