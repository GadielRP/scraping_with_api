from dataclasses import replace
from datetime import datetime, timezone
from fractions import Fraction

from modules.competition import competition_ids as ids
from modules.competition.annual_competition_outcomes import (
    resolve_annual_competition_outcomes,
    resolve_annual_relegation,
)
from modules.competition.competition_profiles import COMPETITION_PROFILES
from modules.competition.competitive_rules import FinalBerthRule, QualificationDestination, TableBerthRule
from modules.competition.final_outcome import CupFinalEvidence, ParticipantIdentity
from modules.competition.rule_terms import DestinationPhase, DestinationRound, ObjectiveType
from modules.competition.season_aggregate import SeasonAggregateStandings, SeasonStanding


PROFILE = COMPETITION_PROFILES[ids.ARGENTINA_CLAUSURA].annual_qualification
RELEGATION_RULES = tuple(
    rule.rank_selection for rule in COMPETITION_PROFILES[ids.ARGENTINA_CLAUSURA].objectives
    if rule.objective_type == ObjectiveType.DIVISION_RETENTION
)
YEAR = 2030


def club(rank):
    return ParticipantIdentity("sofascore", rank)


def annual():
    return SeasonAggregateStandings(YEAR, datetime(YEAR, 12, 31, tzinfo=timezone.utc), (), True, tuple(
        SeasonStanding(club(rank), f"Club {rank}", rank, 60 - rank, 32, 0, 0, 0, 0, 0, 0, None)
        for rank in range(1, 17)
    ))


def final(competition, winner, runner_up=50):
    return CupFinalEvidence(competition, YEAR, "DECIDED", winner=club(winner),
                            home=club(winner), away=club(runner_up))


def resolve(table, finals=(), *, members=None, eliminations=None):
    relegation = resolve_annual_relegation(
        RELEGATION_RULES, table, {row.participant: Fraction(row.points, 32) for row in table.standings},
    )
    result = resolve_annual_competition_outcomes(
        PROFILE, table, finals, relegation, next_season_members=members,
        cup_eliminations={"libertadores_3": eliminations} if eliminations else None,
    )
    assert result.destination_year == table.season_year + 1
    return {berth.slot: berth for berth in result.berths}


def test_missing_finals_keep_the_known_apertura_berth_and_project_the_annual_table():
    berths = resolve(annual(), (final(ids.ARGENTINA_APERTURA_PLAYOFFS, 9),))
    assert berths["libertadores_1"].participant == club(9)
    assert berths["libertadores_2"].reason == "final_absent"
    assert berths["libertadores_3"].reason == "final_absent"
    assert berths["libertadores_4"].participant == club(1)
    assert berths["libertadores_6"].participant == club(3)
    assert berths["libertadores_6"].status == "PROVISIONAL"
    assert berths["libertadores_6"].destination.destination_round == DestinationRound.SECOND_QUALIFYING_ROUND


def test_a_final_reported_as_decided_without_a_winner_stays_pending():
    incomplete = replace(final(ids.COPA_ARGENTINA, 1), winner=None)
    berth = resolve(annual(), (incomplete,))["libertadores_3"]
    assert berth.status == "PENDING"
    assert berth.reason == "incomplete_cup_evidence"


def test_repeated_champion_and_continental_title_reallocate_without_duplicate_clubs():
    members = frozenset(club(rank) for rank in range(1, 15))
    berths = resolve(annual(), (
        final(ids.ARGENTINA_APERTURA_PLAYOFFS, 1),
        final(ids.ARGENTINA_CLAUSURA_PLAYOFFS, 1),
        final(ids.COPA_ARGENTINA, 1, runner_up=7),
        final(ids.CONMEBOL_LIBERTADORES, 2),
        final(ids.CONMEBOL_SUDAMERICANA, 50),
    ), members=members)

    assert [berths[f"libertadores_{i}"].participant for i in range(1, 7)] == [
        club(1), club(3), club(7), club(4), club(5), club(6),
    ]
    assert berths["libertadores_titleholder"].participant == club(2)
    assert berths["sudamericana_titleholder"].status == "NOT_APPLICABLE"
    assert [berths[f"sudamericana_{i}"].participant for i in range(1, 7)] == [
        club(i) for i in range(8, 14)
    ]
    assert berths["sudamericana_1"].destination.destination_phase == DestinationPhase.GROUP_STAGE
    assert berths["libertadores_6"].status == "QUALIFIED"


def test_cup_replacement_breaks_a_shared_knockout_round_by_annual_rank():
    finals = (
        final(ids.ARGENTINA_APERTURA_PLAYOFFS, 1),
        final(ids.ARGENTINA_CLAUSURA_PLAYOFFS, 2),
        final(ids.COPA_ARGENTINA, 1, runner_up=2),
    )
    assert resolve(annual(), finals)["libertadores_3"].reason == "incomplete_cup_bracket"
    berths = resolve(annual(), finals, eliminations={"semifinal": (club(8), club(7))})
    assert berths["libertadores_3"].participant == club(7)


def test_relegation_excludes_the_average_loser_and_uses_an_official_decider_for_points_ties():
    table = annual()
    averages = {row.participant: Fraction(row.points, 32) for row in table.standings}
    rules = RELEGATION_RULES
    result = resolve_annual_relegation(rules, table, averages)
    assert result.participants == (club(16), club(15))

    tied = replace(table, standings=(*table.standings[:-3],
                                     replace(table.standings[-3], points=table.standings[-2].points),
                                     *table.standings[-2:]))
    assert resolve_annual_relegation(rules, tied, averages).reason == "annual_relegation_decider_required"
    assert resolve_annual_relegation(rules, table, None).reason == "incomplete_multiseason_record"


def test_resolver_uses_the_destinations_slots_and_year_offset_supplied_by_the_profile():
    cup = ids.FA_CUP
    destination = QualificationDestination(ids.UEFA_EUROPA_LEAGUE, "Europa League", DestinationPhase.LEAGUE_PHASE)
    profile = replace(
        PROFILE,
        final_berths=(FinalBerthRule("cup_route", destination, cup),),
        table_berths=(TableBerthRule(destination, ("table_route",)),),
        destination_season_offset=2,
    )
    table = annual()
    result = resolve_annual_competition_outcomes(
        profile, table, (final(cup, 1),), resolve_annual_relegation((), table, None),
    )
    assert result.destination_year == YEAR + 2
    assert [(berth.slot, berth.destination, berth.participant) for berth in result.berths] == [
        ("cup_route", destination, club(1)), ("table_route", destination, club(2)),
    ]


