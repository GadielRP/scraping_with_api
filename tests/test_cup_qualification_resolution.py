"""Cup allocation with configurable berths, identities and time of evidence."""

from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from infrastructure.persistence.models import (
    Competition, Event, EventOutcomeObservation, Participant, Result, Season,
)
from infrastructure.persistence.repositories.cup_final_repository import read_cup_final
from modules.competition import competition_ids as ids
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.competition_profiles import COMPETITION_PROFILES
from modules.competition.final_outcome import CupFinalEvidence, ParticipantIdentity, decisive_winner
from modules.competition.qualification_resolution import resolve_qualification
from modules.competition.qualification_reallocation import CupReallocationPolicy
from modules.competition.rule_terms import DestinationPhase, QualificationPath
from modules.pillars.mining.serialization import to_json_value
from modules.pillars.pillar_1_team_structure.module_5.contextual_competitive_cost_engine import (
    calculate_contextual_competitive_cost_engine,
)
from tests.pillars.pillar_1_team_structure.test_competition_rules import _configured_event

NOW = datetime(2027, 5, 1, tzinfo=timezone.utc)
FINAL = NOW - timedelta(days=7)
BASE = COMPETITION_PROFILES[ids.LALIGA]


def identity(rank):
    return ParticipantIdentity("sofascore", rank)


def table():
    return {f"Club {rank}": {"rank": rank, "points": 83 - rank * 3, "games_played": 33,
                              "participant_identity": asdict(identity(rank))}
            for rank in range(1, 21)}


def cup(winner_rank=7):
    return CupFinalEvidence(ids.COPA_DEL_REY, 2026, "DECIDED", event_id=100,
                            season_id=777, starts_at=FINAL, observed_at=FINAL + timedelta(hours=3),
                            observation_id="known-final", evidence_source="sofascore",
                            home=identity(winner_rank), away=identity(101), winner=identity(winner_rank),
                            result={"home_score": 1, "away_score": 0, "winner": "1"}, decision="regulation")


@pytest.mark.parametrize("winner,el,conference", [
    (1, [5, 6], 7), (4, [5, 6], 7), (5, [5, 6], 7), (6, [5, 6], 7),
    (7, [5, 7], 6), (12, [5, 12], 6), (18, [5, 18], 6), (99, [5], 6),
])
def test_current_table_allocations_and_immutable_catalogue(winner, el, conference):
    standings = table()
    before, catalogue = deepcopy(standings), dict(COMPETITION_PROFILES)
    resolution = resolve_qualification(BASE, standings, cup(winner), 2026, NOW)
    assert resolution.status == "AS_OF_TABLE"
    assert resolution.destination_season == "2027/28"
    assert [rank for rank, destination in resolution.league_assignments if destination == ids.UEFA_EUROPA_LEAGUE] == el
    assert (conference, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) in resolution.league_assignments
    assert resolution.cup_allocations[0].champion_destination == (
        ids.UEFA_CHAMPIONS_LEAGUE if winner <= 4 else ids.UEFA_EUROPA_LEAGUE
    )
    assert identity(winner) not in resolution.eligible_participants
    assert standings == before and dict(COMPETITION_PROFILES) == catalogue
    assert COMPETITION_PROFILES[ids.LALIGA] is BASE
    assert resolution.effective_rules is not BASE


def test_final_only_when_every_team_completed_its_season():
    standings = table()
    for row in standings.values():
        row["games_played"] = 38
    assert resolve_qualification(BASE, standings, cup(), 2026, NOW).status == "FINAL"


def test_cup_allocation_preserves_its_explicit_qualification_phase_and_path():
    policy = BASE.cup_qualifications[0]
    cup_destination = replace(
        policy.qualification_destination,
        destination_phase=DestinationPhase.QUALIFICATION_ROUND,
        qualification_path=QualificationPath.DIRECT,
    )
    profile = replace(BASE, cup_qualifications=(replace(
        policy, qualification_destination=cup_destination,
    ),))

    result = resolve_qualification(profile, table(), cup(), 2026, NOW)

    assert result.cup_allocations[0].qualification_destination == cup_destination
    assert result.cup_allocations[0].qualification_destination.destination_phase is DestinationPhase.QUALIFICATION_ROUND
    assert result.cup_allocations[0].qualification_destination.qualification_path is QualificationPath.DIRECT


@pytest.mark.parametrize("winner_rank,recipient_rank,expected_europa_qualifying_rank", [
    (2, 3, 4),
    (3, 3, 4),
    (6, 6, 3),
])
def test_eredivisie_cup_policy_keeps_the_cup_destination_and_reassigns_table_places(
    winner_rank, recipient_rank, expected_europa_qualifying_rank,
):
    profile = COMPETITION_PROFILES[ids.EREDIVISIE]
    evidence = replace(cup(winner_rank), competition=ids.KNVB_BEKER)

    result = resolve_qualification(profile, table(), evidence, 2026, NOW)

    allocation = result.cup_allocations[0]
    assert result.status == "AS_OF_TABLE"
    assert allocation.qualification_destination.destination_competition_identity == ids.UEFA_EUROPA_LEAGUE
    assert allocation.qualification_destination.destination_phase is DestinationPhase.LEAGUE_PHASE
    assert allocation.qualification_destination.qualification_path is QualificationPath.DIRECT
    assert allocation.berth_recipient == identity(recipient_rank)
    assert allocation.berth_recipient_rank == recipient_rank
    assert (expected_europa_qualifying_rank, ids.UEFA_EUROPA_LEAGUE_QUALIFICATION) in result.league_assignments
    conference_ranks = [rank for rank, destination in result.league_assignments
                        if destination == ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION]
    if winner_rank in (2, 3):
        assert conference_ranks == [5, 6, 7, 8]
    elif winner_rank == 6:
        assert conference_ranks == [4, 5, 7, 8]


def test_cup_destination_without_a_matching_league_objective_is_still_resolved():
    base_profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]
    efl_rule = next(rule for rule in base_profile.cup_qualifications
                    if rule.competition_identity == ids.EFL_CUP)
    profile = replace(base_profile, cup_qualifications=(efl_rule,))
    evidence = replace(cup(1), competition=ids.EFL_CUP)

    result = resolve_qualification(profile, table(), evidence, 2026, NOW)

    allocation = result.cup_allocations[0]
    assert result.status == "AS_OF_TABLE"
    assert allocation.qualification_destination.destination_competition_identity == ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION
    assert allocation.qualification_destination.destination_phase is DestinationPhase.QUALIFICATION_ROUND
    assert allocation.berth_recipient == identity(6)
    assert allocation.berth_recipient_rank == 6


@pytest.mark.parametrize("winner,recipient,europa_qualifying,conference", [
    (2, 4, 5, 6),
    (4, 4, 5, 6),
    (5, 5, 4, 6),
    (7, 7, 4, 5),
])
def test_portugal_cup_updates_the_qualifying_round_berths(winner, recipient, europa_qualifying, conference):
    profile = COMPETITION_PROFILES[ids.LIGA_PORTUGAL]
    evidence = replace(cup(winner), competition=ids.TACA_DE_PORTUGAL)

    result = resolve_qualification(profile, table(), evidence, 2026, NOW)

    allocation, = result.cup_allocations
    assert allocation.berth_recipient_rank == recipient
    assert (europa_qualifying, ids.UEFA_EUROPA_LEAGUE_QUALIFICATION) in result.league_assignments
    assert (conference, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) in result.league_assignments


@pytest.mark.parametrize("winner,recipient", [(4, 6), (8, 8)])
def test_saudi_king_cup_cedes_its_berth_only_when_the_winner_has_an_elite_berth(winner, recipient):
    profile = COMPETITION_PROFILES[ids.SAUDI_PRO_LEAGUE]
    evidence = replace(cup(winner), competition=ids.SAUDI_KINGS_CUP)

    result = resolve_qualification(profile, table(), evidence, 2026, NOW)

    allocation, = result.cup_allocations
    assert allocation.berth_recipient_rank == recipient
    assert (recipient, ids.AFC_CHAMPIONS_LEAGUE_TWO) in result.league_assignments


def test_premier_league_applies_each_available_final_and_recomputes_the_shared_berths():
    profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]
    standings = table()
    for row in standings.values():
        row["games_played"] = 38
    fa = replace(cup(1), competition=ids.FA_CUP)
    efl = replace(cup(2), competition=ids.EFL_CUP,
                  starts_at=FINAL - timedelta(days=30), observed_at=FINAL - timedelta(days=29))

    efl_only = resolve_qualification(profile, standings, (fa, efl), 2026, NOW,
                                     target_event_starts_at=FINAL)
    fa_only = resolve_qualification(profile, standings, (fa,), 2026, NOW)
    both = resolve_qualification(profile, standings, (efl, fa), 2026, NOW)

    assert efl_only.status == fa_only.status == "AS_OF_TABLE"
    assert efl_only.pending_cup_reasons == ((ids.FA_CUP, "cup_final_after_target_event"),)
    assert fa_only.pending_cup_reasons == ((ids.EFL_CUP, "final_absent"),)
    assert (6, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) in efl_only.league_assignments
    assert (6, ids.UEFA_EUROPA_LEAGUE) in fa_only.league_assignments
    assert both.status == "FINAL"
    assert both.pending_cup_reasons == ()
    assert (6, ids.UEFA_EUROPA_LEAGUE) in both.league_assignments
    assert (7, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) in both.league_assignments


@pytest.mark.parametrize("winner, reallocate, expected_ranks, lower_ranks", [
    (4, True, [6, 7, 8], [9, 10]),
    (4, False, [6, 7], [8, 9]),
    (7, True, [6, 8], [9, 10]),
])
def test_resolver_uses_the_profile_for_destinations_and_slot_counts(winner, reallocate, expected_ranks, lower_ranks):
    higher, destination, lower, cup_id = (CompetitionIdentity(i, source="test") for i in range(1, 5))
    template = BASE.objectives[0]
    objectives = tuple(
        replace(
            template,
            qualification_destination=replace(
                template.qualification_destination,
                destination_competition_identity=identity,
                destination_competition_name=f"Test destination {identity.source_tournament_id}",
            ),
            rank_selection=replace(
                template.rank_selection, rank_start=start, rank_end=end, reference_rank=end,
            ),
        )
        for identity, start, end in ((higher, 1, 5), (destination, 6, 7), (lower, 8, 9))
    )
    profile = replace(
        BASE,
        reference_season="2030/31",
        objectives=objectives,
        cup_qualifications=(replace(
            BASE.cup_qualifications[0],
            competition_identity=cup_id,
            qualification_destination=replace(
                BASE.cup_qualifications[0].qualification_destination,
                destination_competition_identity=destination,
            ),
            reallocation_policy=CupReallocationPolicy(
                reallocate_if_champion_already_qualified=reallocate,
            ),
        ),),
    )
    evidence = replace(cup(winner), competition=cup_id, season_year=2025)
    standings = {name: row for name, row in table().items() if row["rank"] <= 12}
    result = resolve_qualification(profile, standings, evidence, 2025, NOW)

    assert result.status == "AS_OF_TABLE"
    assert result.cup_allocations[0].champion_destination == (higher if winner == 4 else destination)
    effective = result.effective_rules.objectives
    assert [rank for rule in effective if rule.qualification_destination.destination_competition_identity == destination
            for rank in range(rule.rank_selection.rank_start, rule.rank_selection.rank_end + 1)] == expected_ranks
    assert [rank for rank, identity in result.league_assignments if identity == lower] == lower_ranks


@pytest.mark.parametrize("score,side,decision", [
    ({"home_score": 2, "away_score": 1, "winner": "1"}, "1", "regulation"),
    ({"home_score": 1, "away_score": 1, "winner": "1", "home_sets": "0-1-(1)", "away_sets": "1-0-(0)"}, "1", "extra_time"),
    ({"home_score": 2, "away_score": 1, "winner": "1", "home_sets": "0-1-(1)", "away_sets": "1-0-(0)"}, "1", "extra_time"),
    ({"home_score": 2, "away_score": 2, "winner": "X", "home_sets": "1-1-(0)+3", "away_sets": "2-0-(0)+4"}, "2", "penalties"),
])
def test_decisive_results_preserve_original_result(score, side, decision):
    before = deepcopy(score)
    assert decisive_winner(score) == (side, decision)
    assert score == before


@pytest.mark.parametrize("score", [
    {"home_score": 1, "away_score": 1, "winner": "X"},
    {"home_score": 2, "away_score": 1, "winner": "2"},
    {"home_score": 2, "away_score": 2, "winner": "1", "home_sets": "1-1-(0)+3", "away_sets": "2-0-(0)+4"},
    {"home_score": 2, "away_score": 1, "winner": "1", "home_sets": "1-1+4", "away_sets": "1-0+3"},
])
def test_unfinished_or_contradictory_score_never_names_a_champion(score):
    assert decisive_winner(score)[0] is None


@pytest.mark.parametrize("change,reason", [
    ({"season_year": 2025}, "wrong_cup_edition"),
    ({"starts_at": NOW + timedelta(days=1)}, "cup_evidence_after_cutoff"),
    ({"observed_at": NOW + timedelta(seconds=1)}, "cup_evidence_after_cutoff"),
    ({"status": "final_absent", "winner": None}, "final_absent"),
    ({"status": "final_ambiguous", "winner": None}, "final_ambiguous"),
])
def test_pending_dependency_keeps_base(change, reason):
    resolution = resolve_qualification(BASE, table(), replace(cup(), **change), 2026, NOW)
    assert resolution.status == "PENDING" and resolution.reason == reason
    assert resolution.effective_rules is BASE


def test_missing_identity_does_not_match_by_display_name():
    standings = table()
    standings["Club 7"].pop("participant_identity")
    resolution = resolve_qualification(BASE, standings, cup(), 2026, NOW)
    assert resolution.reason == "incomplete_participant_table"
    assert resolution.effective_rules is BASE


def m5(winner, home_rank, away_rank=10):
    standings = table()
    resolution = resolve_qualification(BASE, standings, cup(winner), 2026, NOW)
    event = _configured_event("laliga")
    event = replace(event, season_year=2026, starts_at=NOW + timedelta(hours=1),
                    home=replace(event.home, name=f"Club {home_rank}"),
                    away=replace(event.away, name=f"Club {away_rank}"))
    event.competition = replace(event.competition, qualification_resolution=resolution)
    result = calculate_contextual_competitive_cost_engine(SimpleNamespace(current_standings=standings),
                                                         event.to_identity(), event.competition)
    return result, event, standings


@pytest.mark.parametrize("winner,rank,reference", [(6, 5, 7), (7, 6, 8), (5, 6, 7)])
def test_m5_cut_skips_already_qualified_cup_winner(winner, rank, reference):
    result, _, standings = m5(winner, rank)
    assert result.raw["m5_status"] == "ACTIVE"
    assert result.raw["home"]["cutoff_reference"] == standings[f"Club {reference}"]
    assert result.raw["home"]["points_cutoff"] == standings[f"Club {reference}"]["points"]
    assert result.raw["qualification_resolution"]["cups"][0]["event_id"] == 100


@pytest.mark.parametrize("winner", [5, 6, 7, 12])
def test_cup_berth_is_protected(winner):
    result, _, _ = m5(winner, winner)
    assert result.raw["home"]["cup_berth_protected"] is True
    assert result.raw["home"]["final_cost"] == 0


@pytest.mark.parametrize("winner,objective", [(4, "COMPETITION_QUALIFICATION"), (18, "DIVISION_RETENTION")])
def test_champions_pressure_and_relegation_survive_cup_title(winner, objective):
    result, _, _ = m5(winner, winner)
    assert result.raw["home"]["cup_berth_protected"] is False
    assert result.raw["home"]["objective_type"] == objective
    assert result.raw["home"]["final_cost"] > 0


def test_capture_is_stable_and_contains_evidence_without_requerying():
    result, event, standings = m5(6, 5)
    captured = to_json_value({"competition_context": event.competition, "streak_analysis": standings})
    again = calculate_contextual_competitive_cost_engine(SimpleNamespace(current_standings=standings),
                                                        event.to_identity(), event.competition)
    assert to_json_value(result) == to_json_value(again)
    assert captured == to_json_value({"competition_context": event.competition, "streak_analysis": standings})
    evidence = captured["competition_context"]["qualification_resolution"]["cups"][0]
    assert evidence["observation_id"] == "known-final"
    assert evidence["result"]["home_score"] == 1
    assert evidence["observed_at"] < NOW.isoformat()


@pytest.fixture
def session():
    engine = create_engine("sqlite://")
    for model in (Participant, Competition, Season, Event, Result, EventOutcomeObservation):
        model.__table__.create(engine)
    with Session(engine) as session:
        session.add_all([
            Participant(participant_id=515, source="sofascore", source_participant_id=1, name="Atlético Madrid"),
            Participant(participant_id=509, source="sofascore", source_participant_id=2, name="Real Sociedad"),
            Competition(competition_id=77, source="sofascore", source_unique_tournament_id=329,
                        source_tournament_id=150, canonical_name="Copa del Rey", display_name="Copa del Rey"),
            Season(id=82988, name="Copa del Rey 25/26", year=2025),
            Season(id=104108, name="Copa del Rey 26/27", year=2026),
        ])
        session.flush()
        session.add(Event(id=83369, slug="final", starts_at=datetime(2026, 4, 18, 19, tzinfo=timezone.utc),
                          sport="Football", competition="Copa", home_team="Atleti", away_team="Real",
                          season_id=82988, round="final", home_participant_id=515,
                          away_participant_id=509, competition_id=77))
        session.flush()
        session.add(Result(event_id=83369, home_score=2, away_score=2, winner="X",
                           home_sets="1-1-(0)+3", away_sets="2-0-(0)+4"))
        session.commit()
        yield session
    engine.dispose()


def test_83369_penalties_and_wrong_edition_are_read_only(session):
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, final_round=BASE.cup_qualifications[0].final_round, read_at=NOW)
    assert evidence.winner == identity(2) and evidence.decision == "penalties"
    assert evidence.observed_at == NOW and evidence.evidence_source == "legacy_result_read"
    assert evidence.event_id == 83369 and evidence.season_id == 82988
    resolution = resolve_qualification(BASE, table(), evidence, 2026, NOW)
    assert resolution.reason == "wrong_cup_edition" and resolution.effective_rules is BASE
    assert read_cup_final(session, ids.COPA_DEL_REY, 2026, NOW, final_round=BASE.cup_qualifications[0].final_round, read_at=NOW).status == "final_absent"
    assert session.get(Result, 83369).winner == "X"
    assert session.scalars(select(EventOutcomeObservation)).all() == []


def test_legacy_result_cannot_be_known_before_its_read(session):
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW - timedelta(days=1), final_round=BASE.cup_qualifications[0].final_round, read_at=NOW)
    assert evidence.status == "legacy_result_not_known_at_cutoff" and evidence.winner is None


def test_repository_accepts_the_configured_round_and_a_tournament_family(session):
    event = session.get(Event, 83369)
    event.round = "title_match"
    session.flush()
    family = replace(ids.COPA_DEL_REY, source_tournament_id=None)

    evidence = read_cup_final(session, family, 2025, NOW, final_round="title_match", read_at=NOW)

    assert evidence.event_id == event.id
    assert evidence.winner == identity(2)


def observation(session, name, when, payload):
    session.add(EventOutcomeObservation(observation_id=name, event_id=83369, source="sofascore",
                                        received_at=when, payload=payload))
    session.flush()


def test_use_known_observation_even_if_result_and_later_observations_differ(session):
    original = {"home_score": 2, "away_score": 2, "winner": "X",
                "home_sets": "1-1-(0)+3", "away_sets": "2-0-(0)+4"}
    observation(session, "old", FINAL, original)
    observation(session, "future", NOW + timedelta(seconds=1), {"home_score": 3, "away_score": 0, "winner": "1"})
    session.get(Result, 83369).winner = "1"
    session.flush()
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, final_round=BASE.cup_qualifications[0].final_round, read_at=NOW)
    assert evidence.observation_id == "old" and evidence.winner == identity(2)
    assert evidence.result == original


def test_future_observation_never_falls_back_to_current_result(session):
    observation(session, "future", NOW + timedelta(seconds=1), {"home_score": 3, "away_score": 0, "winner": "1"})
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, final_round=BASE.cup_qualifications[0].final_round, read_at=NOW)
    assert evidence.status == "no_observation_before_cutoff" and evidence.winner is None


def test_provider_observation_uses_terminal_score_and_penalties(session):
    observation(session, "raw", FINAL, {"status": {"type": "finished", "code": 120}, "winnerCode": 2,
                "homeScore": {"current": 5, "display": 2, "penalties": 3},
                "awayScore": {"current": 6, "display": 2, "penalties": 4}})
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, final_round=BASE.cup_qualifications[0].final_round, read_at=NOW)
    assert evidence.status == "DECIDED" and evidence.winner == identity(2)


@pytest.mark.parametrize("kind,expected", [("future", "final_future"), ("duplicate", "final_ambiguous"),
                                          ("open", "final_unfinished"), ("contradictory", "contradictory_winner")])
def test_unusable_finals_remain_pending(session, kind, expected):
    event = session.get(Event, 83369)
    if kind == "future":
        event.starts_at = NOW + timedelta(days=1)
    elif kind == "duplicate":
        session.add(Event(id=90000, slug="duplicate", starts_at=FINAL, sport="Football", competition="Copa",
                          home_team="Atleti", away_team="Real", season_id=82988, round="final", competition_id=77))
    elif kind == "open":
        session.delete(session.get(Result, 83369))
    else:
        session.get(Result, 83369).winner = "1"
    session.flush()
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, final_round=BASE.cup_qualifications[0].final_round, read_at=NOW)
    assert evidence.status == expected and evidence.winner is None


def test_participant_identity_survives_standings_calculation():
    from modules.alerts.matchup_streak_analysis.standings_engine import (
        _register_match_record, _finalize_standings_snapshot, _serialize_match_records,
    )
    record = {"event_id": 10, "startTimestamp": 1, "home_team": "Club 1", "away_team": "Club 2",
              "home_score": 1, "away_score": 0, "winner": "1", "game_total": 1,
              "home_participant_identity": asdict(identity(1)), "away_participant_identity": asdict(identity(2))}
    stats = {}
    _register_match_record(record, stats, BASE, 999, set())
    standings = _finalize_standings_snapshot(stats, BASE, [record])
    assert standings["Club 1"]["participant_identity"] == asdict(identity(1))
    assert standings["Club 2"]["participant_identity"] == asdict(identity(2))
    assert _serialize_match_records([record])[0]["home_participant_identity"] == asdict(identity(1))


@pytest.mark.parametrize("competition", ["shl", "brasileirao"])
def test_profiles_without_an_executable_cup_rule_do_not_read_cup_evidence(monkeypatch, competition):
    from modules.pillars.pillar_1_team_structure import qualification_preparation as preparation
    def unexpected(*args, **kwargs):
        pytest.fail("Only configured cup rules can read final evidence")
    monkeypatch.setattr(preparation, "read_cup_final", unexpected)
    event = _configured_event(competition)
    before = event.competition.competitive_rules
    resolved = preparation.prepare_qualification_resolution(event.to_identity(), event.competition, None)
    assert resolved.qualification_resolution is None and resolved.competitive_rules is before


def test_pipeline_captures_resolution_after_standings_and_before_calculation(session, monkeypatch):
    from contextlib import contextmanager
    from infrastructure.persistence.database import DatabaseManager
    from infrastructure.persistence.repositories import cup_final_repository, pillar_mining_repository
    from modules.jobs.pre_start_check_job import pillar_pipeline as pipeline
    from modules.pillars.pillar_1_team_structure import qualification_preparation as preparation
    from infrastructure.persistence.repositories.pillar_mining_repository import PillarMiningRepository

    session.add(Event(id=100, slug="current-final", starts_at=FINAL, sport="Football", competition="Copa",
                      home_team="Atleti", away_team="Real", season_id=104108, round="final",
                      competition_id=77, home_participant_id=515, away_participant_id=509))
    session.flush()
    session.add(Result(event_id=100, home_score=0, away_score=1, winner="2"))
    session.commit()
    @contextmanager
    def read_session():
        yield session
    monkeypatch.setattr(preparation, "db_manager", SimpleNamespace(get_session=read_session))
    for module in (preparation, cup_final_repository, pipeline):
        monkeypatch.setattr(module, "utc_now", lambda: NOW)
    manager = DatabaseManager("sqlite://")
    manager.create_tables()
    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    standings = table()
    event = replace(_configured_event("laliga"), season_year=2026, starts_at=NOW + timedelta(minutes=5),
                    round="regular_season", success=True)
    monkeypatch.setattr(pipeline, "resolve_matchup_streak_analysis",
                        lambda *_args, **_kwargs: (SimpleNamespace(current_standings=standings,
                                                  to_dict=lambda: {"current_standings": standings}), False))
    processor = pipeline.EventPillarProcessor(enabled_pillars={f"pillar_{n}": n == 1 for n in range(1, 6)})
    original_calculate = processor._calculate_p1
    def calculate(*args):
        prepared = args[1].qualification_resolution
        assert prepared.applied and prepared.cup.event_id == 100
        # No subsequent cup reads are allowed once preparation is complete.
        monkeypatch.setattr(preparation, "read_cup_final", lambda *_a, **_k: pytest.fail("calculation queried cup"))
        return original_calculate(*args)
    monkeypatch.setattr(processor, "_calculate_p1", calculate)
    try:
        output = processor.process_event(event)
        stored = PillarMiningRepository.get_evaluation(output["evaluation_id"])
        captured = stored["inputs"]["p1"]["competition_context"]["qualification_resolution"]
        captured_cup = captured["cups"][0]
        captured_allocation = captured["cup_allocations"][0]
        assert captured["status"] == "AS_OF_TABLE" and captured_cup["season_id"] == 104108
        assert captured_allocation["configured_competition_name"] == "Copa del Rey"
        assert captured_cup["evidence_source"] == "legacy_result_read"
        assert captured_cup["observed_at"] <= stored["evaluation_as_of"]
        assert captured["effective_rules"]["objectives"][1]["rank_selection"]["rank_end"] == 6
        assert captured_allocation["qualification_destination"]["destination_phase"] == "LEAGUE_PHASE"
        assert captured_allocation["qualification_destination"]["qualification_path"] == "DIRECT"
        assert stored["inputs"]["p1_preparation_error"] is None
        # The stored material inputs remain stable if the operational result changes.
        before = deepcopy(stored)
        session.get(Result, 100).winner = "1"
        session.commit()
        assert PillarMiningRepository.get_evaluation(output["evaluation_id"]) == before
    finally:
        manager.engine.dispose()
