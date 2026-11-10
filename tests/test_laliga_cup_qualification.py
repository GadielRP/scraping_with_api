"""LaLiga 2026/27 -> Europe 2027/28, with identity and knowledge-time evidence."""

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
from modules.competition.competition_profiles import COMPETITION_PROFILES
from modules.competition.qualification_resolution import (
    CupFinalEvidence, ParticipantIdentity, decisive_winner, resolve_laliga_qualification,
)
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
    resolution = resolve_laliga_qualification(BASE, standings, cup(winner), 2026, NOW)
    assert resolution.status == "PROVISIONAL"
    assert resolution.destination_season == "2027/28"
    assert [rank for rank, destination in resolution.league_assignments if destination == ids.UEFA_EUROPA_LEAGUE] == el
    assert (conference, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) in resolution.league_assignments
    assert resolution.champion_destination == (ids.UEFA_CHAMPIONS_LEAGUE if winner <= 4 else ids.UEFA_EUROPA_LEAGUE)
    assert identity(winner) not in resolution.eligible_participants
    assert standings == before and dict(COMPETITION_PROFILES) == catalogue
    assert COMPETITION_PROFILES[ids.LALIGA] is BASE
    assert resolution.effective_rules is not BASE


def test_final_only_when_every_team_completed_its_season():
    standings = table()
    for row in standings.values():
        row["games_played"] = 38
    assert resolve_laliga_qualification(BASE, standings, cup(), 2026, NOW).status == "FINAL"


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
    resolution = resolve_laliga_qualification(BASE, table(), replace(cup(), **change), 2026, NOW)
    assert resolution.status == "PENDING" and resolution.reason == reason
    assert resolution.effective_rules is BASE


def test_missing_identity_does_not_match_by_display_name():
    standings = table()
    standings["Club 7"].pop("participant_identity")
    resolution = resolve_laliga_qualification(BASE, standings, cup(), 2026, NOW)
    assert resolution.reason == "incomplete_participant_table"
    assert resolution.effective_rules is BASE


def m5(winner, home_rank, away_rank=10):
    standings = table()
    resolution = resolve_laliga_qualification(BASE, standings, cup(winner), 2026, NOW)
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
    assert result.raw["qualification_resolution"]["cup"]["event_id"] == 100


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
    evidence = captured["competition_context"]["qualification_resolution"]["cup"]
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
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, read_at=NOW)
    assert evidence.winner == identity(2) and evidence.decision == "penalties"
    assert evidence.observed_at == NOW and evidence.evidence_source == "legacy_result_read"
    assert evidence.event_id == 83369 and evidence.season_id == 82988
    resolution = resolve_laliga_qualification(BASE, table(), evidence, 2026, NOW)
    assert resolution.reason == "wrong_cup_edition" and resolution.effective_rules is BASE
    assert read_cup_final(session, ids.COPA_DEL_REY, 2026, NOW, read_at=NOW).status == "final_absent"
    assert session.get(Result, 83369).winner == "X"
    assert session.scalars(select(EventOutcomeObservation)).all() == []


def test_legacy_result_cannot_be_known_before_its_read(session):
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW - timedelta(days=1), read_at=NOW)
    assert evidence.status == "legacy_result_not_known_at_cutoff" and evidence.winner is None


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
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, read_at=NOW)
    assert evidence.observation_id == "old" and evidence.winner == identity(2)
    assert evidence.result == original


def test_future_observation_never_falls_back_to_current_result(session):
    observation(session, "future", NOW + timedelta(seconds=1), {"home_score": 3, "away_score": 0, "winner": "1"})
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, read_at=NOW)
    assert evidence.status == "no_observation_before_cutoff" and evidence.winner is None


def test_provider_observation_uses_terminal_score_and_penalties(session):
    observation(session, "raw", FINAL, {"status": {"type": "finished", "code": 120}, "winnerCode": 2,
                "homeScore": {"current": 5, "display": 2, "penalties": 3},
                "awayScore": {"current": 6, "display": 2, "penalties": 4}})
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, read_at=NOW)
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
    evidence = read_cup_final(session, ids.COPA_DEL_REY, 2025, NOW, read_at=NOW)
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


def test_other_competitions_do_not_read_cup_evidence(monkeypatch):
    from modules.pillars.pillar_1_team_structure import qualification_preparation as preparation
    def unexpected(*args, **kwargs):
        pytest.fail("Unrelated competitions cannot acquire this dependency")
    monkeypatch.setattr(preparation, "read_cup_final", unexpected)
    event = _configured_event("serie_a")
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
        assert captured["status"] == "PROVISIONAL" and captured["cup"]["season_id"] == 104108
        assert captured["cup"]["evidence_source"] == "legacy_result_read"
        assert captured["cup"]["observed_at"] <= stored["evaluation_as_of"]
        assert captured["effective_rules"]["objectives"][1]["rank_end"] == 6
        assert stored["inputs"]["p1_preparation_error"] is None
        # The stored material inputs remain stable if the operational result changes.
        before = deepcopy(stored)
        session.get(Result, 100).winner = "1"
        session.commit()
        assert PillarMiningRepository.get_evaluation(output["evaluation_id"]) == before
    finally:
        manager.engine.dispose()
