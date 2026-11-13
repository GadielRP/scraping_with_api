"""Qualification berths: allocation from a profile, evidence gating and the M5 view."""

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
from infrastructure.persistence.repositories.cup_final_repository import read_cup_finals
from modules.competition import competition_ids as ids
from modules.competition.berth_allocation_engine.berth_allocation_engine import allocate_berths
from modules.competition.berth_allocation_engine.models import BerthResolution
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.competition_profiles import COMPETITION_PROFILES
from modules.competition.competitive_rules import QualificationSources
from modules.competition.final_outcome import CupFinalEvidence, ParticipantIdentity, decisive_winner
from modules.competition.qualification_rules import build_qualification_rules
from modules.competition.standings_rows import table_rows
from modules.pillars.mining.serialization import to_json_value
from modules.pillars.pillar_1_team_structure import qualification_preparation
from tests.pillars.pillar_1_team_structure.test_competition_rules import _configured_event

NOW = datetime(2027, 5, 1, tzinfo=timezone.utc)
FINAL = NOW - timedelta(days=7)
BASE = COMPETITION_PROFILES[ids.LALIGA]


def identity(rank):
    return ParticipantIdentity("sofascore", rank)


def table(*, games=33):
    return {f"Club {rank}": {"rank": rank, "points": 83 - rank * 3, "games_played": games,
                              "participant_identity": asdict(identity(rank))}
            for rank in range(1, 21)}


def cup(winner_rank=7, competition=ids.COPA_DEL_REY):
    return CupFinalEvidence(competition, 2026, "DECIDED", event_id=100,
                            season_id=777, starts_at=FINAL, observed_at=FINAL + timedelta(hours=3),
                            observation_id="known-final", evidence_source="sofascore",
                            home=identity(winner_rank), away=identity(101), winner=identity(winner_rank),
                            result={"home_score": 1, "away_score": 0, "winner": "1"}, decision="regulation")


def allocate(profile, *cups, standings=None):
    standings = standings or table()
    rules = build_qualification_rules(profile)
    rows = table_rows(standings)
    outcome = allocate_berths(rules, rows, season_year=2026, tournament_evidence=cups)
    return rules, rows, outcome


def destinations(outcome):
    """Table rank -> destination competition. Clubs outside the table use their own id."""
    return {berth.participant.source_participant_id: berth.destination.destination_competition_identity
            for berth in outcome.berths if berth.participant is not None}


def ranks_for(outcome, destination):
    return sorted(rank for rank, target in destinations(outcome).items() if target == destination)


@pytest.mark.parametrize("winner,el,conference", [
    (1, [5, 6], 7), (4, [5, 6], 7), (5, [5, 6], 7), (6, [5, 6], 7),
    (7, [5, 7], 6), (12, [5, 12], 6), (18, [5, 18], 6), (99, [5, 99], 6),
])
def test_laliga_cup_berth_goes_to_the_winner_or_to_the_next_ranked_club(winner, el, conference):
    standings, catalogue = table(), dict(COMPETITION_PROFILES)
    before = deepcopy(standings)
    _, _, outcome = allocate(BASE, cup(winner), standings=standings)
    assert outcome.destination_season_year == 2027
    assert ranks_for(outcome, ids.UEFA_EUROPA_LEAGUE) == el
    assert destinations(outcome)[conference] == ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION
    assert destinations(outcome)[winner] == (ids.UEFA_CHAMPIONS_LEAGUE if winner <= 4 else ids.UEFA_EUROPA_LEAGUE)
    assert standings == before and dict(COMPETITION_PROFILES) == catalogue


def test_table_berths_are_projected_and_a_decided_cup_is_confirmed():
    outcome = allocate(BASE, cup())[2]
    assert {b.resolution for b in outcome.berths if b.source is None} == {BerthResolution.PROJECTED}
    assert outcome.berths_by_source[ids.COPA_DEL_REY].resolution is BerthResolution.CONFIRMED


@pytest.mark.parametrize("winner,cup_holder,europa_qualifying,conference_ranks", [
    (2, 3, 4, [5, 6, 7, 8]),
    (3, 3, 4, [5, 6, 7, 8]),
    (6, 6, 3, [4, 5, 7, 8]),
])
def test_eredivisie_cup_berth_shifts_the_table_places_after_it(winner, cup_holder, europa_qualifying, conference_ranks):
    _, _, outcome = allocate(COMPETITION_PROFILES[ids.EREDIVISIE], cup(winner, ids.KNVB_BEKER))
    assert outcome.berths_by_source[ids.KNVB_BEKER].participant == identity(cup_holder)
    assert ranks_for(outcome, ids.UEFA_EUROPA_LEAGUE_QUALIFICATION) == [europa_qualifying]
    assert ranks_for(outcome, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) == conference_ranks


@pytest.mark.parametrize("winner,recipient,europa_qualifying,conference", [
    (2, 4, 5, 6), (4, 4, 5, 6), (5, 5, 4, 6), (7, 7, 4, 5),
])
def test_portugal_cup_updates_the_qualifying_round_berths(winner, recipient, europa_qualifying, conference):
    _, _, outcome = allocate(COMPETITION_PROFILES[ids.LIGA_PORTUGAL], cup(winner, ids.TACA_DE_PORTUGAL))
    assert outcome.berths_by_source[ids.TACA_DE_PORTUGAL].participant == identity(recipient)
    assert ranks_for(outcome, ids.UEFA_EUROPA_LEAGUE_QUALIFICATION) == [europa_qualifying]
    assert ranks_for(outcome, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) == [conference]


@pytest.mark.parametrize("winner,recipient", [(4, 6), (8, 8)])
def test_saudi_king_cup_cedes_its_berth_only_when_the_winner_has_an_elite_berth(winner, recipient):
    _, _, outcome = allocate(COMPETITION_PROFILES[ids.SAUDI_PRO_LEAGUE], cup(winner, ids.SAUDI_KINGS_CUP))
    assert ranks_for(outcome, ids.AFC_CHAMPIONS_LEAGUE_TWO) == [recipient]


def test_premier_league_cups_share_berths_in_priority_order():
    profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]
    fa, efl = cup(1, ids.FA_CUP), cup(2, ids.EFL_CUP)

    efl_only = allocate(profile, efl)[2]
    fa_only = allocate(profile, fa)[2]
    both = allocate(profile, efl, fa)[2]

    assert efl_only.berths_by_source[ids.FA_CUP].issue == "final_absent"
    assert ranks_for(efl_only, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) == [6]
    assert fa_only.berths_by_source[ids.EFL_CUP].issue == "final_absent"
    assert ranks_for(fa_only, ids.UEFA_EUROPA_LEAGUE) == [5, 6]
    assert ranks_for(both, ids.UEFA_EUROPA_LEAGUE) == [5, 6]
    assert ranks_for(both, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) == [7]
    assert {b.resolution for b in both.berths} == {BerthResolution.PROJECTED}  # reassigned berths follow the table


@pytest.mark.parametrize("winner, reallocate, expected_ranks, lower_ranks", [
    (4, True, [6, 7, 8], [9, 10]),
    (4, False, [6, 7], [8, 9]),
    (7, True, [6, 7, 8], [9, 10]),
])
def test_allocation_uses_the_profile_for_destinations_and_slot_counts(winner, reallocate, expected_ranks, lower_ranks):
    higher, destination, lower, cup_id = (CompetitionIdentity(i, source="test") for i in range(1, 5))
    template = BASE.objectives[0]
    objectives = tuple(
        replace(
            template,
            qualification_destination=replace(
                template.qualification_destination,
                destination_competition_identity=target,
                destination_competition_name=f"Test destination {target.source_tournament_id}",
            ),
            rank_selection=replace(template.rank_selection, rank_start=start, rank_end=end, reference_rank=end),
        )
        for target, start, end in ((higher, 1, 5), (destination, 6, 7), (lower, 8, 9))
    )
    berth, = BASE.qualification_sources.tournament_berths
    profile = replace(BASE, objectives=objectives, qualification_sources=QualificationSources(tournament_berths=(replace(
        berth, competition=cup_id, before_table_slot=6, reallocate_to_table=reallocate,
        destination=replace(berth.destination, destination_competition_identity=destination),
    ),)))
    standings = {name: row for name, row in table().items() if row["rank"] <= 12}

    _, _, outcome = allocate(profile, replace(cup(winner), competition=cup_id), standings=standings)

    assert destinations(outcome)[winner] == (higher if winner == 4 else destination)
    assert ranks_for(outcome, destination) == expected_ranks
    assert ranks_for(outcome, lower) == lower_ranks


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
    ({"season_year": 2025}, "final_absent"),
    ({"status": "final_absent", "winner": None}, "final_absent"),
    ({"status": "final_ambiguous", "winner": None}, "final_ambiguous"),
])
def test_pending_cup_leaves_the_table_berths_unchanged(change, reason):
    _, _, outcome = allocate(BASE, replace(cup(), **change))
    assert outcome.berths_by_source[ids.COPA_DEL_REY].resolution is BerthResolution.UNRESOLVED
    assert outcome.berths_by_source[ids.COPA_DEL_REY].issue == reason
    assert ranks_for(outcome, ids.UEFA_CHAMPIONS_LEAGUE) == [1, 2, 3, 4]
    assert ranks_for(outcome, ids.UEFA_EUROPA_LEAGUE) == [5]
    assert ranks_for(outcome, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION) == [6]


@pytest.mark.parametrize("change", [
    {"starts_at": NOW + timedelta(days=1)},
    {"observed_at": NOW + timedelta(seconds=1)},
    {"starts_at": NOW - timedelta(hours=1)},
])
def test_final_unknown_before_the_event_or_the_cutoff_is_dropped(change):
    event_starts_at = NOW - timedelta(days=1)
    assert qualification_preparation._usable_finals((replace(cup(), **change),), event_starts_at, NOW) == ()
    assert qualification_preparation._usable_finals((cup(),), event_starts_at, NOW) == (cup(),)


def test_missing_identity_does_not_match_by_display_name():
    standings = table()
    standings["Club 7"].pop("participant_identity")
    assert table_rows(standings) is None


def shared_position(standings, *ranks):
    """Mark the clubs at these ranks as sharing one position, like the standings engine does."""
    for row in standings.values():
        if row["rank"] in ranks:
            row.update(ranking_status="SHARED_POSITION", ranking_reason=None)
    return standings


def test_a_tie_below_the_last_berth_does_not_discard_the_table():
    # Early-season tables share positions; the berths above the tie are still assigned.
    standings = shared_position(table(), 6, 7)
    profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]

    _, rows, outcome = allocate(profile, standings=standings)

    assert [row.rank for row in rows[5:7]] == [None, None]
    assert [b.table_rank for b in outcome.berths if b.participant] == [1, 2, 3, 4, 5]
    assert {b.issue for b in outcome.berths if b.participant} <= {None, "earlier_berth_pending"}


def test_a_tie_on_a_berth_boundary_leaves_only_the_berths_that_depend_on_it_pending():
    standings = shared_position(table(), 5, 6)
    profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]

    _, _, outcome = allocate(profile, standings=standings)

    assert [b.table_rank for b in outcome.berths[:4]] == [1, 2, 3, 4]
    europa = outcome.berths[5]
    assert (europa.participant, europa.resolution, europa.issue) == (None, BerthResolution.UNRESOLVED, "shared_position")


def test_rows_follow_the_position_even_if_the_table_is_not_ordered():
    standings = dict(reversed(list(table().items())))
    assert [row.rank for row in table_rows(standings)] == list(range(1, 21))


def test_an_unresolved_position_makes_the_table_unusable():
    standings = table()
    standings["Club 5"].update(rank=None, ranking_status="UNRESOLVED", ranking_reason="OFFICIAL_DECIDER_REQUIRED")
    assert table_rows(standings) is None


def test_each_tournament_berth_carries_the_final_that_decided_it():
    _, _, allocation = allocate(BASE, cup(6))
    captured = to_json_value(allocation)
    assert captured == to_json_value(allocation)
    assert set(captured) == {"destination_season_year", "berths", "table"}
    assert [b["final"] is not None for b in captured["berths"]] == [False] * 4 + [True, False, False]
    evidence = next(b["final"] for b in captured["berths"] if b["final"])
    assert evidence["observation_id"] == "known-final"
    assert evidence["result"]["home_score"] == 1
    assert evidence["observed_at"] < NOW.isoformat()


def test_an_absent_final_adds_no_evidence_beyond_the_issue():
    _, _, allocation = allocate(BASE)
    berth = allocation.berths_by_source[ids.COPA_DEL_REY]
    assert (berth.final, berth.issue) == (None, "final_absent")


def read_final(session, competition, season_year, as_of):
    return read_cup_finals(session, (competition,), season_year, as_of, read_at=NOW)[0]


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
    evidence = read_final(session, ids.COPA_DEL_REY, 2025, NOW)
    assert evidence.winner == identity(2) and evidence.decision == "penalties"
    assert evidence.observed_at == NOW and evidence.evidence_source == "legacy_result_read"
    assert evidence.event_id == 83369 and evidence.season_id == 82988
    assert read_final(session, ids.COPA_DEL_REY, 2026, NOW).status == "final_absent"
    assert session.get(Result, 83369).winner == "X"
    assert session.scalars(select(EventOutcomeObservation)).all() == []


def test_legacy_result_cannot_be_known_before_its_read(session):
    evidence = read_final(session, ids.COPA_DEL_REY, 2025, NOW - timedelta(days=1))
    assert evidence.status == "legacy_result_not_known_at_cutoff" and evidence.winner is None


def test_repository_accepts_a_tournament_family(session):
    family = replace(ids.COPA_DEL_REY, source_tournament_id=None)

    evidence = read_cup_finals(session, (family,), 2025, NOW, read_at=NOW)[0]

    assert evidence.event_id == 83369
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
    evidence = read_final(session, ids.COPA_DEL_REY, 2025, NOW)
    assert evidence.observation_id == "old" and evidence.winner == identity(2)
    assert evidence.result == original


def test_future_observation_never_falls_back_to_current_result(session):
    observation(session, "future", NOW + timedelta(seconds=1), {"home_score": 3, "away_score": 0, "winner": "1"})
    evidence = read_final(session, ids.COPA_DEL_REY, 2025, NOW)
    assert evidence.status == "no_observation_before_cutoff" and evidence.winner is None


def test_provider_observation_uses_terminal_score_and_penalties(session):
    observation(session, "raw", FINAL, {"status": {"type": "finished", "code": 120}, "winnerCode": 2,
                "homeScore": {"current": 5, "display": 2, "penalties": 3},
                "awayScore": {"current": 6, "display": 2, "penalties": 4}})
    evidence = read_final(session, ids.COPA_DEL_REY, 2025, NOW)
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
    evidence = read_final(session, ids.COPA_DEL_REY, 2025, NOW)
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


def test_profiles_without_tournament_berths_do_not_read_cup_evidence(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("Only profiles with qualification berths can read final evidence")
    monkeypatch.setattr(qualification_preparation, "read_cup_finals", unexpected)
    event = _configured_event("shl")
    before = event.competition.competitive_rules
    resolved = qualification_preparation.prepare_qualification(event.to_identity(), event.competition, None)
    assert resolved.berth_allocation is None and resolved.competitive_rules is before


def test_pipeline_captures_allocation_after_standings_and_before_calculation(session, monkeypatch):
    from contextlib import contextmanager
    from infrastructure.persistence.database import DatabaseManager
    from infrastructure.persistence.repositories import cup_final_repository, pillar_mining_repository
    from modules.jobs.pre_start_check_job import pillar_pipeline as pipeline
    from infrastructure.persistence.repositories.pillar_mining_repository import PillarMiningRepository
    preparation = qualification_preparation

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
        allocation = args[1].berth_allocation
        assert allocation.berths_by_source[ids.COPA_DEL_REY].final.event_id == 100
        # No subsequent cup reads are allowed once preparation is complete.
        monkeypatch.setattr(preparation, "read_cup_finals", lambda *_a, **_k: pytest.fail("calculation queried cup"))
        return original_calculate(*args)
    monkeypatch.setattr(processor, "_calculate_p1", calculate)
    try:
        output = processor.process_event(event)
        stored = PillarMiningRepository.get_evaluation(output["evaluation_id"])
        allocation = stored["inputs"]["p1"]["competition_context"]["berth_allocation"]
        cup_berth = next(b for b in allocation["berths"] if b["source_name"] == "Copa del Rey")
        captured_cup = cup_berth["final"]
        assert captured_cup["season_id"] == 104108
        assert captured_cup["evidence_source"] == "legacy_result_read"
        assert captured_cup["observed_at"] <= stored["evaluation_as_of"]
        assert allocation["table"] is None
        assert (cup_berth["source_name"], cup_berth["table_rank"]) == ("Copa del Rey", 5)
        assert cup_berth["destination"]["destination_phase"] == "LEAGUE_PHASE"
        assert cup_berth["destination"]["qualification_path"] == "DIRECT"
        assert stored["inputs"]["p1_preparation_error"] is None
        # The stored material inputs remain stable if the operational result changes.
        before = deepcopy(stored)
        session.get(Result, 100).winner = "1"
        session.commit()
        assert PillarMiningRepository.get_evaluation(output["evaluation_id"]) == before
    finally:
        manager.engine.dispose()


def test_payload_names_competitions_and_describes_each_berth():
    profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]
    fa, efl = cup(2, ids.FA_CUP), cup(2, ids.EFL_CUP)
    rules, _, outcome = allocate(profile, fa, efl)

    assert [b.source_name for b in outcome.berths] == [None, None, None, None, "FA Cup", None, "EFL Cup"]
    assert [b.table_rank for b in outcome.berths] == [1, 2, 3, 4, 5, 6, 7]
    assert [b.issue for b in outcome.berths] == [None] * 4 + ["tournament_winner_already_qualified", None, "tournament_winner_already_qualified"]
    reallocated = outcome.berths_by_source[ids.FA_CUP]
    assert reallocated.participant == identity(5)
    assert reallocated.source_name == "FA Cup"
    assert outcome.berths_by_source[ids.EFL_CUP].destination.destination_competition_name == (
        "UEFA Conference League, Qualification"
    )
    assert (fa.competition_name, rules.tournament_berths[0].competition_name) == ("FA Cup", "FA Cup")
    assert all(b.destination.destination_competition_name for b in outcome.berths)


def test_a_club_outside_the_table_cannot_take_two_cup_berths():
    profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]
    outsider = 99
    _, _, allocation = allocate(profile, cup(outsider, ids.FA_CUP), cup(outsider, ids.EFL_CUP))

    holders = [b.participant for b in allocation.berths if b.participant]
    assert len(holders) == len(set(holders))
    assert allocation.berths_by_source[ids.FA_CUP].participant == identity(outsider)
    efl = allocation.berths_by_source[ids.EFL_CUP]
    assert (efl.participant, efl.issue) == (identity(6), "tournament_winner_already_qualified")


def test_issue_is_empty_for_plain_assignments_and_names_what_a_berth_waits_on():
    profile = COMPETITION_PROFILES[ids.PREMIER_LEAGUE]

    nothing_played = allocate(profile)[2]
    assert [b.issue for b in nothing_played.berths] == [
        None, None, None, None, "final_absent", "earlier_berth_pending", "final_absent",
    ]

    # The EFL Cup is decided, but the FA Cup berth ahead of it is still pending.
    efl_only = allocate(profile, cup(12, ids.EFL_CUP))[2]
    efl = efl_only.berths_by_source[ids.EFL_CUP]
    assert (efl.resolution, efl.issue, efl.table_rank) == (BerthResolution.PROJECTED, "earlier_berth_pending", 12)


def test_a_profile_cannot_repeat_a_tournament():
    berth, = BASE.qualification_sources.tournament_berths
    with pytest.raises(ValueError, match="only one berth rule"):
        QualificationSources(tournament_berths=(berth, berth))
