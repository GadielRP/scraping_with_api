from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from infrastructure.persistence.models import (
    Competition, DisciplinaryIncidentTotal, Event, EventSourceMapping, Participant, Result, Season,
)
from modules.alerts.matchup_streak_analysis import standings_engine
from modules.alerts.matchup_streak_analysis.season_aggregate_standings_calculator import (
    calculate_season_aggregate_standings,
)
from modules.competition import competition_ids as ids
from modules.competition.competition_profiles import get_competition_profile, get_season_aggregate_profile
from modules.pillars.mining.serialization import to_json_value
from modules.pillars.pillar_1_team_structure import qualification_preparation
from tests.pillars.pillar_1_team_structure.test_competition_rules import _configured_event


@pytest.fixture(params=[2026, 2027])
def season_data(request):
    season_year = request.param
    # The season label and calendar year of kickoff can differ.
    cutoff = datetime(season_year + 1, 1, 8, tzinfo=timezone.utc)
    engine = create_engine("sqlite://")
    for model in (Competition, Participant, Season, Event, Result, EventSourceMapping, DisciplinaryIncidentTotal):
        model.__table__.create(engine)
    with Session(engine) as session:
        profile = get_season_aggregate_profile(ids.ARGENTINA_CLAUSURA)
        profile = replace(profile, number_of_teams=2, sources=tuple(
            replace(source, games_per_team=1) for source in profile.sources
        ))
        session.add_all([
            Participant(participant_id=1, source="sofascore", source_participant_id=10, name="Club"),
            Participant(participant_id=2, source="sofascore", source_participant_id=20, name="Club"),
            Season(id=1, year=season_year),
            Season(id=2, year=season_year - 1),
        ])
        for local_id, identity in enumerate(
            (ids.ARGENTINA_APERTURA, ids.ARGENTINA_CLAUSURA, ids.ARGENTINA_APERTURA_PLAYOFFS), start=1,
        ):
            session.add(Competition(
                competition_id=local_id,
                source=identity.source,
                source_unique_tournament_id=identity.source_unique_tournament_id,
                source_tournament_id=identity.source_tournament_id,
                canonical_name="Liga Profesional",
                display_name="Liga Profesional",
            ))

        for event_id, competition, year_id, before, round_name, score in (
            (1, 1, 1, 180, "regular_season", (0, 0)),
            (2, 2, 1, 1, "regular_season", (2, 1)),
            (3, 3, 1, 100, "regular_season", (5, 0)),
            (4, 1, 1, 101, "final", (5, 0)),
            (5, 1, 2, 200, "regular_season", (5, 0)),
            (6, 2, 1, 0, "regular_season", (5, 0)),
        ):
            session.add(Event(
                id=event_id, slug=f"match-{event_id}", sport="Football", competition="Liga Profesional",
                competition_id=competition, season_id=year_id, round=round_name,
                starts_at=cutoff - timedelta(days=before),
                home_team="Club", away_team="Club", home_participant_id=1, away_participant_id=2,
            ))
            session.add(Result(event_id=event_id, home_score=score[0], away_score=score[1]))
        session.flush()
        yield session, profile, season_year, cutoff
    engine.dispose()


def test_annual_table_uses_exact_editions_regular_round_and_cutoff(season_data):
    session, profile, season_year, cutoff = season_data
    interim = calculate_season_aggregate_standings(
        session, profile, season_year=season_year, kickoff_cutoff=cutoff - timedelta(days=2),
    )
    final = calculate_season_aggregate_standings(
        session, profile, season_year=season_year, kickoff_cutoff=cutoff,
    )

    assert not interim.regular_season_complete
    assert len(interim.standings) == 2
    assert {row.points for row in interim.standings} == {1}
    assert {row.ranking_reason for row in interim.standings} == {"MISSING_FAIR_PLAY_POINTS"}
    assert final.regular_season_complete
    assert final.season_year == season_year
    assert [
        (row.participant.source_participant_id, row.rank, row.points, row.games_played)
        for row in final.standings
    ] == [(10, 1, 4, 2), (20, 2, 1, 2)]


def test_only_clausura_selects_shared_annual_profile():
    assert get_season_aggregate_profile(ids.ARGENTINA_APERTURA) is None
    assert get_season_aggregate_profile(ids.ARGENTINA_CLAUSURA) is not None
    assert get_season_aggregate_profile(ids.ARGENTINA_CLAUSURA_PLAYOFFS) is None


def _add_cards(session, event_id, home, away):
    session.add(EventSourceMapping(
        mapping_id=event_id, event_id=event_id, source="sofascore", source_event_id=str(event_id),
    ))
    for side, cards in (("home", home), ("away", away)):
        for kind, count in zip(("yellow_card", "red_card", "red_card_second_yellow"), cards):
            session.add(DisciplinaryIncidentTotal(
                event_source_mapping_id=event_id, type=kind, side=side, count=count, endpoint="comments",
            ))


def test_argentina_annual_fair_play_uses_both_editions_and_fewer_reds(season_data):
    session, profile, season_year, cutoff = season_data
    session.get(Result, 2).home_score = 1
    _add_cards(session, 1, (2, 1, 1), (2, 0, 0))
    _add_cards(session, 2, (0, 0, 0), (2, 0, 0))
    _add_cards(session, 3, (0, 0, 0), (0, 1, 0))  # Playoff is outside the annual table.

    result = calculate_season_aggregate_standings(
        session, profile, season_year=season_year, kickoff_cutoff=cutoff,
    )
    assert [(row.participant.source_participant_id, row.rank, row.fair_play_points,
             row.fair_play_games_counted) for row in result.standings] == [
        (20, 1, 4, 2),
        (10, 2, 4, 2),
    ]


def test_clausura_timeline_carries_apertura_cards_without_its_results(season_data, monkeypatch):
    session, _, season_year, cutoff = season_data
    _add_cards(session, 1, (2, 1, 1), (0, 0, 0))
    profile = replace(get_competition_profile(ids.ARGENTINA_CLAUSURA), number_of_teams=2)
    apertura = replace(get_competition_profile(ids.ARGENTINA_APERTURA), total_regular_season_games=1)
    monkeypatch.setattr(standings_engine, "get_competition_profile", lambda _: profile)
    monkeypatch.setattr(standings_engine, "get_profile_by_identity", lambda _: apertura)
    monkeypatch.setattr(standings_engine.db_manager, "get_session", lambda: nullcontext(session))
    match_time = cutoff - timedelta(days=181)
    record = {
        "event_id": 2, "startTimestamp": int(match_time.timestamp()),
        "starts_at": match_time, "home_team": "Boca Juniors", "away_team": "Vélez Sarsfield",
        "home_score": 1, "away_score": 1, "winner": "X", "game_total": 2,
        "home_participant_identity": {"source": "sofascore", "source_participant_id": 10},
        "away_participant_identity": {"source": "sofascore", "source_participant_id": 20},
        "home_yellow_cards": 0, "home_red_cards": 0, "home_second_yellow_dismissals": 0,
        "away_yellow_cards": 1, "away_red_cards": 0, "away_second_yellow_dismissals": 0,
    }
    calculator = standings_engine.HistoricalStandingsCalculator()
    monkeypatch.setattr(calculator, "_fetch_match_records_before_cutoff", lambda **_: [record])
    early = (match_time + timedelta(hours=1)).timestamp()
    late = cutoff.timestamp()
    snapshots = calculator.calculate_standings_timeline(1, (early, late), competition_id=2, season_year=season_year)
    assert snapshots[early]["Boca Juniors"]["ranking_reason"] == "MISSING_FAIR_PLAY_POINTS"
    boca = snapshots[late]["Boca Juniors"]
    velez = snapshots[late]["Vélez Sarsfield"]
    assert (boca["points"], boca["games_played"], boca["fair_play_games_counted"]) == (1, 1, 2)
    assert (boca["fair_play_points"], boca["rank"], velez["fair_play_points"], velez["rank"]) == (4, 2, 1, 1)
    assert calculator.calculate_standings_at(1, late, competition_id=2) == snapshots[late]

    monkeypatch.setattr(standings_engine, "get_competition_profile", lambda _: apertura)
    local = calculator.calculate_standings_at(1, late, competition_id=1, season_year=season_year)
    assert (local["Boca Juniors"]["fair_play_points"], local["Boca Juniors"]["rank"]) == (0, 1)


def test_pre_start_prepares_annual_standings_for_capture(monkeypatch, season_data):
    session, profile, season_year, cutoff = season_data
    monkeypatch.setattr(qualification_preparation.db_manager, "get_session", lambda: nullcontext(session))
    monkeypatch.setattr(qualification_preparation, "get_season_aggregate_profile", lambda *_: profile)
    event = _configured_event("liga_profesional_de_argentina_clausura")
    identity = replace(event.to_identity(), season_year=season_year, starts_at=cutoff)

    prepared = qualification_preparation.prepare_qualification_resolution(
        identity, event.competition, event.streak_analysis, as_of=cutoff + timedelta(hours=1),
    )

    captured = to_json_value(prepared)["season_aggregate_standings"]
    assert captured["season_year"] == season_year
    assert captured["kickoff_cutoff"] == cutoff.isoformat()
    assert captured["regular_season_complete"]
    assert captured["standings"][0]["participant"]["source_participant_id"] == 10


def test_pre_start_does_not_guess_missing_season_year(monkeypatch):
    def unexpected_session():
        pytest.fail("An annual table requires the event season year")

    monkeypatch.setattr(qualification_preparation.db_manager, "get_session", unexpected_session)
    event = _configured_event("liga_profesional_de_argentina_clausura")
    identity = replace(event.to_identity(), season_year=None)

    prepared = qualification_preparation.prepare_qualification_resolution(identity, event.competition, None)

    assert prepared.season_aggregate_standings is None
