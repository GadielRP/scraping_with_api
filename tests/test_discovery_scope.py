from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Competition
from infrastructure.persistence.repositories.competition_repository import CompetitionRepository
from infrastructure.settings import Config
from modules.competition.discovery_scope import (
    SourceCompetitionIds,
    is_tracked_source_event,
    load_tracked_source_competitions,
)


def test_canonical_competition_resolves_to_both_sofascore_ids_on_its_row():
    engine = create_engine("sqlite:///:memory:")
    Competition.__table__.create(engine)
    with Session(engine) as session:
        session.add_all(
            [
                Competition(
                    competition_id=167,
                    source="sofascore",
                    source_tournament_id=36,
                    source_unique_tournament_id=8,
                    canonical_name="LaLiga",
                    display_name="LaLiga",
                ),
                Competition(
                    competition_id=176,
                    source="sofascore",
                    source_tournament_id=132,
                    source_unique_tournament_id=132,
                    canonical_name="NBA",
                    display_name="NBA",
                ),
                Competition(
                    competition_id=178,
                    source="other-provider",
                    source_tournament_id=132,
                    source_unique_tournament_id=999,
                    canonical_name="NBA",
                    display_name="NBA",
                ),
            ]
        )
        session.flush()

        source_ids = CompetitionRepository.get_source_competition_id_pairs(
            session,
            source="sofascore",
            competition_ids={167},
        )

    assert source_ids == {(36, 8)}
    assert is_tracked_source_event(
        {"id": 1, "tournament": {"id": "36", "uniqueTournament": {"id": "8"}}},
        {SourceCompetitionIds(36, 8)},
    )
    assert is_tracked_source_event(
        {"id": 4, "tournament": {"id": "36"}},
        {SourceCompetitionIds(36, 8)},
    )
    assert is_tracked_source_event(
        {"id": 5, "tournament": {"uniqueTournament": {"id": "8"}}},
        {SourceCompetitionIds(36, 8)},
    )
    assert not is_tracked_source_event(
        {"id": 2, "tournament": {"id": "167", "uniqueTournament": {"id": "167"}}},
        {SourceCompetitionIds(36, 8)},
    )
    assert not is_tracked_source_event(
        {"id": 3, "tournament": {"id": "36", "uniqueTournament": {"id": "9"}}},
        {SourceCompetitionIds(36, 8)},
    )
    engine.dispose()


def test_disabled_competition_filter_skips_lookup_and_accepts_any_source_ids(monkeypatch):
    monkeypatch.setattr(Config, "DISCOVERY_TRACKED_COMPETITIONS_ONLY", False)

    def unexpected_database_lookup():
        raise AssertionError("disabled competition filtering should not query the database")

    monkeypatch.setattr(
        "modules.competition.discovery_scope.db_manager.get_session",
        unexpected_database_lookup,
    )

    scope = load_tracked_source_competitions("sofascore")

    assert scope is None
    assert is_tracked_source_event({"id": 9}, scope)
