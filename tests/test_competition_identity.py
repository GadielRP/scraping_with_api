"""Provider identity survives changes to database primary keys."""
from dataclasses import asdict, replace
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition
from infrastructure.persistence.repositories.competition_repository import get_competition_profile_for_local_id
from modules.competition import competition_ids as ids
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.competition_profiles import get_competition_profile
from modules.competition.competitive_rules import ObjectiveRule
from modules.competition.rule_terms import DestinationPhase
from modules.competition.tracked_competitions import tracked_competition_ids, is_tracked_competition


def test_qualification_record_and_phase_remain_distinct_and_round_trip():
    rule = get_competition_profile(ids.LALIGA).objectives[2]
    assert rule.destination_competition_identity == ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION
    assert rule.destination_competition_identity != ids.UEFA_CONFERENCE_LEAGUE
    assert rule.destination_competition_name == "UEFA Conference League, Qualification"
    assert rule.destination_phase is DestinationPhase.QUALIFICATION_ROUND
    assert ObjectiveRule(**json.loads(json.dumps(asdict(rule)))) == rule
    with pytest.raises(ValueError):
        replace(rule, destination_phase=None)
    with pytest.raises(ValueError):
        replace(rule, destination_competition_identity=6138)


@pytest.mark.parametrize("local_id", [900001, 42])
def test_local_keys_are_resolved_from_provider_identity(monkeypatch, local_id):
    engine = create_engine("sqlite://")
    Competition.__table__.create(engine)
    sessions = sessionmaker(bind=engine)
    identity = ids.LALIGA
    with sessions.begin() as session:
        session.add(Competition(competition_id=local_id, source=identity.source,
                                source_unique_tournament_id=identity.source_unique_tournament_id,
                                source_tournament_id=identity.source_tournament_id,
                                canonical_name="LaLiga", display_name="LaLiga"))
    monkeypatch.setattr(db_manager, "SessionLocal", sessions)
    assert get_competition_profile_for_local_id(local_id) is get_competition_profile(identity)
    assert tracked_competition_ids() == (local_id,)
    assert is_tracked_competition(local_id)
    assert get_competition_profile_for_local_id(999999) is None
    assert get_competition_profile(local_id) is None
    assert get_competition_profile(replace(identity, source="another_provider")) is None
    assert get_competition_profile(replace(identity, source_tournament_id=999999)) is None
    engine.dispose()
