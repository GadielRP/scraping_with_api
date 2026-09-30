from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from infrastructure.persistence.models import Event
from infrastructure.settings import Config
from modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor import (
    OddspapiCandidatePool,
    OddspapiFixtureBatchProcessor,
)
from modules.oddspapi.event_candidate_matcher import EventCandidateScore, MatchDecision
from modules.oddspapi.event_resolver import OddspapiEventResolver
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity
from modules.competition.tracked_competitions import tracked_competition_ids


def _payload(fixture_id: str, sofascore_id: str | None = None) -> dict:
    return {
        "fixtureId": fixture_id,
        "sportId": 10,
        "sportName": "Soccer",
        "startTime": "2026-07-15T12:00:00Z",
        "participant1Name": "Home",
        "participant2Name": "Away",
        "externalProviders": {"sofascoreId": sofascore_id} if sofascore_id else {},
    }


class _Session:
    def flush(self):
        pass

    def commit(self):
        pass

    def expire_all(self):
        pass


class _Matcher:
    def __init__(self, decision):
        self.decision = decision
        self.calls = []

    def find_best_match_from_candidates(self, fixture, candidates):
        self.calls.append(fixture.fixture_id)
        return self.decision


def _empty_candidate_pool(cls, fixtures, session, *, competition_ids=None):
    return cls([])


@pytest.fixture(autouse=True)
def _disable_debug_artifacts(monkeypatch):
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor."
        "OddspapiFixtureResponseDebugWriter.save_if_incomplete",
        lambda payload: None,
    )


def _decision(event_id: int = 999) -> MatchDecision:
    score = EventCandidateScore(
        event_id=event_id,
        score=0.98,
        orientation="ordered",
        start_time_delta_minutes=0,
        sport_score=1,
        time_score=1,
        participant1_score=1,
        participant2_score=1,
        participants_score=1,
        participant1_primary_score=1,
        participant2_primary_score=1,
        participants_primary_score=1,
        tournament_score=1,
        both_teams_strong=True,
    )
    return MatchDecision(
        resolved=True,
        needs_review=False,
        status="resolved",
        match_method="deterministic_candidate_match",
        confidence=0.98,
        canonical_event_id=event_id,
        best_candidate_event_id=event_id,
        second_candidate_event_id=None,
        best_candidate_orientation="ordered",
        score_gap=None,
        candidate_scores=[score],
        best_candidate=score,
    )


def test_batch_uses_bulk_lookups_and_only_unresolved_layer3(monkeypatch):
    lookups = []
    persisted = []
    candidate_pool_calls = []
    matcher = _Matcher(_decision())

    def details_lookup(source, source_event_ids, session=None):
        lookups.append((source, list(source_event_ids)))
        if source == "oddspapi":
            return {"existing": (101, tracked_competition_ids()[0])}
        assert source == "sofascore"
        return {"sofa-2": (202, tracked_competition_ids()[0])}

    def load_candidate_pool(cls, fixtures, session, *, competition_ids=None):
        candidate_pool_calls.append(competition_ids)
        return cls([])

    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        details_lookup,
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(load_candidate_pool))
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", matcher)

    processor = OddspapiFixtureBatchProcessor(
        persistence_writer=lambda _session, writes: persisted.extend(writes) or {
            write.fixture.fixture_id: ["oddspapi"] for write in writes
        },
    )
    result = processor.process_batch(
        [_payload("existing"), _payload("external", "sofa-2"), _payload("layer3"), _payload("layer3")],
        create_mappings=True,
        persist_queue=False,
        session=_Session(),
    )

    assert [call[0] for call in lookups] == ["oddspapi", "sofascore"]
    assert result.resolved_existing_oddspapi == 1
    assert result.resolved_external_sofascore == 1
    assert result.resolved_candidate_match == 1
    assert result.fixtures_deduplicated == 1
    assert matcher.calls == ["layer3"]
    assert candidate_pool_calls == [tracked_competition_ids()]
    assert {write.fixture.fixture_id for write in persisted} == {
        "existing",
        "external",
        "layer3",
    }


def test_untracked_sofascore_reference_does_not_fall_through_to_fuzzy_matching(monkeypatch):
    matcher = _Matcher(_decision())
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        lambda source, **kwargs: {"sofa-untracked": (77, -1)}
        if source == "sofascore"
        else {},
    )
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", matcher)
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(_empty_candidate_pool))

    result = OddspapiFixtureBatchProcessor().process_batch(
        [_payload("untracked", "sofa-untracked")],
        create_mappings=True,
        persist_queue=False,
        session=_Session(),
    )

    assert result.fixtures_skipped_untracked_competition == 1
    assert matcher.calls == []
    assert result.mappings_created == 0


def test_existing_oddspapi_mapping_to_untracked_competition_is_not_reused(monkeypatch):
    matcher = _Matcher(_decision())
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        lambda source, **kwargs: {"oddspapi-untracked": (77, -1)}
        if source == "oddspapi"
        else {},
    )
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", matcher)
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(_empty_candidate_pool))

    result = OddspapiFixtureBatchProcessor().process_batch(
        [_payload("oddspapi-untracked")],
        create_mappings=True,
        persist_queue=False,
        session=_Session(),
    )

    assert result.fixtures_skipped_untracked_competition == 1
    assert matcher.calls == []
    assert result.resolved_existing_oddspapi == 0


def test_disabling_competition_filter_expands_oddspapi_candidate_scope(monkeypatch):
    monkeypatch.setattr(Config, "DISCOVERY_TRACKED_COMPETITIONS_ONLY", False)
    matcher = _Matcher(_decision(456))
    candidate_pool_scopes = []
    persisted = []

    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        lambda source, **kwargs: {},
    )

    def load_candidate_pool(fixtures, session, *, competition_ids=None):
        candidate_pool_scopes.append(competition_ids)
        return OddspapiCandidatePool([])

    processor = OddspapiFixtureBatchProcessor(
        matcher=matcher,
        candidate_pool_loader=load_candidate_pool,
        persistence_writer=lambda _session, writes: persisted.extend(writes) or {
            write.fixture.fixture_id: ["oddspapi"] for write in writes
        },
    )
    result = processor.process_batch(
        [_payload("scope-off")],
        create_mappings=True,
        persist_queue=False,
        session=_Session(),
    )

    assert candidate_pool_scopes == [None]
    assert result.fixtures_skipped_untracked_competition == 0
    assert [write.fixture.fixture_id for write in persisted] == ["scope-off"]


def test_dry_run_does_not_upsert_or_queue(monkeypatch):
    matcher = _Matcher(_decision(123))
    upserts = []
    queue_rows = []
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.upsert_mapping",
        lambda **kwargs: upserts.append(kwargs),
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceResolutionQueueRepository.upsert_unresolved_attempt",
        lambda **kwargs: queue_rows.append(kwargs),
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(_empty_candidate_pool))
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", matcher)

    result = OddspapiFixtureBatchProcessor().process_batch(
        [_payload("dry-run")],
        create_mappings=False,
        persist_queue=False,
        session=_Session(),
    )
    assert result.mappings_created == 0
    assert upserts == []
    assert queue_rows == []


def test_batch_captures_incomplete_raw_fixture_before_normalizing(monkeypatch):
    payload = _payload("missing-participant-id")
    payload["participant1Id"] = None
    captured = []
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor."
        "OddspapiFixtureResponseDebugWriter.save_if_incomplete",
        lambda raw: captured.append(raw),
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(_empty_candidate_pool))
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", _Matcher(_decision()))

    OddspapiFixtureBatchProcessor().process_batch(
        [payload],
        create_mappings=False,
        persist_queue=False,
        session=_Session(),
    )

    assert captured == [payload]


def test_missing_fixture_id_is_invalid_and_duplicates_are_dropped(monkeypatch):
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(_empty_candidate_pool))
    matcher = _Matcher(_decision(321))
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", matcher)

    result = OddspapiFixtureBatchProcessor().process_batch(
        [{}, _payload("same"), _payload("same")],
        create_mappings=False,
        persist_queue=False,
        session=_Session(),
    )
    assert result.invalid_payloads == 1
    assert result.fixtures_deduplicated == 1
    assert matcher.calls == ["same"]


def test_persist_queue_writes_review_but_not_no_candidate_noise(monkeypatch):
    review_score = EventCandidateScore(
        event_id=7,
        score=0.82,
        orientation="ordered",
        start_time_delta_minutes=10,
        sport_score=1,
        time_score=0.85,
        participant1_score=0.8,
        participant2_score=0.8,
        participants_score=0.8,
        participant1_primary_score=0.8,
        participant2_primary_score=0.8,
        participants_primary_score=0.8,
        tournament_score=0.8,
        both_teams_strong=False,
    )
    review = MatchDecision(
        resolved=False,
        needs_review=True,
        status="needs_review_low_confidence",
        match_method="needs_review_low_confidence",
        confidence=0.82,
        canonical_event_id=None,
        best_candidate_event_id=7,
        second_candidate_event_id=None,
        best_candidate_orientation="ordered",
        score_gap=None,
        candidate_scores=[review_score],
        best_candidate=review_score,
    )
    no_candidate = MatchDecision(
        resolved=False,
        needs_review=True,
        status="unresolved_no_candidates",
        match_method="unresolved_no_candidates",
        confidence=None,
        canonical_event_id=None,
        best_candidate_event_id=None,
        second_candidate_event_id=None,
        best_candidate_orientation=None,
        score_gap=None,
    )
    class _SequenceMatcher:
        def __init__(self):
            self.index = 0

        def find_best_match_from_candidates(self, fixture, candidates):
            decision = [review, no_candidate][self.index]
            self.index += 1
            return decision

    queue_rows = []
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_mapping_details_by_source_event_ids",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceResolutionQueueRepository.upsert_unresolved_attempt",
        lambda **kwargs: queue_rows.append(kwargs),
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(_empty_candidate_pool))
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", _SequenceMatcher())

    result = OddspapiFixtureBatchProcessor().process_batch(
        [_payload("review"), _payload("noise")],
        create_mappings=True,
        persist_queue=True,
        session=_Session(),
    )
    assert result.queue_rows_written == 1
    assert [row["fixture"].fixture_id for row in queue_rows] == ["review"]


def test_candidate_pool_uses_same_aware_utc_time_basis_as_matcher():
    fixture_payload = _payload("time-basis")
    event = SimpleNamespace(
        id=120450,
        sport="Football",
        starts_at=datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc),
    )
    fixture = OddspapiFixtureIdentity.from_payload(fixture_payload)
    pool = OddspapiCandidatePool([event])

    assert pool.get_candidates_for(fixture) == [event]


def test_candidate_pool_query_filters_to_the_tracked_competitions():
    class _Query:
        def __init__(self):
            self.filters = []

        def options(self, *args):
            return self

        def filter(self, *expressions):
            self.filters.extend(expressions)
            return self

        def all(self):
            return []

    class _QuerySession:
        def __init__(self):
            self.query_object = _Query()

        def query(self, model):
            assert model is Event
            return self.query_object

    session = _QuerySession()
    competition_id = tracked_competition_ids()[0]
    fixture = OddspapiFixtureIdentity.from_payload(_payload("tracked-pool"))

    OddspapiCandidatePool.load(
        [fixture],
        session,
        competition_ids=(competition_id,),
    )

    assert any(
        getattr(getattr(expression, "left", None), "key", None) == "competition_id"
        and getattr(getattr(expression, "right", None), "value", None)
        == [competition_id]
        for expression in session.query_object.filters
    )
