from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor import (
    OddspapiCandidatePool,
    OddspapiFixtureBatchProcessor,
)
from modules.oddspapi.event_candidate_matcher import EventCandidateScore, MatchDecision
from modules.oddspapi.event_resolver import OddspapiEventResolver
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity


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


class _Matcher:
    def __init__(self, decision):
        self.decision = decision
        self.calls = []

    def find_best_match_from_candidates(self, fixture, candidates):
        self.calls.append(fixture.fixture_id)
        return self.decision


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
    upserts = []
    matcher = _Matcher(_decision())

    def bulk_lookup(source, source_event_ids, session=None):
        lookups.append((source, list(source_event_ids)))
        if source == "oddspapi":
            return {"existing": 101}
        return {"sofa-2": 202}

    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_ids_by_source_event_ids",
        bulk_lookup,
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.upsert_mapping",
        lambda **kwargs: upserts.append(kwargs),
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(lambda cls, fixtures, session: cls([])))
    monkeypatch.setattr(OddspapiEventResolver, "_candidate_matcher", matcher)

    processor = OddspapiFixtureBatchProcessor()
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
    assert any(call["source_event_id"] == "layer3" for call in upserts)


def test_dry_run_does_not_upsert_or_queue(monkeypatch):
    matcher = _Matcher(_decision(123))
    upserts = []
    queue_rows = []
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_ids_by_source_event_ids",
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
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(lambda cls, fixtures, session: cls([])))
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
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_ids_by_source_event_ids",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(lambda cls, fixtures, session: cls([])))
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
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_ids_by_source_event_ids",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(lambda cls, fixtures, session: cls([])))
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
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceMappingRepository.get_event_ids_by_source_event_ids",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor.EventSourceResolutionQueueRepository.upsert_unresolved_attempt",
        lambda **kwargs: queue_rows.append(kwargs),
    )
    monkeypatch.setattr(OddspapiCandidatePool, "load", classmethod(lambda cls, fixtures, session: cls([])))
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
