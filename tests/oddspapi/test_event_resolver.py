from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import modules.oddspapi.event_resolver as resolver_module
from modules.oddspapi import (
    EventCandidateScore,
    MatchDecision,
    OddspapiEventCandidateMatcher,
    OddspapiEventResolver,
)
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity


def _payload(
    *,
    fixture_id: str = "fixture-1",
    start_time: datetime | None = None,
    sofascore_id: str | None = None,
    participant1_name: str = "Liverpool FC",
    participant2_name: str = "Manchester United",
    sport_name: str = "Soccer",
    tournament_name: str = "Premier League",
    tournament_slug: str = "premier-league",
    category_name: str = "England",
    category_slug: str = "england",
    external_providers: dict | None = None,
) -> dict[str, object]:
    payload = {
        "fixtureId": fixture_id,
        "participant1Name": participant1_name,
        "participant2Name": participant2_name,
        "participant1ShortName": participant1_name.split(" ")[0],
        "participant2ShortName": participant2_name.split(" ")[0],
        "participant1Abbr": "LIV",
        "participant2Abbr": "MNU",
        "sportId": 10,
        "sportName": sport_name,
        "tournamentId": 17,
        "tournamentName": tournament_name,
        "tournamentSlug": tournament_slug,
        "categoryName": category_name,
        "categorySlug": category_slug,
        "seasonId": 130281,
        "startTime": (start_time or datetime(2026, 4, 13, 19, 0, tzinfo=timezone.utc)).isoformat().replace(
            "+00:00",
            "Z",
        ),
        "externalProviders": external_providers
        if external_providers is not None
        else {"sofascoreId": sofascore_id},
    }
    if external_providers is None and sofascore_id is None:
        payload["externalProviders"] = {}
    return payload


def _candidate(
    *,
    event_id: int,
    score: float,
    orientation: str = "ordered",
    delta_minutes: float | None = 0,
    participant1_score: float = 0.95,
    participant2_score: float = 0.95,
    tournament_score: float = 0.90,
    sport_score: float = 1.0,
    time_score: float = 1.0,
    both_teams_strong: bool = True,
) -> EventCandidateScore:
    participants_score = (participant1_score + participant2_score) / 2
    return EventCandidateScore(
        event_id=event_id,
        score=score,
        orientation=orientation,
        start_time_delta_minutes=delta_minutes,
        sport_score=sport_score,
        time_score=time_score,
        participant1_score=participant1_score,
        participant2_score=participant2_score,
        participants_score=participants_score,
        tournament_score=tournament_score,
        both_teams_strong=both_teams_strong,
        reasons=[],
    )


def _decision(
    *,
    resolved: bool,
    status: str,
    confidence: float | None,
    canonical_event_id: int | None,
    candidate_scores: list[EventCandidateScore],
    best_candidate_orientation: str | None = None,
    score_gap: float | None = None,
) -> MatchDecision:
    best_candidate = candidate_scores[0] if candidate_scores else None
    second_candidate = candidate_scores[1] if len(candidate_scores) > 1 else None
    return MatchDecision(
        resolved=resolved,
        needs_review=not resolved,
        status=status,
        match_method=status,
        confidence=confidence,
        canonical_event_id=canonical_event_id,
        best_candidate_event_id=best_candidate.event_id if best_candidate else None,
        second_candidate_event_id=second_candidate.event_id if second_candidate else None,
        best_candidate_orientation=best_candidate_orientation
        if best_candidate_orientation is not None
        else (best_candidate.orientation if best_candidate else None),
        score_gap=score_gap,
        candidate_scores=candidate_scores,
        best_candidate=best_candidate,
        second_best_candidate=second_candidate,
    )


def _event(
    *,
    event_id: int,
    start_time: datetime,
    sport: str = "Football",
    home_name: str = "Liverpool FC",
    away_name: str = "Manchester United",
    competition_name: str = "Premier League",
    category_name: str = "England",
) -> SimpleNamespace:
    def _short_name(name: str) -> str:
        tokens = [token for token in name.split(" ") if token]
        if not tokens:
            return ""
        if len(tokens) == 1:
            return tokens[0]
        return " ".join(tokens[:2])

    def _code_name(name: str) -> str:
        initials = "".join(token[0] for token in name.split() if token)
        return initials[:4].upper()

    return SimpleNamespace(
        id=event_id,
        start_time_utc=start_time.replace(tzinfo=None),
        sport=sport,
        home_team=home_name,
        away_team=away_name,
        competition=competition_name,
        country=category_name,
        home_participant=SimpleNamespace(
            name=home_name,
            short_name=_short_name(home_name),
            code_name=_code_name(home_name),
        ),
        away_participant=SimpleNamespace(
            name=away_name,
            short_name=_short_name(away_name),
            code_name=_code_name(away_name),
        ),
        competition_ref=SimpleNamespace(
            display_name=competition_name,
            canonical_name=competition_name,
            slug="premier-league",
            unique_slug="england-premier-league",
            category_name=category_name,
        ),
    )


class _FakeQuery:
    def __init__(self, events: list[SimpleNamespace]):
        self._events = events

    def options(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self._events


class _FakeSession:
    def __init__(self, events: list[SimpleNamespace]):
        self._events = events

    def query(self, model):
        return _FakeQuery(self._events)


@contextmanager
def _fake_session_context(*_args, **_kwargs):
    yield object()


def test_missing_fixture_id_returns_unresolved_without_db_lookup(monkeypatch):
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda *args, **kwargs: pytest.fail("mapping lookup should not be called"),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda *args, **kwargs: pytest.fail("candidate matcher should not be called"),
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response({})

    assert resolution.resolved is False
    assert resolution.skipped_reason == "missing_oddspapi_fixture_id"
    assert resolution.canonical_event_id is None


def test_existing_oddspapi_mapping_resolves_immediately(monkeypatch):
    calls: list[tuple[str, str]] = []

    def fake_lookup(source, source_event_id, session=None):
        calls.append((source, source_event_id))
        if source == "oddspapi":
            return 321
        pytest.fail("unexpected lookup")

    clear_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        fake_lookup,
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "clear_resolved",
        lambda source, source_event_id, session=None: clear_calls.append((source, source_event_id)),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "upsert_mapping",
        lambda *args, **kwargs: pytest.fail("mapping upsert should not be called"),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda *args, **kwargs: pytest.fail("candidate matcher should not be called"),
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(_payload())

    assert resolution.resolved is True
    assert resolution.match_method == "existing_oddspapi_mapping"
    assert resolution.canonical_event_id == 321
    assert calls == [("oddspapi", "fixture-1")]
    assert clear_calls == [("oddspapi", "fixture-1")]


def test_sofascore_mapping_creates_oddspapi_mapping_and_clears_queue(monkeypatch):
    upsert_calls: list[dict] = []
    clear_calls: list[tuple[str, str]] = []

    def fake_lookup(source, source_event_id, session=None):
        if source == "oddspapi":
            return None
        if source == "sofascore":
            return 456
        pytest.fail("unexpected lookup")

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        fake_lookup,
    )
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "upsert_mapping",
        lambda **kwargs: upsert_calls.append(kwargs),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "clear_resolved",
        lambda source, source_event_id, session=None: clear_calls.append((source, source_event_id)),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda *args, **kwargs: pytest.fail("candidate matcher should not be called"),
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(
        _payload(external_providers={"sofascoreId": 999}),
    )

    assert resolution.resolved is True
    assert resolution.match_method == "external_provider_sofascore_id"
    assert resolution.canonical_event_id == 456
    assert resolution.confidence == 1.0
    assert upsert_calls and upsert_calls[0]["source"] == "oddspapi"
    assert upsert_calls[0]["match_method"] == "external_provider_sofascore_id"
    assert clear_calls == [("oddspapi", "fixture-1")]


def test_missing_sofascore_id_falls_through_to_layer3(monkeypatch):
    lookup_calls: list[tuple[str, str]] = []
    matcher_calls: list[OddspapiFixtureIdentity] = []
    queue_calls: list[dict] = []

    def fake_lookup(source, source_event_id, session=None):
        lookup_calls.append((source, source_event_id))
        if source == "oddspapi":
            return None
        if source == "sofascore":
            pytest.fail("sofascore lookup should not run when provider id is missing")
        return None

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        fake_lookup,
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda self, fixture, session=None: matcher_calls.append(fixture)
        or _decision(
            resolved=False,
            status="unresolved_no_candidates",
            confidence=None,
            canonical_event_id=None,
            candidate_scores=[],
        ),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "upsert_unresolved_attempt",
        lambda **kwargs: queue_calls.append(kwargs),
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(_payload(external_providers={}))

    assert ("oddspapi", "fixture-1") in lookup_calls
    assert all(source != "sofascore" for source, _ in lookup_calls)
    assert matcher_calls and isinstance(matcher_calls[0], OddspapiFixtureIdentity)
    assert queue_calls and queue_calls[0]["resolution_status"] == "unresolved_no_candidates"
    assert resolution.resolved is False
    assert resolution.skipped_reason == "unresolved_no_candidates"


def test_deterministic_candidate_match_creates_mapping(monkeypatch):
    upsert_calls: list[dict] = []
    clear_calls: list[tuple[str, str]] = []
    fixture = OddspapiFixtureIdentity.from_payload(_payload(external_providers={}))
    decision = _decision(
        resolved=True,
        status="resolved",
        confidence=0.947,
        canonical_event_id=123,
        score_gap=0.12,
        candidate_scores=[_candidate(event_id=123, score=0.947)],
    )

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda source, source_event_id, session=None: None,
    )
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "upsert_mapping",
        lambda **kwargs: upsert_calls.append(kwargs),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "clear_resolved",
        lambda source, source_event_id, session=None: clear_calls.append((source, source_event_id)),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda self, fixture, session=None: decision,
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(_payload(external_providers={}))

    assert resolution.resolved is True
    assert resolution.match_method == "deterministic_candidate_match"
    assert resolution.canonical_event_id == 123
    assert resolution.best_candidate_event_id == 123
    assert resolution.score_gap == 0.12
    assert upsert_calls[0]["match_method"] == "deterministic_candidate_match"
    assert clear_calls == [("oddspapi", "fixture-1")]
    assert resolution.created_mappings == ["oddspapi"]


def test_swapped_participants_orientation_is_surfaces(monkeypatch):
    upsert_calls: list[dict] = []
    clear_calls: list[tuple[str, str]] = []
    decision = _decision(
        resolved=True,
        status="resolved",
        confidence=0.951,
        canonical_event_id=777,
        score_gap=0.11,
        best_candidate_orientation="swapped",
        candidate_scores=[_candidate(event_id=777, score=0.951, orientation="swapped")],
    )

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda source, source_event_id, session=None: None,
    )
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "upsert_mapping",
        lambda **kwargs: upsert_calls.append(kwargs),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "clear_resolved",
        lambda source, source_event_id, session=None: clear_calls.append((source, source_event_id)),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda self, fixture, session=None: decision,
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(_payload(external_providers={}))

    assert resolution.best_candidate_orientation == "swapped"
    assert resolution.resolved is True
    assert upsert_calls[0]["match_method"] == "deterministic_candidate_match"
    assert clear_calls == [("oddspapi", "fixture-1")]


def test_ambiguous_candidates_write_queue(monkeypatch):
    queue_calls: list[dict] = []
    decision = _decision(
        resolved=False,
        status="needs_review_ambiguous_candidates",
        confidence=0.901,
        canonical_event_id=None,
        score_gap=0.02,
        candidate_scores=[
            _candidate(event_id=11, score=0.912),
            _candidate(event_id=12, score=0.892),
        ],
    )

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda source, source_event_id, session=None: None,
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "upsert_unresolved_attempt",
        lambda **kwargs: queue_calls.append(kwargs),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "upsert_mapping",
        lambda **kwargs: pytest.fail("mapping should not be created for ambiguous candidates"),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda self, fixture, session=None: decision,
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(_payload(external_providers={}))

    assert resolution.resolved is False
    assert resolution.needs_review is True
    assert resolution.match_method == "needs_review_ambiguous_candidates"
    assert queue_calls and queue_calls[0]["resolution_status"] == "needs_review_ambiguous_candidates"


def test_low_confidence_candidate_writes_queue(monkeypatch):
    queue_calls: list[dict] = []
    decision = _decision(
        resolved=False,
        status="needs_review_low_confidence",
        confidence=0.821,
        canonical_event_id=None,
        score_gap=0.03,
        candidate_scores=[_candidate(event_id=55, score=0.821)],
    )

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda source, source_event_id, session=None: None,
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "upsert_unresolved_attempt",
        lambda **kwargs: queue_calls.append(kwargs),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "upsert_mapping",
        lambda **kwargs: pytest.fail("mapping should not be created for low confidence"),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda self, fixture, session=None: decision,
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(_payload(external_providers={}))

    assert resolution.resolved is False
    assert resolution.match_method == "needs_review_low_confidence"
    assert queue_calls and queue_calls[0]["resolution_status"] == "needs_review_low_confidence"


def test_dry_run_does_not_create_mappings_or_queue(monkeypatch):
    upsert_calls: list[dict] = []
    queue_calls: list[dict] = []
    clear_calls: list[tuple[str, str]] = []
    decision = _decision(
        resolved=True,
        status="resolved",
        confidence=0.952,
        canonical_event_id=900,
        score_gap=0.15,
        candidate_scores=[_candidate(event_id=900, score=0.952)],
    )

    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda source, source_event_id, session=None: None,
    )
    monkeypatch.setattr(
        resolver_module.EventSourceMappingRepository,
        "upsert_mapping",
        lambda **kwargs: upsert_calls.append(kwargs),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "upsert_unresolved_attempt",
        lambda **kwargs: queue_calls.append(kwargs),
    )
    monkeypatch.setattr(
        resolver_module.EventSourceResolutionQueueRepository,
        "clear_resolved",
        lambda source, source_event_id, session=None: clear_calls.append((source, source_event_id)),
    )
    monkeypatch.setattr(
        resolver_module.OddspapiEventCandidateMatcher,
        "find_best_match",
        lambda self, fixture, session=None: decision,
    )
    monkeypatch.setattr(resolver_module.db_manager, "get_session", _fake_session_context)

    resolution = OddspapiEventResolver.resolve_from_odds_response(
        _payload(external_providers={}),
        create_mappings=False,
        persist_queue=False,
    )

    assert resolution.resolved is True
    assert resolution.match_method == "deterministic_candidate_match"
    assert upsert_calls == []
    assert queue_calls == []
    assert clear_calls == []


def test_exact_strong_candidate_match_is_resolved_by_matcher():
    fixture = OddspapiFixtureIdentity.from_payload(_payload())
    event = _event(
        event_id=123,
        start_time=fixture.start_time_local,
        home_name="Liverpool FC",
        away_name="Manchester United",
    )
    matcher = OddspapiEventCandidateMatcher()
    decision = matcher.find_best_match(fixture, session=_FakeSession([event]))

    assert decision.resolved is True
    assert decision.canonical_event_id == 123
    assert decision.best_candidate_orientation == "ordered"
    assert decision.best_candidate.score >= matcher.AUTO_LINK_THRESHOLD
    assert decision.best_candidate.both_teams_strong is True


def test_swapped_participants_are_matched_by_matcher():
    fixture = OddspapiFixtureIdentity.from_payload(_payload())
    event = _event(
        event_id=124,
        start_time=fixture.start_time_local,
        home_name="Manchester United",
        away_name="Liverpool FC",
    )
    matcher = OddspapiEventCandidateMatcher()
    decision = matcher.find_best_match(fixture, session=_FakeSession([event]))

    assert decision.resolved is True
    assert decision.canonical_event_id == 124
    assert decision.best_candidate_orientation == "swapped"


def test_ambiguous_candidates_are_flagged_by_matcher():
    fixture = OddspapiFixtureIdentity.from_payload(_payload())
    start_time = fixture.start_time_local
    matcher = OddspapiEventCandidateMatcher()
    events = [
        _event(event_id=201, start_time=start_time),
        _event(event_id=202, start_time=start_time),
    ]

    decision = matcher.find_best_match(fixture, session=_FakeSession(events))

    assert decision.resolved is False
    assert decision.status == "needs_review_ambiguous_candidates"
    assert len(decision.candidate_scores) == 2
    assert decision.score_gap == 0.0


def test_start_time_too_far_forces_review_by_matcher():
    fixture = OddspapiFixtureIdentity.from_payload(
        _payload(start_time=datetime(2026, 4, 13, 19, 0, tzinfo=timezone.utc))
    )
    far_event = _event(
        event_id=303,
        start_time=fixture.start_time_local + timedelta(minutes=45),
    )
    matcher = OddspapiEventCandidateMatcher()
    decision = matcher.find_best_match(fixture, session=_FakeSession([far_event]))

    assert decision.resolved is False
    assert decision.status == "needs_review_low_confidence"
    assert decision.best_candidate.start_time_delta_minutes == 45.0
    assert decision.best_candidate.score < matcher.AUTO_LINK_THRESHOLD
