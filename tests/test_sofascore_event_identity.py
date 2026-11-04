import pytest

from modules.sofascore import event_identity


def test_resolve_sofascore_identity_from_canonical_id(monkeypatch):
    monkeypatch.setattr(
        event_identity.EventSourceMappingRepository,
        "resolve_required_source_event_id",
        lambda *, event_id, source: "9001",
    )

    identity = event_identity.resolve_sofascore_event_identity(
        123,
        input_id_type="canonical",
    )

    assert identity == event_identity.SofaScoreEventIdentity(
        canonical_event_id=123,
        sofascore_event_id=9001,
    )


def test_resolve_sofascore_identity_from_provider_id(monkeypatch):
    monkeypatch.setattr(
        event_identity.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda source, source_event_id: 123,
    )

    identity = event_identity.resolve_sofascore_event_identity(
        9001,
        input_id_type="sofascore",
    )

    assert identity == event_identity.SofaScoreEventIdentity(
        canonical_event_id=123,
        sofascore_event_id=9001,
    )


def test_resolve_sofascore_identity_keeps_unmapped_provider_id(monkeypatch):
    monkeypatch.setattr(
        event_identity.EventSourceMappingRepository,
        "get_event_id_by_source",
        lambda source, source_event_id: None,
    )

    identity = event_identity.resolve_sofascore_event_identity(
        9001,
        input_id_type="sofascore",
    )

    assert identity == event_identity.SofaScoreEventIdentity(
        canonical_event_id=None,
        sofascore_event_id=9001,
    )


def test_resolve_sofascore_identity_rejects_unknown_id_type():
    with pytest.raises(ValueError, match="Unsupported input_id_type"):
        event_identity.resolve_sofascore_event_identity(9001, input_id_type="other")
