"""Unit tests for Oddspapi mainLine outcome cache persistence."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from infrastructure.persistence.models import Base, OddspapiMainlineOutcomeCache
from infrastructure.persistence.repositories.oddspapi_mainline_cache_repository import (
    OddspapiMainlineCacheRepository,
)
from modules.jobs.pre_start_check_job.providers.oddspapi import odds_phase
from modules.jobs.pre_start_check_job.providers.oddspapi.settings import (
    ODDSPAPI_PRE_START_SETTINGS,
)


def _patch_session(monkeypatch, session_factory):
    class _Manager:
        @staticmethod
        def get_session():
            session = session_factory()

            class _Ctx:
                def __enter__(self_inner):
                    return session

                def __exit__(self_inner, exc_type, exc, tb):
                    if exc_type is None:
                        session.commit()
                    else:
                        session.rollback()
                    session.close()
                    return False

            return _Ctx()

    monkeypatch.setattr(
        "infrastructure.persistence.repositories.oddspapi_mainline_cache_repository.db_manager",
        _Manager,
    )


def test_save_and_get_mainline_outcome_ids(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[OddspapiMainlineOutcomeCache.__table__],
    )
    session_factory = sessionmaker(bind=engine)
    _patch_session(monkeypatch, session_factory)

    saved = OddspapiMainlineCacheRepository.save_mainline_outcomes(
        event_id=42,
        fixture_id="fixture-42",
        source_sport_id="10",
        mainline_outcomes=[
            {
                "bookmaker_slug": "pinnacle",
                "source_market_id": "101",
                "source_outcome_id": "201",
                "is_exchange": False,
            },
            {
                "bookmaker_slug": "betfair-ex",
                "source_market_id": "102",
                "source_outcome_id": "301",
                "canonical_market_key": "1x2_full_time",
                "is_exchange": True,
            },
            {
                "bookmaker_slug": "betfair-ex",
                "source_market_id": "102",
                "source_outcome_id": "301",
                "is_exchange": True,
            },
        ],
    )

    assert saved == 2
    assert OddspapiMainlineCacheRepository.get_mainline_outcome_ids(42) == {
        "201",
        "301",
    }


def test_save_mainline_outcomes_overwrites_previous_cache(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[OddspapiMainlineOutcomeCache.__table__],
    )
    session_factory = sessionmaker(bind=engine)
    _patch_session(monkeypatch, session_factory)

    # First acquisition at T-120: Pinnacle has market 1056 (outcome 201)
    OddspapiMainlineCacheRepository.save_mainline_outcomes(
        event_id=42,
        fixture_id="fixture-42",
        source_sport_id="10",
        mainline_outcomes=[
            {
                "bookmaker_slug": "pinnacle",
                "source_market_id": "1056",
                "source_outcome_id": "201",
                "is_exchange": False,
            },
        ],
    )
    assert OddspapiMainlineCacheRepository.get_mainline_outcome_ids(42) == {"201"}

    # Second acquisition at T-30: Line shifts to market 1054 (outcome 202)
    OddspapiMainlineCacheRepository.save_mainline_outcomes(
        event_id=42,
        fixture_id="fixture-42",
        source_sport_id="10",
        mainline_outcomes=[
            {
                "bookmaker_slug": "pinnacle",
                "source_market_id": "1054",
                "source_outcome_id": "202",
                "is_exchange": False,
            },
        ],
    )
    # Stale outcome 201 must be gone; only fresh outcome 202 should remain
    assert OddspapiMainlineCacheRepository.get_mainline_outcome_ids(42) == {"202"}


def test_get_exchange_mainline_selections(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[OddspapiMainlineOutcomeCache.__table__],
    )
    session_factory = sessionmaker(bind=engine)
    _patch_session(monkeypatch, session_factory)

    OddspapiMainlineCacheRepository.save_mainline_outcomes(
        event_id=7,
        fixture_id="fixture-7",
        source_sport_id="13",
        mainline_outcomes=[
            {
                "bookmaker_slug": "pinnacle",
                "source_market_id": "1",
                "source_outcome_id": "11",
                "is_exchange": False,
            },
            {
                "bookmaker_slug": "betfair-ex",
                "source_market_id": "2",
                "source_outcome_id": "22",
                "canonical_market_key": "over_under_full_time",
                "is_exchange": True,
            },
        ],
    )

    selections = OddspapiMainlineCacheRepository.get_exchange_mainline_selections(
        7,
        ["betfair-ex", "matchbook"],
    )
    assert len(selections) == 1
    assert selections[0]["bookmaker_slug"] == "betfair-ex"
    assert selections[0]["source_outcome_id"] == "22"
    assert selections[0]["canonical_market_key"] == "over_under_full_time"


def test_purge_stale_cache(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[OddspapiMainlineOutcomeCache.__table__],
    )
    session_factory = sessionmaker(bind=engine)
    _patch_session(monkeypatch, session_factory)

    with session_factory() as session:
        session.add(
            OddspapiMainlineOutcomeCache(
                event_id=1,
                fixture_id="old",
                bookmaker_slug="pinnacle",
                source_market_id="1",
                source_outcome_id="1",
                is_exchange=False,
                captured_at=datetime(2000, 1, 1),
            )
        )
        session.add(
            OddspapiMainlineOutcomeCache(
                event_id=1,
                fixture_id="fresh",
                bookmaker_slug="pinnacle",
                source_market_id="1",
                source_outcome_id="2",
                is_exchange=False,
                captured_at=datetime.utcnow(),
            )
        )
        session.commit()

    deleted = OddspapiMainlineCacheRepository.purge_stale_cache(days=2)
    assert deleted == 1
    assert OddspapiMainlineCacheRepository.get_mainline_outcome_ids(1) == {"2"}


def test_mainline_cache_cleanup_can_be_disabled(monkeypatch):
    purge_calls = []
    monkeypatch.setattr(
        odds_phase,
        "ODDSPAPI_PRE_START_SETTINGS",
        replace(ODDSPAPI_PRE_START_SETTINGS, mainline_cache_cleanup_enabled=False),
    )
    monkeypatch.setattr(
        odds_phase.OddspapiMainlineCacheRepository,
        "purge_stale_cache",
        lambda **kwargs: purge_calls.append(kwargs) or 1,
    )

    assert odds_phase._cleanup_mainline_cache() == 0
    assert purge_calls == []


def test_mainline_cache_cleanup_uses_retention_when_enabled(monkeypatch):
    purge_calls = []
    monkeypatch.setattr(
        odds_phase,
        "ODDSPAPI_PRE_START_SETTINGS",
        replace(ODDSPAPI_PRE_START_SETTINGS, mainline_cache_cleanup_enabled=True),
    )
    monkeypatch.setattr(
        odds_phase.Config,
        "ODDSPAPI_MAINLINE_CACHE_RETENTION_DAYS",
        7,
    )
    monkeypatch.setattr(
        odds_phase.OddspapiMainlineCacheRepository,
        "purge_stale_cache",
        lambda **kwargs: purge_calls.append(kwargs) or 3,
    )

    assert odds_phase._cleanup_mainline_cache() == 3
    assert purge_calls == [{"days": 7}]
