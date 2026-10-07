from datetime import datetime, timezone
from threading import Event as ThreadEvent, Thread
from time import monotonic
from types import SimpleNamespace

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event
from infrastructure.persistence.repositories.market_repository import MarketRepository
from infrastructure.persistence.repositories.market.market_odds_read_repository import MarketOddsReadRepository
from infrastructure.settings import Config
from modules.alerts.alerts_formatter import odds_alert
from modules.jobs.pre_start_check_job import alert_pipeline
from modules.jobs.pre_start_check_job import run_pre_start_check_job as runner


def _database(tmp_path, monkeypatch):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'ordering.db'}")
    manager.create_tables()
    with manager.get_session() as session:
        event = Event(slug="home-away", starts_at=datetime(2026, 10, 8, tzinfo=timezone.utc),
                      sport="Football", competition="Test", home_team="Home", away_team="Away")
        bookie = Bookie(name="bet365", slug="bet365")
        session.add_all([event, bookie])
        session.flush()
        event_id, bookie_id = event.id, bookie.bookie_id
    monkeypatch.setattr("infrastructure.persistence.repositories.market_repository.db_manager", manager)
    monkeypatch.setattr("infrastructure.persistence.repositories.market.market_odds_read_repository.db_manager", manager)
    batch = [{"bookie_id": bookie_id, "markets": [{"canonicalMarketKey": "1x2_full_time",
              "isLive": False, "choices": [{"name": "1", "initialOdds": "2.1", "currentOdds": "2"}]}]}]
    return event_id, batch


def test_main_flow_commits_ingestion_before_database_read(tmp_path, monkeypatch):
    event_id, batch = _database(tmp_path, monkeypatch)
    calls = []
    plan = SimpleNamespace(candidates=[{"event_id": event_id, "should_extract_odds": True}])
    monkeypatch.setattr(runner, "load_pre_start_odds_source_states", lambda _: {})
    monkeypatch.setattr(runner, "build_pre_start_event_candidates", lambda *_a, **_kw: plan)
    monkeypatch.setattr(runner, "attach_stored_observations", lambda _: None)
    monkeypatch.setattr(runner, "persist_snapshot_observations", lambda _: None)

    def ingest(*_args, **_kwargs):
        MarketRepository.save_canonical_bookmaker_batches(event_id, batch, source="sofascore")
        calls.append("committed")

    def evaluate(*_args, **_kwargs):
        # A fresh read session must see the committed values, not a payload.
        result = MarketOddsReadRepository().get_market_odds_state(event_id)
        assert result.markets[0].choices[0].current.value == 2
        calls.append("read")

    monkeypatch.setattr(runner, "_ingest_provider_odds", ingest)
    monkeypatch.setattr(runner, "evaluate_pre_start_key_moments", evaluate)
    runner.run_pre_start_odds_moments(SimpleNamespace(missing_odds=None), [], {},
                                     key_moments=(5,), oddsportal_context=SimpleNamespace())
    assert calls == ["committed", "read"]


def test_alert_waits_for_oddsportal_commit_without_sofascore_payload(tmp_path, monkeypatch):
    event_id, batch = _database(tmp_path, monkeypatch)
    started, done = ThreadEvent(), ThreadEvent()
    started.set()
    states = {event_id: {"started_event": started, "done_event": done, "started_at_monotonic": monotonic()}}
    context = SimpleNamespace(event_id=event_id, minutes_until_start=5, season_id=None, discovery_source="",
                              home=SimpleNamespace(name="Home"), away=SimpleNamespace(name="Away"), sport="Football",
                              competition=SimpleNamespace(competition_id=1, display_name="Test"), slug="home-away")
    monkeypatch.setattr(odds_alert, "is_tracked_competition", lambda _: True)
    monkeypatch.setattr(odds_alert.pre_start_notifier, "telegram_enabled", True)
    sent = []
    monkeypatch.setattr(odds_alert.pre_start_notifier, "send_telegram_message", lambda message: sent.append(message) or True)
    processor = alert_pipeline.EventAlertProcessor(None, op_event_states=states, op_event_ids={event_id})
    monkeypatch.setattr(processor, "_ensure_dual_process_evaluation", lambda *_args: None)

    allow_commit = ThreadEvent()
    original_wait = done.wait

    def wait_for_commit(timeout):
        allow_commit.set()
        return original_wait(timeout)

    monkeypatch.setattr(done, "wait", wait_for_commit)

    def persist():
        assert allow_commit.wait(timeout=5), "the consumer must wait before reading"
        MarketRepository.save_canonical_bookmaker_batches(event_id, batch, source="oddsportal")
        done.set()

    worker = Thread(target=persist)
    worker.start()
    try:
        processor.process_event(context)
    finally:
        worker.join(timeout=5)
    assert done.is_set()
    assert len(sent) == 1
    assert "bet365: 1: 2.10→N/A" in sent[0]


def test_oddsportal_timeout_prevents_quote_read(monkeypatch):
    started = ThreadEvent()
    started.set()
    states = {7: {"started_event": started, "done_event": ThreadEvent(), "started_at_monotonic": monotonic() - 1}}
    monkeypatch.setattr(Config, "ODDSPORTAL_ALERT_WAIT_TIMEOUT", 0)
    monkeypatch.setattr(alert_pipeline, "MarketOddsReadRepository", lambda: (_ for _ in ()).throw(AssertionError("read before commit")))
    processor = alert_pipeline.EventAlertProcessor(None, op_event_states=states, op_event_ids={7})
    assert processor._sync_oddsportal_data(7) is False
