from datetime import datetime
from types import SimpleNamespace
from modules.jobs.discovery.summary import log_discovery_summary
from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
import logging


def test_summary_keeps_unique_final_calendar_and_local_day(caplog):
    caplog.set_level(logging.INFO)
    event = SimpleNamespace(
        id=1, starts_at=datetime.fromisoformat("2026-10-02T05:59:00+00:00"), sport="Football"
    )
    with DiscoveryRunStore() as store:
        store.record_mapping({"99": event, "100": event})
        event.starts_at = datetime.fromisoformat("2026-10-03T06:00:00+00:00")
        store.record_mapping({"99": event})
        log_discovery_summary(
            store,
            logging.getLogger(__name__),
            job="daily_discovery",
            requested_date="2026-10-02",
            run_slot="actualizacion",
        )
    assert "event_date=2026-10-03 persisted_unique=1" in caplog.text
    assert "job=daily_discovery persisted_unique=1" in caplog.text


def test_invalid_calendar_is_unknown_in_audit(caplog):
    caplog.set_level(logging.INFO)
    with DiscoveryRunStore() as store:
        store.record_mapping(
            {"99": SimpleNamespace(id=1, starts_at=datetime(2026, 10, 2), sport=None)}
        )
        log_discovery_summary(store, logging.getLogger(__name__), job="secondary_sources")
    assert "sport=Unknown event_date=unknown persisted_unique=1" in caplog.text
