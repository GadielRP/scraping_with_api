from datetime import datetime
from types import SimpleNamespace
from modules.jobs.discovery.summary import (
    EVENT_FILTER_REASONS, log_discovery_configuration, log_discovery_summary, log_filter_counts,
)
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


def test_configuration_logs_loaded_switches_lists_and_schedules(caplog, monkeypatch):
    from dataclasses import replace
    from infrastructure.settings import discovery as settings

    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE, tennis_ranking_filter_enabled=True, tennis_ranking_cutoff=77,
        filters=replace(settings.SOFASCORE.filters,
            tracked_competitions_only=False, exclude_sports=False,
            excluded_categories=frozenset({("football", "Example Amateur")}),
            excluded_sofascore_unique_tournament_ids=frozenset({123, 456})),
    ))
    with caplog.at_level(logging.INFO):
        log_discovery_configuration(
            logging.getLogger(__name__), job="daily_discovery", tracked_competitions=None,
        )
    for expected in (
        "job=daily_discovery source=sofascore", "future_only=True", "minimum_lead_minutes=10",
        "tracked_competitions_only=False", "tracked_competition_mappings=disabled",
        "exclude_sports=False", "excluded_categories=[('football', 'Example Amateur')]",
        "excluded_sofascore_unique_tournament_ids=[123, 456]",
        "tennis_ranking_filter_enabled=True", "tennis_ranking_cutoff=77",
        "allowed_rounds=", "daily_refresh_time=", "dropping_times=", "secondary_times=",
        "team_streaks_require_odds=True",
    ):
        assert expected in caplog.text


def test_filter_summary_keeps_zero_counts_and_additional_failure_reasons(caplog):
    with caplog.at_level(logging.INFO):
        log_filter_counts(
            logging.getLogger(__name__), source="winning_odds", stage="feed",
            received=7, accepted=2,
            reasons={"tennis_ranking_excluded": 3, "additional_failure": 2},
        )
    assert "received=7 accepted=2 filtered=5" in caplog.text
    counts = dict(line.split() for line in caplog.records[-1].getMessage().splitlines()[1:])
    assert set(EVENT_FILTER_REASONS) <= counts.keys()
    assert counts["tennis_ranking_excluded"] == "3"
    assert counts["additional_failure"] == "2"
    assert counts["excluded_category"] == "0"
