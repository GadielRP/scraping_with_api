import logging

from app.logging_setup import _OddsPortalOnlyFilter


def _record(name: str, *, tagged: bool = False) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name,
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="test",
        args=(),
        exc_info=None,
    )
    if tagged:
        record.oddsportal = True
    return record


def test_oddsportal_filter_accepts_scraper_owned_namespaces():
    log_filter = _OddsPortalOnlyFilter()

    accepted_names = (
        "oddsportal_scraper",
        "modules.oddsportal.scraper_attempt",
        "modules.jobs.pre_start_check_job.oddsportal_worker",
        "modules.jobs.clean_league_cache.run_clean_league_cache",
        "infrastructure.persistence.repositories.oddsportal_cache_repository",
    )

    assert all(log_filter.filter(_record(name)) for name in accepted_names)


def test_oddsportal_filter_rejects_unrelated_application_logs():
    log_filter = _OddsPortalOnlyFilter()

    rejected_names = (
        "root",
        "shared.runtime_observability",
        "modules.sofascore.event_details",
        "modules.jobs.pre_start_check_job.timing",
        "infrastructure.persistence.repositories.market_repository",
    )

    assert not any(log_filter.filter(_record(name)) for name in rejected_names)


def test_oddsportal_filter_accepts_explicitly_tagged_mixed_module_record():
    log_filter = _OddsPortalOnlyFilter()

    assert log_filter.filter(
        _record(
            "infrastructure.persistence.repositories.market_repository",
            tagged=True,
        )
    )
