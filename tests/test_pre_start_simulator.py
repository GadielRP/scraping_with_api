"""Simulator switches exercise production orchestration without HTTP or live writes."""

from contextlib import nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from modules.jobs.pre_start_check_job import key_moment_evaluation as evaluation
from modules.jobs.pre_start_check_job.event_candidate_builder import PreStartEventPlan
from modules.pillars.context import EventContext
from scripts.development import pre_start_odds_simulation as odds_phase
from scripts.development import simulate_pre_start_check as simulator


@pytest.fixture
def simulation(monkeypatch):
    calls = []
    events = {
        event_id: SimpleNamespace(
            id=event_id,
            home_team="Home",
            away_team="Away",
            sport="Football",
            season_id=2026,
            competition_id=176,
            starts_at=datetime(2026, 10, 12, 18, tzinfo=timezone.utc),
        )
        for event_id in (101, 102)
    }
    candidates = [
        {
            "event_id": event_id,
            "minutes_until_start": 5,
            "event_data": {"competition_id": 176},
        }
        for event_id in events
    ]
    plan = PreStartEventPlan(candidates, {c["event_id"]: c for c in candidates})
    runtime = SimpleNamespace(
        event_repo=SimpleNamespace(get_event_by_id=events.get),
        oddsportal=SimpleNamespace(active_thread=None),
    )
    for constant in simulator._TOGGLE_FLAGS.values():
        monkeypatch.setattr(simulator, constant, False)
    monkeypatch.setattr(simulator, "ENABLE_CUSTOM_PILLAR_FILTER", True)
    monkeypatch.setattr(simulator.Config, "PRE_START_ODDS_MOMENTS", [120, 30, 5, 0, -5])
    monkeypatch.setattr(simulator.Config, "PILLAR_PIPELINE_EXECUTION_MOMENTS", [5])
    monkeypatch.setattr(simulator, "_ensure_logging_configured", lambda: None)
    monkeypatch.setattr(simulator, "observe_operation", lambda *args: nullcontext())
    monkeypatch.setattr(simulator, "PreStartRuntime", lambda _: runtime)
    monkeypatch.setattr(simulator, "_log_pipeline_eligibility", lambda _: True)
    monkeypatch.setattr(
        simulator.EventRepository,
        "_build_event_data_with_legacy_fallback",
        lambda event: {
            "id": event.id,
            "starts_at": event.starts_at,
            "sport": event.sport,
        },
    )
    monkeypatch.setattr(simulator, "load_pre_start_odds_source_states", lambda _: {})
    monkeypatch.setattr(
        simulator, "build_pre_start_event_candidates", lambda *args, **kwargs: plan
    )
    monkeypatch.setattr(
        simulator,
        "run_production_odds_phase",
        lambda *args, **kwargs: calls.append(("providers", kwargs))
        or SimpleNamespace(event_plan=plan),
    )
    monkeypatch.setattr(
        simulator,
        "start_oddsportal_scrape_for_events",
        lambda *args, **kwargs: calls.append(("oddsportal", kwargs))
        or SimpleNamespace(event_states={}, event_ids=set()),
    )
    monkeypatch.setattr(
        simulator, "_wait_for_oddsportal_worker", lambda _: calls.append(("wait", {}))
    )
    monkeypatch.setattr(
        simulator,
        "evaluate_pre_start_key_moments",
        lambda *args, **kwargs: calls.append(("evaluation", kwargs)),
    )
    monkeypatch.setattr(simulator.api_client, "challenge_evidence_enabled", False)
    monkeypatch.setattr(
        simulator.api_client,
        "set_challenge_evidence_enabled",
        lambda enabled: setattr(
            simulator.api_client, "challenge_evidence_enabled", enabled
        ),
    )
    return SimpleNamespace(calls=calls, plan=plan, events=events, runtime=runtime)


@pytest.mark.parametrize(
    "alerts,pillars", [(False, False), (True, False), (False, True), (True, True)]
)
@pytest.mark.parametrize("custom_filter", [False, True])
def test_pipeline_switches_override_production_independently(
    simulation,
    monkeypatch,
    alerts,
    pillars,
    custom_filter,
):
    # Opposite production switches expose the previous custom-filter=False bug.
    monkeypatch.setattr(simulator.Config, "ENABLE_LEGACY_ALERT_PIPELINE", not alerts)
    monkeypatch.setattr(simulator.Config, "ENABLE_PILLAR_PIPELINE", not pillars)
    monkeypatch.setattr(
        simulator.Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False
    )
    context = EventContext(
        event_id=101,
        custom_id=None,
        sport="Football",
        season_id=None,
        season_name=None,
        season_year=None,
        starts_at=simulation.events[101].starts_at,
        minutes_until_start=5,
        discovery_source=None,
        home=None,
        away=None,
        competition=None,
        participants_label="Home vs Away",
        context_status="VALID",
    )
    monkeypatch.setattr(
        simulator,
        "evaluate_pre_start_key_moments",
        evaluation.evaluate_pre_start_key_moments,
    )
    monkeypatch.setattr(
        evaluation, "_hydrate_missing_tennis_metadata", lambda *args: None
    )
    monkeypatch.setattr(
        evaluation, "_build_evaluation_payloads", lambda *args: [context]
    )
    monkeypatch.setattr(
        evaluation, "flush_missing_standings_endpoints", lambda *args: None
    )
    monkeypatch.setattr(evaluation, "_load_trajectory_payloads", lambda *args: {})
    monkeypatch.setattr(
        evaluation,
        "evaluate_and_dispatch_alerts_batch",
        lambda *args, **kwargs: simulation.calls.append(("alerts", kwargs)),
    )
    monkeypatch.setattr(
        evaluation,
        "evaluate_and_calculate_pillars_batch",
        lambda *args, **kwargs: simulation.calls.append(("pillars", kwargs)),
    )
    assert simulator.simulate_pre_start_check(
        101,
        5,
        toggle_overrides={
            "alerts": alerts,
            "pillars": pillars,
            "custom-pillar-filter": custom_filter,
        },
    )
    names = [name for name, _ in simulation.calls]
    assert ("alerts" in names) is alerts
    assert ("pillars" in names) is pillars
    if pillars:
        kwargs = next(kwargs for name, kwargs in simulation.calls if name == "pillars")
        assert kwargs["enabled_pillars"] == (
            {f"pillar_{i}": False for i in range(1, 6)} if custom_filter else None
        )


@pytest.mark.parametrize("pillar", range(1, 6))
def test_each_pillar_switch_selects_only_that_pillar(simulation, pillar):
    assert simulator.simulate_pre_start_check(
        101,
        5,
        toggle_overrides={
            "pillars": True,
            f"pillar-{pillar}": True,
        },
    )
    kwargs = next(kwargs for name, kwargs in simulation.calls if name == "evaluation")
    assert kwargs["enabled_pillars"] == {
        f"pillar_{i}": i == pillar for i in range(1, 6)
    }


def test_pillar_defaults_reach_calculation_through_real_metadata_enrichment(
    simulation, monkeypatch
):
    from tests.pillars.test_structural_target_selection import _event_context
    from modules.pillars import competition_metadata_resolver as resolver

    context = _event_context()
    context.event_id = 101
    context.competition.standings_grouping = "single_table"
    simulation.events[101].round = "regular_season"
    simulation.runtime.recently_rescheduled = set()
    simulation.plan.candidates[:] = [simulation.plan.by_event_id[101]]
    monkeypatch.setattr(simulator, "ENABLE_PILLAR_PIPELINE", True)
    monkeypatch.setattr(simulator, "ENABLE_PILLAR_1", True)
    monkeypatch.setattr(
        simulator.Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False
    )
    # Keep both Config attributes mandatory: monkeypatch must fail if either
    # setting is accidentally removed while cleaning unrelated configuration.
    monkeypatch.setattr(
        simulator.Config, "FORCE_STANDINGS_COMPETITION_METADATA_REFRESH", False
    )
    monkeypatch.setattr(
        simulator.Config, "ENABLE_STANDINGS_COMPETITION_METADATA_ENRICHMENT", True
    )
    monkeypatch.setattr(
        simulator,
        "evaluate_pre_start_key_moments",
        evaluation.evaluate_pre_start_key_moments,
    )
    monkeypatch.setattr(evaluation, "build_event_context", lambda **kwargs: context)
    monkeypatch.setattr(resolver, "get_league_config", lambda **kwargs: None)
    monkeypatch.setattr(resolver, "_COMPETITION_METADATA_REFRESH_ATTEMPTED", set())
    monkeypatch.setattr(evaluation, "_load_trajectory_payloads", lambda _: {})
    monkeypatch.setattr(
        evaluation,
        "evaluate_and_calculate_pillars_batch",
        lambda **kwargs: simulation.calls.append(("pillars", kwargs)),
    )

    def unexpected_standings(*args, **kwargs):
        pytest.fail("Complete metadata must not require a standings request")

    monkeypatch.setattr(
        resolver.api_client, "get_standings_response", unexpected_standings
    )

    assert simulator.simulate_pre_start_check(101, 5)
    kwargs = next(kwargs for name, kwargs in simulation.calls if name == "pillars")
    assert kwargs["events_for_pillars"] == [context]
    assert context.competition_metadata_resolved is True
    assert kwargs["enabled_pillars"] == {f"pillar_{i}": i == 1 for i in range(1, 6)}


@pytest.mark.parametrize("ingestion", [False, True])
@pytest.mark.parametrize("provider", ["sofascore", "oddspapi", "oddsportal"])
def test_provider_switches_obey_ingestion_master(simulation, ingestion, provider):
    assert simulator.simulate_pre_start_check(
        [101, 102],
        5,
        toggle_overrides={
            "odds-ingestion": ingestion,
            provider: True,
        },
    )
    names = [name for name, _ in simulation.calls]
    assert ("providers" in names) is ingestion
    assert ("oddsportal" in names) is (ingestion and provider == "oddsportal")
    if ingestion:
        kwargs = next(
            kwargs for name, kwargs in simulation.calls if name == "providers"
        )
        assert kwargs["enable_sofascore"] is (provider == "sofascore")
        assert kwargs["enable_oddspapi"] is (provider == "oddspapi")
        assert set(kwargs["oddspapi_available_through_utc"]) == {101, 102}


@pytest.mark.parametrize(
    "sofascore,oddspapi", [(False, False), (True, False), (False, True), (True, True)]
)
def test_provider_phase_calls_only_enabled_processors(
    simulation, monkeypatch, sofascore, oddspapi
):
    monkeypatch.setattr(odds_phase, "load_pre_start_odds_source_states", lambda _: {})
    monkeypatch.setattr(
        odds_phase,
        "build_pre_start_event_candidates",
        lambda *args, **kwargs: simulation.plan,
    )
    monkeypatch.setattr(odds_phase, "attach_stored_observations", lambda _: None)
    monkeypatch.setattr(odds_phase, "persist_snapshot_observations", lambda _: None)
    monkeypatch.setattr(
        odds_phase,
        "run_sofascore_pre_start_odds",
        lambda *args, **kwargs: simulation.calls.append(("sofascore", kwargs)),
    )
    monkeypatch.setattr(
        odds_phase,
        "run_oddspapi_pre_start_odds",
        lambda *args, **kwargs: simulation.calls.append(("oddspapi", kwargs))
        or SimpleNamespace(
            requests_attempted=0, events_ingested=0, events_skipped=0, events_failed=0
        ),
    )
    outcome = odds_phase.run_production_odds_phase(
        simulation.events[101],
        5,
        [5],
        debug_mode=True,
        runtime=simulation.runtime,
        enable_sofascore=sofascore,
        enable_oddspapi=oddspapi,
    )
    assert outcome.event_plan == simulation.plan
    names = [name for name, _ in simulation.calls]
    assert ("sofascore" in names) is sofascore
    assert ("oddspapi" in names) is oddspapi


@pytest.mark.parametrize(
    "ingestion,report", [(False, False), (False, True), (True, True)]
)
def test_report_covers_every_event_after_provider_completion(
    simulation, monkeypatch, ingestion, report
):
    def load_ids(event_ids):
        simulation.calls.append(("before", event_ids))
        return {101: {11}, 102: {22}}

    monkeypatch.setattr(simulator, "_load_snapshot_ids_by_event", load_ids)
    monkeypatch.setattr(
        simulator,
        "_log_persisted_market_odds",
        lambda *args: simulation.calls.append(("report", args)),
    )
    assert simulator.simulate_pre_start_check(
        [101, 102],
        5,
        toggle_overrides={
            "odds-ingestion": ingestion,
            "market-report": report,
            "pillars": True,
        },
    )
    names = [name for name, _ in simulation.calls]
    if report:
        assert (
            names.index("before")
            < names.index("wait")
            < names.index("report")
            < names.index("evaluation")
        )
        assert [args for name, args in simulation.calls if name == "report"] == [
            (101, {11}),
            (102, {22}),
        ]
    else:
        assert "before" not in names and "report" not in names


@pytest.mark.parametrize("failure", [None, "provider", "persistence"])
def test_configuration_is_restored_and_worker_is_joined_on_failure(
    simulation, monkeypatch, failure
):
    monkeypatch.setattr(simulator.Config, "ENABLE_ODDS_EXTRACTION", False)
    monkeypatch.setattr(simulator.Config, "ENABLE_ODDSPAPI_PRE_START_ODDS", False)

    def run_phase(*args, **kwargs):
        assert simulator.Config.ENABLE_ODDS_EXTRACTION is True
        assert simulator.Config.ENABLE_ODDSPAPI_PRE_START_ODDS is True
        if failure == "provider":
            raise RuntimeError("provider failed")
        return SimpleNamespace(event_plan=simulation.plan)

    def evaluate(*args, **kwargs):
        assert ("wait", {}) in simulation.calls
        if failure == "persistence":
            raise RuntimeError("persistence failed")

    monkeypatch.setattr(simulator, "run_production_odds_phase", run_phase)
    monkeypatch.setattr(simulator, "evaluate_pre_start_key_moments", evaluate)
    call = lambda: simulator.simulate_pre_start_check(
        101,
        5,
        toggle_overrides={
            "odds-ingestion": True,
            "oddspapi": True,
            "oddsportal": True,
            "pillars": True,
        },
    )
    if failure:
        with pytest.raises(RuntimeError, match=failure):
            call()
    else:
        assert call()
    assert ("wait", {}) in simulation.calls
    assert simulator.Config.ENABLE_ODDS_EXTRACTION is False
    assert simulator.Config.ENABLE_ODDSPAPI_PRE_START_ODDS is False
    assert simulator.ENABLE_ODDS_INGESTION_SIMULATION is False
    assert simulator.ENABLE_PILLAR_PIPELINE is False
    assert simulator.api_client.challenge_evidence_enabled is False


@pytest.mark.parametrize("enabled", [False, True])
def test_cli_accepts_every_toggle(simulation, monkeypatch, enabled):
    flags = [f"--{'' if enabled else 'no-'}{flag}" for flag in simulator._TOGGLE_FLAGS]
    monkeypatch.setattr(
        simulator.sys, "argv", ["simulate", "101,102", "--minutes", "5", *flags]
    )
    monkeypatch.setattr(simulator, "initialize_system", lambda: True)
    captured = []
    monkeypatch.setattr(
        simulator,
        "simulate_pre_start_check",
        lambda *args, **kwargs: captured.append((args, kwargs)) or True,
    )
    assert simulator.main() == 0
    assert captured == [
        (
            ([101, 102], 5),
            {"toggle_overrides": {flag: enabled for flag in simulator._TOGGLE_FLAGS}},
        )
    ]


def test_missing_event_is_reported_as_failure_after_processing_found_events(simulation):
    assert simulator.simulate_pre_start_check([101, 999], 5) is False


def test_disabling_ingestion_avoids_sofascore_timing_fetch(simulation, monkeypatch):
    from modules.jobs.pre_start_check_job.event_candidate_builder import (
        build_pre_start_event_candidates,
    )

    monkeypatch.setattr(
        simulator, "build_pre_start_event_candidates", build_pre_start_event_candidates
    )
    monkeypatch.setattr(simulator.Config, "ENABLE_ODDS_EXTRACTION", True)
    monkeypatch.setattr(simulator.Config, "ENABLE_TIMESTAMP_CORRECTION", True)

    def unexpected_request(*args, **kwargs):
        pytest.fail("Ingestion disabled must not acquire odds or correct kickoff")

    monkeypatch.setattr(simulator.api_client, "get_event_results", unexpected_request)
    assert simulator.simulate_pre_start_check(101, 5)
    assert simulator.Config.ENABLE_ODDS_EXTRACTION is True


@pytest.mark.parametrize(
    "minutes,allowed,expected", [(5, [5], True), (30, [5], False), (17, [], False)]
)
def test_simulated_minutes_respect_production_moment_filters(
    simulation, monkeypatch, minutes, allowed, expected
):
    monkeypatch.setattr(
        simulator,
        "evaluate_pre_start_key_moments",
        evaluation.evaluate_pre_start_key_moments,
    )
    monkeypatch.setattr(
        simulator.Config, "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS", False
    )
    monkeypatch.setattr(simulator.Config, "PILLAR_PIPELINE_EXECUTION_MOMENTS", allowed)
    for candidate in simulation.plan.candidates:
        candidate["minutes_until_start"] = minutes
    monkeypatch.setattr(
        evaluation, "_hydrate_missing_tennis_metadata", lambda *args: None
    )
    # The real contract is typed; supply the minimal fields consumed by selection.
    context = EventContext(
        event_id=101,
        custom_id=None,
        sport="Football",
        season_id=None,
        season_name=None,
        season_year=None,
        starts_at=simulation.events[101].starts_at,
        minutes_until_start=minutes,
        discovery_source=None,
        home=None,
        away=None,
        competition=None,
        participants_label="Home vs Away",
        context_status="VALID",
    )
    monkeypatch.setattr(
        evaluation, "_build_evaluation_payloads", lambda *args: [context]
    )
    monkeypatch.setattr(
        evaluation, "flush_missing_standings_endpoints", lambda *args: None
    )
    monkeypatch.setattr(evaluation, "_load_trajectory_payloads", lambda *args: {})
    monkeypatch.setattr(
        evaluation,
        "evaluate_and_calculate_pillars_batch",
        lambda **kwargs: simulation.calls.append(("pillars", kwargs)),
    )
    assert simulator.simulate_pre_start_check(
        101, minutes, toggle_overrides={"pillars": True}
    )
    assert any(name == "pillars" for name, _ in simulation.calls) is expected


def test_market_report_reads_all_sources_and_keeps_source_timestamps(
    monkeypatch, tmp_path, caplog
):
    from datetime import timedelta
    from decimal import Decimal
    from infrastructure.persistence.database import DatabaseManager
    from infrastructure.persistence.models import (
        Bookie,
        CanonicalMarketType,
        Event,
        Market,
        MarketChoice,
        MarketChoiceQuote,
        MarketChoiceSnapshot,
    )

    manager = DatabaseManager(f"sqlite:///{tmp_path / 'report.db'}")
    manager.create_tables()
    monkeypatch.setattr(simulator, "db_manager", manager)
    observed = datetime(2026, 10, 12, 17, tzinfo=timezone.utc)
    try:
        with manager.get_session() as session:
            market_type = CanonicalMarketType(
                canonical_market_key="simulator_test",
                market_type_id=32000,
                canonical_market_name="1X2",
                canonical_market_group="1X2",
                canonical_market_period="Full Time",
                market_family="side_3way",
            )
            session.add(
                Event(
                    id=101,
                    slug="home-away",
                    sport="Football",
                    competition="Test",
                    home_team="Home",
                    away_team="Away",
                    starts_at=observed,
                    markets=[
                        Market(
                            bookie=Bookie(name=name, slug=source),
                            canonical_market_type=market_type,
                            choices=[
                                MarketChoice(
                                    choice_name="1",
                                    quotes=[
                                        MarketChoiceQuote(
                                            source=source,
                                            snapshots=[
                                                MarketChoiceSnapshot(
                                                    odds_value=Decimal("2.125"),
                                                    collected_at=observed,
                                                    source_collected_at=observed
                                                    - timedelta(minutes=10),
                                                )
                                            ],
                                        )
                                    ],
                                )
                            ],
                        )
                        for source, name in [
                            ("sofascore", "SofaScore"),
                            ("oddspapi", "Pinnacle"),
                            ("oddsportal", "Betfair"),
                        ]
                    ],
                )
            )
        before = simulator._load_snapshot_ids_by_event([101, 102])
        assert len(before[101]) == 3 and before[102] == set()
        with manager.get_session() as session:
            quote = session.query(MarketChoiceQuote).filter_by(source="oddspapi").one()
            session.add(
                MarketChoiceSnapshot(quote_id=quote.quote_id, odds_value=Decimal("2.2"))
            )
        with caplog.at_level("INFO", logger=simulator.__name__):
            simulator._log_persisted_market_odds(101, before[101])
        assert "new_snapshots_this_run=1" in caplog.text
        for source in ("sofascore", "oddspapi", "oddsportal"):
            assert f"source='{source}'" in caplog.text
        assert "source_collected_at=2026-10-12 16:50:00+00:00" in caplog.text
        assert "available_at=" not in caplog.text
        assert "recorded_at=" not in caplog.text
    finally:
        manager.engine.dispose()


def test_ingestion_only_reports_real_clocks_without_mining_configuration(
    simulation, caplog
):
    import logging

    with caplog.at_level(logging.INFO):
        assert simulator.simulate_pre_start_check(
            101,
            5,
            toggle_overrides={
                "odds-ingestion": True,
                "oddspapi": True,
                "pillars": False,
                "alerts": False,
            },
        )
    assert "providers" in [name for name, _ in simulation.calls]
    assert "collected_at and evaluation_as_of retain real UTC" in caplog.text
    assert "historical source cutoff" in caplog.text
