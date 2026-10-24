"""Offline repair contracts: replay current selection on original /odds responses."""
from datetime import datetime, timezone
import json

import pytest

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie, CanonicalMarketType, Event, EventSourceMapping, Market, MarketChoice,
    MarketChoiceQuote, MarketChoiceSnapshot, OddspapiMainlineOutcomeCache,
)
from infrastructure.persistence.repositories.market_mapping_repository import (
    CanonicalMarketResolution, CanonicalOutcomeResolution, MarketMappingIndex,
)
from infrastructure.persistence.backfill.strategy import BackfillConflict
from scripts.maintenance import backfill_oddspapi_lines as repair


@pytest.fixture
def sample(monkeypatch):
    manager = DatabaseManager("sqlite:///:memory:")
    manager.create_tables()
    monkeypatch.setattr(repair, "db_manager", manager)
    tick = datetime(2026, 10, 21, 20, 0, tzinfo=timezone.utc)
    batch = datetime(2026, 10, 21, 20, 55, tzinfo=timezone.utc)
    index = MarketMappingIndex({}, {})
    payload = {"fixtureId": "fixture-1", "sportId": "13", "bookmakerOdds": {"bet365": {"markets": {}}}}
    with manager.get_session() as session:
        event = Event(slug="line-repair", starts_at=datetime(2026, 10, 21, 21, tzinfo=timezone.utc),
                      sport="Football", competition="Test League", home_team="Home", away_team="Away")
        bookie = Bookie(name="bet365", slug="bet365")
        catalog = CanonicalMarketType(canonical_market_key="over_under_full_time", market_type_id=14,
            canonical_market_name="Over/Under Full Time", canonical_market_group="Totals",
            canonical_market_period="Full Time", market_family="totals", requires_line_value=True)
        session.add_all([event, bookie, catalog])
        session.flush()
        session.add(EventSourceMapping(event_id=event.id, source="oddspapi", source_event_id="fixture-1"))
        event_id, bookie_id = event.id, bookie.bookie_id
        for mid, line in (("1", "2.5"), ("2", "3.5")):
            mapping_id = int(mid)
            index.market_mappings[("oddspapi", "13", mid)] = CanonicalMarketResolution(
                resolved=True, mapping_id=mapping_id, market_type_id=14,
                canonical_market_key="over_under_full_time", canonical_market_name="Over/Under Full Time",
                canonical_market_group="Totals", canonical_market_period="Full Time",
                requires_line_value=True, source_handicap=line)
            market = Market(event_id=event.id, bookie_id=bookie.bookie_id, market_type_id=14,
                            line_value=line, is_live=False, collected_at=batch)
            session.add(market)
            session.flush()
            outcomes = {}
            for name, suffix in (("Over", "0"), ("Under", "1")):
                oid = f"{mid}-{suffix}"
                index.outcome_mappings[(mapping_id, oid)] = CanonicalOutcomeResolution(resolved=True, canonical_choice_name=name)
                choice = MarketChoice(market_id=market.market_id, choice_name=name)
                session.add(choice)
                session.flush()
                quote = MarketChoiceQuote(choice_id=choice.choice_id, source="oddspapi", main_line=True,
                    source_market_id=mid, source_outcome_id=oid, current_odds=1.9,
                    current_updated_at=batch, created_at=batch, updated_at=batch)
                session.add(quote)
                session.flush()
                session.add(MarketChoiceSnapshot(quote_id=quote.quote_id, odds_value=1.9,
                    source_collected_at=tick, collected_at=batch))
                outcomes[oid] = {"players": {"0": {"price": 1.9, "active": mid == "2", "mainLine": True,
                    "changedAt": tick.isoformat()}}}
            payload["bookmakerOdds"]["bet365"]["markets"][mid] = {"marketActive": True, "outcomes": outcomes}
        for oid in ("2-0", "2-1"):
            session.add(OddspapiMainlineOutcomeCache(event_id=event.id, fixture_id="fixture-1",
                source_sport_id="13", bookmaker_slug="bet365", source_market_id="2", source_outcome_id=oid,
                canonical_market_key="over_under_full_time", is_exchange=False))
    return manager, event_id, bookie_id, index, payload


def _make_action(sample, tmp_path):
    manager, event_id, _, index, payload = sample
    path = tmp_path / f"{event_id}_fixture-1_t_5_odds_bet365.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with manager.get_session() as session:
        state = repair._load_event(session, event_id)
    captures, issues = repair._captures([path], state, index, False)
    assert not issues
    family = next(iter(state["families"].values()))
    action, reason = repair._action(family, captures)
    assert reason == "planned"
    return state, family, action, captures


def test_apply_without_debug_is_exact_and_idempotent(sample, tmp_path):
    manager, eid, _, _, _ = sample
    state, family, action, _ = _make_action(sample, tmp_path)
    assert len(action["delete_quote_ids"]) == 2
    assert len(action["delete_market_ids"]) == 1
    # Apply needs no captures and leaves the existing cache unchanged.
    for path in tmp_path.iterdir():
        path.unlink()
    expected = {"starts_at": state["starts_at"], "fixtures": state["fixtures"], "actions": [action]}
    assert repair.LineRepair().apply_event(eid, expected_state=expected)["status"] == "APPLIED"
    assert repair.LineRepair().apply_event(eid, expected_state=expected)["status"] == "ALREADY_CORRECT"
    with manager.get_session() as session:
        assert session.query(Market).count() == 1
        assert session.query(MarketChoiceSnapshot).count() == 2
        assert session.query(OddspapiMainlineOutcomeCache).count() == 2


def test_drift_rolls_back_whole_event(sample, tmp_path):
    manager, eid, _, _, _ = sample
    state, _, action, _ = _make_action(sample, tmp_path)
    with manager.get_session() as session:
        session.get(MarketChoiceQuote, action["delete_quote_ids"][0]).current_odds = 2
    with pytest.raises(BackfillConflict, match="changed"):
        repair.LineRepair().apply_event(eid, expected_state={**state, "actions": [action]})
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).count() == 4


def test_shared_provider_survives_current_debug_selection(sample, tmp_path):
    manager, eid, _, index, payload = sample
    for outcome in payload["bookmakerOdds"]["bet365"]["markets"]["2"]["outcomes"].values():
        outcome["players"]["0"]["mainLine"] = False
    with manager.get_session() as session:
        choice = session.query(MarketChoice).order_by(MarketChoice.choice_id).first()
        session.add(MarketChoiceQuote(choice_id=choice.choice_id, source="oddsportal", initial_odds=2))
    path = tmp_path / f"{eid}_fixture-1_t_5_odds_bet365.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with manager.get_session() as session:
        state = repair._load_event(session, eid)
    captures, issues = repair._captures([path], state, index, False)
    assert not issues
    family = next(iter(state["families"].values()))
    action, reason = repair._action(family, captures)
    assert reason == "planned" and action["selected_line"] == "3.5"
    assert not action["delete_market_ids"]
    repair.LineRepair().apply_event(eid, expected_state={**state, "actions": [action]})
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).filter_by(source="oddsportal").count() == 1
        assert session.query(Market).count() == 2
        state = repair._load_event(session, eid)


def test_no_current_line_in_debug_is_omitted(sample, tmp_path):
    _, _, _, index, _ = sample
    state, family, _, captures = _make_action(sample, tmp_path)
    captures[0]["selected"].clear()
    action, reason = repair._action(family, captures)
    assert action is None
    assert reason == "missing_current_selection_in_debug"


def test_files_legacy_and_fixture_mismatch(sample, tmp_path):
    manager, eid, _, index, payload = sample
    path = tmp_path / f"{eid}_fixture-1_bet365.json"
    historical = tmp_path / f"{eid}_fixture-1_t_0_historical_odds_bet365.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    historical.write_text("{}", encoding="utf-8")
    assert repair._files(tmp_path)[eid] == [path]
    with manager.get_session() as session:
        state = repair._load_event(session, eid)
    captures, issues = repair._captures([path], state, index, False)
    assert len(captures) == 1 and not issues
    payload["fixtureId"] = "another-fixture"
    path.write_text(json.dumps(payload), encoding="utf-8")
    captures, issues = repair._captures([path], state, index, False)
    assert not captures and "fixture" in issues[0]["reason"]


def test_deletes_every_discarded_line_without_snapshot_matching(sample, tmp_path):
    _, _, _, index, _ = sample
    state, family, _, captures = _make_action(sample, tmp_path)
    for market in family["markets"]:
        for quote in repair._quotes(market):
            quote["created_at"] = "2026-10-20 20:55:00+00:00"
            quote["snapshots"] = []
    action, reason = repair._action(family, captures)
    assert reason == "planned"
    assert action["selected_line"] == "3.5"
    assert len(action["delete_quote_ids"]) == 2


def test_runner_portable_manifest_and_fresh_checkpoint_repeat(sample, tmp_path, monkeypatch):
    from infrastructure.persistence.backfill import runner
    manager, eid, _, _, _ = sample
    monkeypatch.setattr(runner, "db_manager", manager)
    state, _, action, _ = _make_action(sample, tmp_path)
    manifest = repair.build_manifest(strategy=repair.STRATEGY, scope={"source": "oddspapi"},
        events=[{"event_id": eid, "starts_at": state["starts_at"], "fixtures": state["fixtures"], "actions": [action]}])
    path = tmp_path / "manifest.json"
    repair.write_json_atomic(path, manifest)
    for f in tmp_path.glob("*_odds_*.json"):
        f.unlink()
    for target, expected in (("local", "APPLIED"), ("server", "ALREADY_CORRECT")):
        results = tmp_path / target / "results.jsonl"
        task = runner.BackfillRunner(strategy=repair.LineRepair(), manifest_path=path,
            checkpoint_path=tmp_path / target / "checkpoint.json", results_path=results, refresh_views=False)
        assert task.apply() == 0
        assert json.loads(results.read_text())["status"] == expected
    manifest["events"][0]["actions"][0]["selected_line"] = "99"
    with pytest.raises(ValueError, match="does not match"):
        repair.validate_manifest(manifest)


def test_runner_resumes_embedded_manifest_at_next_event(sample, tmp_path, monkeypatch):
    from infrastructure.persistence.backfill import runner
    manager, _, _, _, _ = sample
    monkeypatch.setattr(runner, "db_manager", manager)
    seen = []

    class Strategy:
        strategy_name = repair.STRATEGY

        def apply_event(self, event_id, *, expected_state):
            seen.append(event_id)
            return {"event_id": event_id, "status": "APPLIED"}

    path = tmp_path / "manifest.json"
    repair.write_json_atomic(path, repair.build_manifest(strategy=repair.STRATEGY, scope={},
        events=[{"event_id": 1, "starts_at": "2026-10-21"}, {"event_id": 2, "starts_at": "2026-10-22"}]))
    task = runner.BackfillRunner(strategy=Strategy(), manifest_path=path, refresh_views=False)
    assert task.apply(limit=1) == 0
    assert seen == [1]
    assert task.apply(limit=1) == 0
    assert seen == [1, 2]
    assert task.apply() == 0
    assert seen == [1, 2]


@pytest.mark.parametrize("cached_book", [None, "bet365", "pinnacle"])
def test_current_selector_ignores_missing_or_contradictory_cache(sample, cached_book, tmp_path):
    manager, eid, _, index, payload = sample
    with manager.get_session() as session:
        session.query(OddspapiMainlineOutcomeCache).filter_by(event_id=eid).delete()
        if cached_book:
            for oid in ("1-0", "1-1"):
                session.add(OddspapiMainlineOutcomeCache(event_id=eid, fixture_id="fixture-1",
                    source_sport_id="13", bookmaker_slug=cached_book, source_market_id="1", source_outcome_id=oid,
                    canonical_market_key="over_under_full_time", is_exchange=False))
    # Both lines are complete and active. Today's selector chooses by price gap,
    # even when the historical cache points at the other line.
    for mid, market in payload["bookmakerOdds"]["bet365"]["markets"].items():
        for n, outcome in enumerate(market["outcomes"].values()):
            outcome["players"]["0"].update(active=True, price=1.9 if n == 0 else (2.5 if mid == "1" else 1.95))
    with manager.get_session() as session:
        state = repair._load_event(session, eid)
    family = next(iter(state["families"].values()))
    path = tmp_path / f"{eid}_fixture-1_t_5_odds_bet365.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    captures, issues = repair._captures([path], state, index, False)
    assert not issues
    action, reason = repair._action(family, captures)
    assert reason == "planned" and action["selected_line"] == "3.5"
    assert len(action["delete_quote_ids"]) == 2
    decision = next(d for d in action["diagnostics"] if d["reason"] == "selected_current_line")
    assert decision["selected"]["line"] == "3.5"
    assert decision["discarded"][0]["line"] == "2.5"
    assert repair.LineRepair().apply_event(eid, expected_state={**state, "actions": [action]})["status"] == "APPLIED"
    with manager.get_session() as session:
        assert session.query(MarketChoiceSnapshot).count() == 2
        assert session.query(MarketChoiceQuote).filter_by(main_line=True).count() == 2


def test_plan_exports_current_selection_and_diagnostics_without_writes(sample, tmp_path, monkeypatch):
    manager, eid, _, index, payload = sample
    monkeypatch.setattr(repair.MarketMappingRepository, "build_index", lambda: index)
    with manager.get_session() as session:
        session.query(OddspapiMainlineOutcomeCache).filter_by(event_id=eid).delete()
    debug_root = tmp_path / "debug"
    debug_root.mkdir()
    capture_path = debug_root / f"{eid}_fixture-1_bet365.json"
    capture_path.write_text(json.dumps(payload), encoding="utf-8")
    original = capture_path.read_bytes()
    with manager.get_session() as session:
        before = repair._load_event(session, eid)
    output = tmp_path / "export" / "manifest.json"
    manifest = repair.plan(output, debug_root, require_active=False)
    repair.validate_manifest(manifest)
    assert manifest["parameters"]["selection_source"] == "select_current_lines"
    action = manifest["events"][0]["actions"][0]
    assert action["selected_line"] == "3.5"
    assert action["mode"] == "current_debug"
    assert len(action["delete_quote_ids"]) == 2
    report = repair.read_json(output.with_name("plan_report.json"))
    diagnostics = report["selections"][0]["diagnostics"]
    assert any(d.get("reason") == "inactive_choice" for d in diagnostics)
    assert any(d.get("selected", {}).get("line") == "3.5" for d in diagnostics)
    with manager.get_session() as session:
        assert repair._load_event(session, eid) == before
    assert capture_path.read_bytes() == original


def test_no_valid_candidate_is_reported_without_deletions(sample, tmp_path, monkeypatch):
    manager, eid, _, index, payload = sample
    monkeypatch.setattr(repair.MarketMappingRepository, "build_index", lambda: index)
    for market in payload["bookmakerOdds"]["bet365"]["markets"].values():
        for outcome in market["outcomes"].values():
            outcome["players"]["0"]["active"] = False
    (tmp_path / f"{eid}_fixture-1_bet365.json").write_text(json.dumps(payload), encoding="utf-8")
    output = tmp_path / "export" / "manifest.json"
    manifest = repair.plan(output, tmp_path, require_active=False)
    assert manifest["events"] == []
    report = repair.read_json(output.with_name("plan_report.json"))
    assert report["omissions"][0]["reason"] == "missing_current_selection_in_debug"
    assert all(d["reason"] == "inactive_choice" for d in report["selections"][0]["diagnostics"])
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).count() == 4
        assert session.query(MarketChoiceSnapshot).count() == 4


@pytest.mark.parametrize("requires_line", [True, False])
@pytest.mark.parametrize("shared", [True, False])
def test_disabled_ingestion_removes_family_without_debug_and_is_idempotent(
    sample, tmp_path, monkeypatch, requires_line, shared,
):
    manager, eid, _, index, _ = sample
    monkeypatch.setattr(repair.MarketMappingRepository, "build_index", lambda: index)
    with manager.get_session() as session:
        catalog = session.query(CanonicalMarketType).filter_by(canonical_market_key="over_under_full_time").one()
        catalog.enabled_for_ingestion = False
        catalog.requires_line_value = requires_line
        if not requires_line:
            session.query(Market).filter_by(event_id=eid).order_by(Market.market_id).first().line_value = None
        if shared:
            choice = session.query(MarketChoice).order_by(MarketChoice.choice_id).first()
            quote = MarketChoiceQuote(choice_id=choice.choice_id, source="oddsportal", current_odds=2)
            session.add(quote)
            session.flush()
            other_quote_id = quote.quote_id
            session.add(MarketChoiceSnapshot(quote_id=quote.quote_id, odds_value=2,
                source_collected_at=datetime(2026, 10, 21, tzinfo=timezone.utc),
                collected_at=datetime(2026, 10, 21, tzinfo=timezone.utc)))
    manifest = repair.plan(tmp_path / "export" / "manifest.json", tmp_path / "missing_debug", require_active=True)
    expected = manifest["events"][0]
    action = expected["actions"][0]
    assert action["mode"] == "disabled_ingestion"
    assert action["diagnostics"][0]["reason"] == "disabled_for_ingestion"
    assert len(action["delete_quote_ids"]) == 4
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).filter_by(source="oddspapi").count() == 4
    assert repair.LineRepair().apply_event(eid, expected_state=expected)["status"] == "APPLIED"
    assert repair.LineRepair().apply_event(eid, expected_state=expected)["status"] == "ALREADY_CORRECT"
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).filter_by(source="oddspapi").count() == 0
        assert session.query(Market).count() == int(shared)
        assert session.query(MarketChoiceSnapshot).count() == int(shared)
        assert session.query(OddspapiMainlineOutcomeCache).count() == 2
        if shared:
            assert session.get(MarketChoiceQuote, other_quote_id).current_odds == 2


def test_reenabled_market_conflicts_with_disabled_deletion(sample, tmp_path):
    manager, eid, _, _, _ = sample
    with manager.get_session() as session:
        session.query(CanonicalMarketType).filter_by(canonical_market_key="over_under_full_time").one().enabled_for_ingestion = False
    with manager.get_session() as session:
        state = repair._load_event(session, eid)
    action, _ = repair._action(next(iter(state["families"].values())), [])
    with manager.get_session() as session:
        session.query(CanonicalMarketType).filter_by(canonical_market_key="over_under_full_time").one().enabled_for_ingestion = True
    with pytest.raises(BackfillConflict, match="changed"):
        repair.LineRepair().apply_event(eid, expected_state={**state, "actions": [action]})
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).count() == 4
