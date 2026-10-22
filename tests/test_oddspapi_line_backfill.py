"""Offline repair contracts: preserve history, isolate providers, reject drift."""
from copy import deepcopy
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
    action, reason = repair._action(family, state, captures, index, False)
    assert reason == "planned"
    return state, family, action, captures


def test_apply_without_debug_is_exact_and_idempotent(sample, tmp_path):
    manager, eid, _, _, _ = sample
    state, family, action, _ = _make_action(sample, tmp_path)
    assert len(action["delete_quote_ids"]) == 2
    assert len(action["delete_market_ids"]) == 1
    # No capture files are consulted by apply, including when it refreshes cache.
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


def test_shared_provider_survives_and_missing_evidence_only_changes_flags(sample, tmp_path):
    manager, eid, _, index, _ = sample
    with manager.get_session() as session:
        choice = session.query(MarketChoice).order_by(MarketChoice.choice_id).first()
        session.add(MarketChoiceQuote(choice_id=choice.choice_id, source="oddsportal", initial_odds=2))
    state, family, action, _ = _make_action(sample, tmp_path)
    assert not action["delete_market_ids"]
    repair.LineRepair().apply_event(eid, expected_state={**state, "actions": [action]})
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).filter_by(source="oddsportal").count() == 1
        assert session.query(Market).count() == 2
        state = repair._load_event(session, eid)
    # A quote not backed by a debug file may be demoted from a coherent cache,
    # but its history is never deleted.
    family = next(iter(state["families"].values()))
    family["markets"] = deepcopy(family["markets"])
    keep = next(m for m in family["markets"] if repair.Decimal(m["line_value"]) == repair.Decimal("3.5"))
    for q in repair._quotes(keep):
        q["main_line"] = False
    action, reason = repair._action(family, state, [], index, False)
    assert reason == "planned"
    assert action["mode"] == "cache_flags_only"
    assert not action["delete_quote_ids"]


def test_preserves_prior_selected_lines_and_ambiguous_history(sample, tmp_path):
    _, _, _, index, _ = sample
    state, family, _, captures = _make_action(sample, tmp_path)
    prior = deepcopy(captures[0])
    fk = ("bet365", 14, False)
    prior["moment"] = 120
    prior["selected"][fk] = "2.5"
    action, reason = repair._action(family, state, [prior, *captures], index, False)
    assert reason == "planned"
    assert not action["delete_quote_ids"]
    assert set(action["main_line"].values()) == {False}
    # Two unknown-moment captures cannot establish the current selection.
    prior["moment"] = None
    assert repair._action(family, state, [prior, *captures], index, False)[1] == "ambiguous_capture_order"
    assert repair._action(family, state, [], index, False)[1] == "missing_or_ambiguous_selection"


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


def test_prior_unproven_observation_is_demoted_but_never_deleted(sample, tmp_path):
    _, _, _, index, _ = sample
    state, family, _, captures = _make_action(sample, tmp_path)
    discarded = family["markets"][0]
    for q in repair._quotes(discarded):
        q["created_at"] = "2026-10-20 20:55:00+00:00"
    action, reason = repair._action(family, state, captures, index, False)
    assert reason == "planned"
    assert not action["delete_quote_ids"]
    assert set(action["main_line"].values()) == {False}
    missing = deepcopy(family)
    missing["markets"] = [discarded]
    assert repair._action(missing, state, captures, index, False)[1] == "selected_market_missing"


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


@pytest.mark.parametrize("donor", ["bet365", "pinnacle"])
def test_cache_fallback_applies_flags_and_preserves_all_history(sample, donor):
    manager, eid, _, index, _ = sample
    with manager.get_session() as session:
        for oid in ("2-0", "2-1"):
            session.add(OddspapiMainlineOutcomeCache(event_id=eid, fixture_id="fixture-1",
                source_sport_id="13", bookmaker_slug=donor, source_market_id="2", source_outcome_id=oid,
                canonical_market_key="over_under_full_time", is_exchange=False))
    with manager.get_session() as session:
        state = repair._load_event(session, eid)
    family = next(iter(state["families"].values()))
    action, reason = repair._action(family, state, [], index, False)
    assert reason == "planned" and action["mode"] == "cache_flags_only"
    assert not action["delete_quote_ids"] and action["cache_after"] is None
    assert repair.LineRepair().apply_event(eid, expected_state={**state, "actions": [action]})["status"] == "APPLIED"
    with manager.get_session() as session:
        assert session.query(MarketChoiceSnapshot).count() == 4
        assert session.query(MarketChoiceQuote).filter_by(main_line=True).count() == 2
