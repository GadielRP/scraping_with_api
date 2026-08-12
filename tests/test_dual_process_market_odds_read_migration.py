from pathlib import Path

from infrastructure.persistence.models import (
    EVENT_ALL_ODDS_VIEW_SQL,
    MV_ALERT_EVENTS_SQL,
    build_dual_process_event_odds_view_sql,
    build_dual_process_public_view_sql,
)


ROOT = Path(__file__).resolve().parents[1]


def test_dual_process_event_odds_view_uses_market_tables():
    sql = build_dual_process_event_odds_view_sql(["Full time", "Home/Away"], ["Full-time", "Match"])

    assert "v_dual_process_event_odds" in sql
    assert "FROM event_odds" not in sql
    assert "FROM markets m" in sql
    assert "JOIN market_choices mc" in sql
    assert "FROM market_choice_snapshots mcs" in sql
    assert "m.bookie_id = 1" in sql
    assert "m.is_live = false" in sql
    assert "m.market_name IN ('Full time', 'Home/Away')" in sql
    assert "OR m.market_group IN ('Full time', 'Home/Away')" in sql
    assert "m.market_period IN ('Full-time', 'Match')" in sql
    assert "ORDER BY mcs.collected_at DESC, mcs.snapshot_id DESC" in sql


def test_quote_view_selects_exact_sofascore_quote_and_latest_tick_by_quote():
    sql = build_dual_process_event_odds_view_sql(
        ["Full time", "Home/Away"],
        ["Full-time", "Match"],
        view_name="v_dual_process_event_odds_quotes",
        quote_aware=True,
    )

    assert "JOIN LATERAL" in sql
    assert "quote_candidate.source = 'sofascore'" in sql
    assert "quote_candidate.exchange_side IS NULL" in sql
    assert "quote_candidate.exchange_level = 0" in sql
    assert "ORDER BY quote_candidate.quote_id" in sql
    assert "LIMIT 1" in sql
    assert "mcs.quote_id = mcq.quote_id" in sql
    assert "mcq.initial_odds" in sql
    assert "COALESCE(latest.odds_value, mcq.current_odds)" in sql
    assert "mcq.current_updated_at" in sql
    assert "mcq.initial_captured_at" in sql


def test_dual_process_public_wrapper_is_explicit_and_reversible():
    assert "v_dual_process_event_odds_legacy" in build_dual_process_public_view_sql("legacy")
    assert "v_dual_process_event_odds_quotes" in build_dual_process_public_view_sql("quotes")


def test_alert_and_reporting_views_read_dual_process_view():
    assert "FROM event_odds" not in EVENT_ALL_ODDS_VIEW_SQL
    assert "FROM v_dual_process_event_odds eo" in EVENT_ALL_ODDS_VIEW_SQL
    assert "FROM event_odds" not in MV_ALERT_EVENTS_SQL
    assert "FROM v_dual_process_event_odds eo" in MV_ALERT_EVENTS_SQL


def test_dual_process_modules_do_not_import_old_odds_repository():
    files = [
        ROOT / "modules" / "alerts" / "dual_process" / "process_1" / "engine.py",
        ROOT / "modules" / "alerts" / "dual_process" / "process_1" / "candidate_search.py",
        ROOT / "modules" / "alerts" / "dual_process" / "process_2" / "engine.py",
    ]

    for path in files:
        source = path.read_text(encoding="utf-8")
        assert "import OddsRepository" not in source
        assert " OddsRepository." not in source
        assert "EventOdds" not in source
