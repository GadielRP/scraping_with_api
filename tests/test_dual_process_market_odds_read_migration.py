from pathlib import Path

from infrastructure.persistence.views.dual_process_views import (
    EVENT_ALL_ODDS_VIEW_SQL,
    MV_ALERT_EVENTS_SQL,
    build_dual_process_event_odds_view_sql,
)


ROOT = Path(__file__).resolve().parents[1]


def test_dual_process_event_odds_view_uses_market_tables():
    sql = build_dual_process_event_odds_view_sql(["Full time", "Home/Away"], ["Full-time", "Match"])

    assert "v_dual_process_event_odds" in sql
    assert "FROM event_odds" not in sql
    assert "FROM markets m" in sql
    assert "JOIN market_choices mc" in sql
    assert "FROM market_choice_snapshots mcs" in sql
    assert "JOIN canonical_market_types cmt" in sql
    assert "m.bookie_id = 1" in sql
    assert "m.is_live = false" in sql
    assert "cmt.canonical_market_name IN ('Full time', 'Home/Away')" in sql
    assert "OR cmt.canonical_market_group IN ('Full time', 'Home/Away')" in sql
    assert "cmt.canonical_market_period IN ('Full-time', 'Match')" in sql
    assert "m.market_name" not in sql
    assert "m.market_group" not in sql
    assert "m.market_period" not in sql
    assert "ORDER BY mcs.collected_at DESC, mcs.snapshot_id DESC" in sql


def test_canonical_view_selects_exact_sofascore_quote_and_latest_tick_by_quote():
    sql = build_dual_process_event_odds_view_sql(
        ["Full time", "Home/Away"],
        ["Full-time", "Match"],
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
    assert "mc.initial_odds" not in sql
    assert "mc.current_odds" not in sql
    assert "v_dual_process_event_odds_legacy" not in sql
    assert "v_dual_process_event_odds_quotes" not in sql


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


def test_dual_process_view_supports_canonical_overtime_period():
    from infrastructure.settings import Config

    sql = build_dual_process_event_odds_view_sql(
        Config.MARKETS_DUAL_PROCESS,
        Config.PERIODS_DUAL_PROCESS,
    )
    assert "Full Time Including Overtime" in sql
    assert "Home/Away" in sql
    assert "1X2" in sql
