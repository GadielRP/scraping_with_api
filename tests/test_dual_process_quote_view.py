from infrastructure.persistence.models import build_dual_process_event_odds_view_sql


def test_quote_and_legacy_dual_views_keep_identical_select_contract():
    legacy = build_dual_process_event_odds_view_sql(
        ["1X2"],
        ["Full Time"],
        view_name="v_dual_process_event_odds_legacy",
        quote_aware=False,
    )
    quotes = build_dual_process_event_odds_view_sql(
        ["1X2"],
        ["Full Time"],
        view_name="v_dual_process_event_odds_quotes",
        quote_aware=True,
    )
    public_columns = (
        "event_id",
        "market_id",
        "market_name",
        "market_group",
        "market_period",
        "bookie_id",
        "collected_at",
        "one_open",
        "one_final",
        "x_open",
        "x_final",
        "two_open",
        "two_final",
        "var_one",
        "var_x",
        "var_two",
        "var_shape",
        "last_sync_at",
    )
    for column in public_columns:
        assert column in legacy
        assert column in quotes


def test_legacy_and_quote_latest_snapshot_predicates_cannot_be_confused():
    legacy = build_dual_process_event_odds_view_sql(
        ["1X2"], ["Full Time"], quote_aware=False
    )
    quotes = build_dual_process_event_odds_view_sql(
        ["1X2"], ["Full Time"], quote_aware=True
    )
    assert "mcs.choice_id = mc.choice_id" in legacy
    assert "mcs.quote_id = mcq.quote_id" not in legacy
    assert "mcs.quote_id = mcq.quote_id" in quotes
    assert "mcs.choice_id = mc.choice_id" not in quotes
