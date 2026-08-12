from infrastructure.persistence.models import (
    PRE_START_ODDS_TRAJECTORY_VIEW_SQL,
)


def test_quote_view_uses_exact_quote_lineage_and_quote_metadata():
    sql = PRE_START_ODDS_TRAJECTORY_VIEW_SQL
    assert "JOIN eligible_quotes mcq" in sql
    assert "mcq.quote_id = mcs.quote_id" in sql
    assert "mc.choice_id = mcq.choice_id" in sql
    assert "mcs.choice_id" not in sql
    assert "mcq.initial_odds AS initial_odds" in sql
    assert "mcq.source AS source" in sql
    assert "mcq.exchange_side AS exchange_side" in sql
    assert "mcq.exchange_level AS exchange_level" in sql


def test_top_level_is_selected_only_from_quotes_with_history():
    sql = PRE_START_ODDS_TRAJECTORY_VIEW_SQL
    assert "WHERE history.quote_id = mcq.quote_id" in sql
    assert "ORDER BY mcq.exchange_level, mcq.quote_id" in sql
    assert "WHERE ranked.depth_rank = 1" in sql
    assert "explicit_history.quote_id = explicit_q.quote_id" in sql


def test_canonical_view_has_no_rollout_variants():
    sql = PRE_START_ODDS_TRAJECTORY_VIEW_SQL
    assert "CREATE OR REPLACE VIEW v_pre_start_odds_trajectory AS" in sql
    assert "v_pre_start_odds_trajectory_legacy" not in sql
    assert "v_pre_start_odds_trajectory_quotes" not in sql
    assert "mc.initial_odds" not in sql
