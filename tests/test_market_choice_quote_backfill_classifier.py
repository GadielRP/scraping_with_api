"""Pure classifier tests for Phase 4b MarketChoiceQuote backfill."""

from datetime import datetime
from types import SimpleNamespace

from modules.odds_ingestion.backfill.market_choice_quote_backfill import (
    BackfillCandidate,
    ClassificationStatus,
    classify_candidate,
    parse_legacy_back_lay_choice_group,
)


def _candidate(**kwargs):
    base = dict(
        kind="snapshot",
        snapshot_id=1,
        choice_id=10,
        market_id=100,
        event_id=1000,
        bookie_id=2,
        market_name="Over/Under Full Time",
        market_period="Full Time",
        choice_group=None,
        is_live=False,
        choice_name="over",
        raw_source="oddspapi",
        exchange_side=None,
        exchange_level=0,
        odds_value=1.9,
        collected_at=datetime(2026, 6, 20, 12, 0, 0),
    )
    base.update(kwargs)
    return BackfillCandidate(**base)


def test_parse_legacy_back_lay_variants():
    assert parse_legacy_back_lay_choice_group("Back") == ("back", None)
    assert parse_legacy_back_lay_choice_group("Lay") == ("lay", None)
    assert parse_legacy_back_lay_choice_group("Back 2.5") == ("back", "2.5")
    assert parse_legacy_back_lay_choice_group("lay 3.0") == ("lay", "3.0")
    assert parse_legacy_back_lay_choice_group("2.5") == (None, None)


def test_sportsbook_with_source_resolves_null_side():
    decision = classify_candidate(
        _candidate(raw_source="oddsportal", exchange_side=None),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.identity is not None
    assert decision.identity.source == "oddsportal"
    assert decision.identity.exchange_side is None
    assert decision.identity.exchange_level == 0


def test_channel_sources_rewrite_to_sofascore():
    for channel in (
        "daily_discovery",
        "dropping_odds",
        "winning_odds",
        "secondary_discovery",
        "parallel_odds_checking",
        "sofascore_daily_discovery",
        "sofascore_dropping_odds",
    ):
        decision = classify_candidate(
            _candidate(raw_source=channel, bookie_id=1),
            bookie_sources={},
            exchange_choice_ids=set(),
            canonical_markets={},
            choices_by_market_name={},
        )
        assert decision.status is ClassificationStatus.RESOLVED, channel
        assert decision.identity.source == "sofascore"
        assert decision.evidence.get("canonicalized_from") == channel


def test_channel_ticks_share_sofascore_identity_and_latest_current():
    from modules.odds_ingestion.backfill.market_choice_quote_backfill import (
        build_quote_state_candidates,
    )

    early = classify_candidate(
        _candidate(
            snapshot_id=1,
            raw_source="daily_discovery",
            bookie_id=1,
            odds_value=1.11,
            collected_at=datetime(2026, 8, 9, 23, 0, 0),
        ),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    mid = classify_candidate(
        _candidate(
            snapshot_id=2,
            raw_source="dropping_odds",
            bookie_id=1,
            odds_value=1.09,
            collected_at=datetime(2026, 8, 10, 12, 0, 0),
        ),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    late = classify_candidate(
        _candidate(
            snapshot_id=3,
            raw_source="sofascore",
            bookie_id=1,
            odds_value=1.08,
            collected_at=datetime(2026, 8, 10, 18, 0, 0),
        ),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert {early.identity, mid.identity, late.identity} == {early.identity}
    assert early.identity.source == "sofascore"

    states = build_quote_state_candidates([early, mid, late])
    assert len(states) == 1
    assert states[0].current_price == 1.08
    assert states[0].current_captured_at == datetime(2026, 8, 10, 18, 0, 0)
    assert states[0].snapshot_ids == (1, 2, 3)


def test_null_source_infers_sofascore_from_bookie_id():
    decision = classify_candidate(
        _candidate(raw_source=None, bookie_id=1),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.identity.source == "sofascore"


def test_null_source_without_ownership_is_ambiguous():
    """Two mapped sources that could both write snapshots (default policy) stay
    ambiguous - write-policy elimination only helps when a source is
    structurally incapable of producing this row shape."""
    decision = classify_candidate(
        _candidate(raw_source=None, bookie_id=42),
        bookie_sources={42: {"oddspapi", "sofascore"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.AMBIGUOUS
    assert decision.reason_code == "ambiguous_source"


def test_null_source_snapshot_resolves_via_write_policy_elimination():
    """OddsPortal's ``ODDSPORTAL_OPENING_ONLY_POLICY`` forbids persisting any
    snapshot (opening or current), so it can never be the true source of a
    ``snapshot`` candidate. When a bookie maps to both oddspapi and
    oddsportal (e.g. bet365, ``bookie_id=3`` in production), that lets us
    resolve deterministically to oddspapi instead of blocking as ambiguous.
    """
    decision = classify_candidate(
        _candidate(raw_source=None, bookie_id=3),
        bookie_sources={3: {"oddspapi", "oddsportal"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.identity.source == "oddspapi"
    assert decision.evidence.get("resolved_by") == "write_policy_elimination"
    assert decision.evidence.get("eliminated_sources") == ["oddsportal"]


def test_null_source_unique_bookie_mapping_uses_provider_not_bookie_name():
    """BookieSourceMapping.source is a provider key (oddspapi), never Bookie.name."""
    decision = classify_candidate(
        _candidate(raw_source=None, bookie_id=77),
        bookie_sources={77: {"oddspapi"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.identity.source == "oddspapi"


def test_preload_bookie_sources_ignores_canonical(tmp_path):
    from infrastructure.persistence.database import DatabaseManager
    from infrastructure.persistence.models import Bookie, BookieSourceMapping
    from infrastructure.persistence.repositories.market.market_choice_quote_backfill_repository import (
        MarketChoiceQuoteBackfillRepository,
    )

    manager = DatabaseManager(f"sqlite:///{tmp_path / 'bsm.db'}")
    manager.create_tables()
    with manager.get_session() as session:
        bookie = Bookie(name="bet365", slug="bet365")
        session.add(bookie)
        session.flush()
        session.add_all(
            [
                BookieSourceMapping(
                    bookie_id=bookie.bookie_id,
                    source="canonical",
                    source_bookie_name="bet365",
                    source_bookie_slug="bet365",
                ),
                BookieSourceMapping(
                    bookie_id=bookie.bookie_id,
                    source="oddspapi",
                    source_bookie_name="bet365",
                    source_bookie_slug="bet365",
                ),
            ]
        )
        bookie_id = bookie.bookie_id

    with manager.get_session() as session:
        mapped = MarketChoiceQuoteBackfillRepository.preload_bookie_sources(
            session, [bookie_id]
        )
        assert mapped[bookie_id] == {"oddspapi"}
        unique = MarketChoiceQuoteBackfillRepository.bookie_ids_uniquely_mapped_to_source(
            session, "oddspapi"
        )
        assert bookie_id in unique


def test_build_quote_state_leaves_initial_null_without_choice_initial():
    from modules.odds_ingestion.backfill.market_choice_quote_backfill import (
        ClassificationDecision,
        QuoteIdentity,
        build_quote_state_candidates,
    )

    decision = ClassificationDecision(
        status=ClassificationStatus.RESOLVED,
        reason_code="classified",
        candidate=_candidate(
            raw_source="oddspapi",
            choice_initial_odds=None,
            odds_value=2.05,
        ),
        identity=QuoteIdentity(
            choice_id=10,
            source="oddspapi",
            exchange_side=None,
            exchange_level=0,
        ),
    )
    states = build_quote_state_candidates([decision])
    assert len(states) == 1
    assert states[0].initial_price is None
    assert float(states[0].current_price) == 2.05


def test_sofascore_and_oddsportal_default_main_line_true():
    from modules.odds_ingestion.backfill.market_choice_quote_backfill import (
        ClassificationDecision,
        QuoteIdentity,
        build_quote_state_candidates,
        default_main_line_for_source,
    )

    assert default_main_line_for_source("sofascore") is True
    assert default_main_line_for_source("daily_discovery") is True
    assert default_main_line_for_source("oddsportal") is True
    assert default_main_line_for_source("oddspapi") is None
    assert default_main_line_for_source("oddspapi", existing_main_line=False) is False

    decision = ClassificationDecision(
        status=ClassificationStatus.RESOLVED,
        reason_code="classified",
        candidate=_candidate(raw_source="oddsportal", main_line=None),
        identity=QuoteIdentity(
            choice_id=10,
            source="oddsportal",
            exchange_side=None,
            exchange_level=0,
        ),
    )
    states = build_quote_state_candidates([decision])
    assert len(states) == 1
    assert states[0].main_line is True




def test_ambiguous_when_two_sources_possible():
    decision = classify_candidate(
        _candidate(raw_source=None, bookie_id=5),
        bookie_sources={5: {"oddspapi", "sofascore"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.AMBIGUOUS
    assert decision.reason_code == "ambiguous_source"


def test_contradictory_raw_source_vs_legacy_back_lay():
    """Back/Lay markets are abandoned before source contradiction checks."""
    decision = classify_candidate(
        _candidate(raw_source="oddspapi", choice_group="Back 2.5"),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.INVALID
    assert decision.reason_code == "legacy_back_lay_abandoned"


def test_oddsportal_legacy_back_lay_is_abandoned_not_rematerialized():
    market = SimpleNamespace(market_id=200)
    choice = SimpleNamespace(choice_id=99)
    key = (1000, 2, "Over/Under Full Time", "Full Time", "2.5", False)
    decision = classify_candidate(
        _candidate(
            raw_source=None,
            choice_group="Back 2.5",
            choice_name="over",
            bookie_id=2,
        ),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={key: market},
        choices_by_market_name={(200, "over"): choice},
    )
    assert decision.status is ClassificationStatus.INVALID
    assert decision.reason_code == "legacy_back_lay_abandoned"
    assert decision.identity is None
    assert decision.evidence.get("legacy_side") == "back"


def test_oddspapi_null_side_maps_to_back_with_exchange_proof():
    decision = classify_candidate(
        _candidate(
            raw_source="oddspapi",
            exchange_side=None,
            choice_id=10,
        ),
        bookie_sources={},
        exchange_choice_ids={10},
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.identity.exchange_side == "back"
    assert decision.identity.exchange_level == 0


def test_oddspapi_null_side_stays_null_without_exchange_proof():
    decision = classify_candidate(
        _candidate(raw_source="oddspapi", exchange_side=None, choice_id=10),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.identity.exchange_side is None


def test_invalid_negative_level():
    decision = classify_candidate(
        _candidate(exchange_level=-1),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.INVALID
    assert decision.reason_code == "invalid_side_or_level"


def test_choice_state_without_ownership_is_ambiguous():
    """Snapless choice_state with dual mapping stays ambiguous (no write-policy)."""
    decision = classify_candidate(
        _candidate(
            kind="choice_state",
            snapshot_id=None,
            raw_source=None,
            bookie_id=9,
            choice_initial_odds=1.5,
            choice_current_odds=None,
        ),
        bookie_sources={9: {"oddspapi", "oddsportal"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.AMBIGUOUS
    assert decision.reason_code == "ambiguous_choice_state"


def test_choice_state_with_current_odds_not_resolved_by_write_policy():
    """Opening-only is recent — do not use it to attribute historical mirrors."""
    decision = classify_candidate(
        _candidate(
            kind="choice_state",
            snapshot_id=None,
            raw_source=None,
            bookie_id=3,
            choice_initial_odds=1.5,
            choice_current_odds=1.6,
        ),
        bookie_sources={3: {"oddspapi", "oddsportal"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.AMBIGUOUS
    assert decision.reason_code == "ambiguous_choice_state"


def test_oddsportal_era_choice_state_not_unique_mapped_to_oddspapi():
    """Even with only oddspapi left in mappings, bet365 snapless stays ambiguous."""
    decision = classify_candidate(
        _candidate(
            kind="choice_state",
            snapshot_id=None,
            raw_source=None,
            bookie_id=3,
            choice_initial_odds=2.0,
            choice_current_odds=2.08,
        ),
        bookie_sources={3: {"oddspapi"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.AMBIGUOUS
    assert decision.reason_code == "ambiguous_choice_state"


def test_non_oddsportal_era_choice_state_unique_maps_to_oddspapi():
    decision = classify_candidate(
        _candidate(
            kind="choice_state",
            snapshot_id=None,
            raw_source=None,
            bookie_id=13,
            choice_initial_odds=1.5,
            choice_current_odds=1.6,
        ),
        bookie_sources={13: {"oddspapi"}},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.identity.source == "oddspapi"


def test_resolution_file_wins():
    decision = classify_candidate(
        _candidate(raw_source=None, bookie_id=9),
        bookie_sources={},
        exchange_choice_ids=set(),
        canonical_markets={},
        choices_by_market_name={},
        resolutions={
            1: {
                "snapshot_id": 1,
                "canonical_choice_id": 55,
                "source": "oddsportal",
                "exchange_side": "lay",
                "exchange_level": 0,
            }
        },
    )
    assert decision.status is ClassificationStatus.RESOLVED
    assert decision.reason_code == "resolution_file"
    assert decision.identity.choice_id == 55
    assert decision.identity.exchange_side == "lay"
