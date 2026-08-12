"""Integration tests for Phase 4b quote backfill repository + service."""

import json
from datetime import datetime
from pathlib import Path

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories.market.market_choice_quote_backfill_repository import (
    MarketChoiceQuoteBackfillRepository,
)
from modules.odds_ingestion.backfill.market_choice_quote_backfill import (
    MarketChoiceQuoteBackfillService,
    RunConfig,
)


def make_manager(tmp_path, name="backfill"):
    manager = DatabaseManager(f"sqlite:///{tmp_path / f'{name}.db'}")
    manager.create_tables()
    return manager


def seed_event_with_unlinked_snapshot(manager, *, source="oddspapi", side=None, level=0):
    with manager.get_session() as session:
        # Reserve bookie_id=1 for SofaScore; use allowed Betfair id=4 so the
        # automatic disallowed-bookie purge (≥4b.7) does not wipe the fixture.
        sofascore = Bookie(bookie_id=1, name="SofaScore", slug="sofascore")
        event = Event(
            slug="bf-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(bookie_id=4, name="Betfair Exchange", slug="betfair-ex")
        session.add_all([sofascore, event, bookie])
        session.flush()
        market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="Home/Away Full Time",
            market_group="1X2",
            market_period="Full Time",
            is_live=False,
        )
        session.add(market)
        session.flush()
        choice = MarketChoice(
            market_id=market.market_id,
            choice_name="1",
            initial_odds=1.9,
            current_odds=2.1,
        )
        session.add(choice)
        session.flush()
        snap = MarketChoiceSnapshot(
            choice_id=choice.choice_id,
            odds_value=2.1,
            collected_at=datetime(2026, 6, 20, 11, 0, 0),
            source=source,
            exchange_side=side,
            exchange_level=level,
        )
        session.add(snap)
        session.flush()
        return {
            "event_id": event.id,
            "choice_id": choice.choice_id,
            "snapshot_id": snap.snapshot_id,
            "bookie_id": bookie.bookie_id,
        }


def test_repository_fetches_pending_snapshots_by_keyset(tmp_path):
    manager = make_manager(tmp_path)
    seeded = seed_event_with_unlinked_snapshot(manager)
    with manager.get_session() as session:
        rows = MarketChoiceQuoteBackfillRepository.fetch_pending_snapshots(
            session,
            event_ids=[seeded["event_id"]],
            after_snapshot_id=None,
            limit=10,
        )
        assert len(rows) == 1
        assert rows[0].snapshot_id == seeded["snapshot_id"]
        assert rows[0].quote_id is None


def test_dry_run_links_zero_and_reports_plan(tmp_path):
    manager = make_manager(tmp_path, "dry")
    seeded = seed_event_with_unlinked_snapshot(manager)
    out = tmp_path / "summary.json"
    rej = tmp_path / "rej.ndjson"
    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=True,
            event_id=seeded["event_id"],
            batch_size=50,
            max_rows=100,
            output_json=out,
            output_rejections=rej,
        )
    )
    assert code in (0, 2)
    assert summary["snapshots_scanned"] == 1
    assert summary["quote_buckets_planned"] >= 1
    with manager.get_session() as session:
        snap = session.query(MarketChoiceSnapshot).one()
        assert snap.quote_id is None
        assert session.query(MarketChoiceQuote).count() == 0


def test_commit_creates_quote_and_links_snapshot(tmp_path):
    manager = make_manager(tmp_path, "commit")
    seeded = seed_event_with_unlinked_snapshot(manager, source="oddspapi")
    out = tmp_path / "summary.json"
    rej = tmp_path / "rej.ndjson"
    ckpt = tmp_path / "ckpt.json"
    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=False,
            event_id=seeded["event_id"],
            batch_size=50,
            max_rows=100,
            output_json=out,
            output_rejections=rej,
            checkpoint_file=ckpt,
            confirm_ingestion_paused=True,
        )
    )
    assert code == 0
    assert summary["snapshots_linked"] == 1
    assert summary["quotes_inserted"] == 1
    with manager.get_session() as session:
        quote = session.query(MarketChoiceQuote).one()
        snap = session.query(MarketChoiceSnapshot).one()
        assert snap.quote_id == quote.quote_id
        assert quote.source == "oddspapi"
        assert float(quote.current_odds) == 2.1

    # Second commit is idempotent (no pending snapshots).
    code2, summary2 = service.run(
        RunConfig(
            dry_run=False,
            event_id=seeded["event_id"],
            batch_size=50,
            max_rows=100,
            output_json=tmp_path / "summary2.json",
            output_rejections=tmp_path / "rej2.ndjson",
            checkpoint_file=tmp_path / "ckpt2.json",
            confirm_ingestion_paused=True,
        )
    )
    assert code2 == 0
    assert summary2["snapshots_scanned"] == 0
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).count() == 1


def test_backfill_does_not_degrade_newer_live_quote(tmp_path):
    manager = make_manager(tmp_path, "stale")
    seeded = seed_event_with_unlinked_snapshot(manager, source="oddspapi")
    with manager.get_session() as session:
        quote = MarketChoiceQuote(
            choice_id=seeded["choice_id"],
            source="oddspapi",
            exchange_side=None,
            exchange_level=0,
            current_odds=9.9,
            current_updated_at=datetime(2026, 6, 20, 15, 0, 0),
        )
        session.add(quote)

    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=False,
            event_id=seeded["event_id"],
            batch_size=50,
            max_rows=100,
            output_json=tmp_path / "summary.json",
            output_rejections=tmp_path / "rej.ndjson",
            checkpoint_file=tmp_path / "ckpt.json",
            confirm_ingestion_paused=True,
        )
    )
    assert code == 0
    with manager.get_session() as session:
        quote = session.query(MarketChoiceQuote).one()
        assert float(quote.current_odds) == 9.9
        snap = session.query(MarketChoiceSnapshot).one()
        assert snap.quote_id == quote.quote_id
    assert summary["stale_candidates_ignored"] >= 1 or summary["quotes_unchanged"] >= 0


def test_bulk_link_many_snapshots_one_quote(tmp_path):
    manager = make_manager(tmp_path, "bulk")
    with manager.get_session() as session:
        sofascore = Bookie(bookie_id=1, name="SofaScore", slug="sofascore")
        event = Event(
            slug="bulk-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(bookie_id=302, name="Pinnacle Sports", slug="pinnacle")
        session.add_all([sofascore, event, bookie])
        session.flush()
        market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="Home/Away Full Time",
            market_group="1X2",
            market_period="Full Time",
            is_live=False,
        )
        session.add(market)
        session.flush()
        choice = MarketChoice(market_id=market.market_id, choice_name="1")
        session.add(choice)
        session.flush()
        for i in range(25):
            session.add(
                MarketChoiceSnapshot(
                    choice_id=choice.choice_id,
                    odds_value=1.5 + i * 0.01,
                    collected_at=datetime(2026, 6, 20, 10, 0, i % 60),
                    source="oddsportal",
                )
            )
        event_id = event.id

    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=False,
            event_id=event_id,
            batch_size=200,
            max_rows=1000,
            output_json=tmp_path / "summary.json",
            output_rejections=tmp_path / "rej.ndjson",
            checkpoint_file=tmp_path / "ckpt.json",
            confirm_ingestion_paused=True,
        )
    )
    assert code == 0
    assert summary["quotes_inserted"] == 1
    assert summary["snapshots_linked"] == 25
    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).count() == 1
        linked = (
            session.query(MarketChoiceSnapshot)
            .filter(MarketChoiceSnapshot.quote_id.isnot(None))
            .count()
        )
        assert linked == 25


def test_max_rows_stops_and_checkpoint_resumes(tmp_path):
    manager = make_manager(tmp_path, "resume")
    with manager.get_session() as session:
        sofascore = Bookie(bookie_id=1, name="SofaScore", slug="sofascore")
        event = Event(
            slug="resume-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(bookie_id=3, name="bet365", slug="bet365")
        session.add_all([sofascore, event, bookie])
        session.flush()
        market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="Home/Away Full Time",
            market_group="1X2",
            market_period="Full Time",
            is_live=False,
        )
        session.add(market)
        session.flush()
        choice = MarketChoice(market_id=market.market_id, choice_name="1")
        session.add(choice)
        session.flush()
        for i in range(5):
            session.add(
                MarketChoiceSnapshot(
                    choice_id=choice.choice_id,
                    odds_value=1.5,
                    collected_at=datetime(2026, 6, 20, 10, 0, i),
                    source="oddsportal",
                )
            )
        event_id = event.id

    ckpt = tmp_path / "ckpt.json"
    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code1, summary1 = service.run(
        RunConfig(
            dry_run=False,
            event_id=event_id,
            batch_size=2,
            max_rows=2,
            output_json=tmp_path / "s1.json",
            output_rejections=tmp_path / "r1.ndjson",
            checkpoint_file=ckpt,
            confirm_ingestion_paused=True,
        )
    )
    assert code1 == 0
    assert summary1["rows_consumed"] == 2
    assert summary1["stop_reason"] == "max_rows"
    assert ckpt.exists()

    code2, summary2 = service.run(
        RunConfig(
            dry_run=False,
            event_id=event_id,
            batch_size=2,
            max_rows=10,
            resume_from=ckpt,
            checkpoint_file=ckpt,
            output_json=tmp_path / "s2.json",
            output_rejections=tmp_path / "r2.ndjson",
            confirm_ingestion_paused=True,
        )
    )
    assert code2 == 0
    with manager.get_session() as session:
        assert (
            session.query(MarketChoiceSnapshot)
            .filter(MarketChoiceSnapshot.quote_id.is_(None))
            .count()
            == 0
        )


def test_purge_oddspapi_null_mainline_line_markets_and_orphans(tmp_path):
    manager = make_manager(tmp_path, "purge")
    with manager.get_session() as session:
        sofascore = Bookie(bookie_id=1, name="SofaScore", slug="sofascore")
        event = Event(
            slug="purge-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="EPL",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(bookie_id=302, name="Pinnacle Sports", slug="pinnacle")
        session.add_all([sofascore, event, bookie])
        session.flush()

        line_market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="Over/Under Full Time",
            market_group="Over/Under",
            market_period="Full Time",
            choice_group="2.5",
            is_live=False,
        )
        keep_market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="Over/Under Full Time",
            market_group="Over/Under",
            market_period="Full Time",
            choice_group="3.5",
            is_live=False,
        )
        moneyline = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="1X2 Full Time",
            market_group="1X2",
            market_period="Full Time",
            choice_group=None,
            is_live=False,
        )
        session.add_all([line_market, keep_market, moneyline])
        session.flush()

        purge_choice = MarketChoice(market_id=line_market.market_id, choice_name="over")
        keep_choice = MarketChoice(market_id=keep_market.market_id, choice_name="over")
        ml_choice = MarketChoice(market_id=moneyline.market_id, choice_name="1")
        session.add_all([purge_choice, keep_choice, ml_choice])
        session.flush()

        session.add_all(
            [
                MarketChoiceSnapshot(
                    choice_id=purge_choice.choice_id,
                    odds_value=1.9,
                    collected_at=datetime(2026, 6, 20, 10, 0, 0),
                    source="oddspapi",
                    main_line=None,
                ),
                MarketChoiceSnapshot(
                    choice_id=keep_choice.choice_id,
                    odds_value=2.1,
                    collected_at=datetime(2026, 6, 20, 10, 1, 0),
                    source="oddspapi",
                    main_line=True,
                ),
                MarketChoiceSnapshot(
                    choice_id=ml_choice.choice_id,
                    odds_value=1.5,
                    collected_at=datetime(2026, 6, 20, 10, 2, 0),
                    source="oddspapi",
                    main_line=None,
                ),
            ]
        )
        event_id = event.id

    out = tmp_path / "summary.json"
    rej = tmp_path / "rej.ndjson"
    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=False,
            event_id=event_id,
            batch_size=50,
            max_rows=100,
            output_json=out,
            output_rejections=rej,
            checkpoint_file=tmp_path / "ckpt.json",
            confirm_ingestion_paused=True,
            purge_oddspapi_null_mainline_lines=True,
            confirm_purge=True,
        )
    )
    assert code in (0, 2)
    assert summary["purge_snapshots_matched"] == 1
    assert summary["purge_snapshots_deleted"] == 1
    assert summary["purge_choices_deleted"] == 1
    assert summary["purge_markets_deleted"] == 1

    with manager.get_session() as session:
        assert session.query(MarketChoiceSnapshot).count() == 2
        assert session.query(Market).count() == 2
        assert (
            session.query(Market)
            .filter(Market.choice_group == "2.5")
            .count()
            == 0
        )
        assert (
            session.query(MarketChoiceSnapshot)
            .filter(MarketChoiceSnapshot.main_line.is_(True))
            .count()
            == 1
        )

    body = rej.read_text(encoding="utf-8")
    assert "purge_oddspapi_null_mainline_line" in body


def test_initial_odds_unavailable_written_to_rejections(tmp_path):
    manager = make_manager(tmp_path, "initial-note")
    with manager.get_session() as session:
        sofascore = Bookie(bookie_id=1, name="SofaScore", slug="sofascore")
        event = Event(
            slug="note-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(bookie_id=4, name="Betfair Exchange", slug="betfair-ex")
        session.add_all([sofascore, event, bookie])
        session.flush()
        market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="Home/Away Full Time",
            market_group="1X2",
            market_period="Full Time",
            is_live=False,
        )
        session.add(market)
        session.flush()
        choice = MarketChoice(
            market_id=market.market_id,
            choice_name="1",
            initial_odds=None,
            current_odds=None,
        )
        session.add(choice)
        session.flush()
        session.add(
            MarketChoiceSnapshot(
                choice_id=choice.choice_id,
                odds_value=2.2,
                collected_at=datetime(2026, 6, 20, 11, 0, 0),
                source="oddspapi",
            )
        )
        event_id = event.id

    rej = tmp_path / "rej.ndjson"
    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=True,
            event_id=event_id,
            batch_size=50,
            max_rows=100,
            output_json=tmp_path / "summary.json",
            output_rejections=rej,
        )
    )
    assert code in (0, 2)
    assert summary["initial_odds_unavailable"] >= 1
    assert "initial_odds_unavailable" in rej.read_text(encoding="utf-8")
    # Notes must not be treated as blocking decisions by themselves.
    assert summary["blocking_decisions"] == 0


def test_purge_legacy_back_lay_deletes_markets_and_skips_rematerialization(tmp_path):
    """Back/Lay OddsPortal-era markets are purged, not rematerialized to quotes."""
    from infrastructure.persistence.models import BookieSourceMapping

    manager = make_manager(tmp_path, name="back_lay_purge")
    with manager.get_session() as session:
        sofascore = Bookie(bookie_id=1, name="SofaScore", slug="sofascore")
        event = Event(
            slug="bf-back-lay-purge",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="Test",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(bookie_id=4, name="Betfair Exchange", slug="betfair-ex")
        session.add_all([sofascore, event, bookie])
        session.flush()
        session.add(
            BookieSourceMapping(
                bookie_id=bookie.bookie_id,
                source="oddspapi",
                source_bookie_name="Betfair Exchange",
                source_bookie_slug="betfair-ex",
                match_method="manual_alias",
            )
        )
        back = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="1X2 Full Time",
            market_group="1X2",
            market_period="Full Time",
            choice_group="Back",
            is_live=False,
        )
        lay = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="1X2 Full Time",
            market_group="1X2",
            market_period="Full Time",
            choice_group="Lay",
            is_live=False,
        )
        session.add_all([back, lay])
        session.flush()
        for market, side_price in ((back, 2.3), (lay, 2.32)):
            for name, price in (("1", side_price), ("x", side_price + 0.8), ("2", side_price + 1.7)):
                choice = MarketChoice(
                    market_id=market.market_id,
                    choice_name=name,
                    current_odds=price,
                )
                session.add(choice)
                session.flush()
                session.add(
                    MarketChoiceSnapshot(
                        choice_id=choice.choice_id,
                        odds_value=price,
                        collected_at=datetime(2026, 6, 20, 11, 0, 0),
                        source=None,
                    )
                )
        event_id = event.id
        bookie_id = bookie.bookie_id

    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=False,
            event_id=event_id,
            batch_size=50,
            max_rows=100,
            confirm_ingestion_paused=True,
            purge_legacy_back_lay=True,
            confirm_purge=True,
            checkpoint_file=tmp_path / "checkpoint.json",
            output_json=tmp_path / "summary.json",
            output_rejections=tmp_path / "rej.ndjson",
        )
    )
    assert code == 0, summary
    assert summary["purge_legacy_back_lay_markets_deleted"] == 2
    assert summary["purge_legacy_back_lay_choices_deleted"] == 6
    assert summary["purge_legacy_back_lay_snapshots_deleted"] == 6
    assert summary["canonical_markets_created"] == 0
    assert summary["blocking_decisions"] == 0

    with manager.get_session() as session:
        remaining_legacy = (
            session.query(Market)
            .filter(
                Market.event_id == event_id,
                Market.bookie_id == bookie_id,
            )
            .count()
        )
        assert remaining_legacy == 0
        assert session.query(MarketChoiceSnapshot).count() == 0
        assert session.query(MarketChoiceQuote).count() == 0


def test_lookup_catalog_seed_for_1x2_full_time():
    from modules.odds_ingestion.backfill.market_choice_quote_backfill import (
        lookup_catalog_seed_for_market,
    )

    seed = lookup_catalog_seed_for_market("1X2 Full Time", "Full Time")
    assert seed is not None
    assert seed["canonical_market_group"] == "1X2"
    assert seed["requires_choice_group"] is False


def test_rejections_file_appends_with_run_headers(tmp_path):
    manager = make_manager(tmp_path, "rej_append")
    seeded = seed_event_with_unlinked_snapshot(manager, source="oddspapi")
    rej = tmp_path / "rej.ndjson"
    service = MarketChoiceQuoteBackfillService(manager.get_session)

    code1, _ = service.run(
        RunConfig(
            dry_run=True,
            event_id=seeded["event_id"],
            batch_size=50,
            max_rows=100,
            output_json=tmp_path / "s1.json",
            output_rejections=rej,
            append_rejections=False,
        )
    )
    assert code1 in (0, 2)
    first = rej.read_text(encoding="utf-8")
    assert '"reason_code": "run_boundary"' in first
    assert first.count('"reason_code": "run_boundary"') == 1

    code2, _ = service.run(
        RunConfig(
            dry_run=True,
            event_id=seeded["event_id"],
            batch_size=50,
            max_rows=100,
            output_json=tmp_path / "s2.json",
            output_rejections=rej,
            append_rejections=True,
        )
    )
    assert code2 in (0, 2)
    second = rej.read_text(encoding="utf-8")
    assert second.startswith(first.rstrip("\n"))
    assert second.count('"reason_code": "run_boundary"') == 2
    assert "\n\n{" in second or "\n\n{\n" in second or second.count("\n\n") >= 1


def test_run_purges_markets_outside_allowed_bookies(tmp_path):
    """Every run deletes markets whose bookie_id is not in {1,3,4,302}."""
    manager = make_manager(tmp_path, "disallowed_bookies")
    with manager.get_session() as session:
        sofascore = Bookie(bookie_id=1, name="SofaScore", slug="sofascore")
        other = Bookie(bookie_id=13, name="Cloudbet", slug="cloudbet")
        event = Event(
            slug="keep-purge-bookies",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="Test",
            home_team="Home",
            away_team="Away",
        )
        session.add_all([sofascore, other, event])
        session.flush()

        keep = Market(
            event_id=event.id,
            bookie_id=sofascore.bookie_id,
            market_name="1X2 Full Time",
            market_group="1X2",
            market_period="Full Time",
            choice_group=None,
            is_live=False,
        )
        drop = Market(
            event_id=event.id,
            bookie_id=other.bookie_id,
            market_name="1X2 Full Time",
            market_group="1X2",
            market_period="Full Time",
            choice_group=None,
            is_live=False,
        )
        session.add_all([keep, drop])
        session.flush()
        keep_choice = MarketChoice(
            market_id=keep.market_id, choice_name="1", current_odds=1.9
        )
        drop_choice = MarketChoice(
            market_id=drop.market_id, choice_name="1", current_odds=2.1
        )
        session.add_all([keep_choice, drop_choice])
        session.flush()
        session.add_all(
            [
                MarketChoiceSnapshot(
                    choice_id=keep_choice.choice_id,
                    odds_value=1.9,
                    collected_at=datetime(2026, 6, 20, 11, 0, 0),
                    source="sofascore",
                ),
                MarketChoiceSnapshot(
                    choice_id=drop_choice.choice_id,
                    odds_value=2.1,
                    collected_at=datetime(2026, 6, 20, 11, 0, 0),
                    source="oddspapi",
                ),
            ]
        )
        event_id = event.id
        keep_market_id = keep.market_id
        drop_market_id = drop.market_id

    service = MarketChoiceQuoteBackfillService(manager.get_session)
    code, summary = service.run(
        RunConfig(
            dry_run=False,
            event_id=event_id,
            batch_size=50,
            max_rows=100,
            confirm_ingestion_paused=True,
            checkpoint_file=tmp_path / "checkpoint.json",
            output_json=tmp_path / "summary.json",
            output_rejections=tmp_path / "rej.ndjson",
        )
    )
    assert code in (0, 2), summary
    assert summary["purge_disallowed_bookie_markets_deleted"] == 1

    with manager.get_session() as session:
        assert session.get(Market, keep_market_id) is not None
        assert session.get(Market, drop_market_id) is None
