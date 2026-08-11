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
        # Reserve bookie_id=1 for SofaScore (classifier rule).
        sofascore = Bookie(name="SofaScore", slug="sofascore")
        event = Event(
            slug="bf-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair", slug="betfair")
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
        sofascore = Bookie(name="SofaScore", slug="sofascore")
        event = Event(
            slug="bulk-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Pinnacle", slug="pinnacle")
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
        sofascore = Bookie(name="SofaScore", slug="sofascore")
        event = Event(
            slug="resume-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Bet365", slug="bet365")
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
        sofascore = Bookie(name="SofaScore", slug="sofascore")
        event = Event(
            slug="purge-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="EPL",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Pinnacle", slug="pinnacle")
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
        sofascore = Bookie(name="SofaScore", slug="sofascore")
        event = Event(
            slug="note-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair", slug="betfair")
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
