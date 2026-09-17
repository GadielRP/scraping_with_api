from unittest.mock import patch

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie
from infrastructure.persistence.repositories.bookie_repository import BookieRepository


def make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'bookies.db'}")
    manager.create_tables()
    return manager


def seed_bookies(manager):
    with manager.get_session() as session:
        session.add_all(
            [
                Bookie(name="SofaScore", slug="sofascore"),
                Bookie(name="Betfair Exchange", slug="betfair-ex"),
                Bookie(name="Pinnacle Sports", slug="pinnacle"),
            ]
        )


def test_existing_mapping_resolves(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        with manager.get_session() as session:
            bookie = session.query(Bookie).filter(Bookie.slug == "pinnacle").one()
            BookieRepository.upsert_source_mapping(
                bookie_id=bookie.bookie_id,
                source="oddspapi",
                source_bookie_name="Pinnacle Sports",
                source_bookie_slug="pinnacle",
                session=session,
            )

        result = BookieRepository.resolve_bookie_from_source(
            source="oddspapi",
            source_bookie_name="Pinnacle Sports",
            source_bookie_slug="pinnacle",
        )

    assert result.resolved is True
    assert result.reused is True
    assert result.created is False
    assert result.bookie.slug == "pinnacle"
    assert result.match_method == "existing_source_mapping"


def test_direct_canonical_slug_match_creates_source_mapping(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        result = BookieRepository.resolve_bookie_from_source(
            source="oddspapi",
            source_bookie_name="Pinnacle Sports",
            source_bookie_slug="pinnacle",
        )
        mapping = BookieRepository.get_bookie_source_mapping("oddspapi", "pinnacle")

    assert result.resolved is True
    assert result.bookie.slug == "pinnacle"
    assert result.mapping_created is True
    assert result.match_method == "canonical_slug_match"
    assert mapping is not None
    assert mapping.source_bookie_name == "Pinnacle Sports"


def test_betfair_oddsportal_alias_resolves_to_canonical_betfair_ex(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        result = BookieRepository.resolve_bookie_from_source(
            source="oddsportal",
            source_bookie_name="Betfair Exchange",
            source_bookie_slug="betfair-exchange",
        )

    assert result.resolved is True
    assert result.bookie.slug == "betfair-ex"
    assert result.mapping_created is True
    assert result.match_method == "manual_alias"


def test_betfair_oddspapi_alias_resolves_to_canonical_betfair_ex(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        result = BookieRepository.resolve_bookie_from_source(
            source="oddspapi",
            source_bookie_name="BetFair Exchange",
            source_bookie_slug="betfair-ex",
        )

    assert result.resolved is True
    assert result.bookie.slug == "betfair-ex"
    assert result.mapping_created is True
    assert result.match_method == "canonical_slug_seed"


def test_exact_name_match_creates_source_mapping(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        result = BookieRepository.resolve_bookie_from_source(
            source="someprovider",
            source_bookie_name="Pinnacle Sports",
            source_bookie_slug="opaque-slug",
        )
        mapping = BookieRepository.get_bookie_source_mapping("someprovider", "opaque-slug")

    assert result.resolved is True
    assert result.bookie.slug == "pinnacle"
    assert result.mapping_created is True
    assert result.match_method == "exact_name_match"
    assert mapping is not None
    assert mapping.source_bookie_name == "Pinnacle Sports"


def test_unknown_source_bookie_does_not_create_bookie_when_creation_disabled(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        with manager.get_session() as session:
            before_count = session.query(Bookie).count()

        result = BookieRepository.resolve_bookie_from_source(
            source="oddspapi",
            source_bookie_name="Missing Bookie",
            source_bookie_slug="missing-bookie",
            allow_create=False,
        )

        with manager.get_session() as session:
            after_count = session.query(Bookie).count()

    assert result.resolved is False
    assert result.reason == "canonical_bookie_not_found"
    assert before_count == after_count


def test_upsert_mapping_is_idempotent_when_unchanged(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        with manager.get_session() as session:
            bookie = session.query(Bookie).filter(Bookie.slug == "pinnacle").one()
            mapping1 = BookieRepository.upsert_source_mapping(
                bookie_id=bookie.bookie_id,
                source="oddspapi",
                source_bookie_name="Pinnacle Sports",
                source_bookie_slug="pinnacle",
                session=session,
            )
            first_updated_at = mapping1.updated_at
            mapping2 = BookieRepository.upsert_source_mapping(
                bookie_id=bookie.bookie_id,
                source="oddspapi",
                source_bookie_name="Pinnacle Sports",
                source_bookie_slug="pinnacle",
                session=session,
            )

    assert mapping2.mapping_id == mapping1.mapping_id
    assert mapping2.updated_at == first_updated_at


def test_upsert_mapping_updates_when_name_changes(tmp_path):
    manager = make_manager(tmp_path)
    seed_bookies(manager)

    with patch("infrastructure.persistence.repositories.bookie_repository.db_manager", manager):
        with manager.get_session() as session:
            bookie = session.query(Bookie).filter(Bookie.slug == "pinnacle").one()
            mapping1 = BookieRepository.upsert_source_mapping(
                bookie_id=bookie.bookie_id,
                source="oddspapi",
                source_bookie_name="Pinnacle Sports",
                source_bookie_slug="pinnacle",
                session=session,
            )
            first_updated_at = mapping1.updated_at
            mapping2 = BookieRepository.upsert_source_mapping(
                bookie_id=bookie.bookie_id,
                source="oddspapi",
                source_bookie_name="Pinnacle",
                source_bookie_slug="pinnacle",
                session=session,
            )

    assert mapping2.source_bookie_name == "Pinnacle"
    assert mapping2.updated_at >= first_updated_at


def test_check_and_migrate_schema_seeds_canonical_bookies(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'seeded_bookies.db'}")
    manager.create_tables()

    assert manager.check_and_migrate_schema() is True

    with manager.get_session() as session:
        slugs = {slug for (slug,) in session.query(Bookie.slug).all()}
        sofascore = session.query(Bookie).filter(Bookie.slug == "sofascore").one()

    assert "sofascore" in slugs
    assert "bet365" in slugs
    assert "pinnacle" in slugs
    assert "betfair-ex" in slugs
    assert sofascore.bookie_id == 1
