from sqlalchemy import create_engine, event as sqlalchemy_event, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import Generator
import logging
import traceback
from infrastructure.settings import Config
from infrastructure.persistence.orm_base import Base
# Import model modules for mapper/metadata registration without making the
# declarative registry depend on this engine/session module.
from infrastructure.persistence import models as _registered_models  # noqa: F401

logger = logging.getLogger(__name__)

# Application startup validates this revision; schema changes are applied by
# the deployment migration job (`alembic upgrade head`), not by every process.
ALEMBIC_HEAD_REVISION = "20260918_04"

class DatabaseManager:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.engine = None
        self.SessionLocal = None
        self._setup_engine()
    
    def _setup_engine(self):
        """Setup database engine and session factory"""
        try:
            connect_args = {}
            if not self.database_url.startswith("sqlite"):
                connect_args["connect_timeout"] = Config.DB_CONNECT_TIMEOUT
            self.engine = create_engine(
                self.database_url,
                echo=False,  # Set to True for SQL debugging
                pool_pre_ping=True,
                pool_recycle=300,
                connect_args=connect_args,
            )
            if self.engine.dialect.name == "postgresql":
                @sqlalchemy_event.listens_for(self.engine, "connect")
                def _set_utc_session_timezone(dbapi_connection, _connection_record):
                    cursor = dbapi_connection.cursor()
                    try:
                        cursor.execute("SET TIME ZONE 'UTC'")
                    finally:
                        cursor.close()
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, expire_on_commit=False, bind=self.engine)
            logger.info(f"Database engine created for: {self.database_url}")
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
    
    def create_tables(self):
        """Create all tables defined in models"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def drop_tables(self):
        """Drop all tables (use with caution!)"""
        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.warning("All database tables dropped")
        except Exception as e:
            logger.error(f"Failed to drop tables: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"Unexpected error in database session: {e}")
            raise
        finally:
            session.close()
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def verify_schema_at_head(self, expected_revision: str = ALEMBIC_HEAD_REVISION) -> bool:
        """Read-only production gate for the versioned database schema.

        This deliberately performs no DDL and no backfill.  A deployment must
        run Alembic before the application is allowed to write odds.
        """
        try:
            from sqlalchemy import inspect

            inspector = inspect(self.engine)
            tables = set(inspector.get_table_names())
            if "alembic_version" not in tables:
                logger.error(
                    "Schema is not versioned: alembic_version is missing; "
                    "run `alembic upgrade head`."
                )
                return False

            with self.engine.connect() as connection:
                revisions = {
                    str(row[0])
                    for row in connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    )
                }
            if revisions != {expected_revision}:
                logger.error(
                    "Schema revision mismatch: expected=%s actual=%s; "
                    "run `alembic upgrade head`.",
                    expected_revision,
                    sorted(revisions),
                )
                return False

            required_columns = {
                "canonical_market_types": {"market_type_id", "requires_line_value"},
                "market_source_mappings": {"market_type_id"},
                "markets": {"market_type_id", "line_value"},
            }
            for table, columns in required_columns.items():
                if table not in tables:
                    logger.error("Schema table is missing: %s", table)
                    return False
                actual_columns = {
                    item["name"] for item in inspector.get_columns(table)
                }
                missing = columns - actual_columns
                if missing:
                    logger.error(
                        "Schema columns are missing from %s: %s",
                        table,
                        sorted(missing),
                    )
                    return False
            forbidden_columns = {
                "markets": {
                    "market_name",
                    "market_group",
                    "market_period",
                    "choice_group",
                },
                "market_source_mappings": {
                    "canonical_market_name",
                    "canonical_market_group",
                    "canonical_market_period",
                },
                "pillar_mining_units": {
                    "market_name",
                    "market_group",
                    "market_period",
                    "choice_group",
                },
            }
            for table, columns in forbidden_columns.items():
                if table not in tables:
                    continue
                actual_columns = {
                    item["name"] for item in inspector.get_columns(table)
                }
                remaining = columns & actual_columns
                if remaining:
                    logger.error(
                        "Legacy market columns remain in %s: %s",
                        table,
                        sorted(remaining),
                    )
                    return False
            return True
        except Exception:
            logger.error("Schema verification failed", exc_info=True)
            return False

    def _create_table_and_indexes(self, model, index_statements: list[str]) -> None:
        with self.get_session() as session:
            model.__table__.create(session.connection(), checkfirst=True)
            for statement in index_statements:
                session.execute(text(statement))
            session.commit()

    def _migrate_pillar_mining_schema_v2(self) -> None:
        """Replace the undeployed single-row mining experiment with graph storage."""

        from sqlalchemy import inspect
        from infrastructure.persistence.models import (
            PillarMiningMetricValue,
            PillarMiningRun,
            PillarMiningUnit,
        )

        old_table = "pillar_mining_observations"
        had_old_table = old_table in inspect(self.engine).get_table_names()
        with self.engine.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {old_table}"))
            PillarMiningRun.__table__.create(connection, checkfirst=True)
            PillarMiningUnit.__table__.create(connection, checkfirst=True)
            PillarMiningMetricValue.__table__.create(connection, checkfirst=True)
        if had_old_table:
            logger.warning(
                "Removed undeployed experimental table %s and installed mining schema v2",
                old_table,
            )

    def _migrate_market_source_mappings(self):
        """Ensure market source mappings table exists with runtime indexes."""
        from infrastructure.persistence.models import MarketSourceMapping

        self._create_table_and_indexes(
            MarketSourceMapping,
            [
                "CREATE UNIQUE INDEX IF NOT EXISTS unique_market_source_mapping "
                "ON market_source_mappings (source, source_sport_id, source_market_id)",
                "CREATE INDEX IF NOT EXISTS idx_market_source_mappings_canonical_key "
                "ON market_source_mappings (canonical_market_key)",
                "CREATE INDEX IF NOT EXISTS idx_market_source_mappings_source_market "
                "ON market_source_mappings (source, source_sport_id, source_market_id)",
            ],
        )

    def _migrate_market_mapping_schema_cleanup(self):
        """Retained only for old SQLite tooling; PostgreSQL uses Alembic."""
        return None
    def _migrate_market_outcome_source_mappings(self):
        """Ensure market outcome source mappings table exists with runtime indexes."""
        from infrastructure.persistence.models import MarketOutcomeSourceMapping

        self._create_table_and_indexes(
            MarketOutcomeSourceMapping,
            [
                "CREATE UNIQUE INDEX IF NOT EXISTS unique_market_outcome_source_mapping "
                "ON market_outcome_source_mappings (market_source_mapping_id, source_outcome_id)",
                "CREATE INDEX IF NOT EXISTS idx_market_outcome_source_mappings_market "
                "ON market_outcome_source_mappings (market_source_mapping_id)",
                "CREATE INDEX IF NOT EXISTS idx_market_outcome_source_mappings_choice "
                "ON market_outcome_source_mappings (canonical_choice_name)",
            ],
        )

    def _migrate_source_catalog_syncs(self):
        """Ensure local catalog import metadata table exists."""
        from infrastructure.persistence.models import SourceCatalogSync

        self._create_table_and_indexes(
            SourceCatalogSync,
            [
                "CREATE INDEX IF NOT EXISTS idx_source_catalog_syncs_source_type "
                "ON source_catalog_syncs (source, catalog_type)",
                "CREATE INDEX IF NOT EXISTS idx_source_catalog_syncs_hash "
                "ON source_catalog_syncs (payload_hash)",
            ],
        )

    def _migrate_bookie_source_mappings(self):
        """Create and seed canonical/source bookie mappings without creating new canonical bookies."""
        try:
            from sqlalchemy import inspect
            from infrastructure.persistence.models import Bookie, BookieSourceMapping
            from infrastructure.persistence.repositories.bookie_repository import BookieRepository

            inspector = inspect(self.engine)
            table_names = set(inspector.get_table_names())
            if 'bookies' not in table_names:
                logger.debug("Bookies table doesn't exist yet, skipping bookie source mapping migration")
                return

            with self.get_session() as session:
                connection = session.connection()

                if 'bookie_source_mappings' not in table_names:
                    BookieSourceMapping.__table__.create(connection, checkfirst=True)
                    logger.info("Created bookie_source_mappings table")

                index_statements = [
                    "CREATE UNIQUE INDEX IF NOT EXISTS unique_bookie_source_slug ON bookie_source_mappings (source, source_bookie_slug)",
                    "CREATE INDEX IF NOT EXISTS idx_bookie_source_mappings_bookie_id ON bookie_source_mappings (bookie_id)",
                    "CREATE INDEX IF NOT EXISTS idx_bookie_source_mappings_source_slug ON bookie_source_mappings (source, source_bookie_slug)",
                    "CREATE INDEX IF NOT EXISTS idx_bookie_source_mappings_source_name ON bookie_source_mappings (source, source_bookie_name)",
                ]
                for statement in index_statements:
                    session.execute(text(statement))

                bookies = session.query(Bookie).order_by(Bookie.bookie_id).all()
                for bookie in bookies:
                    BookieRepository.upsert_source_mapping(
                        bookie_id=bookie.bookie_id,
                        source="canonical",
                        source_bookie_name=bookie.name,
                        source_bookie_slug=bookie.slug,
                        match_method="canonical_seed",
                        confidence=1.000,
                        session=session,
                    )
                    BookieRepository.upsert_source_mapping(
                        bookie_id=bookie.bookie_id,
                        source="oddspapi",
                        source_bookie_name=bookie.name,
                        source_bookie_slug=bookie.slug,
                        match_method="canonical_slug_seed",
                        confidence=1.000,
                        session=session,
                    )

                sofascore_bookie = session.query(Bookie).filter(Bookie.slug == "sofascore").first()
                if sofascore_bookie is not None:
                    BookieRepository.upsert_source_mapping(
                        bookie_id=sofascore_bookie.bookie_id,
                        source="sofascore",
                        source_bookie_name="SofaScore",
                        source_bookie_slug="sofascore",
                        match_method="pseudobookie_seed",
                        confidence=1.000,
                        session=session,
                    )
                else:
                    logger.warning("Canonical bookie slug 'sofascore' missing while seeding source mappings")

                betfair_exchange = session.query(Bookie).filter(Bookie.slug == "betfair-ex").first()
                if betfair_exchange is not None:
                    BookieRepository.upsert_source_mapping(
                        bookie_id=betfair_exchange.bookie_id,
                        source="oddsportal",
                        source_bookie_name="Betfair Exchange",
                        source_bookie_slug="betfair-ex",
                        match_method="manual_alias",
                        confidence=1.000,
                        session=session,
                    )
                    BookieRepository.upsert_source_mapping(
                        bookie_id=betfair_exchange.bookie_id,
                        source="oddspapi",
                        source_bookie_name="BetFair Exchange",
                        source_bookie_slug="betfair-ex",
                        match_method="canonical_slug_seed",
                        confidence=1.000,
                        session=session,
                    )
                else:
                    logger.warning("Canonical bookie slug 'betfair-ex' missing while seeding alias mappings")

                session.commit()
                logger.info("Bookie source mappings migration completed")
        except Exception as e:
            logger.error(f"Bookie source mappings migration failed: {e}")
            logger.error(traceback.format_exc())

    def _migrate_event_source_resolution_queue(self):
        """Create the unresolved provider event queue used by deterministic matching."""
        try:
            from sqlalchemy import inspect
            from infrastructure.persistence.models import EventSourceResolutionQueue

            inspector = inspect(self.engine)
            table_names = set(inspector.get_table_names())

            with self.get_session() as session:
                connection = session.connection()
                if 'event_source_resolution_queue' not in table_names:
                    EventSourceResolutionQueue.__table__.create(connection, checkfirst=True)
                    logger.info("Created event_source_resolution_queue table")

                index_statements = [
                    "CREATE UNIQUE INDEX IF NOT EXISTS unique_event_source_resolution_queue_source_event "
                    "ON event_source_resolution_queue (source, source_event_id)",
                    "CREATE INDEX IF NOT EXISTS idx_event_source_resolution_queue_status "
                    "ON event_source_resolution_queue (source, resolution_status)",
                    "CREATE INDEX IF NOT EXISTS idx_event_source_resolution_queue_start_time "
                    "ON event_source_resolution_queue (source_starts_at)",
                    "CREATE INDEX IF NOT EXISTS idx_event_source_resolution_queue_best_candidate "
                    "ON event_source_resolution_queue (best_candidate_event_id)",
                ]
                for statement in index_statements:
                    session.execute(text(statement))

                session.commit()
                logger.info("Event source resolution queue migration completed")
        except Exception as e:
            logger.error(f"Event source resolution queue migration failed: {e}")
            logger.error(traceback.format_exc())

    def _migrate_oddspapi_api_key_usage(self):
        """Ensure the OddsPapi usage table exists under the UTC contract."""
        try:
            from infrastructure.persistence.models import OddspapiApiKeyUsage

            self._create_table_and_indexes(OddspapiApiKeyUsage, [])
        except Exception as e:
            logger.error("Oddspapi API key usage migration failed: %s", e)
            logger.error(traceback.format_exc())
            raise

    def _migrate_oddspapi_mainline_outcome_cache(self):
        """Ensure the OddsPapi mainLine outcome cache table exists with indexes."""
        try:
            from infrastructure.persistence.models import OddspapiMainlineOutcomeCache

            self._create_table_and_indexes(
                OddspapiMainlineOutcomeCache,
                [
                    "CREATE UNIQUE INDEX IF NOT EXISTS unique_oddspapi_mainline_outcome_cache "
                    "ON oddspapi_mainline_outcome_cache "
                    "(event_id, bookmaker_slug, source_market_id, source_outcome_id)",
                    "CREATE INDEX IF NOT EXISTS idx_oddspapi_mainline_cache_event_id "
                    "ON oddspapi_mainline_outcome_cache (event_id)",
                    "CREATE INDEX IF NOT EXISTS idx_oddspapi_mainline_cache_is_exchange "
                    "ON oddspapi_mainline_outcome_cache (is_exchange)",
                ],
            )
            logger.info("Oddspapi mainline outcome cache schema is ready")
        except Exception as e:
            logger.error(f"Oddspapi mainline outcome cache migration failed: {e}")
            logger.error(traceback.format_exc())

    def _migrate_market_choice_quotes(self):
        """Ensure the market_choice_quotes table exists with a NULL-safe unique index.

        See docs/refactors/db-schema-odds-refactor.md (Fase 1) for the design
        rationale: this table is the current-state cache for a price
        instrument scoped by (choice, source, exchange_side, exchange_level),
        replacing ad-hoc side/source handling previously mixed into the odds
        persistence layer.

        exchange_side is NULL for non-exchange bookies rather than a 'single'
        sentinel). NULL != NULL in a UNIQUE constraint (Postgres and SQLite
        alike), so the plain UniqueConstraint declared on the ORM model
        (unique_market_choice_quote) does not by itself reject duplicate
        NULL-side rows. Real enforcement is this functional index using
        COALESCE(exchange_side, ''), deliberately named differently so
        "CREATE ... IF NOT EXISTS" doesn't silently no-op against the plain
        constraint's own auto-created index of the same name.

        Tables created under the earlier 'single'-sentinel design still have
        ``exchange_side TEXT NOT NULL DEFAULT 'single'``. create_all(checkfirst)
        will not rewrite that column, so this migration also drops the NOT NULL
        / DEFAULT on Postgres and rewrites any leftover 'single' rows to NULL
        - otherwise live ingestion hits NotNullViolation the moment it tries to
        insert a non-exchange quote.
        """
        try:
            from sqlalchemy import inspect

            from infrastructure.persistence.models import MarketChoiceQuote

            self._create_table_and_indexes(
                MarketChoiceQuote,
                [
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    "unique_market_choice_quote_side_null_safe "
                    "ON market_choice_quotes "
                    "(choice_id, source, COALESCE(exchange_side, ''), exchange_level)",
                    "CREATE INDEX IF NOT EXISTS idx_market_choice_quotes_choice "
                    "ON market_choice_quotes (choice_id)",
                    "CREATE INDEX IF NOT EXISTS idx_market_choice_quotes_source "
                    "ON market_choice_quotes (source)",
                ],
            )

            inspector = inspect(self.engine)
            if "market_choice_quotes" not in inspector.get_table_names():
                return

            exchange_side_meta = next(
                (
                    col
                    for col in inspector.get_columns("market_choice_quotes")
                    if col["name"] == "exchange_side"
                ),
                None,
            )
            if exchange_side_meta is None:
                return

            with self.get_session() as session:
                # Postgres only: SQLite cannot ALTER COLUMN nullability in place,
                # and fresh test DBs already create the column as nullable from
                # the current ORM model.
                if (
                    self.engine.dialect.name == "postgresql"
                    and exchange_side_meta.get("nullable") is False
                ):
                    try:
                        session.execute(
                            text(
                                "ALTER TABLE market_choice_quotes "
                                "ALTER COLUMN exchange_side DROP NOT NULL"
                            )
                        )
                        logger.info(
                            "Dropped NOT NULL on market_choice_quotes.exchange_side"
                        )
                    except Exception as exc:
                        logger.debug(
                            "Could not DROP NOT NULL on exchange_side "
                            "(may already be nullable): %s",
                            exc,
                        )
                    try:
                        session.execute(
                            text(
                                "ALTER TABLE market_choice_quotes "
                                "ALTER COLUMN exchange_side DROP DEFAULT"
                            )
                        )
                        logger.info(
                            "Dropped DEFAULT on market_choice_quotes.exchange_side"
                        )
                    except Exception as exc:
                        logger.debug(
                            "Could not DROP DEFAULT on exchange_side "
                            "(may already be gone): %s",
                            exc,
                        )

                # Rewrite leftover sentinel rows from the pre-NULL design so
                # readers and the COALESCE unique index see one convention.
                rewritten = session.execute(
                    text(
                        "UPDATE market_choice_quotes "
                        "SET exchange_side = NULL "
                        "WHERE exchange_side = 'single'"
                    )
                )
                if rewritten.rowcount:
                    logger.info(
                        "Rewrote %s market_choice_quotes row(s) from "
                        "exchange_side='single' to NULL",
                        rewritten.rowcount,
                    )
                session.commit()

            logger.info("Market choice quotes schema is ready")
        except Exception as e:
            logger.error(f"Market choice quotes migration failed: {e}")
            logger.error(traceback.format_exc())

    def _validate_market_choice_snapshot_schema(self):
        """Require the final quote-linked snapshot schema without mutating it."""
        try:
            from sqlalchemy import inspect

            inspector = inspect(self.engine)
            if 'market_choice_snapshots' not in inspector.get_table_names():
                return

            db_columns = {col['name'] for col in inspector.get_columns('market_choice_snapshots')}
            required = {
                'snapshot_id',
                'quote_id',
                'odds_value',
                'collected_at',
                'source_collected_at',
                'source_limit',
                'exchange_size',
            }
            missing = sorted(required - db_columns)
            if missing:
                raise RuntimeError(
                    "market_choice_snapshots is not Phase 6 compatible; "
                    "missing columns: " + ", ".join(missing)
                )
            redundant = {
                'choice_id',
                'source',
                'source_market_id',
                'source_outcome_id',
                'bookmaker_outcome_id',
                'main_line',
                'exchange_side',
                'exchange_level',
            }
            if redundant & db_columns:
                raise RuntimeError(
                    "Unsupported expanded market_choice_snapshots schema; "
                    "this release requires the final quote-linked schema"
                )

            logger.debug(
                "market_choice_snapshots final lineage contract validated"
            )
        except Exception as e:
            logger.error(f"Market choice snapshot lineage migration failed: {e}")
            logger.error(traceback.format_exc())
            raise

    def _validate_market_choice_price_state_schema(self):
        """Require the final identity-only ``market_choices`` schema."""
        try:
            from sqlalchemy import inspect

            inspector = inspect(self.engine)
            if "market_choices" not in inspector.get_table_names():
                return

            columns = {
                item["name"]
                for item in inspector.get_columns("market_choices")
            }
            required = {"choice_id", "market_id", "choice_name"}
            missing = sorted(required - columns)
            if missing:
                raise RuntimeError(
                    "market_choices is not Phase 7 compatible; missing "
                    "identity columns: " + ", ".join(missing)
                )

            legacy = sorted(
                {"initial_odds", "current_odds", "change"} & columns
            )
            if legacy:
                raise RuntimeError(
                    "Unsupported expanded market_choices schema; this release "
                    "requires identity-only rows. Remaining columns: "
                    + ", ".join(legacy)
                )

            logger.debug(
                "market_choices final identity-only contract validated"
            )
        except Exception as exc:
            logger.error(
                "Market choice price-state validation failed: %s", exc
            )
            logger.error(traceback.format_exc())
            raise

    def _reorder_markets_columns(self):
        """
        Ensure 'bookie_id' is positioned correctly (after 'event_id') in 'markets' table.
        PostgreSQL appends new columns to the end. To change order, we must recreate the table.
        
        Steps if reordering is needed:
        1. Rename 'markets' -> 'markets_old'
        2. Rename valid constraints/indexes on 'markets_old' to avoid name collisions
        3. Create new 'markets' table (SQLAlchemy uses model definition order)
        4. Copy data from 'markets_old' to 'markets'
        5. Drop 'markets_old' (cascades to drop dependent FKs)
        6. Restore FK on 'market_choices'
        """
        try:
            from sqlalchemy import inspect
            inspector = inspect(self.engine)
            
            # Check if markets table exists
            if 'markets' not in inspector.get_table_names():
                return

            columns = inspector.get_columns('markets')
            col_names = [c['name'] for c in columns]
            
            # Expected order: market_id (0), event_id (1), bookie_id (2)...
            if 'bookie_id' not in col_names:
                return
                
            bookie_idx = col_names.index('bookie_id')
            event_idx = col_names.index('event_id') if 'event_id' in col_names else -1
            
            # If bookie_id is already effectively after event_id, skip
            if bookie_idx <= event_idx + 1:
                logger.debug("✅ Markets table columns already ordered correctly")
                return

            logger.info(f"🔄 Reordering 'markets' table columns (bookie_id is at index {bookie_idx}, moving to {event_idx + 1})")
            
            with self.get_session() as session:
                # 1. Rename current table
                session.execute(text("ALTER TABLE markets RENAME TO markets_old"))
                logger.info("  1️⃣  Renamed 'markets' to 'markets_old'")
                
                # 2. Rename constraints/indexes on old table to free up names
                # Rename Indexes
                try:
                    indexes = session.execute(text(
                        "SELECT indexname FROM pg_indexes WHERE tablename = 'markets_old'"
                    )).fetchall()
                    for (idx_name,) in indexes:
                        session.execute(text(f'ALTER INDEX "{idx_name}" RENAME TO "{idx_name}_old"'))
                except Exception as e:
                    logger.warning(f"  Warning renaming indexes: {e}")

                # Rename Constraints
                try:
                    constraints = session.execute(text(
                        "SELECT conname FROM pg_constraint WHERE conrelid = 'markets_old'::regclass"
                    )).fetchall()
                    for (con_name,) in constraints:
                        # Skip if it's already renamed (e.g. from previous run) or auto-generated weirdly
                        if not con_name.endswith('_old'):
                            session.execute(text(f'ALTER TABLE markets_old RENAME CONSTRAINT "{con_name}" TO "{con_name}_old"'))
                except Exception as e:
                    logger.warning(f"  Warning renaming constraints: {e}")
                
                # 3. Create new table using CURRENT TRANSACTION connection
                from infrastructure.persistence.models import Market
                Market.__table__.create(session.connection())
                logger.info("  2️⃣  Created new 'markets' table with correct column order")
                
                # 4. Copy data
                common_cols = [c.name for c in Market.__table__.columns if c.name in col_names]
                cols_str = ", ".join(common_cols)
                
                session.execute(text(
                    f"INSERT INTO markets ({cols_str}) SELECT {cols_str} FROM markets_old"
                ))
                logger.info(f"  3️⃣  Copied data into new table")
                
                # 5. Update sequences
                try:
                    session.execute(text(
                        "SELECT setval(pg_get_serial_sequence('markets', 'market_id'), coalesce(max(market_id), 1)) FROM markets"
                    ))
                except Exception:
                    pass # Ignore provided it might be identity column
                
                # 6. Drop old table (cascade drops dependent FKs from market_choices)
                session.execute(text("DROP TABLE markets_old CASCADE"))
                logger.info("  5️⃣  Dropped 'markets_old'")
                
                # 7. Restore FK on market_choices
                # We need to know the constraint logic. usually FK to markets.market_id
                # Check if market_choices exists
                if 'market_choices' in inspector.get_table_names():
                    try:
                        # Add FK back
                        session.execute(text(
                            "ALTER TABLE market_choices ADD CONSTRAINT fk_market_choices_market "
                            "FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE CASCADE"
                        ))
                        logger.info("  6️⃣  Restored FK on 'market_choices'")
                    except Exception as e:
                        logger.warning(f"  Could not restore market_choices FK (might exist): {e}")

                session.commit()
                logger.info("✅ Table reordering completed successfully")
                
        except Exception as e:
            logger.error(f"❌ Table reordering failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Attempt rollback
            try:
                with self.get_session() as session:
                    # Very basic check - if markets_old exists but markets doesn't, try to rename back
                    # Real rollback is handled by session transaction rollback usually, 
                    # but if we committed partway (not doing here), we'd need manual fix.
                    # Since we only commit at the end, session.rollback() (automatic on context exit) should handle DB state,
                    # EXCEPT for CREATE TABLE if it auto-commits (SQLAlchemy create() usually doesn't on connection).
                    pass
            except Exception:
                pass

    def _migrate_events_to_participants_competitions(self):
        """
        Idempotent migration for normalized event participants and competitions.

        This intentionally does not backfill from legacy text fields because those
        names are display snapshots and do not carry reliable external IDs.
        Existing events continue to work through COALESCE fallbacks in the views.
        """
        try:
            from sqlalchemy import inspect
            from infrastructure.persistence.models import Participant, Competition

            inspector = inspect(self.engine)
            table_names = set(inspector.get_table_names())

            with self.get_session() as session:
                connection = session.connection()

                if 'participants' not in table_names:
                    Participant.__table__.create(connection, checkfirst=True)
                    logger.info("Created participants table")

                if 'competitions' not in table_names:
                    Competition.__table__.create(connection, checkfirst=True)
                    logger.info("Created competitions table")

                if 'events' not in table_names:
                    return

                event_columns = {col['name'] for col in inspector.get_columns('events')}
                missing_event_columns = {
                    'home_participant_id': 'INTEGER',
                    'away_participant_id': 'INTEGER',
                    'competition_id': 'INTEGER',
                }

                for column_name, column_type in missing_event_columns.items():
                    if column_name not in event_columns:
                        session.execute(text(f"ALTER TABLE events ADD COLUMN {column_name} {column_type}"))
                        logger.info("Added events.%s", column_name)

                existing_fk_columns = {
                    tuple(constraint.get('constrained_columns') or [])
                    for constraint in inspector.get_foreign_keys('events')
                }

                fk_statements = [
                    (
                        'fk_events_home_participant',
                        ('home_participant_id',),
                        "ALTER TABLE events ADD CONSTRAINT fk_events_home_participant "
                        "FOREIGN KEY (home_participant_id) REFERENCES participants(participant_id) ON DELETE SET NULL",
                    ),
                    (
                        'fk_events_away_participant',
                        ('away_participant_id',),
                        "ALTER TABLE events ADD CONSTRAINT fk_events_away_participant "
                        "FOREIGN KEY (away_participant_id) REFERENCES participants(participant_id) ON DELETE SET NULL",
                    ),
                    (
                        'fk_events_competition',
                        ('competition_id',),
                        "ALTER TABLE events ADD CONSTRAINT fk_events_competition "
                        "FOREIGN KEY (competition_id) REFERENCES competitions(competition_id) ON DELETE SET NULL",
                    ),
                ]

                for constraint_name, constrained_columns, statement in fk_statements:
                    if constrained_columns not in existing_fk_columns:
                        try:
                            session.execute(text(statement))
                            logger.info("Added FK constraint %s", constraint_name)
                        except Exception as exc:
                            logger.debug("FK constraint %s may already exist or be equivalent: %s", constraint_name, exc)

                index_statements = [
                    "CREATE INDEX IF NOT EXISTS idx_events_home_participant_id ON events (home_participant_id)",
                    "CREATE INDEX IF NOT EXISTS idx_events_away_participant_id ON events (away_participant_id)",
                    "CREATE INDEX IF NOT EXISTS idx_events_competition_id ON events (competition_id)",
                    "CREATE INDEX IF NOT EXISTS idx_events_sport_starts_at ON events (sport, starts_at)",
                    "CREATE INDEX IF NOT EXISTS idx_participants_source_participant ON participants (source, source_participant_id)",
                    "CREATE INDEX IF NOT EXISTS idx_competitions_source_tournament ON competitions (source, source_tournament_id)",
                ]

                for statement in index_statements:
                    session.execute(text(statement))

                session.commit()
                logger.info("Normalized event entity migration completed")

        except Exception as e:
            logger.error(f"Events participant/competition migration failed: {e}")
            logger.error(traceback.format_exc())

    def _migrate_daily_discovery_log_run_slots(self):
        """Backfill run slots and rebuild DailyDiscoveryLog uniqueness."""
        try:
            from sqlalchemy import inspect
            from infrastructure.persistence.models import DailyDiscoveryLog

            inspector = inspect(self.engine)
            table_names = set(inspector.get_table_names())
            if 'daily_discovery_log' not in table_names:
                return

            db_columns = {col['name'] for col in inspector.get_columns('daily_discovery_log')}
            unique_constraints = {
                tuple(uc.get('column_names') or ()): uc.get('name')
                for uc in inspector.get_unique_constraints('daily_discovery_log')
            }

            has_legacy_unique = ('date', 'sport') in unique_constraints
            has_expected_unique = ('date', 'run_slot', 'sport') in unique_constraints
            has_run_slot_column = 'run_slot' in db_columns
            dialect_name = self.engine.dialect.name

            with self.get_session() as session:
                connection = session.connection()

                if dialect_name == 'sqlite' and has_legacy_unique:
                    logger.info("Rebuilding daily_discovery_log for SQLite slot migration")
                    session.execute(text("DROP TABLE IF EXISTS daily_discovery_log_old"))
                    session.execute(text("ALTER TABLE daily_discovery_log RENAME TO daily_discovery_log_old"))
                    DailyDiscoveryLog.__table__.create(connection, checkfirst=True)
                    run_slot_select = "COALESCE(NULLIF(run_slot, ''), 'AM')" if has_run_slot_column else "'AM'"
                    insert_sql = f"""
                        INSERT INTO daily_discovery_log (
                            id, date, run_slot, sport, status, attempts, last_attempt_at, created_at
                        )
                        SELECT
                            id,
                            date,
                            {run_slot_select},
                            sport,
                            status,
                            attempts,
                            last_attempt_at,
                            created_at
                        FROM daily_discovery_log_old
                    """
                    session.execute(text(insert_sql))
                    session.execute(text("DROP TABLE daily_discovery_log_old"))
                    session.execute(text(
                        "CREATE INDEX IF NOT EXISTS idx_daily_discovery_log_date_slot_status "
                        "ON daily_discovery_log (date, run_slot, status)"
                    ))
                    session.execute(text(
                        "CREATE INDEX IF NOT EXISTS idx_daily_discovery_log_date_slot_sport "
                        "ON daily_discovery_log (date, run_slot, sport)"
                    ))
                    session.commit()
                    logger.info("DailyDiscoveryLog run-slot migration completed for SQLite")
                    return

                if 'run_slot' not in db_columns:
                    session.execute(text(
                        "ALTER TABLE daily_discovery_log "
                        "ADD COLUMN run_slot VARCHAR(20) NOT NULL DEFAULT 'AM'"
                    ))
                    logger.info("Added run_slot column to daily_discovery_log")

                session.execute(text(
                    "UPDATE daily_discovery_log "
                    "SET run_slot = 'AM' "
                    "WHERE run_slot IS NULL OR run_slot = ''"
                ))

                if dialect_name != 'sqlite':
                    session.execute(text(
                        "ALTER TABLE daily_discovery_log DROP CONSTRAINT IF EXISTS unique_date_sport_discovery"
                    ))
                    session.execute(text(
                        "ALTER TABLE daily_discovery_log DROP CONSTRAINT IF EXISTS unique_date_slot_sport_discovery"
                    ))
                    if not has_expected_unique:
                        session.execute(text(
                            "ALTER TABLE daily_discovery_log "
                            "ADD CONSTRAINT unique_date_slot_sport_discovery "
                            "UNIQUE (date, run_slot, sport)"
                        ))
                        logger.info("Added unique_date_slot_sport_discovery constraint")
                else:
                    if not has_expected_unique:
                        session.execute(text(
                            "CREATE UNIQUE INDEX IF NOT EXISTS unique_date_slot_sport_discovery "
                            "ON daily_discovery_log (date, run_slot, sport)"
                        ))
                        logger.info("Added unique index unique_date_slot_sport_discovery")

                session.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_daily_discovery_log_date_slot_status "
                    "ON daily_discovery_log (date, run_slot, status)"
                ))
                session.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_daily_discovery_log_date_slot_sport "
                    "ON daily_discovery_log (date, run_slot, sport)"
                ))

                session.commit()
                logger.info("DailyDiscoveryLog run-slot migration completed")

        except Exception as e:
            logger.error(f"DailyDiscoveryLog run-slot migration failed: {e}")
            logger.error(traceback.format_exc())

    def _event_identity_migration_already_applied(self, session) -> bool:
        """Return True when events already use canonical IDs and SofaScore mappings point to them."""
        try:
            marker_exists = session.execute(text("""
                SELECT 1
                FROM event_migration_status
                WHERE migration_key = 'event_identity_canonicalization'
            """)).scalar() is not None
            if marker_exists:
                return True

            events_count = session.execute(text("SELECT COUNT(*) FROM events")).scalar() or 0
            if events_count == 0:
                return True
            min_event_id, max_event_id = session.execute(text("""
                SELECT COALESCE(MIN(id), 0), COALESCE(MAX(id), 0)
                FROM events
            """)).fetchone()
            mapped_events_count = session.execute(text("""
                SELECT COUNT(*)
                FROM events e
                JOIN event_source_mappings esm
                    ON esm.event_id = e.id
                   AND esm.source = 'sofascore'
            """)).scalar() or 0

            return (
                mapped_events_count == events_count
                and min_event_id == 1
                and max_event_id == events_count
            )
        except Exception as exc:
            logger.debug("Could not determine if event identity migration was already applied: %s", exc)
            return False

    def _ensure_event_migration_status_table(self, session) -> None:
        """Create the migration status table used to record completed one-time migrations."""
        try:
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS event_migration_status (
                    migration_key TEXT PRIMARY KEY,
                    completed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    details TEXT
                )
            """))
        except Exception as exc:
            logger.debug("Could not ensure event_migration_status table: %s", exc)

    def _mark_event_identity_migration_completed(self, session) -> None:
        """Persist a durable marker indicating the canonical event migration succeeded."""
        session.execute(text("""
            INSERT INTO event_migration_status (migration_key, completed_at, details)
            VALUES (
                'event_identity_canonicalization',
                CURRENT_TIMESTAMP,
                'events.id canonicalized and SofaScore mappings materialized'
            )
            ON CONFLICT (migration_key) DO UPDATE SET
                completed_at = EXCLUDED.completed_at,
                details = EXCLUDED.details
        """))

    def _ensure_event_source_mappings_table_ready(self, session, dialect_name: str) -> None:
        """Ensure the source mapping table exists and has the expected indexes/constraint."""
        try:
            with session.begin_nested():
                session.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS unique_event_source_mapping "
                    "ON event_source_mappings (source, source_event_id)"
                ))
            with session.begin_nested():
                session.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_event_source_mappings_event_id "
                    "ON event_source_mappings (event_id)"
                ))
            with session.begin_nested():
                session.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_event_source_mappings_source_event_id "
                    "ON event_source_mappings (source, source_event_id)"
                ))
            with session.begin_nested():
                session.execute(text(
                    "CREATE INDEX IF NOT EXISTS idx_event_source_mappings_source "
                    "ON event_source_mappings (source)"
                ))

            if dialect_name == "postgresql":
                try:
                    with session.begin_nested():
                        session.execute(text(
                            "ALTER TABLE event_source_mappings "
                            "ADD CONSTRAINT unique_event_source_mapping UNIQUE (source, source_event_id)"
                        ))
                except Exception:
                    # The table may already have the named constraint or an equivalent unique index.
                    pass
            logger.info("Ensured event_source_mappings table constraints/indexes")
        except Exception as exc:
            logger.debug("Could not ensure event_source_mappings constraints/indexes: %s", exc)

    def _cleanup_orphan_event_source_mappings(self, session) -> int:
        """Delete stale source mappings whose event row no longer exists."""
        try:
            orphan_count = session.execute(text("""
                SELECT COUNT(*)
                FROM event_source_mappings esm
                LEFT JOIN events e ON esm.event_id = e.id
                WHERE e.id IS NULL
            """)).scalar() or 0

            if orphan_count:
                session.execute(text("""
                    DELETE FROM event_source_mappings
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM events e
                        WHERE e.id = event_source_mappings.event_id
                    )
                """))
                logger.warning("Removed %s orphan event source mapping(s) during migration cleanup", orphan_count)

            return orphan_count
        except Exception as exc:
            logger.debug("Could not cleanup orphan event source mappings: %s", exc)
            return 0

    def cleanup_orphan_event_source_mappings(self) -> int:
        """Expose cleanup of orphan event source mappings publicly, managing its own session."""
        try:
            with self.get_session() as session:
                return self._cleanup_orphan_event_source_mappings(session)
        except Exception as exc:
            logger.error("Failed to run public cleanup of orphan event source mappings: %s", exc)
            return 0

    def _drop_event_identity_foreign_keys(self, session, inspector) -> None:
        """Drop foreign keys that still point at events.id so the PK rewrite can happen safely."""
        tables = [
            "results",
            "markets",
            "event_observations",
            "prediction_logs",
            "event_source_mappings",
        ]

        for table_name in tables:
            try:
                foreign_keys = inspector.get_foreign_keys(table_name)
            except Exception as exc:
                logger.debug("Could not inspect foreign keys for %s: %s", table_name, exc)
                continue

            for fk in foreign_keys:
                if fk.get("referred_table") != "events":
                    continue
                constrained_columns = fk.get("constrained_columns") or []
                if constrained_columns != ["event_id"]:
                    continue
                constraint_name = fk.get("name")
                if not constraint_name:
                    continue
                try:
                    with session.begin_nested():
                        session.execute(text(f'ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS "{constraint_name}"'))
                    logger.info("Dropped FK constraint %s on %s", constraint_name, table_name)
                except Exception as exc:
                    logger.warning(
                        "Could not drop FK constraint %s on %s before event identity migration: %s",
                        constraint_name,
                        table_name,
                        exc,
                    )

    def _restore_event_identity_foreign_keys(self, session, inspector, dialect_name: str) -> None:
        """Recreate the canonical event_id foreign keys after the rewrite."""
        if dialect_name == "sqlite":
            return

        from sqlalchemy import inspect

        fresh_inspector = inspect(self.engine)

        fk_statements = [
            (
                "results",
                "fk_results_event_id",
                "ALTER TABLE results ADD CONSTRAINT fk_results_event_id "
                "FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE",
            ),
            (
                "markets",
                "fk_markets_event_id",
                "ALTER TABLE markets ADD CONSTRAINT fk_markets_event_id "
                "FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE",
            ),
            (
                "event_observations",
                "fk_event_observations_event_id",
                "ALTER TABLE event_observations ADD CONSTRAINT fk_event_observations_event_id "
                "FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE",
            ),
            (
                "prediction_logs",
                "fk_prediction_logs_event_id",
                "ALTER TABLE prediction_logs ADD CONSTRAINT fk_prediction_logs_event_id "
                "FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE",
            ),
            (
                "event_source_mappings",
                "fk_event_source_mappings_event_id",
                "ALTER TABLE event_source_mappings ADD CONSTRAINT fk_event_source_mappings_event_id "
                "FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE",
            ),
        ]

        for table_name, constraint_name, statement in fk_statements:
            try:
                existing_constraints = {
                    fk.get("name")
                    for fk in fresh_inspector.get_foreign_keys(table_name)
                    if fk.get("referred_table") == "events" and (fk.get("constrained_columns") or []) == ["event_id"]
                }
                if constraint_name in existing_constraints:
                    continue
                with session.begin_nested():
                    session.execute(text(statement))
                logger.info("Added FK constraint %s on %s", constraint_name, table_name)
            except Exception as exc:
                logger.warning("Could not restore FK constraint %s on %s: %s", constraint_name, table_name, exc)

    def _ensure_events_id_default(self, session) -> None:
        """Ensure events.id has a sequence-backed default on PostgreSQL."""
        try:
            dialect_name = self.engine.dialect.name
            if dialect_name != "postgresql":
                return

            with session.begin_nested():
                session.execute(text("CREATE SEQUENCE IF NOT EXISTS events_id_seq"))
            with session.begin_nested():
                session.execute(text("ALTER SEQUENCE events_id_seq OWNED BY events.id"))
            with session.begin_nested():
                session.execute(text("""
                    SELECT setval(
                        'events_id_seq',
                        COALESCE((SELECT MAX(id) FROM events), 0),
                        true
                    )
                """))
            with session.begin_nested():
                session.execute(text("ALTER TABLE events ALTER COLUMN id SET DEFAULT nextval('events_id_seq')"))
            logger.info("Ensured events.id uses the events_id_seq sequence")
        except Exception as exc:
            logger.warning("Could not ensure sequence-backed default for events.id: %s", exc)

    def _validate_event_identity_migration(self, session) -> None:
        """Validate the event identity migration state and raise if critical checks fail."""
        dialect_name = self.engine.dialect.name

        checks = {
            "events_total": session.execute(text("SELECT COUNT(*) FROM events")).scalar() or 0,
            "sofascore_mappings_total": session.execute(text("""
                SELECT COUNT(*)
                FROM event_source_mappings
                WHERE source = 'sofascore'
            """)).scalar() or 0,
            "events_without_sofascore_mapping": session.execute(text("""
                SELECT COUNT(*)
                FROM events e
                LEFT JOIN event_source_mappings esm
                    ON esm.event_id = e.id
                   AND esm.source = 'sofascore'
                WHERE esm.mapping_id IS NULL
            """)).scalar() or 0,
            "orphan_results": session.execute(text("""
                SELECT COUNT(*)
                FROM results r
                LEFT JOIN events e ON r.event_id = e.id
                WHERE e.id IS NULL
            """)).scalar() or 0,
            "orphan_markets": session.execute(text("""
                SELECT COUNT(*)
                FROM markets m
                LEFT JOIN events e ON m.event_id = e.id
                WHERE e.id IS NULL
            """)).scalar() or 0,
            "orphan_event_observations": session.execute(text("""
                SELECT COUNT(*)
                FROM event_observations eo
                LEFT JOIN events e ON eo.event_id = e.id
                WHERE e.id IS NULL
            """)).scalar() or 0,
            "orphan_prediction_logs": session.execute(text("""
                SELECT COUNT(*)
                FROM prediction_logs pl
                LEFT JOIN events e ON pl.event_id = e.id
                WHERE e.id IS NULL
            """)).scalar() or 0,
            "orphan_event_source_mappings": session.execute(text("""
                SELECT COUNT(*)
                FROM event_source_mappings esm
                LEFT JOIN events e ON esm.event_id = e.id
                WHERE e.id IS NULL
            """)).scalar() or 0,
            "duplicate_mappings": session.execute(text("""
                SELECT COUNT(*)
                FROM (
                    SELECT source, source_event_id
                    FROM event_source_mappings
                    GROUP BY source, source_event_id
                    HAVING COUNT(*) > 1
                ) duplicates
            """)).scalar() or 0,
        }

        if dialect_name == "postgresql":
            events_id_default = session.execute(text("""
                SELECT column_default
                FROM information_schema.columns
                WHERE table_name = 'events'
                  AND column_name = 'id'
            """)).scalar()
            checks["events_id_default_is_sequence"] = bool(events_id_default and "nextval" in str(events_id_default))
        else:
            checks["events_id_default_is_sequence"] = True

        logger.info(
            "Event identity migration validation: events=%s sofascore_mappings=%s missing_sofascore=%s orphan_results=%s orphan_markets=%s orphan_observations=%s orphan_prediction_logs=%s orphan_source_mappings=%s duplicate_mappings=%s default_ok=%s",
            checks["events_total"],
            checks["sofascore_mappings_total"],
            checks["events_without_sofascore_mapping"],
            checks["orphan_results"],
            checks["orphan_markets"],
            checks["orphan_event_observations"],
            checks["orphan_prediction_logs"],
            checks["orphan_event_source_mappings"],
            checks["duplicate_mappings"],
            checks["events_id_default_is_sequence"],
        )

        if checks["events_without_sofascore_mapping"] != 0:
            raise RuntimeError(f"Event identity migration validation failed: {checks['events_without_sofascore_mapping']} events without SofaScore mapping")
        if checks["orphan_results"] != 0:
            raise RuntimeError(f"Event identity migration validation failed: {checks['orphan_results']} orphan results")
        if checks["orphan_markets"] != 0:
            raise RuntimeError(f"Event identity migration validation failed: {checks['orphan_markets']} orphan markets")
        if checks["orphan_event_observations"] != 0:
            raise RuntimeError(f"Event identity migration validation failed: {checks['orphan_event_observations']} orphan event observations")
        if checks["orphan_prediction_logs"] != 0:
            raise RuntimeError(f"Event identity migration validation failed: {checks['orphan_prediction_logs']} orphan prediction logs")
        if checks["orphan_event_source_mappings"] != 0:
            raise RuntimeError(f"Event identity migration validation failed: {checks['orphan_event_source_mappings']} orphan event source mappings")
        if checks["duplicate_mappings"] != 0:
            raise RuntimeError(f"Event identity migration validation failed: {checks['duplicate_mappings']} duplicate event source mappings")
        if dialect_name == "postgresql" and not checks["events_id_default_is_sequence"]:
            raise RuntimeError("Event identity migration validation failed: events.id is missing the sequence-backed default")

    def _migrate_events_to_canonical_identity(self):
        """
        Convert events.id from SofaScore external IDs to canonical autoincrement IDs.

        This migration is designed to be idempotent and safe to rerun. It creates the
        event_source_mappings table if needed, seeds SofaScore mappings from the current
        events table, rewrites dependent FKs, and restores the canonical sequence/default.
        """
        try:
            from sqlalchemy import inspect
            from infrastructure.persistence.models import EventSourceMapping

            inspector = inspect(self.engine)
            table_names = set(inspector.get_table_names())
            if "events" not in table_names:
                return

            with self.get_session() as session:
                connection = session.connection()
                dialect_name = self.engine.dialect.name
                batch_size = 1000
                total_processed = 0

                self._ensure_event_migration_status_table(session)

                if "event_source_mappings" not in table_names:
                    EventSourceMapping.__table__.create(connection, checkfirst=True)
                    logger.info("Created event_source_mappings table")

                self._ensure_event_source_mappings_table_ready(session, dialect_name)
                self._cleanup_orphan_event_source_mappings(session)

                if self._event_identity_migration_already_applied(session):
                    logger.info("Event identity migration already applied; refreshing defaults and validations")
                    self._ensure_events_id_default(session)
                    self._validate_event_identity_migration(session)
                    self._mark_event_identity_migration_completed(session)
                    return

                total_events = session.execute(text("SELECT COUNT(*) FROM events")).scalar() or 0
                if not total_events:
                    logger.info("No events found; skipping event identity migration")
                    self._ensure_events_id_default(session)
                    return

                logger.info("Migrating %s event(s) to canonical IDs in batches of %s", total_events, batch_size)

                event_ids = [
                    row[0]
                    for row in session.execute(text("SELECT id FROM events ORDER BY id")).fetchall()
                ]
                translation_rows = [
                    {"old_id": old_id, "new_id": index + 1}
                    for index, old_id in enumerate(event_ids)
                ]

                foreign_keys_were_disabled = False
                foreign_keys_were_dropped = False
                if dialect_name == "postgresql":
                    self._drop_event_identity_foreign_keys(session, inspector)
                    foreign_keys_were_dropped = True
                else:
                    session.execute(text("PRAGMA foreign_keys = OFF"))
                    foreign_keys_were_disabled = True

                try:
                    batch_number = 0
                    for batch_start in range(0, len(translation_rows), batch_size):
                        batch_number += 1
                        batch_translation_rows = translation_rows[batch_start:batch_start + batch_size]
                        batch_old_ids = [row["old_id"] for row in batch_translation_rows]
                        batch_new_ids = [row["new_id"] for row in batch_translation_rows]

                        logger.info(
                            "Processing event identity batch %s: %s event(s), old_id_range=%s..%s, next_new_id_start=%s",
                            batch_number,
                            len(batch_old_ids),
                            batch_old_ids[0],
                            batch_old_ids[-1],
                            batch_new_ids[0],
                        )

                        session.execute(text("""
                            CREATE TEMPORARY TABLE event_id_translation (
                                old_id INTEGER PRIMARY KEY,
                                new_id INTEGER NOT NULL
                            )
                        """))
                        session.execute(
                            text("INSERT INTO event_id_translation (old_id, new_id) VALUES (:old_id, :new_id)"),
                            batch_translation_rows,
                        )

                        session.execute(text("""
                            INSERT INTO event_source_mappings (
                                event_id,
                                source,
                                source_event_id,
                                match_method,
                                confidence,
                                created_at,
                                updated_at
                            )
                            SELECT
                                t.new_id,
                                'sofascore',
                                CAST(t.old_id AS TEXT),
                                'legacy_primary_key_migration',
                                1.000,
                                CURRENT_TIMESTAMP,
                                CURRENT_TIMESTAMP
                            FROM event_id_translation t
                            LEFT JOIN event_source_mappings esm
                                ON esm.source = 'sofascore'
                               AND esm.source_event_id = CAST(t.old_id AS TEXT)
                            WHERE esm.mapping_id IS NULL
                            ON CONFLICT (source, source_event_id) DO NOTHING
                        """))

                        session.execute(text("""
                            UPDATE event_source_mappings esm
                            SET event_id = t.new_id,
                                match_method = 'legacy_primary_key_migration',
                                confidence = 1.000,
                                updated_at = CURRENT_TIMESTAMP
                            FROM event_id_translation t
                            WHERE esm.source = 'sofascore'
                              AND esm.source_event_id = CAST(t.old_id AS TEXT)
                        """))

                        if dialect_name == "postgresql":
                            session.execute(text("""
                                UPDATE results r
                                SET event_id = t.new_id
                                FROM event_id_translation t
                                WHERE r.event_id = t.old_id
                            """))
                            session.execute(text("""
                                UPDATE markets m
                                SET event_id = t.new_id
                                FROM event_id_translation t
                                WHERE m.event_id = t.old_id
                            """))
                            session.execute(text("""
                                UPDATE event_observations eo
                                SET event_id = t.new_id
                                FROM event_id_translation t
                                WHERE eo.event_id = t.old_id
                            """))
                            session.execute(text("""
                                UPDATE prediction_logs pl
                                SET event_id = t.new_id
                                FROM event_id_translation t
                                WHERE pl.event_id = t.old_id
                            """))
                            session.execute(text("""
                                UPDATE event_source_mappings esm
                                SET event_id = t.new_id
                                FROM event_id_translation t
                                WHERE esm.event_id = t.old_id
                            """))
                            session.execute(text("""
                                UPDATE events e
                                SET id = t.new_id
                                FROM event_id_translation t
                                WHERE e.id = t.old_id
                            """))
                        else:
                            update_statements = [
                                "UPDATE results SET event_id = (SELECT new_id FROM event_id_translation WHERE old_id = results.event_id) WHERE event_id IN (SELECT old_id FROM event_id_translation)",
                                "UPDATE markets SET event_id = (SELECT new_id FROM event_id_translation WHERE old_id = markets.event_id) WHERE event_id IN (SELECT old_id FROM event_id_translation)",
                                "UPDATE event_observations SET event_id = (SELECT new_id FROM event_id_translation WHERE old_id = event_observations.event_id) WHERE event_id IN (SELECT old_id FROM event_id_translation)",
                                "UPDATE prediction_logs SET event_id = (SELECT new_id FROM event_id_translation WHERE old_id = prediction_logs.event_id) WHERE event_id IN (SELECT old_id FROM event_id_translation)",
                                "UPDATE event_source_mappings SET event_id = (SELECT new_id FROM event_id_translation WHERE old_id = event_source_mappings.event_id) WHERE event_id IN (SELECT old_id FROM event_id_translation)",
                                "UPDATE events SET id = (SELECT new_id FROM event_id_translation WHERE old_id = events.id) WHERE id IN (SELECT old_id FROM event_id_translation)",
                            ]
                            for statement in update_statements:
                                session.execute(text(statement))

                        total_processed += len(batch_old_ids)
                        logger.info(
                            "Completed event identity batch %s: processed=%s total_processed=%s remaining=%s",
                            batch_number,
                            len(batch_old_ids),
                            total_processed,
                            total_events - total_processed,
                        )

                        session.execute(text("DROP TABLE event_id_translation"))

                    if foreign_keys_were_disabled:
                        session.execute(text("PRAGMA foreign_keys = ON"))
                        foreign_keys_were_disabled = False

                    self._ensure_events_id_default(session)
                    self._restore_event_identity_foreign_keys(session, inspector, dialect_name)
                    foreign_keys_were_dropped = False
                    self._validate_event_identity_migration(session)
                    self._mark_event_identity_migration_completed(session)

                    session.commit()
                    logger.info("Event identity migration completed successfully")
                finally:
                    if foreign_keys_were_dropped:
                        try:
                            self._restore_event_identity_foreign_keys(session, inspector, dialect_name)
                            session.commit()
                        except Exception as restore_exc:
                            logger.warning(
                                "Could not restore event identity foreign keys after migration attempt: %s",
                                restore_exc,
                            )
                            session.rollback()
                    if dialect_name == "postgresql":
                        foreign_keys_were_dropped = False
                    if foreign_keys_were_disabled:
                        try:
                            session.execute(text("PRAGMA foreign_keys = ON"))
                            session.commit()
                        except Exception:
                            session.rollback()

        except Exception as e:
            logger.error(f"Event identity migration failed: {e}")
            logger.error(traceback.format_exc())
            raise

    def _get_column_type_sql(self, col) -> str:
        """
        Convert SQLAlchemy column type to SQL type string.
        
        Args:
            col: SQLAlchemy Column object
            
        Returns:
            str: SQL type string (e.g., 'VARCHAR(50)', 'INTEGER', 'NUMERIC(6,2)')
        """
        from sqlalchemy.types import String, Integer, Text, DateTime, Numeric, BigInteger
        from infrastructure.persistence.types import UTCDateTime
        
        col_type = col.type
        
        if isinstance(col_type, UTCDateTime):
            if self.engine.dialect.name == "postgresql":
                return "TIMESTAMP WITH TIME ZONE"
            return "TIMESTAMP"
        if isinstance(col_type, Text):
            return "TEXT"
        elif isinstance(col_type, String):
            if col_type.length:
                return f"VARCHAR({col_type.length})"
            else:
                return "VARCHAR"
        elif isinstance(col_type, Integer):
            return "INTEGER"
        elif isinstance(col_type, BigInteger):
            return "BIGINT"
        elif isinstance(col_type, Numeric):
            if col_type.precision and col_type.scale:
                return f"NUMERIC({col_type.precision},{col_type.scale})"
            else:
                return "NUMERIC"
        elif isinstance(col_type, DateTime):
            return "TIMESTAMP"
        else:
            # Fallback to string representation
            return str(col_type)
    

# Global database manager instance
db_manager = DatabaseManager()
# Note: Removed automatic table dropping - tables are now created only when needed
