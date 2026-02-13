from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from typing import Generator
import logging
from config import Config
from models import Base

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.engine = None
        self.SessionLocal = None
        self._setup_engine()
    
    def _setup_engine(self):
        """Setup database engine and session factory"""
        try:
            self.engine = create_engine(
                self.database_url,
                echo=False,  # Set to True for SQL debugging
                pool_pre_ping=True,
                pool_recycle=300,
                connect_args={"connect_timeout": Config.DB_CONNECT_TIMEOUT}
            )
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
    
    def check_and_migrate_schema(self) -> bool:
        """
        Automatically detect and apply schema migrations by comparing models.py with database.
        This is a generic migration system that syncs database with SQLAlchemy models.
        
        Capabilities:
        - Detects missing columns in existing tables
        - Adds missing columns with appropriate types and defaults
        - Handles NOT NULL constraints with defaults
        - Creates indexes for new columns when appropriate
        - Runs one-time manual migrations (e.g., markets→bookies)
        
        Limitations:
        - Does NOT drop columns (requires manual intervention via _migrate_* methods)
        - Does NOT modify existing column types (requires manual intervention)
        - Does NOT rename columns (requires manual intervention)
        
        Returns:
            bool: True if schema is up to date or successfully migrated, False on error
        """
        try:
            from sqlalchemy import inspect
            from sqlalchemy.types import String, Integer, Text, DateTime, Numeric, BigInteger
            
            inspector = inspect(self.engine)
            migrations_applied = []
            
            # Run one-time manual migrations FIRST (before generic column migration)
            # This ensures complex migrations (e.g., backfilling NOT NULL columns) run
            # before the generic migration tries to add them with NOT NULL constraint.
            self._migrate_markets_to_bookies()
            
            # Check and fix column order (bookie_id should be next to event_id)
            self._reorder_markets_columns()
            
            # Re-create inspector after manual migrations may have changed schema
            inspector = inspect(self.engine)
            
            # Iterate through all tables defined in models
            for table_name, table in Base.metadata.tables.items():
                # Get actual columns in database
                try:
                    db_columns = {col['name']: col for col in inspector.get_columns(table_name)}
                except Exception:
                    # Table doesn't exist yet, skip migration (will be created by create_tables)
                    logger.debug(f"Table {table_name} doesn't exist yet, skipping migration check")
                    continue
                
                # Get expected columns from model
                model_columns = {col.name: col for col in table.columns}
                
                # Find missing columns (in model but not in database)
                missing_columns = set(model_columns.keys()) - set(db_columns.keys())
                
                if not missing_columns:
                    logger.debug(f"✅ Table '{table_name}' schema is up to date")
                    continue
                
                # Apply migrations for missing columns
                logger.info(f"🔄 Migrating table '{table_name}': Found {len(missing_columns)} missing column(s)")
                
                with self.get_session() as session:
                    for col_name in missing_columns:
                        col = model_columns[col_name]
                        
                        # Skip computed columns (they're generated by database)
                        if hasattr(col, 'computed') and col.computed is not None:
                            logger.debug(f"  ⏭️ Skipping computed column: {col_name}")
                            continue
                        
                        # Build column type string
                        col_type = self._get_column_type_sql(col)
                        
                        # Build NULL/NOT NULL constraint
                        nullable_str = "" if col.nullable else "NOT NULL"
                        
                        # Build DEFAULT constraint
                        default_str = ""
                        if col.default is not None:
                            if hasattr(col.default, 'arg'):
                                # Scalar default value
                                default_value = col.default.arg
                                if isinstance(default_value, str):
                                    default_str = f"DEFAULT '{default_value}'"
                                elif callable(default_value):
                                    # Skip callable defaults (like datetime.now) - they're for ORM, not DB
                                    default_str = ""
                                else:
                                    default_str = f"DEFAULT {default_value}"
                        
                        # Build and execute ALTER TABLE statement
                        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {nullable_str} {default_str}".strip()
                        
                        logger.info(f"  ➕ Adding column: {col_name} ({col_type})")
                        logger.debug(f"  SQL: {alter_sql}")
                        
                        session.execute(text(alter_sql))
                        migrations_applied.append(f"{table_name}.{col_name}")
                        
                        # Create index if column name suggests it should be indexed
                        # (discovery_source, sport, gender, etc.)
                        if any(keyword in col_name.lower() for keyword in ['source', 'sport', 'type', 'status', 'gender']):
                            index_name = f"idx_{table_name}_{col_name}"
                            index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col_name})"
                            logger.info(f"  📊 Creating index: {index_name}")
                            session.execute(text(index_sql))
                    
                    session.commit()
            
            if migrations_applied:
                logger.info(f"✅ Migration completed: Added {len(migrations_applied)} column(s) - {', '.join(migrations_applied)}")
            else:
                logger.info("✅ Database schema is up to date with models")
            
            return True
                    
        except Exception as e:
            logger.error(f"❌ Schema migration failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _migrate_markets_to_bookies(self):
        """
        One-time migration: transition markets table from sofascore_market_id to bookie_id.
        
        Steps:
        1. Ensure default 'SofaScore' bookie exists (id=1)
        2. If 'sofascore_market_id' column still exists in markets table:
           a. Drop old unique constraint
           b. Set bookie_id=1 on all existing rows
           c. Drop sofascore_market_id column
           d. Add new unique constraint
        
        This migration is idempotent — safe to run multiple times.
        """
        try:
            from sqlalchemy import inspect
            inspector = inspect(self.engine)
            
            # Check if bookies table exists
            if 'bookies' not in inspector.get_table_names():
                logger.debug("Bookies table doesn't exist yet, skipping bookie migration")
                return
            
            # Ensure default SofaScore bookie exists
            with self.get_session() as session:
                result = session.execute(text("SELECT bookie_id FROM bookies WHERE slug = 'sofascore'")).fetchone()
                if not result:
                    session.execute(text(
                        "INSERT INTO bookies (name, slug) VALUES ('SofaScore', 'sofascore')"
                    ))
                    session.commit()
                    logger.info("📌 Created default 'SofaScore' bookie")
            
            # Check if markets table still has sofascore_market_id (migration needed)
            if 'markets' not in inspector.get_table_names():
                return
            
            db_columns = {col['name'] for col in inspector.get_columns('markets')}
            
            if 'sofascore_market_id' not in db_columns:
                logger.debug("✅ Markets table already migrated (no sofascore_market_id)")
                return
            
            logger.info("🔄 Migrating markets table: removing sofascore_market_id, adding bookie support")
            
            # Get the SofaScore bookie_id
            with self.get_session() as session:
                sofascore_bookie = session.execute(
                    text("SELECT bookie_id FROM bookies WHERE slug = 'sofascore'")
                ).fetchone()
                sofascore_bookie_id = sofascore_bookie[0]
                
                # 1. Drop old unique constraint (if exists)
                try:
                    session.execute(text(
                        "ALTER TABLE markets DROP CONSTRAINT IF EXISTS unique_market_per_event"
                    ))
                    logger.info("  🗑️ Dropped old constraint: unique_market_per_event")
                except Exception as e:
                    logger.debug(f"  Old constraint may not exist: {e}")
                
                # 2. Add bookie_id column if not present (auto-migration may have added it as nullable)
                if 'bookie_id' not in db_columns:
                    session.execute(text(
                        "ALTER TABLE markets ADD COLUMN bookie_id INTEGER"
                    ))
                    logger.info("  ➕ Added bookie_id column")
                
                # 3. Set all existing rows to SofaScore bookie
                updated = session.execute(text(
                    f"UPDATE markets SET bookie_id = {sofascore_bookie_id} WHERE bookie_id IS NULL"
                ))
                logger.info(f"  📝 Set bookie_id={sofascore_bookie_id} on {updated.rowcount} existing market(s)")
                
                # 4. Make bookie_id NOT NULL and add FK
                try:
                    session.execute(text(
                        "ALTER TABLE markets ALTER COLUMN bookie_id SET NOT NULL"
                    ))
                    logger.info("  🔒 Set bookie_id to NOT NULL")
                except Exception as e:
                    logger.warning(f"  Could not set NOT NULL (may already be set): {e}")
                
                try:
                    session.execute(text(
                        "ALTER TABLE markets ADD CONSTRAINT fk_markets_bookie "
                        "FOREIGN KEY (bookie_id) REFERENCES bookies(bookie_id) ON DELETE CASCADE"
                    ))
                    logger.info("  🔗 Added FK constraint: fk_markets_bookie")
                except Exception as e:
                    logger.debug(f"  FK may already exist: {e}")
                
                # 5. Drop sofascore_market_id column
                session.execute(text(
                    "ALTER TABLE markets DROP COLUMN sofascore_market_id"
                ))
                logger.info("  🗑️ Dropped column: sofascore_market_id")
                
                # 6. Add new unique constraint
                try:
                    session.execute(text(
                        "ALTER TABLE markets ADD CONSTRAINT unique_market_per_event_bookie "
                        "UNIQUE (event_id, bookie_id, market_name, choice_group)"
                    ))
                    logger.info("  ✅ Added new constraint: unique_market_per_event_bookie")
                except Exception as e:
                    logger.debug(f"  New constraint may already exist: {e}")
                
                session.commit()
                logger.info("✅ Markets→Bookies migration completed successfully")
                
        except Exception as e:
            logger.error(f"❌ Markets→Bookies migration failed: {e}")
            logger.error(traceback.format_exc())
    
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
                from models import Market
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
    
    def _get_column_type_sql(self, col) -> str:
        """
        Convert SQLAlchemy column type to SQL type string.
        
        Args:
            col: SQLAlchemy Column object
            
        Returns:
            str: SQL type string (e.g., 'VARCHAR(50)', 'INTEGER', 'NUMERIC(6,2)')
        """
        from sqlalchemy.types import String, Integer, Text, DateTime, Numeric, BigInteger
        
        col_type = col.type
        
        if isinstance(col_type, String):
            if col_type.length:
                return f"VARCHAR({col_type.length})"
            else:
                return "VARCHAR"
        elif isinstance(col_type, Text):
            return "TEXT"
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