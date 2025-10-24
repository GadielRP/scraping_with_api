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
        
        Limitations:
        - Does NOT drop columns (requires manual intervention)
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