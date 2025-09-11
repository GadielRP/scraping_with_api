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

# Global database manager instance
db_manager = DatabaseManager()
# Note: Removed automatic table dropping - tables are now created only when needed