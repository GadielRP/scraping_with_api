import logging
from typing import Optional, Dict

from infrastructure.persistence.models import OddsPortalLeagueCache
from infrastructure.persistence.database import db_manager
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class OddsPortalCacheRepository:
    """
    Repository for caching OddsPortal league page match URLs.

    Stores all match URLs from a league page so subsequent scrapes
    can skip league page navigation entirely (~14s saved per cache hit).
    """

    @staticmethod
    def save_league_cache(season_id: int, match_urls_dict: Dict) -> bool:
        """
        Save or update cached match URLs for a season_id (today's date).
        Uses upsert: if a row for this season_id already exists, updates it.
        """
        try:
            with db_manager.get_session() as session:
                today = get_local_now().replace(hour=0, minute=0, second=0, microsecond=0)

                existing = session.query(OddsPortalLeagueCache).filter(
                    OddsPortalLeagueCache.season_id == season_id
                ).first()

                if existing:
                    existing.match_urls = match_urls_dict
                    existing.cached_date = today
                    existing.created_at = get_local_now()
                    logger.debug(f"Updated league cache for season {season_id}: {len(match_urls_dict)} URLs")
                else:
                    cache_entry = OddsPortalLeagueCache(
                        season_id=season_id,
                        cached_date=today,
                        match_urls=match_urls_dict
                    )
                    session.add(cache_entry)
                    logger.debug(f"Created league cache for season {season_id}: {len(match_urls_dict)} URLs")

                return True

        except Exception as e:
            logger.error(f"Error saving league cache for season {season_id}: {e}")
            return False

    @staticmethod
    def get_league_cache(season_id: int, valid_days: int = 3) -> Optional[Dict]:
        """
        Get cached match URLs for a season_id, valid for the past valid_days.
        """
        try:
            from datetime import timedelta
            with db_manager.get_session() as session:
                cutoff_date = get_local_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=valid_days - 1)

                cache = session.query(OddsPortalLeagueCache).filter(
                    OddsPortalLeagueCache.season_id == season_id,
                    OddsPortalLeagueCache.cached_date >= cutoff_date
                ).order_by(OddsPortalLeagueCache.cached_date.desc()).first()

                if cache:
                    return cache.match_urls
                return None

        except Exception as e:
            logger.error(f"Error getting league cache for season {season_id}: {e}")
            return None

    @staticmethod
    def cleanup_old_caches(retention_days: int = 3) -> int:
        """
        Delete all cache entries older than retention_days.
        Should be called periodically (e.g. from job_clean_league_cache).
        """
        try:
            from datetime import timedelta
            with db_manager.get_session() as session:
                cutoff_date = get_local_now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=retention_days)

                deleted = session.query(OddsPortalLeagueCache).filter(
                    OddsPortalLeagueCache.cached_date < cutoff_date
                ).delete()

                if deleted > 0:
                    logger.info(f"🧹 Cleaned up {deleted} old OddsPortal league cache entries (older than {retention_days} days)")
                return deleted

        except Exception as e:
            logger.error(f"Error cleaning up old league caches: {e}")
            return 0
