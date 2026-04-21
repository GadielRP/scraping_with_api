from .oddsportal_scraper_core import OddsPortalScraper
from .models import MatchOddsData, ScrapeAttemptResult, GroupSeedResult
from .oddsportal_config import SEASON_ODDSPORTAL_MAP
from .cache_utils import _coerce_current_date, _build_league_group_key, _format_group_key
from .logging_context import _log_prefix

_scraper = None


def get_scaler():
    from oddsportal_scraper import get_scaler as root_get_scaler

    return root_get_scaler()


def scrape_match_odds(match_url: str):
    from oddsportal_scraper import scrape_match_odds as root_scrape_match_odds

    return root_scrape_match_odds(match_url)


def scrape_match_sync(*args, **kwargs):
    from oddsportal_scraper import scrape_match_sync as root_scrape_match_sync

    return root_scrape_match_sync(*args, **kwargs)


def _resolve_task_sport(task):
    from oddsportal_scraper import _resolve_task_sport as root_resolve_task_sport

    return root_resolve_task_sport(task)


def _resolve_task_match_url(*args, **kwargs):
    from oddsportal_scraper import _resolve_task_match_url as root_resolve_task_match_url

    return root_resolve_task_match_url(*args, **kwargs)


def _scrape_task_with_recovery(*args, **kwargs):
    from oddsportal_scraper import _scrape_task_with_recovery as root_scrape_task_with_recovery

    return root_scrape_task_with_recovery(*args, **kwargs)


def _seed_group_cache_only(*args, **kwargs):
    from oddsportal_scraper import _seed_group_cache_only as root_seed_group_cache_only

    return root_seed_group_cache_only(*args, **kwargs)


def _build_dispatch_groups(*args, **kwargs):
    from oddsportal_scraper import _build_dispatch_groups as root_build_dispatch_groups

    return root_build_dispatch_groups(*args, **kwargs)


def _attach_cached_match_urls(*args, **kwargs):
    from oddsportal_scraper import _attach_cached_match_urls as root_attach_cached_match_urls

    return root_attach_cached_match_urls(*args, **kwargs)


def scrape_multiple_matches_sync(*args, **kwargs):
    from oddsportal_scraper import scrape_multiple_matches_sync as root_scrape_multiple_matches_sync

    return root_scrape_multiple_matches_sync(*args, **kwargs)


def scrape_multiple_matches_parallel_sync(*args, **kwargs):
    from oddsportal_scraper import scrape_multiple_matches_parallel_sync as root_scrape_multiple_matches_parallel_sync

    return root_scrape_multiple_matches_parallel_sync(*args, **kwargs)

