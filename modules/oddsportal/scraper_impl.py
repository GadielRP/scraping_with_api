
"""Concrete OddsPortal scraper implementation composed from mixins."""

from .scraper_attempt import OddsPortalAttemptMixin
from .scraper_browser import OddsPortalBrowserMixin
from .scraper_data import OddsPortalDataMixin
from .scraper_hover import OddsPortalHoverMixin
from .scraper_lookup import OddsPortalLookupMixin
from .scraper_page_state import OddsPortalPageStateMixin
from .scraper_render import OddsPortalRenderMixin
from .scraper_resume import OddsPortalResumeMixin


class OddsPortalScraper(
    OddsPortalAttemptMixin,
    OddsPortalBrowserMixin,
    OddsPortalPageStateMixin,
    OddsPortalResumeMixin,
    OddsPortalLookupMixin,
    OddsPortalRenderMixin,
    OddsPortalHoverMixin,
    OddsPortalDataMixin,
):
    pass
