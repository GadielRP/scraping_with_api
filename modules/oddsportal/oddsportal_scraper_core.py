class OddsPortalScraper:
    """Lazy wrapper that instantiates the legacy root scraper on demand."""

    def __new__(cls, *args, **kwargs):
        from oddsportal_scraper import OddsPortalScraper as RootOddsPortalScraper

        return RootOddsPortalScraper(*args, **kwargs)

