# Bridge to the new modular repository structure
# All repositories are now located in infrastructure/persistence/repositories/

from infrastructure.persistence.repositories import (
    EventRepository,
    SeasonRepository,
    OddsRepository,
    ResultRepository,
    ObservationRepository,
    MarketRepository,
    OddsPortalCacheRepository,
    DailyDiscoveryRepository,
    # Constants and Utils
    NBA_SEASONS
)

# This maintains backward compatibility for existing imports like:
# from infrastructure.persistence.repositories import EventRepository