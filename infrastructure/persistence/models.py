from sqlalchemy import Column, Integer, String, Numeric, DateTime, BigInteger, Text, CheckConstraint, ForeignKey, UniqueConstraint, Boolean, Index, JSON, SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB
from shared.timezone_utils import get_local_now

Base = declarative_base()


class Participant(Base):
    __tablename__ = 'participants'

    participant_id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(Text, nullable=False)
    source_participant_id = Column(BigInteger, nullable=False)
    name = Column(Text, nullable=False)
    slug = Column(Text)
    short_name = Column(Text)
    code_name = Column(Text)
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    __table_args__ = (
        UniqueConstraint('source', 'source_participant_id', name='unique_participant_source_external_id'),
        Index('idx_participants_name', 'name'),
    )


class Competition(Base):
    __tablename__ = 'competitions'

    competition_id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(Text, nullable=False)
    source_tournament_id = Column(BigInteger, nullable=False)
    source_unique_tournament_id = Column(BigInteger)
    # canonical name is unique tournament name
    canonical_name = Column(Text, nullable=False)
    # display name is tournament name
    display_name = Column(Text, nullable=False)
    # slug is tournament slug
    slug = Column(Text)
    # unique slug is tournament unique slug
    unique_slug = Column(Text)
    
    category_id = Column(BigInteger)
    category_name = Column(Text)
    number_of_teams = Column(Integer)
    total_regular_season_games = Column(Integer)
    standings_grouping = Column(Text)
    league_config_source = Column(Text)
    has_standings_source_endpoint = Column(Boolean, default=True)
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    __table_args__ = (
        UniqueConstraint('source', 'source_tournament_id', name='unique_competition_source_tournament_id'),
        Index('idx_competitions_source_tournament_id', 'source', 'source_tournament_id'),
        Index('idx_competitions_canonical_name', 'canonical_name'),
    )


class Event(Base):
    __tablename__ = 'events'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    custom_id = Column(Text)
    slug = Column(Text, nullable=False)
    start_time_utc = Column(DateTime, nullable=False)
    sport = Column(Text, nullable=False)
    # LEGACY_EVENT_TEXT_FIELDS:
    # Kept for backward compatibility with historical rows and old runtime paths.
    # Do not use as source of truth when normalized Participant/Competition relations exist.
    # Remove only after full DB backfill and downstream migration.
    competition = Column(Text, nullable=False)
    country = Column(Text)
    # LEGACY_EVENT_TEXT_FIELDS:
    # Kept for backward compatibility with historical rows and old runtime paths.
    # Do not use as source of truth when normalized Participant/Competition relations exist.
    # Remove only after full DB backfill and downstream migration.
    home_team = Column(Text, nullable=False)
    # LEGACY_EVENT_TEXT_FIELDS:
    # Kept for backward compatibility with historical rows and old runtime paths.
    # Do not use as source of truth when normalized Participant/Competition relations exist.
    # Remove only after full DB backfill and downstream migration.
    away_team = Column(Text, nullable=False)
    gender = Column(String(10), nullable=False, default="unknown")  # 'Men' or 'Women' or 'Mixed'
    discovery_source = Column(String(50), nullable=False, default='dropping_odds')  # 'dropping_odds', 'high_value_streaks', 'h2h', 'winning_odds', 'team_streaks'
    season_id = Column(Integer, ForeignKey('seasons.id', ondelete='SET NULL'))  # Season ID from SofaScore API (foreign key to seasons table)
    round = Column(Text)  # Round information (e.g., 'regular_season', 'knockouts/playoffs', 'final')
    alert_sent = Column(Boolean, default=False, nullable=False)  # True if 4th quarter alert sent, False otherwise
    home_participant_id = Column(Integer, ForeignKey('participants.participant_id', ondelete='SET NULL'))
    away_participant_id = Column(Integer, ForeignKey('participants.participant_id', ondelete='SET NULL'))
    competition_id = Column(Integer, ForeignKey('competitions.competition_id', ondelete='SET NULL'))

    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)
    
    # Relationships
    result = relationship("Result", back_populates="event", uselist=False, cascade="all, delete-orphan")
    observations = relationship("EventObservation", back_populates="event", cascade="all, delete-orphan")
    prediction_logs = relationship("PredictionLog", back_populates="event", uselist=False, cascade="all, delete-orphan")
    season = relationship("Season", back_populates="events")
    markets = relationship("Market", back_populates="event", cascade="all, delete-orphan")
    home_participant = relationship("Participant", foreign_keys=[home_participant_id])
    away_participant = relationship("Participant", foreign_keys=[away_participant_id])
    competition_ref = relationship("Competition", foreign_keys=[competition_id])
    source_mappings = relationship("EventSourceMapping", back_populates="event", cascade="all, delete-orphan")


class EventSourceMapping(Base):
    __tablename__ = 'event_source_mappings'

    mapping_id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(
        Integer,
        ForeignKey('events.id', ondelete='CASCADE', name='fk_event_source_mappings_event_id'),
        nullable=False,
    )
    source = Column(Text, nullable=False)
    source_event_id = Column(Text, nullable=False)
    source_sport_id = Column(Text)
    source_tournament_id = Column(Text)
    source_season_id = Column(Text)
    participant_home_id = Column(
        Integer,
        ForeignKey(
            'participants.participant_id',
            ondelete='SET NULL',
            name='fk_event_source_mappings_participant_home_id',
        ),
    )
    participant_away_id = Column(
        Integer,
        ForeignKey(
            'participants.participant_id',
            ondelete='SET NULL',
            name='fk_event_source_mappings_participant_away_id',
        ),
    )
    has_odds = Column(Boolean, nullable=False, default=True, server_default=text("true"))
    match_method = Column(Text, nullable=False, default='direct')
    confidence = Column(Numeric(5, 3))
    raw_external_providers = Column(JSONB().with_variant(JSON(), 'sqlite'))
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    event = relationship("Event", back_populates="source_mappings")
    participant_home = relationship("Participant", foreign_keys=[participant_home_id])
    participant_away = relationship("Participant", foreign_keys=[participant_away_id])

    __table_args__ = (
        UniqueConstraint('source', 'source_event_id', name='unique_event_source_mapping'),
        Index('idx_event_source_mappings_event_id', 'event_id'),
        Index('idx_event_source_mappings_source', 'source'),
        Index('idx_event_source_mappings_participant_home_id', 'participant_home_id'),
        Index('idx_event_source_mappings_participant_away_id', 'participant_away_id'),
    )


class EventSourceResolutionQueue(Base):
    __tablename__ = 'event_source_resolution_queue'

    queue_id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(Text, nullable=False)
    source_event_id = Column(Text, nullable=False)
    resolution_status = Column(Text, nullable=False)
    best_candidate_event_id = Column(
        Integer,
        ForeignKey('events.id', ondelete='SET NULL'),
        nullable=True,
    )
    best_candidate_confidence = Column(Numeric(5, 3), nullable=True)
    second_candidate_event_id = Column(
        Integer,
        ForeignKey('events.id', ondelete='SET NULL'),
        nullable=True,
    )
    second_candidate_confidence = Column(Numeric(5, 3), nullable=True)
    score_gap = Column(Numeric(5, 3), nullable=True)
    source_sport_id = Column(Text, nullable=True)
    source_sport_name = Column(Text, nullable=True)
    normalized_sport = Column(Text, nullable=True)
    source_tournament_id = Column(Text, nullable=True)
    source_tournament_name = Column(Text, nullable=True)
    source_tournament_slug = Column(Text, nullable=True)
    source_category_name = Column(Text, nullable=True)
    source_category_slug = Column(Text, nullable=True)
    source_season_id = Column(Text, nullable=True)
    participant1_id = Column(Text, nullable=True)
    participant1_name = Column(Text, nullable=True)
    participant1_short_name = Column(Text, nullable=True)
    participant1_abbr = Column(Text, nullable=True)
    participant2_id = Column(Text, nullable=True)
    participant2_name = Column(Text, nullable=True)
    participant2_short_name = Column(Text, nullable=True)
    participant2_abbr = Column(Text, nullable=True)
    source_start_time_utc = Column(DateTime, nullable=True)
    raw_external_providers = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=True)
    raw_payload = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=True)
    candidate_scores = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=True)
    attempt_count = Column(Integer, nullable=False, default=1)
    first_seen_at = Column(DateTime, default=get_local_now)
    last_attempted_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    __table_args__ = (
        UniqueConstraint(
            'source',
            'source_event_id',
            name='unique_event_source_resolution_queue_source_event',
        ),
        Index('idx_event_source_resolution_queue_status', 'source', 'resolution_status'),
        Index('idx_event_source_resolution_queue_start_time', 'source_start_time_utc'),
        Index('idx_event_source_resolution_queue_best_candidate', 'best_candidate_event_id'),
    )

class Season(Base):
    __tablename__ = 'seasons'
    
    id = Column(Integer, primary_key=True)  # Season ID from SofaScore API
    name = Column(String(100))  # Season name (e.g., "NBA 24/25", "Erovnuli Liga 2025")
    year = Column(Integer)  # Season year (e.g., 2025, 2024)
    sport = Column(String(50))  # Sport name (e.g., "Basketball", "Football", "Ice hockey")
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('id', name='unique_season_id'),
    )
    
    # Relationships
    events = relationship("Event", back_populates="season")

class Result(Base):
    __tablename__ = 'results'
    
    event_id = Column(Integer, ForeignKey('events.id', ondelete='CASCADE'), primary_key=True)
    home_score = Column(Integer)
    away_score = Column(Integer)
    winner = Column(Text)  # '1' | 'X' | '2' or NULL
    home_sets = Column(Text)  # Sets string for home team (e.g., '23-23-31-24' for basketball, '2-0-1+4' for football with penalties)
    away_sets = Column(Text)  # Sets string for away team (e.g., '19-35-24-31' for basketball, '0-2' for football)
    
    # Relationships
    event = relationship("Event", back_populates="result")

class EventObservation(Base):
    __tablename__ = 'event_observations'
    
    observation_id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey('events.id', ondelete='CASCADE'), nullable=False)
    observation_type = Column(String(50), nullable=False)  # 'ground_type', 'weather', etc.
    observation_value = Column(Text)  # Flexible value storage
    sport = Column(String(50))  # For quick filtering
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('event_id', 'observation_type', name='unique_event_observation_type'),
    )
    
    # Relationships
    event = relationship("Event", back_populates="observations")

    def __repr__(self):
        return f"<EventObservation(event_id={self.event_id}, type='{self.observation_type}', value='{self.observation_value}')>"


class Bookie(Base):
    """
    Stores bookmaker/sportsbook information.
    
    Each bookie can have odds for multiple events through the Market table.
    """
    __tablename__ = 'bookies'
    
    bookie_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text, nullable=False, unique=True)   # "Bet365", "1xBet", "Pinnacle", "SofaScore"
    slug = Column(Text, nullable=False, unique=True)   # "bet365", "1xbet", "pinnacle", "sofascore"
    
    # Relationships
    markets = relationship("Market", back_populates="bookie")
    source_mappings = relationship(
        "BookieSourceMapping",
        back_populates="bookie",
        cascade="all, delete-orphan",
    )
    
    def __repr__(self):
        return f"<Bookie(bookie_id={self.bookie_id}, name='{self.name}')>"


class BookieSourceMapping(Base):
    __tablename__ = "bookie_source_mappings"

    mapping_id = Column(Integer, primary_key=True, autoincrement=True)
    bookie_id = Column(
        Integer,
        ForeignKey("bookies.bookie_id", ondelete="CASCADE"),
        nullable=False,
    )
    source = Column(Text, nullable=False)
    source_bookie_name = Column(Text, nullable=False)
    source_bookie_slug = Column(Text, nullable=False)
    match_method = Column(Text, nullable=False, default="direct")
    confidence = Column(Numeric(5, 3), nullable=True)
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    bookie = relationship("Bookie", back_populates="source_mappings")

    __table_args__ = (
        UniqueConstraint("source", "source_bookie_slug", name="unique_bookie_source_slug"),
        Index("idx_bookie_source_mappings_bookie_id", "bookie_id"),
        Index("idx_bookie_source_mappings_source_slug", "source", "source_bookie_slug"),
        Index("idx_bookie_source_mappings_source_name", "source", "source_bookie_name"),
    )


class CanonicalMarketType(Base):
    __tablename__ = "canonical_market_types"

    canonical_market_key = Column(Text, primary_key=True)
    canonical_market_name = Column(Text, nullable=False)
    canonical_market_group = Column(Text, nullable=False)
    canonical_market_period = Column(Text, nullable=False)
    market_family = Column(Text, nullable=False)
    requires_choice_group = Column(Boolean, nullable=False, default=False)
    enabled_for_ingestion = Column(Boolean, nullable=False, default=True)
    enabled_for_trajectory = Column(Boolean, nullable=False, default=False)
    display_order = Column(Integer)
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    source_mappings = relationship(
        "MarketSourceMapping",
        back_populates="canonical_market_type",
    )

    __table_args__ = (
        Index(
            "idx_canonical_market_types_group_period",
            "canonical_market_group",
            "canonical_market_period",
        ),
        Index(
            "idx_canonical_market_types_enabled",
            "enabled_for_ingestion",
            "enabled_for_trajectory",
        ),
    )


class MarketSourceMapping(Base):
    __tablename__ = "market_source_mappings"

    mapping_id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_market_key = Column(
        Text,
        ForeignKey("canonical_market_types.canonical_market_key", ondelete="RESTRICT"),
        nullable=False,
    )
    source = Column(Text, nullable=False)
    source_sport_id = Column(Text, nullable=True)
    source_market_id = Column(Text, nullable=False)
    source_market_name = Column(Text, nullable=False)
    source_market_group = Column(Text)
    source_period = Column(Text)
    source_handicap = Column(Text)
    player_prop = Column(Boolean)
    canonical_market_name = Column(Text, nullable=False)
    canonical_market_group = Column(Text, nullable=False)
    canonical_market_period = Column(Text, nullable=False)
    match_method = Column(Text, nullable=False, default="catalog_rule")
    confidence = Column(Numeric(5, 3))
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    canonical_market_type = relationship(
        "CanonicalMarketType",
        back_populates="source_mappings",
    )
    outcome_mappings = relationship(
        "MarketOutcomeSourceMapping",
        back_populates="market_source_mapping",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint(
            "source",
            "source_sport_id",
            "source_market_id",
            name="unique_market_source_mapping",
        ),
        Index("idx_market_source_mappings_canonical_key", "canonical_market_key"),
        Index(
            "idx_market_source_mappings_source_market",
            "source",
            "source_sport_id",
            "source_market_id",
        ),
        Index(
            "idx_market_source_mappings_group_period",
            "canonical_market_group",
            "canonical_market_period",
        ),
    )


class MarketOutcomeSourceMapping(Base):
    __tablename__ = "market_outcome_source_mappings"

    outcome_mapping_id = Column(Integer, primary_key=True, autoincrement=True)
    market_source_mapping_id = Column(
        Integer,
        ForeignKey("market_source_mappings.mapping_id", ondelete="CASCADE"),
        nullable=False,
    )
    source_outcome_id = Column(Text, nullable=False)
    source_outcome_name = Column(Text, nullable=False)
    canonical_choice_name = Column(Text, nullable=False)
    display_order = Column(Integer)
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    market_source_mapping = relationship(
        "MarketSourceMapping",
        back_populates="outcome_mappings",
    )

    __table_args__ = (
        UniqueConstraint(
            "market_source_mapping_id",
            "source_outcome_id",
            name="unique_market_outcome_source_mapping",
        ),
        Index("idx_market_outcome_source_mappings_market", "market_source_mapping_id"),
        Index("idx_market_outcome_source_mappings_choice", "canonical_choice_name"),
    )


class SourceCatalogSync(Base):
    __tablename__ = "source_catalog_syncs"

    sync_id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(Text, nullable=False)
    catalog_type = Column(Text, nullable=False)
    language = Column(Text)
    file_path = Column(Text, nullable=False)
    payload_hash = Column(Text, nullable=False)
    item_count = Column(Integer, nullable=False)
    imported_at = Column(DateTime, default=get_local_now)
    created_at = Column(DateTime, default=get_local_now)

    __table_args__ = (
        Index("idx_source_catalog_syncs_source_type", "source", "catalog_type"),
        Index("idx_source_catalog_syncs_hash", "payload_hash"),
    )


class Market(Base):
    """
    Stores individual betting markets for an event.
    
    Each event can have multiple canonical markets (1X2 Full Time, Over/Under Full Time, Asian Handicap Full Time, etc.)
    Each market belongs to a specific bookie.
    Each market has multiple choices stored in MarketChoice table.
    """
    __tablename__ = 'markets'
    
    # Primary Key
    market_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Keys
    event_id = Column(Integer, ForeignKey('events.id', ondelete='CASCADE'), nullable=False)
    bookie_id = Column(Integer, ForeignKey('bookies.bookie_id', ondelete='CASCADE'), nullable=False)
    
    # Market description
    market_name = Column(Text, nullable=False)  # "Home/Away Full Time", "Over/Under Full Time"
    market_group = Column(Text)  # "1X2", "Home/Away", "Over/Under", "Asian Handicap"
    market_period = Column(Text, nullable=False, default="Full Time")  # "Full Time", "1st Half"
    choice_group = Column(Text)  # For Over/Under: "2.5", "3.5", etc. NULL for non-line markets
    is_live = Column(Boolean, default=False, nullable=False)
    
    # Timestamps
    collected_at = Column(DateTime, default=get_local_now, nullable=False)
    
    # Constraints
    __table_args__ = (
        # Each bookie can have one market per event+name+line+live-status combination
        UniqueConstraint('event_id', 'bookie_id', 'market_name', 'market_period', 'choice_group', 'is_live', name='unique_market_per_event_bookie'),
        Index('idx_markets_event_bookie_live_name_period', 'event_id', 'bookie_id', 'is_live', 'market_name', 'market_period'),
        Index('idx_markets_event_bookie_live_group_period', 'event_id', 'bookie_id', 'is_live', 'market_group', 'market_period'),
    )
    
    # Relationships
    event = relationship("Event", back_populates="markets")
    bookie = relationship("Bookie", back_populates="markets")
    choices = relationship("MarketChoice", back_populates="market", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Market(market_id={self.market_id}, name='{self.market_name}', choice_group='{self.choice_group}')>"


class MarketChoice(Base):
    """
    Stores individual odds choices for a market.
    
    Each market has canonical choices (e.g., "1", "x", "2" or "over", "under").
    """
    __tablename__ = 'market_choices'
    
    # Primary Key
    choice_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Key: links to the parent market
    market_id = Column(Integer, ForeignKey('markets.market_id', ondelete='CASCADE'), nullable=False)
    
    # Choice identification
    choice_name = Column(Text, nullable=False)  # "1", "x", "2", "over", "under", "yes", "no", "no_goal"
    
    # Odds values (stored as decimals for easy math)
    # FROZEN as of docs/refactors/db-schema-odds-refactor.md §3.2:
    # save_canonical_bookmaker_batches (the live ingestion path for all 3
    # providers) no longer writes these - MarketChoiceQuote is now the sole
    # persistence target for price state. These columns keep whatever value
    # they had before that change (stale for old rows, always NULL/0 for
    # choices created after it) purely so non-migrated readers (odds_alert.py,
    # odds_trajectory_context.py, drift_engine.py, dual-process view) don't
    # crash on a missing column while Fase 5 migrates them to read
    # MarketChoiceQuote instead. Only `save_markets_from_response_with_stats`
    # (LEGACY_MAINTENANCE_ONLY, scripts only) still writes them. Dropped
    # entirely in Fase 7 once Fase 5 is done.
    initial_odds = Column(Numeric(8, 3))  # Opening odds (decimal, e.g., 1.53)
    current_odds = Column(Numeric(8, 3))  # Current/final odds (decimal, e.g., 1.48)
    
    # Movement indicator: -1 = odds dropped, 0 = unchanged, +1 = odds increased
    change = Column(Integer, default=0)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('market_id', 'choice_name', name='unique_choice_per_market'),
        Index('idx_market_choices_market_choice_name', 'market_id', 'choice_name'),
    )
    
    # Relationships
    market = relationship("Market", back_populates="choices")
    snapshots = relationship("MarketChoiceSnapshot", back_populates="choice", cascade="all, delete-orphan")
    quotes = relationship("MarketChoiceQuote", back_populates="choice", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<MarketChoice(choice_id={self.choice_id}, name='{self.choice_name}', initial={self.initial_odds}, current={self.current_odds})>"


class MarketChoiceSnapshot(Base):
    """
    Append-only snapshots of market choice odds at specific points in time.
    Used for historical graph generation.
    """
    __tablename__ = 'market_choice_snapshots'
    
    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    choice_id = Column(Integer, ForeignKey('market_choices.choice_id', ondelete='CASCADE'), nullable=False)
    quote_id = Column(Integer, ForeignKey('market_choice_quotes.quote_id', ondelete='CASCADE'))
    odds_value = Column(Numeric(8, 3), nullable=False)
    collected_at = Column(DateTime, default=get_local_now, nullable=False)
    source = Column(Text)
    source_collected_at = Column(DateTime)
    source_market_id = Column(Text)
    source_outcome_id = Column(Text)
    bookmaker_outcome_id = Column(Text)
    main_line = Column(Boolean)
    source_limit = Column(Numeric(12, 3))
    exchange_side = Column(Text)
    exchange_level = Column(Integer)
    exchange_size = Column(Numeric(18, 3))
    
    # Constraints & Indexes
    __table_args__ = (
        Index('idx_choice_collected', 'choice_id', 'collected_at'),
        Index(
            'idx_market_choice_snapshots_quote_collected',
            quote_id,
            collected_at.desc(),
            snapshot_id.desc(),
        ),
        Index('idx_market_choice_snapshots_source', 'source'),
        Index('idx_market_choice_snapshots_source_collected', 'source', 'source_collected_at'),
        Index('idx_market_choice_snapshots_source_market', 'source', 'source_market_id'),
    )
    
    # Relationships
    choice = relationship("MarketChoice", back_populates="snapshots")
    quote = relationship("MarketChoiceQuote", back_populates="snapshots")


class MarketChoiceQuote(Base):
    """
    Current-state price instrument for one canonical choice, scoped by source
    and exchange side/level (e.g. Betfair back vs lay from OddsPortal vs Oddspapi).

    This is the "current state" counterpart to MarketChoiceSnapshot's pure
    append-only history: exactly one row per (choice, source, exchange_side,
    exchange_level), updated in place as new prices arrive, independently for
    initial_odds and current_odds so partial arrival (initial-only, current-only,
    or either one first) never requires special-case handling.

    See docs/refactors/db-schema-odds-refactor.md for the full design rationale.
    """
    __tablename__ = 'market_choice_quotes'

    quote_id = Column(Integer, primary_key=True, autoincrement=True)
    choice_id = Column(Integer, ForeignKey('market_choices.choice_id', ondelete='CASCADE'), nullable=False)

    # Identity of the price instrument. exchange_side is NULL for non-exchange
    # bookies (single price, no back/lay split) - same NULL-for-"not
    # applicable" convention as Market.choice_group, instead of a 'single'
    # sentinel string.
    #
    # Postgres/SQLite both treat NULL != NULL in UNIQUE constraints, so the
    # plain UniqueConstraint below does NOT by itself reject two NULL-side
    # rows for the same (choice_id, source, exchange_level). Real enforcement
    # is the functional index
    # unique_market_choice_quote_side_null_safe (choice_id, source,
    # COALESCE(exchange_side, ''), exchange_level), created in
    # database.py::_migrate_market_choice_quotes. It has a different name so
    # it doesn't collide with (and get skipped by "IF NOT EXISTS" behind) the
    # plain constraint's own auto-created index - mirrors how Market keeps
    # unique_market_per_event_bookie alongside the functional
    # unique_market_per_event_bookie_period_line.
    source = Column(Text, nullable=False)
    exchange_side = Column(Text)
    exchange_level = Column(SmallInteger, nullable=False, default=0, server_default=text("0"))

    # Metadata of this instrument (previously only available on snapshots)
    main_line = Column(Boolean)
    source_market_id = Column(Text)
    source_outcome_id = Column(Text)
    bookmaker_outcome_id = Column(Text)
    source_limit = Column(Numeric(12, 3))

    # Current state (independently nullable by design, see class docstring)
    initial_odds = Column(Numeric(8, 3))
    initial_captured_at = Column(DateTime)
    current_odds = Column(Numeric(8, 3))
    current_updated_at = Column(DateTime)
    movement = Column(SmallInteger, default=0)  # -1 = dropped, 0 = unchanged, +1 = increased

    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)

    __table_args__ = (
        # NOTE: incomplete on its own for NULL exchange_side (see comment on
        # the column above) - kept for ORM/introspection parity with the rest
        # of the schema. Real protection is the functional index created in
        # database.py::_migrate_market_choice_quotes.
        UniqueConstraint(
            'choice_id', 'source', 'exchange_side', 'exchange_level',
            name='unique_market_choice_quote',
        ),
        Index('idx_market_choice_quotes_choice', 'choice_id'),
        Index('idx_market_choice_quotes_source', 'source'),
    )

    # Relationships
    choice = relationship("MarketChoice", back_populates="quotes")
    snapshots = relationship("MarketChoiceSnapshot", back_populates="quote")

    def __repr__(self):
        return (
            f"<MarketChoiceQuote(quote_id={self.quote_id}, choice_id={self.choice_id}, "
            f"source='{self.source}', exchange_side='{self.exchange_side}', "
            f"initial={self.initial_odds}, current={self.current_odds})>"
        )


class PredictionLog(Base):
    __tablename__ = 'prediction_logs'
    
    event_id = Column(Integer, ForeignKey('events.id', ondelete='CASCADE'), primary_key=True)
    sport = Column(String(50))
    participants = Column(Text)
    competition = Column(String(100))
    prediction_type = Column(String(20), nullable=False, default='process1')
    confidence_level = Column(String(20))  # 'high', 'medium', 'low', '100.0%'
    prediction_winner = Column(String(10))  # '1', 'X', '2'
    prediction_point_diff = Column(Integer)
    tier1_count = Column(Integer, default=0)  # Number of Tier 1 activations
    tier2_count = Column(Integer, default=0)  # Number of Tier 2 activations
    # Fields for actual results (initially NULL)
    actual_result = Column(Text)
    actual_winner = Column(String(10))  # '1', 'X', '2'
    actual_point_diff = Column(Integer)
    status = Column(String(20), default='pending')  # 'pending', 'completed', 'cancelled'
    
    # Relationships
    event = relationship("Event", back_populates="prediction_logs")

    def __repr__(self):
        return f"<PredictionLog(event_id={self.event_id}, prediction_type='{self.prediction_type}', status='{self.status}')>"


class OddsPortalLeagueCache(Base):
    """
    Caches match URLs scraped from OddsPortal league pages.
    
    One row per season_id — cleaned daily so only today's data is stored.
    The match_urls JSONB maps relative URL paths to row display text for
    offline team matching without browser navigation.
    """
    __tablename__ = 'oddsportal_league_cache'
    
    season_id = Column(Integer, primary_key=True)                  # e.g. 80229 (NBA)
    cached_date = Column(DateTime, nullable=False)                  # Date the cache was populated
    match_urls = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=False)                      # { "/basketball/usa/nba/team-a-team-b-xYZ/": "Team A - Team B" }
    created_at = Column(DateTime, default=get_local_now)


class DailyDiscoveryLog(Base):
    """
    Tracks the success/failure of the daily discovery job for each sport and date.
    Used by the retry queue to guarantee that missed events (e.g. from proxy failures)
    are retried throughout the day until successful.
    """
    __tablename__ = 'daily_discovery_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(10), nullable=False)  # 'YYYY-MM-DD'
    run_slot = Column(String(20), nullable=False, default='AM')
    sport = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False, default='pending')  # 'pending', 'completed', 'failed'
    attempts = Column(Integer, default=0)
    last_attempt_at = Column(DateTime)
    created_at = Column(DateTime, default=get_local_now)
    
    __table_args__ = (
        UniqueConstraint('date', 'run_slot', 'sport', name='unique_date_slot_sport_discovery'),
        Index('idx_daily_discovery_log_date_slot_status', 'date', 'run_slot', 'status'),
        Index('idx_daily_discovery_log_date_slot_sport', 'date', 'run_slot', 'sport'),
    )


class OddspapiFixtureDiscoveryRun(Base):
    """Durable execution marker used to recover missed daily discovery slots."""

    __tablename__ = 'oddspapi_fixture_discovery_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    target_date = Column(String(10), nullable=False)
    sport_scope = Column(String(255), nullable=False, default='all', server_default='all')
    scheduled_local_date = Column(String(10), nullable=False)
    scheduled_time = Column(String(5), nullable=False)
    trigger = Column(String(20), nullable=False, default='scheduled')
    status = Column(String(20), nullable=False, default='running')
    process_id = Column(Integer)
    started_at = Column(DateTime, nullable=False, default=get_local_now)
    heartbeat_at = Column(DateTime, nullable=False, default=get_local_now)
    finished_at = Column(DateTime)
    summary = Column(JSONB().with_variant(JSON(), 'sqlite'))
    error = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'target_date',
            'sport_scope',
            name='unique_oddspapi_fixture_discovery_target_scope',
        ),
        Index(
            'idx_oddspapi_fixture_discovery_runs_status_target_scope',
            'status',
            'target_date',
            'sport_scope',
        ),
    )


class OddspapiMainlineOutcomeCache(Base):
    """Cached mainLine=true outcomes from OddsPapi /odds for live enrichment."""

    __tablename__ = 'oddspapi_mainline_outcome_cache'

    cache_id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(
        Integer,
        ForeignKey('events.id', ondelete='CASCADE', name='fk_oddspapi_mainline_cache_event_id'),
        nullable=False,
    )
    fixture_id = Column(Text, nullable=False)
    source_sport_id = Column(Text)
    bookmaker_slug = Column(Text, nullable=False)
    source_market_id = Column(Text, nullable=False)
    source_outcome_id = Column(Text, nullable=False)
    canonical_market_key = Column(Text)
    is_exchange = Column(Boolean, nullable=False, default=False, server_default=text("false"))
    captured_at = Column(DateTime, default=get_local_now, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            'event_id',
            'bookmaker_slug',
            'source_market_id',
            'source_outcome_id',
            name='unique_oddspapi_mainline_outcome_cache',
        ),
        Index('idx_oddspapi_mainline_cache_event_id', 'event_id'),
        Index('idx_oddspapi_mainline_cache_is_exchange', 'is_exchange'),
    )


# ---------------------------------------------------------------------------
# SQL view helper – unified odds view (no filtering by var_one)
# ---------------------------------------------------------------------------

def _sql_string_list(values):
    cleaned = []
    for value in values:
        if value is None:
            continue
        escaped = str(value).replace("'", "''")
        cleaned.append(f"'{escaped}'")
    return ", ".join(cleaned) if cleaned else "''"


def build_dual_process_event_odds_view_sql(
    markets,
    periods,
    *,
    view_name: str = "v_dual_process_event_odds",
    quote_aware: bool = False,
) -> str:
    if view_name not in {
        "v_dual_process_event_odds",
        "v_dual_process_event_odds_legacy",
        "v_dual_process_event_odds_quotes",
    }:
        raise ValueError(f"Unsupported dual-process view name: {view_name}")
    market_values = _sql_string_list(markets)
    period_values = _sql_string_list(periods)
    if quote_aware:
        initial_expression = "mcq.initial_odds"
        current_expression = "COALESCE(latest.odds_value, mcq.current_odds)"
        quote_join = """
        JOIN LATERAL (
            SELECT quote_candidate.*
            FROM market_choice_quotes quote_candidate
            WHERE quote_candidate.choice_id = mc.choice_id
              AND quote_candidate.source = 'sofascore'
              AND quote_candidate.exchange_side IS NULL
              AND quote_candidate.exchange_level = 0
            ORDER BY quote_candidate.quote_id
            LIMIT 1
        ) mcq ON TRUE
        """
        latest_filter = "mcs.quote_id = mcq.quote_id"
        sync_expression = """
        COALESCE(
            latest.collected_at,
            mcq.current_updated_at,
            mcq.initial_captured_at,
            m.collected_at
        )
        """
    else:
        initial_expression = "mc.initial_odds"
        current_expression = "COALESCE(latest.odds_value, mc.current_odds)"
        quote_join = ""
        latest_filter = "mcs.choice_id = mc.choice_id"
        sync_expression = "COALESCE(latest.collected_at, m.collected_at)"
    return f"""
    CREATE OR REPLACE VIEW {view_name} AS
    WITH choice_values AS (
        SELECT
            m.event_id,
            m.market_id,
            m.market_name,
            m.market_group,
            m.market_period,
            m.bookie_id,
            m.collected_at,
            mc.choice_name,
            {initial_expression} AS initial_odds,
            {current_expression} AS current_odds,
            {sync_expression} AS latest_snapshot_at
        FROM markets m
        JOIN market_choices mc ON mc.market_id = m.market_id
        {quote_join}
        LEFT JOIN LATERAL (
            SELECT mcs.odds_value, mcs.collected_at
            FROM market_choice_snapshots mcs
            WHERE {latest_filter}
            ORDER BY mcs.collected_at DESC, mcs.snapshot_id DESC
            LIMIT 1
        ) latest ON TRUE
        WHERE m.bookie_id = 1
          AND m.is_live = false
          AND (
              m.market_name IN ({market_values})
              OR m.market_group IN ({market_values})
          )
          AND m.market_period IN ({period_values})
          AND mc.choice_name IN ('1', 'x', '2')
    ),
    pivoted AS (
        SELECT
            event_id,
            market_id,
            market_name,
            market_group,
            market_period,
            bookie_id,
            collected_at,
            MAX(CASE WHEN choice_name = '1' THEN initial_odds END) AS one_open,
            MAX(CASE WHEN choice_name = '1' THEN current_odds END) AS one_final,
            MAX(CASE WHEN choice_name = 'x' THEN initial_odds END) AS x_open,
            MAX(CASE WHEN choice_name = 'x' THEN current_odds END) AS x_final,
            MAX(CASE WHEN choice_name = '2' THEN initial_odds END) AS two_open,
            MAX(CASE WHEN choice_name = '2' THEN current_odds END) AS two_final,
            MAX(latest_snapshot_at) AS last_sync_at
        FROM choice_values
        GROUP BY event_id, market_id, market_name, market_group, market_period, bookie_id, collected_at
    ),
    valid_markets AS (
        SELECT
            *,
            (one_final - one_open)::numeric(8,3) AS var_one,
            CASE
                WHEN x_open IS NOT NULL AND x_final IS NOT NULL
                THEN (x_final - x_open)::numeric(8,3)
                ELSE NULL
            END AS var_x,
            (two_final - two_open)::numeric(8,3) AS var_two,
            (x_open IS NOT NULL AND x_final IS NOT NULL) AS var_shape,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY collected_at DESC, market_id DESC
            ) AS rn
        FROM pivoted
        WHERE one_open IS NOT NULL
          AND one_final IS NOT NULL
          AND two_open IS NOT NULL
          AND two_final IS NOT NULL
    )
    SELECT
        event_id,
        market_id,
        market_name,
        market_group,
        market_period,
        bookie_id,
        collected_at,
        one_open,
        one_final,
        x_open,
        x_final,
        two_open,
        two_final,
        var_one,
        var_x,
        var_two,
        var_shape,
        COALESCE(last_sync_at, collected_at) AS last_sync_at
    FROM valid_markets
    WHERE rn = 1;
    """


EVENT_ALL_ODDS_VIEW_SQL = (
    """
    CREATE OR REPLACE VIEW event_all_odds AS
    SELECT
        e.start_time_utc AS start_time_utc,
        (hp.name || ' / ' || ap.name) AS participants,
        eo.one_open::numeric(6,2) AS odds1a,
        eo.one_final::numeric(6,2) AS odds1b,
        eo.x_open::numeric(6,2) AS momEa,
        eo.x_final::numeric(6,2) AS momeEb,
        eo.two_open::numeric(6,2) AS odds2a,
        eo.two_final::numeric(6,2) AS odds2b,
        eo.var_one::numeric(6,2) AS var_1,
        eo.var_x::numeric(6,2) AS var_x,
        eo.var_two::numeric(6,2) AS var_2,
        CASE
            WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL
            THEN (r.home_score::text || ' - ' || r.away_score::text)
            ELSE NULL
        END AS result,
        c.display_name AS competition,
        e.sport AS sport
    FROM v_dual_process_event_odds eo
    JOIN events e ON e.id = eo.event_id
    JOIN participants hp ON hp.participant_id = e.home_participant_id
    JOIN participants ap ON ap.participant_id = e.away_participant_id
    JOIN competitions c ON c.competition_id = e.competition_id
    LEFT JOIN results r ON r.event_id = eo.event_id
    """
)

# ---------------------------------------------------------------------------
# Basketball results view - parses quarter scores from sets strings
# ---------------------------------------------------------------------------

BASKETBALL_RESULTS_VIEW_SQL = (
    """
    CREATE OR REPLACE VIEW basketball_results AS
    SELECT
        e.id AS event_id,
        hp.name AS home_team,
        ap.name AS away_team,
        e.round,
        e.season_id,
        e.start_time_utc AS start_time,
        r.home_score,
        r.away_score,
        r.winner,
        -- Parse home_sets string (format: '23-23-31-24' or '23-23-31-24-(16)' for overtime)
        -- Remove overtime (parentheses) and penalties (plus signs) before parsing
        CASE 
            WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 1) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 1)::INTEGER
            ELSE NULL
        END AS quarter_1_home,
        CASE 
            WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 2) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 2)::INTEGER
            ELSE NULL
        END AS quarter_2_home,
        CASE 
            WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 3) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 3)::INTEGER
            ELSE NULL
        END AS quarter_3_home,
        CASE 
            WHEN r.home_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 4) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.home_sets, '\\(.*', ''), '-', 4)::INTEGER
            ELSE NULL
        END AS quarter_4_home,
        -- Extract overtime score from home_sets (format: '23-23-31-24-(16)' where (16) is OT)
        CASE 
            WHEN r.home_sets IS NOT NULL AND r.home_sets ~ '\\([0-9]+'
            THEN (regexp_match(r.home_sets, '\\(([0-9]+)'))[1]::INTEGER
            ELSE NULL
        END AS ot_home,
        -- Parse away_sets string (format: '19-35-24-31' or '19-35-24-31-(16)' for overtime)
        CASE 
            WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 1) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 1)::INTEGER
            ELSE NULL
        END AS quarter_1_away,
        CASE 
            WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 2) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 2)::INTEGER
            ELSE NULL
        END AS quarter_2_away,
        CASE 
            WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 3) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 3)::INTEGER
            ELSE NULL
        END AS quarter_3_away,
        CASE 
            WHEN r.away_sets IS NOT NULL AND split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 4) ~ '^[0-9]+$'
            THEN split_part(REGEXP_REPLACE(r.away_sets, '\\(.*', ''), '-', 4)::INTEGER
            ELSE NULL
        END AS quarter_4_away,
        -- Extract overtime score from away_sets (format: '19-35-24-31-(16)' where (16) is OT)
        CASE 
            WHEN r.away_sets IS NOT NULL AND r.away_sets ~ '\\([0-9]+'
            THEN (regexp_match(r.away_sets, '\\(([0-9]+)'))[1]::INTEGER
            ELSE NULL
        END AS ot_away
    FROM events e
    JOIN results r ON r.event_id = e.id
    JOIN participants hp ON hp.participant_id = e.home_participant_id
    JOIN participants ap ON ap.participant_id = e.away_participant_id
    WHERE e.sport = 'Basketball'
      AND r.home_sets IS NOT NULL
      AND r.away_sets IS NOT NULL
    """
)

# ---------------------------------------------------------------------------
# Materialized view for fast alert processing
# ---------------------------------------------------------------------------

MV_ALERT_EVENTS_SQL = (
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_alert_events AS
    SELECT
        e.id AS event_id,
        e.sport,
        e.gender,
        e.discovery_source,
        e.start_time_utc,
        (hp.name || ' vs ' || ap.name) AS participants,
        hp.name AS home_team,
        ap.name AS away_team,
        c.display_name AS competition,
        eo.one_open,
        eo.one_final,
        eo.x_open,
        eo.x_final,
        eo.two_open,
        eo.two_final,
        eo.var_one,
        eo.var_x,
        eo.var_two,
        -- Computed fields for matching
        eo.var_shape,
        (COALESCE(eo.var_one, 0) + COALESCE(eo.var_x, 0) + COALESCE(eo.var_two, 0)) AS var_total,
        ROUND((COALESCE(eo.var_one, 0) + COALESCE(eo.var_x, 0) + COALESCE(eo.var_two, 0))::numeric, 2) AS var_total_rounded,
        -- Result fields
        r.home_score,
        r.away_score,
        r.winner AS winner_side,  -- '1', 'X', '2' or NULL
        CASE
            WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL
            THEN ABS(r.home_score - r.away_score)
            ELSE NULL
        END AS point_diff,
        CASE
            WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL
            THEN (r.home_score::text || '-' || r.away_score::text)
            ELSE NULL
        END AS result_text
    FROM v_dual_process_event_odds eo
    JOIN events e ON e.id = eo.event_id
    JOIN participants hp ON hp.participant_id = e.home_participant_id
    JOIN participants ap ON ap.participant_id = e.away_participant_id
    JOIN competitions c ON c.competition_id = e.competition_id
    LEFT JOIN results r ON r.event_id = eo.event_id
    WHERE r.home_score IS NOT NULL AND r.away_score IS NOT NULL  -- Only finished events
    """
)

# Indexes for fast alert queries
MV_ALERT_EVENTS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_sport_shape_total ON mv_alert_events (sport, var_shape, var_total);",
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_sport_winner_diff ON mv_alert_events (sport, winner_side, point_diff);",
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_start_time ON mv_alert_events (start_time_utc);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_alert_event_id ON mv_alert_events (event_id);",
    "CREATE INDEX IF NOT EXISTS idx_mv_alert_sport_gender ON mv_alert_events (sport, gender);"
]

DUAL_PROCESS_MARKET_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_markets_event_bookie_live_name_period ON markets (event_id, bookie_id, is_live, market_name, market_period);",
    "CREATE INDEX IF NOT EXISTS idx_markets_event_bookie_live_group_period ON markets (event_id, bookie_id, is_live, market_group, market_period);",
    "CREATE INDEX IF NOT EXISTS idx_market_choices_market_choice_name ON market_choices (market_id, choice_name);",
    "CREATE INDEX IF NOT EXISTS idx_choice_collected ON market_choice_snapshots (choice_id, collected_at);",
    """CREATE INDEX IF NOT EXISTS idx_market_choice_quotes_dual_sofascore
       ON market_choice_quotes (choice_id, quote_id)
       INCLUDE (initial_odds, current_odds, current_updated_at, initial_captured_at)
       WHERE source = 'sofascore' AND exchange_side IS NULL AND exchange_level = 0;""",
]

PRE_START_ODDS_TRAJECTORY_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_events_start_time_utc ON events (start_time_utc);",
    "CREATE INDEX IF NOT EXISTS idx_events_sport_start_time_utc ON events (sport, start_time_utc);",
    "CREATE INDEX IF NOT EXISTS idx_events_season_start_time_utc ON events (season_id, start_time_utc);",
    "CREATE INDEX IF NOT EXISTS idx_market_choice_snapshots_choice_collected_desc ON market_choice_snapshots (choice_id, collected_at DESC, snapshot_id DESC);",
    "CREATE INDEX IF NOT EXISTS idx_market_choice_snapshots_source ON market_choice_snapshots (source);",
    "CREATE INDEX IF NOT EXISTS idx_market_choice_snapshots_source_collected ON market_choice_snapshots (source, source_collected_at);",
    "CREATE INDEX IF NOT EXISTS idx_market_choice_snapshots_source_market ON market_choice_snapshots (source, source_market_id);",
]

# ---------------------------------------------------------------------------
# View for historical standings: season events with results
# Used to compute standings at any point in time for collected seasons
# ---------------------------------------------------------------------------

SEASON_EVENTS_WITH_RESULTS_VIEW_SQL = (
    """
    CREATE OR REPLACE VIEW season_events_with_results AS
    SELECT
        e.id AS event_id,
        e.season_id,
        e.start_time_utc,
        e.competition_id,
        hp.name AS home_team,
        ap.name AS away_team,
        e.sport,
        c.display_name AS competition,
        c.source_tournament_id,
        c.source_unique_tournament_id,
        r.home_score,
        r.away_score,
        r.winner,
        CASE
            WHEN r.winner = 'X' THEN 'DRAW'
            WHEN e.sport = 'Ice hockey'
                 AND (POSITION('+' IN COALESCE(r.home_sets, '')) > 0 OR POSITION('+' IN COALESCE(r.away_sets, '')) > 0)
            THEN 'SO'
            WHEN e.sport = 'Ice hockey'
                AND (POSITION('(' IN COALESCE(r.home_sets, '')) > 0 OR POSITION('(' IN COALESCE(r.away_sets, '')) > 0)
            THEN 'OT'
            ELSE 'REG'
        END AS result_subtype,
        e.round
    FROM events e
    JOIN results r ON r.event_id = e.id
    JOIN participants hp ON hp.participant_id = e.home_participant_id
    JOIN participants ap ON ap.participant_id = e.away_participant_id
    JOIN competitions c ON c.competition_id = e.competition_id
    WHERE e.season_id IS NOT NULL
      AND r.home_score IS NOT NULL
      AND r.away_score IS NOT NULL;
    """
)


def build_pre_start_odds_trajectory_view_sql(view_name: str, *, quote_aware: bool) -> str:
    """Build one private trajectory view with a stable public column contract."""
    if view_name not in {
        "v_pre_start_odds_trajectory_legacy",
        "v_pre_start_odds_trajectory_quotes",
    }:
        raise ValueError(f"Unsupported trajectory view name: {view_name}")

    if quote_aware:
        eligibility_cte = """
        eligible_quotes AS (
            SELECT ranked.*
            FROM (
                SELECT
                    mcq.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY mcq.choice_id, mcq.source, mcq.exchange_side
                        ORDER BY mcq.exchange_level, mcq.quote_id
                    ) AS depth_rank
                FROM market_choice_quotes mcq
                WHERE EXISTS (
                    SELECT 1 FROM market_choice_snapshots history
                    WHERE history.quote_id = mcq.quote_id
                )
                  AND NOT (
                    mcq.exchange_side IS NULL
                    AND EXISTS (
                        SELECT 1
                        FROM market_choice_quotes explicit_q
                        WHERE explicit_q.choice_id = mcq.choice_id
                          AND explicit_q.source = mcq.source
                          AND explicit_q.exchange_side IN ('back', 'lay')
                          AND EXISTS (
                              SELECT 1 FROM market_choice_snapshots explicit_history
                              WHERE explicit_history.quote_id = explicit_q.quote_id
                          )
                    )
                  )
            ) ranked
            WHERE ranked.depth_rank = 1
        ),
        """
        quote_join = """
        JOIN eligible_quotes mcq
          ON mcq.quote_id = mcs.quote_id
         AND mcq.choice_id = mcs.choice_id
        JOIN market_choices mc ON mc.choice_id = mcq.choice_id
        """
        initial_expression = "mcq.initial_odds"
        quote_id_expression = "mcq.quote_id"
        source_expression = "mcq.source"
        source_market_expression = "mcq.source_market_id"
        source_outcome_expression = "mcq.source_outcome_id"
        bookmaker_outcome_expression = "mcq.bookmaker_outcome_id"
        main_line_expression = "mcq.main_line"
        side_expression = "mcq.exchange_side"
        level_expression = "mcq.exchange_level"
    else:
        eligibility_cte = ""
        quote_join = "JOIN market_choices mc ON mc.choice_id = mcs.choice_id"
        initial_expression = "mc.initial_odds"
        quote_id_expression = "mcs.quote_id"
        source_expression = "mcs.source"
        source_market_expression = "mcs.source_market_id"
        source_outcome_expression = "mcs.source_outcome_id"
        bookmaker_outcome_expression = "mcs.bookmaker_outcome_id"
        main_line_expression = "mcs.main_line"
        side_expression = "mcs.exchange_side"
        level_expression = "COALESCE(mcs.exchange_level, 0)"

    return f"""
    CREATE OR REPLACE VIEW {view_name} AS
    WITH
    {eligibility_cte}
    snapshot_context AS (
        SELECT
            e.id AS event_id,
            e.start_time_utc,
            m.market_id,
            m.market_name,
            m.market_group,
            m.market_period,
            m.choice_group,
            m.bookie_id,
            b.name AS bookie_name,
            mc.choice_id,
            mc.choice_name,
            {initial_expression} AS initial_odds,
            mcs.snapshot_id,
            {source_expression} AS source,
            mcs.source_collected_at,
            {source_market_expression} AS source_market_id,
            {source_outcome_expression} AS source_outcome_id,
            {bookmaker_outcome_expression} AS bookmaker_outcome_id,
            {main_line_expression} AS main_line,
            mcs.source_limit,
            mcs.odds_value,
            mcs.collected_at,
            esm.source_sport_id AS event_source_sport_id,
            ROUND(EXTRACT(EPOCH FROM (e.start_time_utc - mcs.collected_at)) / 60)::int AS minutes_before_start,
            {quote_id_expression} AS quote_id,
            {side_expression} AS exchange_side,
            {level_expression} AS exchange_level
        FROM market_choice_snapshots mcs
        {quote_join}
        JOIN markets m ON m.market_id = mc.market_id
        JOIN events e ON e.id = m.event_id
        JOIN bookies b ON b.bookie_id = m.bookie_id
        LEFT JOIN event_source_mappings esm
            ON esm.event_id = e.id
           AND esm.source = {source_expression}
        WHERE m.is_live = false
    ),
    source_mapped AS (
        SELECT
            sc.*,
            COALESCE(msm_exact.mapping_id, msm_fallback.mapping_id) AS market_source_mapping_id,
            COALESCE(msm_exact.canonical_market_key, msm_fallback.canonical_market_key) AS mapped_canonical_market_key
        FROM snapshot_context sc
        LEFT JOIN market_source_mappings msm_exact
            ON msm_exact.source = sc.source
           AND msm_exact.source_market_id = sc.source_market_id
           AND msm_exact.source_sport_id = sc.event_source_sport_id
        LEFT JOIN market_source_mappings msm_fallback
            ON msm_fallback.source = sc.source
           AND msm_fallback.source_market_id = sc.source_market_id
           AND msm_fallback.source_sport_id IS NULL
    ),
    textual_canonical_match AS (
        SELECT
            sm.*,
            cmt.canonical_market_key AS textual_canonical_market_key,
            cmt.canonical_market_name AS textual_market_name,
            cmt.canonical_market_group AS textual_market_group,
            cmt.canonical_market_period AS textual_market_period,
            cmt.market_family AS textual_market_family,
            cmt.requires_choice_group AS textual_requires_choice_group,
            cmt.enabled_for_trajectory AS textual_enabled_for_trajectory,
            cmt.display_order AS textual_market_display_order
        FROM source_mapped sm
        LEFT JOIN canonical_market_types cmt
            ON LOWER(REPLACE(REPLACE(REPLACE(COALESCE(sm.market_name, ''), '-', ''), '_', ''), ' ', '')) =
               LOWER(REPLACE(REPLACE(REPLACE(cmt.canonical_market_name, '-', ''), '_', ''), ' ', ''))
           AND LOWER(REPLACE(REPLACE(REPLACE(COALESCE(sm.market_group, ''), '-', ''), '_', ''), ' ', '')) =
               LOWER(REPLACE(REPLACE(REPLACE(cmt.canonical_market_group, '-', ''), '_', ''), ' ', ''))
           AND LOWER(REPLACE(REPLACE(REPLACE(COALESCE(sm.market_period, ''), '-', ''), '_', ''), ' ', '')) =
               LOWER(REPLACE(REPLACE(REPLACE(cmt.canonical_market_period, '-', ''), '_', ''), ' ', ''))
    )
    SELECT
        tcm.event_id,
        tcm.start_time_utc,
        tcm.market_id,
        COALESCE(mapped_cmt.canonical_market_key, tcm.textual_canonical_market_key) AS canonical_market_key,
        COALESCE(mapped_cmt.market_family, tcm.textual_market_family) AS market_family,
        COALESCE(mapped_cmt.display_order, tcm.textual_market_display_order) AS market_display_order,
        COALESCE(mapped_cmt.canonical_market_name, tcm.textual_market_name, tcm.market_name) AS market_name,
        COALESCE(mapped_cmt.canonical_market_group, tcm.textual_market_group, tcm.market_group) AS market_group,
        COALESCE(mapped_cmt.canonical_market_period, tcm.textual_market_period, tcm.market_period) AS market_period,
        tcm.choice_group,
        tcm.bookie_id,
        tcm.bookie_name,
        tcm.choice_id,
        tcm.choice_name,
        mo.display_order AS choice_display_order,
        tcm.initial_odds,
        tcm.snapshot_id,
        tcm.source,
        tcm.source_collected_at,
        tcm.source_market_id,
        tcm.source_outcome_id,
        tcm.bookmaker_outcome_id,
        tcm.main_line,
        tcm.source_limit,
        tcm.odds_value,
        tcm.collected_at,
        tcm.minutes_before_start,
        tcm.quote_id,
        tcm.exchange_side,
        tcm.exchange_level
    FROM textual_canonical_match tcm
    LEFT JOIN canonical_market_types mapped_cmt
        ON mapped_cmt.canonical_market_key = tcm.mapped_canonical_market_key
    LEFT JOIN market_outcome_source_mappings mo
        ON mo.market_source_mapping_id = tcm.market_source_mapping_id
       AND mo.source_outcome_id = tcm.source_outcome_id
    WHERE COALESCE(mapped_cmt.enabled_for_trajectory, tcm.textual_enabled_for_trajectory, false) = true
      AND (
          COALESCE(mapped_cmt.requires_choice_group, tcm.textual_requires_choice_group, false) = false
          OR tcm.choice_group IS NOT NULL
      );
    """


def build_dual_process_public_view_sql(mode: str) -> str:
    if mode not in {"legacy", "quotes"}:
        raise ValueError(f"Unsupported dual-process read mode: {mode}")
    source_view = f"v_dual_process_event_odds_{mode}"
    return f"""
    CREATE OR REPLACE VIEW v_dual_process_event_odds AS
    SELECT * FROM {source_view};
    """


PRE_START_ODDS_TRAJECTORY_LEGACY_VIEW_SQL = build_pre_start_odds_trajectory_view_sql(
    "v_pre_start_odds_trajectory_legacy", quote_aware=False
)
PRE_START_ODDS_TRAJECTORY_QUOTES_VIEW_SQL = build_pre_start_odds_trajectory_view_sql(
    "v_pre_start_odds_trajectory_quotes", quote_aware=True
)


def build_pre_start_odds_trajectory_public_view_sql(mode: str) -> str:
    if mode not in {"legacy", "shadow", "quotes"}:
        raise ValueError(f"Unsupported trajectory read mode: {mode}")
    source_view = (
        "v_pre_start_odds_trajectory_quotes"
        if mode == "quotes"
        else "v_pre_start_odds_trajectory_legacy"
    )
    return f"""
    CREATE OR REPLACE VIEW v_pre_start_odds_trajectory AS
    SELECT * FROM {source_view};
    """


# LEGACY_ODDS_READ compatibility constant. New code must use the two private
# definitions plus build_pre_start_odds_trajectory_public_view_sql().
PRE_START_ODDS_TRAJECTORY_VIEW_SQL = build_pre_start_odds_trajectory_public_view_sql(
    "legacy"
)


def create_or_replace_views(engine):
    """Create or replace reporting SQL views. Call this after engine init."""
    from infrastructure.settings import Config

    with engine.begin() as conn:
        for index_sql in DUAL_PROCESS_MARKET_INDEXES_SQL:
            conn.exec_driver_sql(index_sql)
        for index_sql in PRE_START_ODDS_TRAJECTORY_INDEXES_SQL:
            conn.exec_driver_sql(index_sql)
        conn.exec_driver_sql(
            build_dual_process_event_odds_view_sql(
                Config.MARKETS_DUAL_PROCESS,
                Config.PERIODS_DUAL_PROCESS,
                view_name="v_dual_process_event_odds_legacy",
                quote_aware=False,
            )
        )
        conn.exec_driver_sql(
            build_dual_process_event_odds_view_sql(
                Config.MARKETS_DUAL_PROCESS,
                Config.PERIODS_DUAL_PROCESS,
                view_name="v_dual_process_event_odds_quotes",
                quote_aware=True,
            )
        )
        conn.exec_driver_sql(
            build_dual_process_public_view_sql(Config.DUAL_PROCESS_ODDS_READ_MODE)
        )
        conn.exec_driver_sql(EVENT_ALL_ODDS_VIEW_SQL)
        # Drop basketball_results view first if it exists (to handle column removal)
        conn.exec_driver_sql("DROP VIEW IF EXISTS basketball_results CASCADE;")
        conn.exec_driver_sql(BASKETBALL_RESULTS_VIEW_SQL)
        # Drop season events view first to ensure schema updates correctly (like adding the 'round' column)
        conn.exec_driver_sql("DROP VIEW IF EXISTS season_events_with_results CASCADE;")
        # Create season events with results view for historical standings
        conn.exec_driver_sql(SEASON_EVENTS_WITH_RESULTS_VIEW_SQL)
        conn.exec_driver_sql(PRE_START_ODDS_TRAJECTORY_LEGACY_VIEW_SQL)
        conn.exec_driver_sql(PRE_START_ODDS_TRAJECTORY_QUOTES_VIEW_SQL)
        conn.exec_driver_sql(
            build_pre_start_odds_trajectory_public_view_sql(
                Config.PRE_START_TRAJECTORY_READ_MODE
            )
        )

def create_or_replace_materialized_views(engine):
    """Create or replace materialized views for alerts. Call this after engine init."""
    from infrastructure.settings import Config

    with engine.begin() as conn:
        for index_sql in DUAL_PROCESS_MARKET_INDEXES_SQL:
            conn.exec_driver_sql(index_sql)
        for index_sql in PRE_START_ODDS_TRAJECTORY_INDEXES_SQL:
            conn.exec_driver_sql(index_sql)
        conn.exec_driver_sql(
            build_dual_process_event_odds_view_sql(
                Config.MARKETS_DUAL_PROCESS,
                Config.PERIODS_DUAL_PROCESS,
                view_name="v_dual_process_event_odds_legacy",
                quote_aware=False,
            )
        )
        conn.exec_driver_sql(
            build_dual_process_event_odds_view_sql(
                Config.MARKETS_DUAL_PROCESS,
                Config.PERIODS_DUAL_PROCESS,
                view_name="v_dual_process_event_odds_quotes",
                quote_aware=True,
            )
        )
        conn.exec_driver_sql(
            build_dual_process_public_view_sql(Config.DUAL_PROCESS_ODDS_READ_MODE)
        )
        conn.exec_driver_sql(EVENT_ALL_ODDS_VIEW_SQL)
        # Drop existing materialized view to recreate with new schema
        conn.exec_driver_sql("DROP MATERIALIZED VIEW IF EXISTS mv_alert_events;")
        conn.exec_driver_sql(MV_ALERT_EVENTS_SQL)
        for index_sql in MV_ALERT_EVENTS_INDEXES_SQL:
            conn.exec_driver_sql(index_sql)

def refresh_materialized_views(engine):
    """Refresh materialized views with latest data."""
    with engine.begin() as conn:
        conn.exec_driver_sql("REFRESH MATERIALIZED VIEW mv_alert_events;")


# Views are created explicitly after migrations via create_or_replace_views().
# Do not register view DDL on Base.metadata.after_create: create_all() may run
# before additive migrations on existing databases, and views can reference
# columns that are about to be added by check_and_migrate_schema().
# NOTE: Materialized views are created after schema migrations in main.py.
