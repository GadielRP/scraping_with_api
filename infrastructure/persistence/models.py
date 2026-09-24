from sqlalchemy import Column, Integer, String, Numeric, BigInteger, Text, CheckConstraint, ForeignKey, UniqueConstraint, Boolean, Index, JSON, SmallInteger
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB
from shared.temporal import utc_now
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.types import UTCDateTime


class Participant(Base):
    __tablename__ = 'participants'

    participant_id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(Text, nullable=False)
    source_participant_id = Column(BigInteger, nullable=False)
    name = Column(Text, nullable=False)
    slug = Column(Text)
    short_name = Column(Text)
    code_name = Column(Text)
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

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
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

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
    # Absolute kickoff instant. PostgreSQL stores it as timestamptz and the
    # Python contract always exposes an aware datetime normalized to UTC.
    starts_at = Column(UTCDateTime(), nullable=False)
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

    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)
    
    # Relationships
    result = relationship("Result", back_populates="event", uselist=False, cascade="all, delete-orphan")
    observations = relationship("EventObservation", back_populates="event", cascade="all, delete-orphan")
    pillar_mining_runs = relationship(
        "PillarMiningRun",
        back_populates="event",
        cascade="all, delete-orphan",
    )
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
    # These are FKs to source-scoped Participant rows, not provider's raw IDs.
    source_participant_home_id = Column(
        Integer,
        ForeignKey(
            'participants.participant_id',
            ondelete='SET NULL',
            name='fk_event_source_mappings_source_participant_home_id',
        ),
    )
    source_participant_away_id = Column(
        Integer,
        ForeignKey(
            'participants.participant_id',
            ondelete='SET NULL',
            name='fk_event_source_mappings_source_participant_away_id',
        ),
    )
    # None means provider availability has not been confirmed yet.
    has_odds = Column(Boolean, nullable=True)
    match_method = Column(Text, nullable=False, default='direct')
    confidence = Column(Numeric(5, 3))
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

    event = relationship("Event", back_populates="source_mappings")
    source_participant_home = relationship("Participant", foreign_keys=[source_participant_home_id])
    source_participant_away = relationship("Participant", foreign_keys=[source_participant_away_id])

    __table_args__ = (
        UniqueConstraint('source', 'source_event_id', name='unique_event_source_mapping'),
        Index('idx_event_source_mappings_event_id', 'event_id'),
        Index('idx_event_source_mappings_source', 'source'),
        Index('idx_event_source_mappings_source_participant_home_id', 'source_participant_home_id'),
        Index('idx_event_source_mappings_source_participant_away_id', 'source_participant_away_id'),
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
    source_starts_at = Column(UTCDateTime(), nullable=True)
    raw_external_providers = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=True)
    raw_payload = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=True)
    candidate_scores = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=True)
    attempt_count = Column(Integer, nullable=False, default=1)
    first_seen_at = Column(UTCDateTime(), default=utc_now)
    last_attempted_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

    __table_args__ = (
        UniqueConstraint(
            'source',
            'source_event_id',
            name='unique_event_source_resolution_queue_source_event',
        ),
        Index('idx_event_source_resolution_queue_status', 'source', 'resolution_status'),
        Index('idx_event_source_resolution_queue_start_time', 'source_starts_at'),
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
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('event_id', 'observation_type', name='unique_event_observation_type'),
    )
    
    # Relationships
    event = relationship("Event", back_populates="observations")

    def __repr__(self):
        return f"<EventObservation(event_id={self.event_id}, type='{self.observation_type}', value='{self.observation_value}')>"


class PillarMiningRun(Base):
    """One canonical pillar execution for an event and execution slot."""

    __tablename__ = 'pillar_mining_runs'

    id = Column(
        BigInteger().with_variant(Integer(), 'sqlite'),
        primary_key=True,
        autoincrement=True,
    )
    event_id = Column(
        Integer,
        ForeignKey('events.id', ondelete='CASCADE'),
        nullable=False,
    )
    pillar_id = Column(String(100), nullable=False)
    result_scope = Column(String(100), nullable=False)
    execution_slot = Column(String(64), nullable=False)
    engine_version = Column(String(150), nullable=False)
    payload_schema_version = Column(SmallInteger, nullable=False, default=1)
    producer_status = Column(String(50), nullable=False)
    canonical_status = Column(String(20), nullable=False)
    evaluation_minute = Column(SmallInteger)
    target_minute = Column(SmallInteger)
    calculated_at = Column(UTCDateTime(), nullable=False, default=utc_now)
    sport = Column(String(50), nullable=False)
    competition_id = Column(
        Integer,
        ForeignKey('competitions.competition_id', ondelete='SET NULL'),
    )
    context = Column(
        JSONB().with_variant(JSON(), 'sqlite'), nullable=False, default=dict
    )
    inputs = Column(
        JSONB().with_variant(JSON(), 'sqlite'), nullable=False, default=dict
    )
    diagnostics = Column(
        JSONB().with_variant(JSON(), 'sqlite'), nullable=False, default=dict
    )
    output_payload = Column(
        JSONB().with_variant(JSON(), 'sqlite'), nullable=False, default=dict
    )
    created_at = Column(UTCDateTime(), nullable=False, default=utc_now)
    updated_at = Column(
        UTCDateTime(), nullable=False, default=utc_now, onupdate=utc_now
    )

    event = relationship("Event", back_populates="pillar_mining_runs")
    competition = relationship("Competition", foreign_keys=[competition_id])
    units = relationship(
        "PillarMiningUnit",
        back_populates="run",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        CheckConstraint(
            'payload_schema_version >= 1',
            name='ck_pillar_mining_run_payload_schema_version_positive',
        ),
        CheckConstraint(
            "canonical_status IN ('SUCCESS', 'PARTIAL', 'INSUFFICIENT', "
            "'ERROR', 'SKIPPED')",
            name='ck_pillar_mining_run_canonical_status',
        ),
        UniqueConstraint(
            'event_id',
            'pillar_id',
            'result_scope',
            'execution_slot',
            'engine_version',
            name='uq_pillar_mining_run_identity',
        ),
        Index(
            'idx_pillar_mining_run_event_pillar_evaluation',
            'event_id',
            'pillar_id',
            'evaluation_minute',
        ),
        Index(
            'idx_pillar_mining_run_pillar_status_calculated',
            'pillar_id',
            'canonical_status',
            'calculated_at',
        ),
        Index('idx_pillar_mining_run_sport_pillar', 'sport', 'pillar_id'),
    )


class PillarMiningUnit(Base):
    """Hierarchical and independently evaluable node within a mining run."""

    __tablename__ = 'pillar_mining_units'

    id = Column(
        BigInteger().with_variant(Integer(), 'sqlite'),
        primary_key=True,
        autoincrement=True,
    )
    run_id = Column(
        BigInteger().with_variant(Integer(), 'sqlite'),
        ForeignKey('pillar_mining_runs.id', ondelete='CASCADE'),
        nullable=False,
    )
    parent_unit_id = Column(
        BigInteger().with_variant(Integer(), 'sqlite'),
        ForeignKey('pillar_mining_units.id', ondelete='CASCADE'),
    )
    unit_type = Column(String(50), nullable=False)
    unit_key = Column(String(512), nullable=False)
    ordinal = Column(Integer)
    module_id = Column(String(100))
    producer_status = Column(String(50), nullable=False)
    canonical_status = Column(String(20), nullable=False)
    signal_axis = Column(String(50))
    is_valid = Column(Boolean)
    score_name = Column(String(100))
    score = Column(Numeric(30, 12))
    direction = Column(String(50))
    strength = Column(String(50))
    target_minute = Column(SmallInteger)
    market_type_id = Column(
        SmallInteger,
        ForeignKey('canonical_market_types.market_type_id', ondelete='RESTRICT'),
    )
    line_value = Column(Numeric)
    choice_name = Column(Text)
    bookie_id = Column(
        Integer,
        ForeignKey('bookies.bookie_id', ondelete='SET NULL'),
    )
    quote_id = Column(BigInteger)
    source = Column(String(50))
    exchange_side = Column(String(20))
    exchange_level = Column(SmallInteger)
    dimensions = Column(
        JSONB().with_variant(JSON(), 'sqlite'), nullable=False, default=dict
    )
    payload = Column(
        JSONB().with_variant(JSON(), 'sqlite'), nullable=False, default=dict
    )
    diagnostics = Column(
        JSONB().with_variant(JSON(), 'sqlite'), nullable=False, default=dict
    )
    created_at = Column(UTCDateTime(), nullable=False, default=utc_now)
    updated_at = Column(
        UTCDateTime(), nullable=False, default=utc_now, onupdate=utc_now
    )

    run = relationship("PillarMiningRun", back_populates="units")
    parent = relationship(
        "PillarMiningUnit",
        remote_side=[id],
        back_populates="children",
    )
    children = relationship(
        "PillarMiningUnit",
        back_populates="parent",
        cascade="all, delete-orphan",
        single_parent=True,
    )
    bookie = relationship("Bookie", foreign_keys=[bookie_id])
    metrics = relationship(
        "PillarMiningMetricValue",
        back_populates="unit",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        CheckConstraint(
            "canonical_status IN ('SUCCESS', 'PARTIAL', 'INSUFFICIENT', "
            "'ERROR', 'SKIPPED')",
            name='ck_pillar_mining_unit_canonical_status',
        ),
        UniqueConstraint(
            'run_id', 'unit_key', name='uq_pillar_mining_unit_identity'
        ),
        Index('idx_pillar_mining_unit_run_parent', 'run_id', 'parent_unit_id'),
        Index(
            'idx_pillar_mining_unit_type_status',
            'unit_type',
            'canonical_status',
        ),
        Index('idx_pillar_mining_unit_market_type', 'market_type_id', 'line_value'),
        Index('idx_pillar_mining_unit_bookie', 'bookie_id'),
        Index(
            'idx_pillar_mining_unit_dimensions_gin',
            'dimensions',
            postgresql_using='gin',
        ),
    )


class PillarMiningMetricValue(Base):
    """Typed scalar projection used by cross-pillar statistical queries."""

    __tablename__ = 'pillar_mining_metric_values'

    id = Column(
        BigInteger().with_variant(Integer(), 'sqlite'),
        primary_key=True,
        autoincrement=True,
    )
    unit_id = Column(
        BigInteger().with_variant(Integer(), 'sqlite'),
        ForeignKey('pillar_mining_units.id', ondelete='CASCADE'),
        nullable=False,
    )
    metric_name = Column(String(150), nullable=False)
    metric_group = Column(String(100))
    value_type = Column(String(10), nullable=False)
    numeric_value = Column(Numeric(30, 12))
    text_value = Column(Text)
    boolean_value = Column(Boolean)
    created_at = Column(UTCDateTime(), nullable=False, default=utc_now)
    updated_at = Column(
        UTCDateTime(), nullable=False, default=utc_now, onupdate=utc_now
    )

    unit = relationship("PillarMiningUnit", back_populates="metrics")

    __table_args__ = (
        CheckConstraint(
            "(value_type = 'number' AND numeric_value IS NOT NULL "
            "AND text_value IS NULL AND boolean_value IS NULL) OR "
            "(value_type = 'text' AND numeric_value IS NULL "
            "AND text_value IS NOT NULL AND boolean_value IS NULL) OR "
            "(value_type = 'boolean' AND numeric_value IS NULL "
            "AND text_value IS NULL AND boolean_value IS NOT NULL)",
            name='ck_pillar_mining_metric_typed_value',
        ),
        UniqueConstraint(
            'unit_id', 'metric_name', name='uq_pillar_mining_metric_name'
        ),
        Index(
            'idx_pillar_mining_metric_numeric', 'metric_name', 'numeric_value'
        ),
        Index('idx_pillar_mining_metric_text', 'metric_name', 'text_value'),
    )


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
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

    bookie = relationship("Bookie", back_populates="source_mappings")

    __table_args__ = (
        UniqueConstraint("source", "source_bookie_slug", name="unique_bookie_source_slug"),
        Index("idx_bookie_source_mappings_bookie_id", "bookie_id"),
        Index("idx_bookie_source_mappings_source_slug", "source", "source_bookie_slug"),
        Index("idx_bookie_source_mappings_source_name", "source", "source_bookie_name"),
    )


from infrastructure.persistence.odds_models import (
    CanonicalMarketType,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
    MarketOutcomeSourceMapping,
    MarketSourceMapping,
    SourceCatalogSync,
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
    cached_date = Column(UTCDateTime(), nullable=False)                  # Date the cache was populated
    match_urls = Column(JSONB().with_variant(JSON(), 'sqlite'), nullable=False)                      # { "/basketball/usa/nba/team-a-team-b-xYZ/": "Team A - Team B" }
    created_at = Column(UTCDateTime(), default=utc_now)


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
    last_attempt_at = Column(UTCDateTime())
    created_at = Column(UTCDateTime(), default=utc_now)
    
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
    started_at = Column(UTCDateTime(), nullable=False, default=utc_now)
    heartbeat_at = Column(UTCDateTime(), nullable=False, default=utc_now)
    finished_at = Column(UTCDateTime())
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


class OddspapiApiKeyUsage(Base):
    """Durable quota snapshot keyed by a non-secret API-key fingerprint."""

    __tablename__ = 'oddspapi_api_key_usage'

    key_fingerprint = Column(String(64), primary_key=True)
    subscription_id = Column(String(255))
    subscription_valid_from = Column(UTCDateTime())
    subscription_valid_until = Column(UTCDateTime())
    request_limit = Column(Integer)
    reported_request_count = Column(Integer)
    estimated_request_count = Column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    status = Column(
        String(32),
        nullable=False,
        default='unknown',
        server_default='unknown',
    )
    account_refreshed_at = Column(UTCDateTime())
    last_error_code = Column(String(100))
    last_error_at = Column(UTCDateTime())
    updated_at = Column(
        UTCDateTime(),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
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
    captured_at = Column(UTCDateTime(), default=utc_now, nullable=False)

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
