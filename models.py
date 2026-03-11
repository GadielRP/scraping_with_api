from sqlalchemy import create_engine, Column, Integer, String, Numeric, DateTime, BigInteger, Text, CheckConstraint, ForeignKey, UniqueConstraint, Computed, Boolean, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event, DDL, text
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
import json
from timezone_utils import get_local_now

Base = declarative_base()

class Event(Base):
    __tablename__ = 'events'
    
    id = Column(Integer, primary_key=True)
    custom_id = Column(Text)
    slug = Column(Text, nullable=False)
    start_time_utc = Column(DateTime, nullable=False)
    sport = Column(Text, nullable=False)
    competition = Column(Text, nullable=False)
    country = Column(Text)
    home_team = Column(Text, nullable=False)
    away_team = Column(Text, nullable=False)
    gender = Column(String(10), nullable=False, default="unknown")  # 'Men' or 'Women' or 'Mixed'
    discovery_source = Column(String(50), nullable=False, default='dropping_odds')  # 'dropping_odds', 'high_value_streaks', 'h2h', 'winning_odds', 'team_streaks'
    season_id = Column(Integer, ForeignKey('seasons.id', ondelete='SET NULL'))  # Season ID from SofaScore API (foreign key to seasons table)
    round = Column(Text)  # Round information (e.g., 'regular_season', 'knockouts/playoffs', 'final')
    alert_sent = Column(Boolean, default=False, nullable=False)  # True if 4th quarter alert sent, False otherwise

    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)
    
    # Relationships
    odds_snapshots = relationship("OddsSnapshot", back_populates="event", cascade="all, delete-orphan")
    event_odds = relationship("EventOdds", back_populates="event", uselist=False, cascade="all, delete-orphan")
    result = relationship("Result", back_populates="event", uselist=False, cascade="all, delete-orphan")
    observations = relationship("EventObservation", back_populates="event", cascade="all, delete-orphan")
    prediction_logs = relationship("PredictionLog", back_populates="event", uselist=False, cascade="all, delete-orphan")
    season = relationship("Season", back_populates="events")
    markets = relationship("Market", back_populates="event", cascade="all, delete-orphan")

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

class OddsSnapshot(Base):
    __tablename__ = 'odds_snapshot'
    
    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey('events.id', ondelete='CASCADE'), nullable=False)
    collected_at = Column(DateTime, nullable=False)
    market = Column(Text, nullable=False)
    one_open = Column(Numeric(6, 2))
    x_open = Column(Numeric(6, 2))
    two_open = Column(Numeric(6, 2))
    one_cur = Column(Numeric(6, 2))
    x_cur = Column(Numeric(6, 2))
    two_cur = Column(Numeric(6, 2))
    raw_fractional = Column(Text)  # JSON as text for SQLite compatibility
    
    # Constraints
    __table_args__ = (
        CheckConstraint("market = '1X2'", name='check_market_1x2'),
        UniqueConstraint('event_id', 'collected_at', 'market', name='unique_event_collected_market'),
    )
    
    # Relationships
    event = relationship("Event", back_populates="odds_snapshots")
    
    def set_raw_fractional(self, data):
        """Store raw fractional data as JSON string"""
        if isinstance(data, dict):
            self.raw_fractional = json.dumps(data)
        else:
            self.raw_fractional = str(data)
    
    def get_raw_fractional(self):
        """Retrieve raw fractional data as dict"""
        if self.raw_fractional:
            try:
                return json.loads(self.raw_fractional)
            except json.JSONDecodeError:
                return None
        return None

class EventOdds(Base):
    __tablename__ = 'event_odds'
    
    event_id = Column(Integer, ForeignKey('events.id', ondelete='CASCADE'), primary_key=True)
    market = Column(Text, nullable=False)
    
    # Reordered for readability: open/final pairs, then computed deltas
    one_open = Column(Numeric(6, 2))
    one_final = Column(Numeric(6, 2))
    x_open = Column(Numeric(6, 2))
    x_final = Column(Numeric(6, 2))
    two_open = Column(Numeric(6, 2))
    two_final = Column(Numeric(6, 2))

    # Generated columns in Postgres (computed differences open → final)
    var_one = Column(Numeric(6, 2), Computed("(one_final - one_open)::numeric(6,2)", persisted=True))
    var_x = Column(Numeric(6, 2), Computed("(x_final - x_open)::numeric(6,2)", persisted=True))
    var_two = Column(Numeric(6, 2), Computed("(two_final - two_open)::numeric(6,2)", persisted=True))

    last_sync_at = Column(DateTime, nullable=False)
    
    # Constraints
    __table_args__ = (
        CheckConstraint("market = '1X2'", name='check_market_1x2_event_odds'),
    )
    
    # Relationships
    event = relationship("Event", back_populates="event_odds")

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
    
    def __repr__(self):
        return f"<Bookie(bookie_id={self.bookie_id}, name='{self.name}')>"


class Market(Base):
    """
    Stores individual betting markets for an event.
    
    Each event can have multiple markets (Full time, Match goals 2.5, Asian handicap, etc.)
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
    market_name = Column(Text, nullable=False)  # "Full time", "Match goals", "Asian handicap"
    market_group = Column(Text)  # "1X2", "Match goals", "Asian Handicap"
    market_period = Column(Text)  # "Full-time", "1st half"
    choice_group = Column(Text)  # For Over/Under: "2.5", "3.5", etc. NULL for non-line markets
    
    # Timestamps
    collected_at = Column(DateTime, default=get_local_now, nullable=False)
    
    # Constraints
    __table_args__ = (
        # Each bookie can have one market per event+name+line combination
        UniqueConstraint('event_id', 'bookie_id', 'market_name', 'choice_group', name='unique_market_per_event_bookie'),
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
    
    Each market has multiple choices (e.g., "1", "X", "2" for Full time, or "Over", "Under" for Match goals).
    """
    __tablename__ = 'market_choices'
    
    # Primary Key
    choice_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Key: links to the parent market
    market_id = Column(Integer, ForeignKey('markets.market_id', ondelete='CASCADE'), nullable=False)
    
    # Choice identification
    choice_name = Column(Text, nullable=False)  # "1", "X", "2", "Over", "Under", team name, etc.
    
    # Odds values (stored as decimals for easy math)
    initial_odds = Column(Numeric(8, 3))  # Opening odds (decimal, e.g., 1.53)
    current_odds = Column(Numeric(8, 3))  # Current/final odds (decimal, e.g., 1.48)
    
    # Movement indicator: -1 = odds dropped, 0 = unchanged, +1 = odds increased
    change = Column(Integer, default=0)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('market_id', 'choice_name', name='unique_choice_per_market'),
    )
    
    # Relationships
    market = relationship("Market", back_populates="choices")
    snapshots = relationship("MarketChoiceSnapshot", back_populates="choice", cascade="all, delete-orphan")
    
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
    odds_value = Column(Numeric(8, 3), nullable=False)
    collected_at = Column(DateTime, default=get_local_now, nullable=False)
    
    # Constraints & Indexes
    __table_args__ = (
        Index('idx_choice_collected', 'choice_id', 'collected_at'),
    )
    
    # Relationships
    choice = relationship("MarketChoice", back_populates="snapshots")


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
    match_urls = Column(JSONB, nullable=False)                      # { "/basketball/usa/nba/team-a-team-b-xYZ/": "Team A - Team B" }
    created_at = Column(DateTime, default=get_local_now)


# ---------------------------------------------------------------------------
# SQL view helper – unified odds view (no filtering by var_one)
# ---------------------------------------------------------------------------

EVENT_ALL_ODDS_VIEW_SQL = (
    """
    CREATE OR REPLACE VIEW event_all_odds AS
    SELECT
        e.start_time_utc AS start_time_utc,
        (e.home_team || ' / ' || e.away_team) AS participants,
        eo.one_open AS odds1a,
        eo.one_final AS odds1b,
        eo.x_open AS momEa,
        eo.x_final AS momeEb,
        eo.two_open AS odds2a,
        eo.two_final AS odds2b,
        eo.var_one AS var_1,
        eo.var_x AS var_x,
        eo.var_two AS var_2,
        CASE
            WHEN r.home_score IS NOT NULL AND r.away_score IS NOT NULL
            THEN (r.home_score::text || ' - ' || r.away_score::text)
            ELSE NULL
        END AS result,
        e.competition AS competition,
        e.sport AS sport
    FROM event_odds eo
    JOIN events e ON e.id = eo.event_id
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
        e.home_team,
        e.away_team,
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
        (e.home_team || ' vs ' || e.away_team) AS participants,
        e.home_team,
        e.away_team,
        e.competition,
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
        (eo.var_x IS NOT NULL) AS var_shape,  -- true if draw sport, false if not
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
    FROM event_odds eo
    JOIN events e ON e.id = eo.event_id
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
        e.home_team,
        e.away_team,
        e.sport,
        e.competition,
        r.home_score,
        r.away_score,
        r.winner
    FROM events e
    JOIN results r ON r.event_id = e.id
    WHERE e.season_id IS NOT NULL
      AND r.home_score IS NOT NULL
      AND r.away_score IS NOT NULL;
    """
)


MARKET_CHOICE_TRAJECTORY_VIEW_SQL = (
    """
    CREATE OR REPLACE VIEW v_market_choice_trajectory AS
    SELECT 
        e.id AS event_id,
        e.start_time_utc,
        m.market_name,
        m.market_group,
        m.market_period,
        m.choice_group,
        b.name AS bookie_name,
        mc.choice_name,
        mcs.odds_value,
        mcs.collected_at,
        ROUND(EXTRACT(EPOCH FROM (e.start_time_utc - mcs.collected_at)) / 60) AS minutes_before_start
    FROM market_choice_snapshots mcs
    JOIN market_choices mc ON mc.choice_id = mcs.choice_id
    JOIN markets m ON m.market_id = mc.market_id
    JOIN events e ON e.id = m.event_id
    JOIN bookies b ON b.bookie_id = m.bookie_id;
    """
)


def create_or_replace_views(engine):
    """Create or replace reporting SQL views. Call this after engine init."""
    with engine.begin() as conn:
        conn.exec_driver_sql(EVENT_ALL_ODDS_VIEW_SQL)
        # Drop basketball_results view first if it exists (to handle column removal)
        conn.exec_driver_sql("DROP VIEW IF EXISTS basketball_results CASCADE;")
        conn.exec_driver_sql(BASKETBALL_RESULTS_VIEW_SQL)
        # Create season events with results view for historical standings
        conn.exec_driver_sql(SEASON_EVENTS_WITH_RESULTS_VIEW_SQL)
        # Create market choice trajectory view
        conn.exec_driver_sql(MARKET_CHOICE_TRAJECTORY_VIEW_SQL)

def create_or_replace_materialized_views(engine):
    """Create or replace materialized views for alerts. Call this after engine init."""
    with engine.begin() as conn:
        # Drop existing materialized view to recreate with new schema
        conn.exec_driver_sql("DROP MATERIALIZED VIEW IF EXISTS mv_alert_events CASCADE;")
        conn.exec_driver_sql(MV_ALERT_EVENTS_SQL)
        for index_sql in MV_ALERT_EVENTS_INDEXES_SQL:
            conn.exec_driver_sql(index_sql)

def refresh_materialized_views(engine):
    """Refresh materialized views with latest data."""
    with engine.begin() as conn:
        conn.exec_driver_sql("REFRESH MATERIALIZED VIEW mv_alert_events;")


# Register only the regular view for automatic creation
# Materialized views are created manually in initialize_system() after migrations
# NOTE: basketball_results view is NOT auto-created here - it's created manually in create_or_replace_views()
#       to allow dropping the view first when columns change (PostgreSQL doesn't allow CREATE OR REPLACE VIEW to drop columns)
event.listen(Base.metadata, 'after_create', DDL(EVENT_ALL_ODDS_VIEW_SQL))
event.listen(Base.metadata, 'after_create', DDL(MARKET_CHOICE_TRAJECTORY_VIEW_SQL))
# NOTE: Materialized view is NOT auto-created here - it's created after schema migrations in main.py
