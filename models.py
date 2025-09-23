from sqlalchemy import create_engine, Column, Integer, String, Numeric, DateTime, BigInteger, Text, CheckConstraint, ForeignKey, UniqueConstraint, Computed
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
    created_at = Column(DateTime, default=get_local_now)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)
    
    # Relationships
    odds_snapshots = relationship("OddsSnapshot", back_populates="event", cascade="all, delete-orphan")
    event_odds = relationship("EventOdds", back_populates="event", uselist=False, cascade="all, delete-orphan")
    result = relationship("Result", back_populates="event", uselist=False, cascade="all, delete-orphan")
    observations = relationship("EventObservation", back_populates="event", cascade="all, delete-orphan")

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
    ended_at = Column(DateTime)
    updated_at = Column(DateTime, default=get_local_now, onupdate=get_local_now)
    
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
# Materialized view for fast alert processing
# ---------------------------------------------------------------------------

MV_ALERT_EVENTS_SQL = (
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_alert_events AS
    SELECT
        e.id AS event_id,
        e.sport,
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
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_alert_event_id ON mv_alert_events (event_id);"
]


def create_or_replace_views(engine):
    """Create or replace reporting SQL views. Call this after engine init."""
    with engine.begin() as conn:
        conn.exec_driver_sql(EVENT_ALL_ODDS_VIEW_SQL)

def create_or_replace_materialized_views(engine):
    """Create or replace materialized views for alerts. Call this after engine init."""
    with engine.begin() as conn:
        conn.exec_driver_sql(MV_ALERT_EVENTS_SQL)
        for index_sql in MV_ALERT_EVENTS_INDEXES_SQL:
            conn.exec_driver_sql(index_sql)

def refresh_materialized_views(engine):
    """Refresh materialized views with latest data."""
    with engine.begin() as conn:
        conn.exec_driver_sql("REFRESH MATERIALIZED VIEW mv_alert_events;")


# Also register for metadata create events so new databases get views automatically
event.listen(Base.metadata, 'after_create', DDL(EVENT_ALL_ODDS_VIEW_SQL))
event.listen(Base.metadata, 'after_create', DDL(MV_ALERT_EVENTS_SQL))
