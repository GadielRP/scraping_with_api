from sqlalchemy import create_engine, Column, Integer, String, Numeric, DateTime, BigInteger, Text, CheckConstraint, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
import json

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
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    odds_snapshots = relationship("OddsSnapshot", back_populates="event", cascade="all, delete-orphan")
    event_odds = relationship("EventOdds", back_populates="event", uselist=False, cascade="all, delete-orphan")
    result = relationship("Result", back_populates="event", uselist=False, cascade="all, delete-orphan")

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
    one_open = Column(Numeric(6, 2))
    x_open = Column(Numeric(6, 2))
    two_open = Column(Numeric(6, 2))
    one_final = Column(Numeric(6, 2))
    x_final = Column(Numeric(6, 2))
    two_final = Column(Numeric(6, 2))
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
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    event = relationship("Event", back_populates="result")

class AlertLog(Base):
    __tablename__ = 'alerts_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey('events.id', ondelete='CASCADE'), nullable=False)
    rule_key = Column(Text, nullable=False)
    triggered_at = Column(DateTime, nullable=False)
    payload = Column(Text)  # JSON as text for SQLite compatibility
    
    def set_payload(self, data):
        """Store payload data as JSON string"""
        if isinstance(data, dict):
            self.payload = json.dumps(data)
        else:
            self.payload = str(data)
    
    def get_payload(self):
        """Retrieve payload data as dict"""
        if self.payload:
            try:
                return json.loads(self.payload)
            except json.JSONDecodeError:
                return None
        return None
