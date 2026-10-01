"""Memory survives deletion of both the canonical event and its mappings."""
from sqlalchemy import Column, Text, Integer, Index, JSON
from sqlalchemy.dialects.postgresql import JSONB
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.types import UTCDateTime


class EventDiscardMemory(Base):
    __tablename__ = 'event_discard_memory'
    source = Column(Text, primary_key=True)
    source_event_id = Column(Text, primary_key=True)
    original_event_id = Column(Integer, nullable=False)  # Deliberately not a FK.
    parser_kind = Column(Text, nullable=False)
    deletion_reason = Column(Text, nullable=False)
    snapshot = Column(JSON().with_variant(JSONB(), 'postgresql'), nullable=False)
    observed_at = Column(UTCDateTime(), nullable=False)
    discarded_at = Column(UTCDateTime(), nullable=False)
    origin = Column(Text, nullable=False)
    policy_version = Column(Integer, nullable=False, default=1)
    __table_args__ = (Index('ix_event_discard_memory_discarded_at', 'discarded_at'),)
