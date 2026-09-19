"""ORM models for canonical odds catalogs and persisted market prices.

    The catalog owns provider-independent market semantics.  ``Market`` stores a
    compact numeric reference to that catalog.  Price state and history
remain deliberately separate: ``MarketChoiceQuote`` is the current-state
projection and ``MarketChoiceSnapshot`` is append-only observation history.
"""

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import relationship

from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.types import UTCDateTime
from shared.temporal import utc_now


class CanonicalMarketType(Base):
    __tablename__ = "canonical_market_types"

    # Keep the textual key as the ORM/database primary key during the additive
    # rollout so existing source-mapping FKs remain valid.  market_type_id is a
    # stable numeric candidate key used by high-cardinality market rows.
    canonical_market_key = Column(Text, primary_key=True)
    market_type_id = Column(SmallInteger, nullable=False, unique=True)
    canonical_market_name = Column(Text, nullable=False)
    canonical_market_group = Column(Text, nullable=False)
    canonical_market_period = Column(Text, nullable=False)
    market_family = Column(Text, nullable=False)
    requires_line_value = Column(Boolean, nullable=False, default=False)
    enabled_for_ingestion = Column(Boolean, nullable=False, default=True)
    enabled_for_trajectory = Column(Boolean, nullable=False, default=False)
    display_order = Column(Integer)
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

    source_mappings = relationship(
        "MarketSourceMapping",
        back_populates="canonical_market_type",
        foreign_keys="MarketSourceMapping.market_type_id",
    )
    markets = relationship(
        "Market",
        back_populates="canonical_market_type",
        foreign_keys="Market.market_type_id",
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
    # Numeric identity used by all runtime code.  The canonical key remains a
    # stable business key for provider mappings and catalog imports.
    market_type_id = Column(
        SmallInteger,
        ForeignKey("canonical_market_types.market_type_id", ondelete="RESTRICT"),
        nullable=False,
    )
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
    match_method = Column(Text, nullable=False, default="catalog_rule")
    confidence = Column(Numeric(5, 3))
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

    canonical_market_type = relationship(
        "CanonicalMarketType",
        back_populates="source_mappings",
        foreign_keys=[market_type_id],
    )
    outcome_mappings = relationship(
        "MarketOutcomeSourceMapping",
        back_populates="market_source_mapping",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        # Kept for compatibility; the two partial indexes below provide the
        # intended NULL-safe source-sport semantics.
        UniqueConstraint(
            "source",
            "source_sport_id",
            "source_market_id",
            name="unique_market_source_mapping",
        ),
        Index("idx_market_source_mappings_market_type", "market_type_id"),
        Index("idx_market_source_mappings_canonical_key", "canonical_market_key"),
        Index(
            "idx_market_source_mappings_source_market",
            "source",
            "source_sport_id",
            "source_market_id",
        ),
        Index(
            "uq_market_source_mapping_scoped",
            "source",
            "source_sport_id",
            "source_market_id",
            unique=True,
            postgresql_where=text("source_sport_id IS NOT NULL"),
            sqlite_where=text("source_sport_id IS NOT NULL"),
        ),
        Index(
            "uq_market_source_mapping_global",
            "source",
            "source_market_id",
            unique=True,
            postgresql_where=text("source_sport_id IS NULL"),
            sqlite_where=text("source_sport_id IS NULL"),
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
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

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
    imported_at = Column(UTCDateTime(), default=utc_now)
    created_at = Column(UTCDateTime(), default=utc_now)

    __table_args__ = (
        Index("idx_source_catalog_syncs_source_type", "source", "catalog_type"),
        Index("idx_source_catalog_syncs_hash", "payload_hash"),
    )


class Market(Base):
    """Canonical market shell for one event, bookmaker, line and live state."""

    __tablename__ = "markets"

    market_id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Integer, ForeignKey("events.id", ondelete="CASCADE"), nullable=False)
    bookie_id = Column(
        Integer,
        ForeignKey("bookies.bookie_id", ondelete="CASCADE"),
        nullable=False,
    )
    market_type_id = Column(
        SmallInteger,
        ForeignKey("canonical_market_types.market_type_id", ondelete="RESTRICT"),
        nullable=False,
    )

    # Canonical line/handicap value. It is nullable because moneyline and
    # other non-line markets legitimately have no line.
    line_value = Column(Numeric(18, 6))
    is_live = Column(Boolean, default=False, nullable=False)
    collected_at = Column(UTCDateTime(), default=utc_now, nullable=False)

    __table_args__ = (
        Index("idx_markets_event_market_type", "event_id", "market_type_id"),
        Index(
            "uq_markets_canonical_without_line",
            "event_id",
            "bookie_id",
            "market_type_id",
            "is_live",
            unique=True,
            postgresql_where=text(
                "market_type_id IS NOT NULL AND line_value IS NULL"
            ),
            sqlite_where=text("market_type_id IS NOT NULL AND line_value IS NULL"),
        ),
        Index(
            "uq_markets_canonical_with_line",
            "event_id",
            "bookie_id",
            "market_type_id",
            "line_value",
            "is_live",
            unique=True,
            postgresql_where=text(
                "market_type_id IS NOT NULL AND line_value IS NOT NULL"
            ),
            sqlite_where=text(
                "market_type_id IS NOT NULL AND line_value IS NOT NULL"
            ),
        ),
    )

    event = relationship("Event", back_populates="markets")
    bookie = relationship("Bookie", back_populates="markets")
    canonical_market_type = relationship(
        "CanonicalMarketType",
        back_populates="markets",
        foreign_keys=[market_type_id],
    )
    choices = relationship(
        "MarketChoice",
        back_populates="market",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return (
            f"<Market(market_id={self.market_id}, market_type_id={self.market_type_id}, "
            f"line_value='{self.line_value}', is_live={self.is_live})>"
        )


class MarketChoice(Base):
    """Provider-independent outcome identity within one canonical market."""

    __tablename__ = "market_choices"

    choice_id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(
        Integer,
        ForeignKey("markets.market_id", ondelete="CASCADE"),
        nullable=False,
    )
    choice_name = Column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("market_id", "choice_name", name="unique_choice_per_market"),
        Index("idx_market_choices_market_choice_name", "market_id", "choice_name"),
    )

    market = relationship("Market", back_populates="choices")
    quotes = relationship(
        "MarketChoiceQuote",
        back_populates="choice",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return (
            f"<MarketChoice(choice_id={self.choice_id}, "
            f"market_id={self.market_id}, name='{self.choice_name}')>"
        )


class MarketChoiceSnapshot(Base):
    """Append-only price observation linked to one exact quote instrument."""

    __tablename__ = "market_choice_snapshots"

    snapshot_id = Column(Integer, primary_key=True, autoincrement=True)
    quote_id = Column(
        Integer,
        ForeignKey("market_choice_quotes.quote_id", ondelete="CASCADE"),
        nullable=False,
    )
    odds_value = Column(Numeric(8, 3), nullable=False)
    collected_at = Column(UTCDateTime(), default=utc_now, nullable=False)
    source_collected_at = Column(UTCDateTime())
    source_limit = Column(Numeric(12, 3))
    exchange_size = Column(Numeric(18, 3))

    __table_args__ = (
        Index(
            "idx_market_choice_snapshots_quote_collected",
            quote_id,
            collected_at.desc(),
            snapshot_id.desc(),
        ),
    )

    quote = relationship("MarketChoiceQuote", back_populates="snapshots")


class MarketChoiceQuote(Base):
    """Current-state price instrument for one choice/source/side/depth."""

    __tablename__ = "market_choice_quotes"

    quote_id = Column(Integer, primary_key=True, autoincrement=True)
    choice_id = Column(
        Integer,
        ForeignKey("market_choices.choice_id", ondelete="CASCADE"),
        nullable=False,
    )
    source = Column(Text, nullable=False)
    exchange_side = Column(Text)
    exchange_level = Column(
        SmallInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    main_line = Column(Boolean)
    source_market_id = Column(Text)
    source_outcome_id = Column(Text)
    bookmaker_outcome_id = Column(Text)
    source_limit = Column(Numeric(12, 3))
    initial_odds = Column(Numeric(8, 3))
    initial_captured_at = Column(UTCDateTime())
    current_odds = Column(Numeric(8, 3))
    current_updated_at = Column(UTCDateTime())
    movement = Column(SmallInteger, default=0)
    created_at = Column(UTCDateTime(), default=utc_now)
    updated_at = Column(UTCDateTime(), default=utc_now, onupdate=utc_now)

    __table_args__ = (
        UniqueConstraint(
            "choice_id",
            "source",
            "exchange_side",
            "exchange_level",
            name="unique_market_choice_quote",
        ),
        Index("idx_market_choice_quotes_choice", "choice_id"),
        Index("idx_market_choice_quotes_source", "source"),
    )

    choice = relationship("MarketChoice", back_populates="quotes")
    snapshots = relationship("MarketChoiceSnapshot", back_populates="quote")

    def __repr__(self):
        return (
            f"<MarketChoiceQuote(quote_id={self.quote_id}, choice_id={self.choice_id}, "
            f"source='{self.source}', exchange_side='{self.exchange_side}', "
            f"initial={self.initial_odds}, current={self.current_odds})>"
        )


__all__ = [
    "CanonicalMarketType",
    "Market",
    "MarketChoice",
    "MarketChoiceQuote",
    "MarketChoiceSnapshot",
    "MarketOutcomeSourceMapping",
    "MarketSourceMapping",
    "SourceCatalogSync",
]
