"""Durable reporting invalidations, separate from sports domain models."""

from sqlalchemy import BigInteger, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.types import UTCDateTime


class ReportingRefreshState(Base):
    __tablename__ = "reporting_refresh_state"
    view_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    requested_generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    completed_generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at = mapped_column(UTCDateTime(), nullable=True)
    last_error = mapped_column(String(1024), nullable=True)
