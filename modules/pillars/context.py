from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ParticipantContext:
    participant_id: Optional[int]
    source: Optional[str]
    source_participant_id: Optional[int]
    name: str
    slug: Optional[str]
    short_name: Optional[str]
    source_status: str


@dataclass
class CompetitionContext:
    competition_id: Optional[int]
    source: Optional[str]
    source_tournament_id: Optional[int]
    source_unique_tournament_id: Optional[int]
    canonical_name: Optional[str]
    display_name: str
    slug: Optional[str]
    unique_slug: Optional[str]
    category_id: Optional[int]
    category_name: Optional[str]
    number_of_teams: Optional[int]
    number_of_teams_source: Optional[str]
    source_status: str


@dataclass
class EventContext:
    event_id: int
    custom_id: Optional[str]
    sport: str
    season_id: Optional[int]
    season_name: Optional[str]
    season_year: Optional[int]
    start_time_utc: datetime
    minutes_until_start: Optional[int]
    discovery_source: Optional[str]
    home: ParticipantContext
    away: ParticipantContext
    competition: CompetitionContext
    participants_label: str
    context_status: str
