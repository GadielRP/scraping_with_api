"""Deterministic candidate matching for Oddspapi fixtures."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from difflib import SequenceMatcher
import logging
import re
import unicodedata
from typing import Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from shared.timezone_utils import convert_utc_to_local

from .fixture_normalizer import OddspapiFixtureIdentity

logger = logging.getLogger(__name__)


SPORT_ALIASES = {
    "soccer": "football",
    "football": "football",
    "basketball": "basketball",
    "baseball": "baseball",
    "ice hockey": "ice hockey",
    "hockey": "ice hockey",
    "tennis": "tennis",
    "volleyball": "volleyball",
}

GENERIC_TEAM_SUFFIXES = {
    "fc",
    "cf",
    "club",
    "afc",
    "sc",
    "fk",
    "ac",
}
MAX_LOGGED_CANDIDATES = 10


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().casefold()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_generic_suffixes(value: str) -> str:
    tokens = [token for token in _normalize_text(value).split(" ") if token]
    if len(tokens) <= 1:
        return " ".join(tokens)

    start = 0
    end = len(tokens)
    while start < end and tokens[start] in GENERIC_TEAM_SUFFIXES:
        start += 1
    while end > start and tokens[end - 1] in GENERIC_TEAM_SUFFIXES:
        end -= 1
    stripped = " ".join(tokens[start:end]).strip()
    return stripped or " ".join(tokens)


def _compact_text(value: str) -> str:
    return _normalize_text(value).replace(" ", "")


def _text_similarity(left: object, right: object) -> float:
    left_normalized = _normalize_text(left)
    right_normalized = _normalize_text(right)
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0

    left_stripped = _strip_generic_suffixes(left_normalized)
    right_stripped = _strip_generic_suffixes(right_normalized)
    if left_stripped == right_stripped:
        return 1.0

    scores = [
        SequenceMatcher(None, left_normalized, right_normalized).ratio(),
        SequenceMatcher(None, left_stripped, right_stripped).ratio(),
    ]
    if _compact_text(left_normalized) == _compact_text(right_normalized):
        scores.append(1.0)
    return max(scores)


def _best_text_match(left_values: Iterable[object], right_values: Iterable[object]) -> float:
    best = 0.0
    left_list = [value for value in left_values if _normalize_text(value)]
    right_list = [value for value in right_values if _normalize_text(value)]
    for left_value in left_list:
        for right_value in right_list:
            best = max(best, _text_similarity(left_value, right_value))
            if best >= 1.0:
                return 1.0
    return best


def _normalize_sport(value: object) -> str:
    normalized = _normalize_text(value)
    return SPORT_ALIASES.get(normalized, normalized)


def _fixture_start_time_local(fixture: OddspapiFixtureIdentity) -> object | None:
    if fixture.start_time_local is not None:
        return fixture.start_time_local
    if fixture.start_time_utc is not None:
        return convert_utc_to_local(fixture.start_time_utc, keep_tzinfo=False)
    return None


def _event_start_time_local(event_start_time) -> object | None:
    if event_start_time is None:
        return None
    if getattr(event_start_time, "tzinfo", None) is not None:
        return convert_utc_to_local(event_start_time, keep_tzinfo=False)
    return event_start_time


def _candidate_preview(candidate: EventCandidateScore) -> str:
    delta = (
        f"{candidate.start_time_delta_minutes:.3f}m"
        if candidate.start_time_delta_minutes is not None
        else "n/a"
    )
    return (
        f"id={candidate.event_id} score={candidate.score:.3f} "
        f"orientation={candidate.orientation} delta={delta}"
    )


@dataclass
class EventCandidateScore:
    event_id: int
    score: float
    orientation: str
    start_time_delta_minutes: float | None
    sport_score: float
    time_score: float
    participant1_score: float
    participant2_score: float
    participants_score: float
    tournament_score: float
    both_teams_strong: bool
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "score": round(float(self.score), 3),
            "orientation": self.orientation,
            "start_time_delta_minutes": self.start_time_delta_minutes,
            "sport_score": round(float(self.sport_score), 3),
            "time_score": round(float(self.time_score), 3),
            "participant1_score": round(float(self.participant1_score), 3),
            "participant2_score": round(float(self.participant2_score), 3),
            "participants_score": round(float(self.participants_score), 3),
            "tournament_score": round(float(self.tournament_score), 3),
            "both_teams_strong": self.both_teams_strong,
            "reasons": list(self.reasons),
        }


@dataclass
class MatchDecision:
    resolved: bool
    needs_review: bool
    status: str
    match_method: str
    confidence: float | None
    canonical_event_id: int | None
    best_candidate_event_id: int | None
    second_candidate_event_id: int | None
    best_candidate_orientation: str | None
    score_gap: float | None
    candidate_scores: list[EventCandidateScore] = field(default_factory=list)
    best_candidate: EventCandidateScore | None = None
    second_best_candidate: EventCandidateScore | None = None


class OddspapiEventCandidateMatcher:
    AUTO_LINK_THRESHOLD = 0.93
    MIN_SCORE_GAP = 0.08
    MAX_AUTO_LINK_TIME_DELTA_MINUTES = 30
    STRONG_TEAM_THRESHOLD = 0.82

    @contextmanager
    def _session_scope(self, session: Session | None):
        if session is not None:
            yield session
            return
        with db_manager.get_session() as scoped_session:
            yield scoped_session

    @staticmethod
    def _time_score(delta_minutes: float | None) -> float:
        if delta_minutes is None:
            return 0.10
        if delta_minutes <= 5:
            return 1.00
        if delta_minutes <= 15:
            return 0.85
        if delta_minutes <= 30:
            return 0.70
        if delta_minutes <= 60:
            return 0.40
        return 0.10

    @staticmethod
    def _sport_score(fixture_sport: str | None, event_sport: str | None) -> float:
        if fixture_sport and event_sport:
            return 1.0 if _normalize_sport(fixture_sport) == _normalize_sport(event_sport) else 0.0
        if fixture_sport or event_sport:
            return 0.5
        return 0.5

    @staticmethod
    def _participant_values(participant, legacy_name: str | None) -> list[object]:
        values: list[object] = []
        if participant is not None:
            values.extend(
                [
                    getattr(participant, "name", None),
                    getattr(participant, "short_name", None),
                    getattr(participant, "code_name", None),
                ]
            )
        if legacy_name:
            values.append(legacy_name)
        return [value for value in values if value is not None]

    @staticmethod
    def _tournament_score(fixture: OddspapiFixtureIdentity, event: Event) -> float:
        competition = getattr(event, "competition_ref", None)
        fixture_competition_values = [
            fixture.tournament_name,
            fixture.tournament_slug,
        ]
        fixture_category_values = [
            fixture.category_name,
            fixture.category_slug,
        ]
        competition_values = [
            getattr(competition, "display_name", None) if competition else None,
            getattr(competition, "canonical_name", None) if competition else None,
            getattr(competition, "slug", None) if competition else None,
            getattr(competition, "unique_slug", None) if competition else None,
            getattr(competition, "category_name", None) if competition else None,
            getattr(event, "competition", None),
            getattr(event, "country", None),
        ]
        category_values = [
            getattr(competition, "category_name", None) if competition else None,
            getattr(event, "country", None),
        ]

        competition_score = _best_text_match(fixture_competition_values, competition_values)
        category_score = _best_text_match(fixture_category_values, category_values)

        if competition_score and category_score:
            return round((competition_score * 0.7) + (category_score * 0.3), 3)
        if competition_score:
            return round(competition_score, 3)
        if category_score:
            return round(category_score, 3)
        return 0.5

    @staticmethod
    def _orientation_score(
        fixture_participant_a: list[object],
        fixture_participant_b: list[object],
        home_values: list[object],
        away_values: list[object],
    ) -> tuple[str, float, float, float]:
        ordered_a = _best_text_match(fixture_participant_a, home_values)
        ordered_b = _best_text_match(fixture_participant_b, away_values)
        ordered_score = (ordered_a + ordered_b) / 2

        swapped_a = _best_text_match(fixture_participant_a, away_values)
        swapped_b = _best_text_match(fixture_participant_b, home_values)
        swapped_score = (swapped_a + swapped_b) / 2

        if swapped_score > ordered_score:
            return "swapped", swapped_a, swapped_b, swapped_score
        return "ordered", ordered_a, ordered_b, ordered_score

    def _score_candidate(self, fixture: OddspapiFixtureIdentity, event: Event) -> EventCandidateScore:
        fixture_sport = fixture.normalized_sport
        event_sport = getattr(event, "sport", None)
        sport_score = self._sport_score(fixture_sport, event_sport)

        start_time_delta_minutes: float | None = None
        fixture_start_time = _fixture_start_time_local(fixture)
        event_start_time = _event_start_time_local(getattr(event, "start_time_utc", None))
        if fixture_start_time is not None and event_start_time is not None:
            delta = abs(event_start_time - fixture_start_time)
            start_time_delta_minutes = round(abs(delta.total_seconds()) / 60.0, 3)
        time_score = self._time_score(start_time_delta_minutes)

        home_values = self._participant_values(
            getattr(event, "home_participant", None),
            getattr(event, "home_team", None),
        )
        away_values = self._participant_values(
            getattr(event, "away_participant", None),
            getattr(event, "away_team", None),
        )
        participant1_values = [
            fixture.participant1_name,
            fixture.participant1_short_name,
            fixture.participant1_abbr,
        ]
        participant2_values = [
            fixture.participant2_name,
            fixture.participant2_short_name,
            fixture.participant2_abbr,
        ]
        orientation, participant1_score, participant2_score, participants_score = self._orientation_score(
            participant1_values,
            participant2_values,
            home_values,
            away_values,
        )
        tournament_score = self._tournament_score(fixture, event)
        both_teams_strong = (
            participant1_score >= self.STRONG_TEAM_THRESHOLD
            and participant2_score >= self.STRONG_TEAM_THRESHOLD
        )

        score = (
            (sport_score * 0.20)
            + (time_score * 0.25)
            + (participants_score * 0.40)
            + (tournament_score * 0.15)
        )

        reasons: list[str] = []
        if sport_score == 0.0 and fixture_sport and event_sport:
            reasons.append("sport_mismatch")
            score = min(score, 0.25)
        if fixture_start_time is None:
            reasons.append("missing_start_time")
        elif start_time_delta_minutes is not None and start_time_delta_minutes > self.MAX_AUTO_LINK_TIME_DELTA_MINUTES:
            reasons.append("start_time_delta_gt_30")
        if not both_teams_strong:
            reasons.append("participant_match_weak")
        if tournament_score < 0.55:
            reasons.append("weak_tournament_alignment")

        return EventCandidateScore(
            event_id=event.id,
            score=round(score, 3),
            orientation=orientation,
            start_time_delta_minutes=start_time_delta_minutes,
            sport_score=sport_score,
            time_score=time_score,
            participant1_score=participant1_score,
            participant2_score=participant2_score,
            participants_score=participants_score,
            tournament_score=tournament_score,
            both_teams_strong=both_teams_strong,
            reasons=reasons,
        )

    def _query_candidates(self, fixture: OddspapiFixtureIdentity, session: Session) -> list[Event]:
        query = (
            session.query(Event)
            .options(
                joinedload(Event.home_participant),
                joinedload(Event.away_participant),
                joinedload(Event.competition_ref),
            )
        )

        fixture_start_time = _fixture_start_time_local(fixture)
        if fixture_start_time is not None:
            window_start = fixture_start_time - timedelta(hours=1)
            window_end = fixture_start_time + timedelta(hours=1)
            logger.info(
                "OddsPapi candidate search window for fixture %s: %s -> %s",
                fixture.fixture_id,
                window_start,
                window_end,
            )
            query = query.filter(
                Event.start_time_utc >= window_start,
                Event.start_time_utc <= window_end,
            )

        if fixture.normalized_sport:
            query = query.filter(func.lower(Event.sport) == fixture.normalized_sport.casefold())

        events = query.all()
        logger.info(
            "OddsPapi candidate query returned %s event(s) for fixture %s",
            len(events),
            fixture.fixture_id,
        )
        return events

    @staticmethod
    def _sort_key(candidate: EventCandidateScore) -> tuple[float, float, int]:
        delta = candidate.start_time_delta_minutes
        delta_sort = delta if delta is not None else 10_000.0
        return (-candidate.score, delta_sort, candidate.event_id)

    @staticmethod
    def _score_gap(best: EventCandidateScore, second: EventCandidateScore | None) -> float | None:
        if second is None:
            return None
        return round(best.score - second.score, 3)

    def find_best_match(
        self,
        fixture: OddspapiFixtureIdentity,
        session: Session | None = None,
    ) -> MatchDecision:
        with self._session_scope(session) as scoped_session:
            events = self._query_candidates(fixture, scoped_session)
            scored_candidates = [
                self._score_candidate(fixture, event)
                for event in events
            ]
            scored_candidates.sort(key=self._sort_key)

        if scored_candidates:
            preview_items = [
                _candidate_preview(candidate)
                for candidate in scored_candidates[:MAX_LOGGED_CANDIDATES]
            ]
            suffix = ""
            if len(scored_candidates) > MAX_LOGGED_CANDIDATES:
                suffix = f" ... (+{len(scored_candidates) - MAX_LOGGED_CANDIDATES} more)"
            logger.info(
                "OddsPapi candidate ranking for fixture %s: %s%s",
                fixture.fixture_id,
                " | ".join(preview_items),
                suffix,
            )

        if not scored_candidates:
            logger.info("OddsPapi fixture %s had no viable candidates", fixture.fixture_id)
            return MatchDecision(
                resolved=False,
                needs_review=True,
                status="unresolved_no_candidates",
                match_method="unresolved_no_candidates",
                confidence=None,
                canonical_event_id=None,
                best_candidate_event_id=None,
                second_candidate_event_id=None,
                best_candidate_orientation=None,
                score_gap=None,
                candidate_scores=[],
            )

        best_candidate = scored_candidates[0]
        second_best_candidate = scored_candidates[1] if len(scored_candidates) > 1 else None
        score_gap = self._score_gap(best_candidate, second_best_candidate)
        auto_link_allowed = (
            best_candidate.score >= self.AUTO_LINK_THRESHOLD
            and best_candidate.start_time_delta_minutes is not None
            and best_candidate.start_time_delta_minutes <= self.MAX_AUTO_LINK_TIME_DELTA_MINUTES
            and best_candidate.both_teams_strong
        )

        if second_best_candidate is None:
            auto_link_allowed = auto_link_allowed and best_candidate.score >= 0.96
        else:
            auto_link_allowed = auto_link_allowed and score_gap is not None and score_gap >= self.MIN_SCORE_GAP

        if auto_link_allowed:
            logger.info(
                "OddsPapi fixture %s resolved to event %s via %s (score=%.3f, gap=%s)",
                fixture.fixture_id,
                best_candidate.event_id,
                best_candidate.orientation,
                best_candidate.score,
                score_gap,
            )
            return MatchDecision(
                resolved=True,
                needs_review=False,
                status="resolved",
                match_method="deterministic_candidate_match",
                confidence=best_candidate.score,
                canonical_event_id=best_candidate.event_id,
                best_candidate_event_id=best_candidate.event_id,
                second_candidate_event_id=second_best_candidate.event_id if second_best_candidate else None,
                best_candidate_orientation=best_candidate.orientation,
                score_gap=score_gap,
                candidate_scores=scored_candidates,
                best_candidate=best_candidate,
                second_best_candidate=second_best_candidate,
            )

        if second_best_candidate is not None and score_gap is not None and score_gap < self.MIN_SCORE_GAP:
            status = "needs_review_ambiguous_candidates"
        else:
            status = "needs_review_low_confidence"

        logger.info(
            "OddsPapi fixture %s not auto-resolved: status=%s best_event=%s score=%.3f gap=%s orientation=%s",
            fixture.fixture_id,
            status,
            best_candidate.event_id,
            best_candidate.score,
            score_gap,
            best_candidate.orientation,
        )

        return MatchDecision(
            resolved=False,
            needs_review=True,
            status=status,
            match_method=status,
            confidence=best_candidate.score,
            canonical_event_id=None,
            best_candidate_event_id=best_candidate.event_id,
            second_candidate_event_id=second_best_candidate.event_id if second_best_candidate else None,
            best_candidate_orientation=best_candidate.orientation,
            score_gap=score_gap,
            candidate_scores=scored_candidates,
            best_candidate=best_candidate,
            second_best_candidate=second_best_candidate,
        )
