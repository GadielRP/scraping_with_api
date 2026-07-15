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
# Youth/national category markers omitted by some providers when the
# tournament already encodes the age band (e.g. "Albania" vs "Albania U20").
AGE_GROUP_TOKENS = {
    "u15",
    "u16",
    "u17",
    "u18",
    "u19",
    "u20",
    "u21",
    "u22",
    "u23",
}
AGE_GROUP_YEARS = {15, 16, 17, 18, 19, 20, 21, 22, 23}
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


def _normalize_person_text(value: object) -> str:
    """Normalize person names across provider-specific presentation styles."""
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""

    # Tennis feeds commonly use Surname, Given while canonical rows use
    # Given Surname. Treat the comma as a name-order delimiter.
    if "," in raw_value:
        comma_parts = [part.strip() for part in raw_value.split(",") if part.strip()]
        if len(comma_parts) == 2:
            raw_value = f"{comma_parts[1]} {comma_parts[0]}"

    return _normalize_text(raw_value)


def _person_text_similarity(left: object, right: object) -> float:
    left_normalized = _normalize_person_text(left)
    right_normalized = _normalize_person_text(right)
    if not left_normalized or not right_normalized:
        return 0.0
    if left_normalized == right_normalized:
        return 1.0

    left_tokens = sorted(left_normalized.split())
    right_tokens = sorted(right_normalized.split())
    if len(left_tokens) >= 2 and left_tokens == right_tokens:
        return 1.0

    return _text_similarity(left_normalized, right_normalized)


def _iter_age_group_spans(tokens: list[str]) -> list[tuple[int, int, str]]:
    """Return (start, end_exclusive, canonical_age_token) spans in token list."""
    spans: list[tuple[int, int, str]] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in AGE_GROUP_TOKENS:
            spans.append((index, index + 1, token))
            index += 1
            continue
        if (
            token in {"u", "under"}
            and index + 1 < len(tokens)
            and tokens[index + 1].isdigit()
            and int(tokens[index + 1]) in AGE_GROUP_YEARS
        ):
            year = int(tokens[index + 1])
            spans.append((index, index + 2, f"u{year}"))
            index += 2
            continue
        index += 1
    return spans


def _extract_age_group_token(value: str) -> str | None:
    tokens = [token for token in _normalize_text(value).split(" ") if token]
    spans = _iter_age_group_spans(tokens)
    if not spans:
        return None
    # Prefer a single consistent marker; mixed markers are left untouched.
    markers = {span[2] for span in spans}
    if len(markers) != 1:
        return None
    return next(iter(markers))


def _strip_age_group_tokens(value: str) -> str:
    tokens = [token for token in _normalize_text(value).split(" ") if token]
    if not tokens:
        return ""
    drop_indexes: set[int] = set()
    for start, end, _marker in _iter_age_group_spans(tokens):
        drop_indexes.update(range(start, end))
    stripped = [token for index, token in enumerate(tokens) if index not in drop_indexes]
    return " ".join(stripped).strip() or " ".join(tokens)


def _age_normalized_forms(left: str, right: str) -> tuple[str, str] | None:
    """Return comparable forms after safe age-group normalization, if applicable.

    - One side has Uxx and the other does not → strip the marked side.
    - Both sides share the same Uxx → strip both.
    - Different age bands (U20 vs U21) → no normalization (avoid false equals).
    """
    left_age = _extract_age_group_token(left)
    right_age = _extract_age_group_token(right)
    if left_age and not right_age:
        return _strip_age_group_tokens(left), right
    if right_age and not left_age:
        return left, _strip_age_group_tokens(right)
    if left_age and right_age and left_age == right_age:
        return _strip_age_group_tokens(left), _strip_age_group_tokens(right)
    return None


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

    left_compare = left_normalized
    right_compare = right_normalized
    age_forms = _age_normalized_forms(left_normalized, right_normalized)
    if age_forms is not None:
        left_compare, right_compare = age_forms
        if left_compare and right_compare and left_compare == right_compare:
            return 1.0

    left_stripped = _strip_generic_suffixes(left_compare)
    right_stripped = _strip_generic_suffixes(right_compare)
    if left_stripped == right_stripped:
        return 1.0

    scores = [
        SequenceMatcher(None, left_normalized, right_normalized).ratio(),
        SequenceMatcher(None, left_compare, right_compare).ratio(),
        SequenceMatcher(None, left_stripped, right_stripped).ratio(),
    ]
    if _compact_text(left_compare) == _compact_text(right_compare):
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


def _best_person_match(left_values: Iterable[object], right_values: Iterable[object]) -> float:
    best = 0.0
    left_list = [value for value in left_values if _normalize_person_text(value)]
    right_list = [value for value in right_values if _normalize_person_text(value)]
    for left_value in left_list:
        for right_value in right_list:
            best = max(best, _person_text_similarity(left_value, right_value))
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


def _stringify_values(values: Iterable[object]) -> list[str]:
    return [str(value) for value in values if value is not None and str(value).strip()]


def _normalize_values(values: Iterable[object]) -> set[str]:
    return {
        _normalize_text(value)
        for value in values
        if _normalize_text(value)
    }


def _unmatched_values(left_values: list[str], right_values: list[str]) -> list[str]:
    right_normalized = _normalize_values(right_values)
    unmatched = [
        value
        for value in left_values
        if _normalize_text(value) not in right_normalized
    ]
    return unmatched


def _unmatched_person_values(left_values: list[str], right_values: list[str]) -> list[str]:
    right_normalized = {
        _normalize_person_text(value)
        for value in right_values
        if _normalize_person_text(value)
    }
    return [
        value
        for value in left_values
        if _normalize_person_text(value) not in right_normalized
    ]


def _candidate_preview(candidate: EventCandidateScore, *, include_unmatched: bool) -> str:
    delta = (
        f"{candidate.start_time_delta_minutes:.1f}m"
        if candidate.start_time_delta_minutes is not None
        else "n/a"
    )
    db_teams = f"{candidate.event_home_values[0] if candidate.event_home_values else 'None'} vs {candidate.event_away_values[0] if candidate.event_away_values else 'None'}"
    subscores = (
        f"sport={candidate.sport_score:.2f} "
        f"time={candidate.time_score:.2f} "
        f"teams={candidate.participants_score:.2f} "
        f"(p1={candidate.participant1_score:.2f}, p2={candidate.participant2_score:.2f}) "
        f"tournament={candidate.tournament_score:.2f}"
    )
    
    db_tournament = candidate.event_tournament_values[0] if candidate.event_tournament_values else "None"

    preview = (
        f"  - [Event #{candidate.event_id}] Score: {candidate.score:.3f} | {candidate.orientation} | Delta: {delta}\n"
        f"    DB Event: {db_teams} | DB Tournament: {db_tournament}\n"
        f"    Fixture P1: {candidate.fixture_participant1_values}\n"
        f"    Fixture P2: {candidate.fixture_participant2_values}\n"
        f"    Event Home: {candidate.event_home_values}\n"
        f"    Event Away: {candidate.event_away_values}\n"
        f"    Fixture Tournament: {candidate.fixture_tournament_values}\n"
        f"    Event Tournament: {candidate.event_tournament_values}\n"
        f"    Scores: {subscores}"
    )
    if include_unmatched:
        preview += (
            f"\n    Unmatched Fixture P1: {candidate.fixture_participant1_unmatched_values}"
            f"\n    Unmatched Fixture P2: {candidate.fixture_participant2_unmatched_values}"
            f"\n    Unmatched Event Home: {candidate.event_home_unmatched_values}"
            f"\n    Unmatched Event Away: {candidate.event_away_unmatched_values}"
            f"\n    Unmatched Tournament: {candidate.event_tournament_unmatched_values}"
        )
    return preview


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
    fixture_participant1_values: list[str] = field(default_factory=list)
    fixture_participant2_values: list[str] = field(default_factory=list)
    event_home_values: list[str] = field(default_factory=list)
    event_away_values: list[str] = field(default_factory=list)
    fixture_participant1_unmatched_values: list[str] = field(default_factory=list)
    fixture_participant2_unmatched_values: list[str] = field(default_factory=list)
    event_home_unmatched_values: list[str] = field(default_factory=list)
    event_away_unmatched_values: list[str] = field(default_factory=list)
    fixture_tournament_values: list[str] = field(default_factory=list)
    event_tournament_values: list[str] = field(default_factory=list)
    event_tournament_unmatched_values: list[str] = field(default_factory=list)
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
            "fixture_participant1_values": list(self.fixture_participant1_values),
            "fixture_participant2_values": list(self.fixture_participant2_values),
            "event_home_values": list(self.event_home_values),
            "event_away_values": list(self.event_away_values),
            "fixture_participant1_unmatched_values": list(self.fixture_participant1_unmatched_values),
            "fixture_participant2_unmatched_values": list(self.fixture_participant2_unmatched_values),
            "event_home_unmatched_values": list(self.event_home_unmatched_values),
            "event_away_unmatched_values": list(self.event_away_unmatched_values),
            "fixture_tournament_values": list(self.fixture_tournament_values),
            "event_tournament_values": list(self.event_tournament_values),
            "event_tournament_unmatched_values": list(self.event_tournament_unmatched_values),
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
    NEAR_EXACT_TIME_DELTA_MINUTES = 5
    STRONG_TEAM_THRESHOLD = 0.82
    # Contextual team relaxation: one near-perfect anchor + strong tournament/time
    # may absorb colloquial/nickname variance on the weaker side.
    RELAXED_TEAM_THRESHOLD = 0.70
    ANCHOR_TEAM_THRESHOLD = 0.95
    STRONG_TOURNAMENT_FOR_TEAM_RELAXATION = 0.90
    IDENTITY_PARTICIPANT_THRESHOLD = 0.98
    IDENTITY_TOURNAMENT_THRESHOLD = 0.75

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

    @classmethod
    def _is_near_exact_time(cls, candidate: EventCandidateScore) -> bool:
        delta = candidate.start_time_delta_minutes
        return delta is not None and delta <= cls.NEAR_EXACT_TIME_DELTA_MINUTES

    @classmethod
    def _teams_acceptable_for_auto_link(cls, candidate: EventCandidateScore) -> bool:
        """Strict both-teams gate, with one contextual relaxation path.

        Relaxation requires sport agreement, near-exact kickoff, a strong
        tournament alignment, one near-perfect participant, and a still-plausible
        weaker participant. This avoids hard-blocking aliases such as
        "Weston Bears" vs "Weston Workers" when every other signal is strong.
        """
        if candidate.both_teams_strong:
            return True

        weaker = min(candidate.participant1_score, candidate.participant2_score)
        stronger = max(candidate.participant1_score, candidate.participant2_score)
        return (
            candidate.sport_score == 1.0
            and cls._is_near_exact_time(candidate)
            and candidate.tournament_score >= cls.STRONG_TOURNAMENT_FOR_TEAM_RELAXATION
            and stronger >= cls.ANCHOR_TEAM_THRESHOLD
            and weaker >= cls.RELAXED_TEAM_THRESHOLD
        )

    @classmethod
    def _is_strong_identity_match(cls, candidate: EventCandidateScore) -> bool:
        """High-confidence identity that can bypass the composite score floor.

        Tournament taxonomy often diverges across providers (cup vs friendly).
        When both participants are identity-grade and kickoff is near-exact,
        tournament mismatch alone should not force manual review.
        """
        if candidate.sport_score != 1.0:
            return False
        if (
            candidate.participant1_score < cls.IDENTITY_PARTICIPANT_THRESHOLD
            or candidate.participant2_score < cls.IDENTITY_PARTICIPANT_THRESHOLD
        ):
            return False
        if candidate.tournament_score >= cls.IDENTITY_TOURNAMENT_THRESHOLD:
            return True
        return cls._is_near_exact_time(candidate)

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
        ordered_a = _best_person_match(fixture_participant_a, home_values)
        ordered_b = _best_person_match(fixture_participant_b, away_values)
        ordered_score = (ordered_a + ordered_b) / 2

        swapped_a = _best_person_match(fixture_participant_a, away_values)
        swapped_b = _best_person_match(fixture_participant_b, home_values)
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
        fixture_participant1_unmatched_values = _unmatched_person_values(_stringify_values(participant1_values), home_values + away_values)
        fixture_participant2_unmatched_values = _unmatched_person_values(_stringify_values(participant2_values), home_values + away_values)
        event_home_unmatched_values = _unmatched_person_values(home_values, _stringify_values(participant1_values) + _stringify_values(participant2_values))
        event_away_unmatched_values = _unmatched_person_values(away_values, _stringify_values(participant1_values) + _stringify_values(participant2_values))

        competition = getattr(event, "competition_ref", None)
        raw_competition_values = [
            getattr(competition, "display_name", None) if competition else None,
            getattr(competition, "category_name", None) if competition else None,
            getattr(event, "competition", None),
            getattr(event, "country", None),
        ]
        event_tournament_values = _stringify_values(raw_competition_values)
        fixture_tournament_values = _stringify_values([
            fixture.tournament_name,
            fixture.category_name,
            fixture.tournament_slug,
            fixture.category_slug,
        ])
        event_tournament_unmatched_values = _unmatched_values(
            event_tournament_values,
            fixture_tournament_values
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
            fixture_participant1_values=_stringify_values(participant1_values),
            fixture_participant2_values=_stringify_values(participant2_values),
            event_home_values=_stringify_values(home_values),
            event_away_values=_stringify_values(away_values),
            fixture_participant1_unmatched_values=fixture_participant1_unmatched_values,
            fixture_participant2_unmatched_values=fixture_participant2_unmatched_values,
            event_home_unmatched_values=event_home_unmatched_values,
            event_away_unmatched_values=event_away_unmatched_values,
            fixture_tournament_values=fixture_tournament_values,
            event_tournament_values=event_tournament_values,
            event_tournament_unmatched_values=event_tournament_unmatched_values,
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
        return self.find_best_match_from_candidates(fixture, events)

    def find_best_match_from_candidates(
        self,
        fixture: OddspapiFixtureIdentity,
        candidate_events: list[Event],
    ) -> MatchDecision:
        """Score an already-loaded candidate set without issuing DB queries."""
        scored_candidates = [
            self._score_candidate(fixture, event)
            for event in candidate_events
        ]
        scored_candidates.sort(key=self._sort_key)

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
        strong_identity_match = self._is_strong_identity_match(best_candidate)
        teams_acceptable = self._teams_acceptable_for_auto_link(best_candidate)
        auto_link_allowed = (
            (
                best_candidate.score >= self.AUTO_LINK_THRESHOLD
                or strong_identity_match
            )
            and best_candidate.start_time_delta_minutes is not None
            and best_candidate.start_time_delta_minutes <= self.MAX_AUTO_LINK_TIME_DELTA_MINUTES
            and teams_acceptable
        )

        competing_candidate_is_strong = bool(
            second_best_candidate is not None
            and second_best_candidate.both_teams_strong
        )
        if second_best_candidate is None:
            auto_link_allowed = auto_link_allowed and (
                best_candidate.score >= 0.96
                or strong_identity_match
            )
        else:
            auto_link_allowed = auto_link_allowed and (
                (
                    score_gap is not None
                    and score_gap >= self.MIN_SCORE_GAP
                )
                or not competing_candidate_is_strong
            )

        if auto_link_allowed:
            logger.info(
                "OddsPapi fixture %s resolved to event %s via %s (score=%.3f, gap=%s%s)",
                fixture.fixture_id,
                best_candidate.event_id,
                best_candidate.orientation,
                best_candidate.score,
                score_gap,
                ", strong identity" if strong_identity_match else "",
            )
            logger.info(
                "Matched OddsPapi candidate for fixture %s:\n%s",
                fixture.fixture_id,
                _candidate_preview(best_candidate, include_unmatched=False),
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

        if (
            competing_candidate_is_strong
            and score_gap is not None
            and score_gap < self.MIN_SCORE_GAP
        ):
            status = "needs_review_ambiguous_candidates"
        else:
            status = "needs_review_low_confidence"

        logger.warning(
            "OddsPapi fixture %s not auto-resolved: status=%s best_event=%s score=%.3f gap=%s orientation=%s",
            fixture.fixture_id,
            status,
            best_candidate.event_id,
            best_candidate.score,
            score_gap,
            best_candidate.orientation,
        )
        preview_items = [
            _candidate_preview(candidate, include_unmatched=True)
            for candidate in scored_candidates[:2]
        ]
        suffix = ""
        if len(scored_candidates) > 2:
            suffix = f"\n  ... (+{len(scored_candidates) - 2} more candidates omitted; showing top 2)"
        logger.info(
            "OddsPapi unresolved candidate ranking for fixture %s [showing top 2]:\n%s%s",
            fixture.fixture_id,
            "\n".join(preview_items),
            suffix,
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
