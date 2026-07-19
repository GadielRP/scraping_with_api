"""Cheap prefilter before fuzzy OddsPapi candidate scoring in discovery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
import unicodedata
from typing import Iterable

from infrastructure.persistence.models import Event
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity

MIN_SHORTLIST = 8
TOP_K = 40
TIME_WINDOWS_MINUTES = (5, 15, 30, 60)
MIN_TOKEN_OVERLAP = 0.01
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


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().casefold()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _strip_age_tokens(normalized: str) -> str:
    tokens = [token for token in normalized.split(" ") if token]
    kept = [token for token in tokens if token not in AGE_GROUP_TOKENS]
    return " ".join(kept) if kept else normalized


def _token_set(*values: object) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        normalized = _normalize_text(value)
        if not normalized:
            continue
        stripped = _strip_age_tokens(normalized)
        tokens.update(token for token in stripped.split() if token)
    return tokens


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def _best_side_overlap(left: set[str], right_a: set[str], right_b: set[str]) -> float:
    return max(_jaccard(left, right_a), _jaccard(left, right_b))


def _event_time(event: Event) -> datetime | None:
    start = getattr(event, "start_time_utc", None)
    return start if isinstance(start, datetime) else None


def _event_home_tokens(event: Event) -> set[str]:
    participant = getattr(event, "home_participant", None)
    return _token_set(
        getattr(participant, "name", None) if participant else None,
        getattr(participant, "short_name", None) if participant else None,
        getattr(participant, "code_name", None) if participant else None,
        getattr(event, "home_team", None),
    )


def _event_away_tokens(event: Event) -> set[str]:
    participant = getattr(event, "away_participant", None)
    return _token_set(
        getattr(participant, "name", None) if participant else None,
        getattr(participant, "short_name", None) if participant else None,
        getattr(participant, "code_name", None) if participant else None,
        getattr(event, "away_team", None),
    )


def _event_tournament_tokens(event: Event) -> set[str]:
    competition = getattr(event, "competition_ref", None)
    return _token_set(
        getattr(competition, "display_name", None) if competition else None,
        getattr(competition, "canonical_name", None) if competition else None,
        getattr(competition, "slug", None) if competition else None,
        getattr(event, "competition", None),
        getattr(event, "country", None),
    )


@dataclass(frozen=True)
class ShortlistResult:
    events: list[Event]
    pool_size: int
    shortlist_size: int
    widened_time_window: bool
    used_temporal_fallback: bool


@dataclass
class _RankedCandidate:
    event: Event
    delta_minutes: float
    token_score: float


def shortlist_candidates(
    fixture: OddspapiFixtureIdentity,
    candidate_events: Iterable[Event],
    *,
    fixture_time: datetime | None,
    min_shortlist: int = MIN_SHORTLIST,
    top_k: int = TOP_K,
) -> ShortlistResult:
    """Reduce a ±1h pool to a small fuzzy-scoring set.

    Stage A: widen time windows until enough candidates exist.
    Stage B: drop near-zero team-token overlap when possible.
    Stage C: keep top-K by (delta asc, token_score desc).
    """
    pool = list(candidate_events)
    pool_size = len(pool)
    if pool_size <= top_k:
        return ShortlistResult(
            events=pool,
            pool_size=pool_size,
            shortlist_size=pool_size,
            widened_time_window=False,
            used_temporal_fallback=False,
        )

    p1_tokens = _token_set(
        fixture.participant1_name,
        fixture.participant1_short_name,
        fixture.participant1_abbr,
    )
    p2_tokens = _token_set(
        fixture.participant2_name,
        fixture.participant2_short_name,
        fixture.participant2_abbr,
    )
    tournament_tokens = _token_set(
        fixture.tournament_name,
        fixture.tournament_slug,
        fixture.category_name,
        fixture.category_slug,
    )

    ranked: list[_RankedCandidate] = []
    for event in pool:
        event_time = _event_time(event)
        if fixture_time is None or event_time is None:
            delta_minutes = 10_000.0
        else:
            delta_minutes = abs((event_time - fixture_time).total_seconds()) / 60.0

        home_tokens = _event_home_tokens(event)
        away_tokens = _event_away_tokens(event)
        ordered = (
            _best_side_overlap(p1_tokens, home_tokens, away_tokens)
            + _best_side_overlap(p2_tokens, away_tokens, home_tokens)
        ) / 2
        swapped = (
            _best_side_overlap(p1_tokens, away_tokens, home_tokens)
            + _best_side_overlap(p2_tokens, home_tokens, away_tokens)
        ) / 2
        team_score = max(ordered, swapped)
        tournament_score = _jaccard(tournament_tokens, _event_tournament_tokens(event))
        token_score = (team_score * 0.85) + (tournament_score * 0.15)
        ranked.append(
            _RankedCandidate(
                event=event,
                delta_minutes=delta_minutes,
                token_score=token_score,
            )
        )

    widened = False
    selected: list[_RankedCandidate] = []
    if fixture_time is None:
        selected = list(ranked)
    else:
        for window in TIME_WINDOWS_MINUTES:
            selected = [item for item in ranked if item.delta_minutes <= window]
            if window > TIME_WINDOWS_MINUTES[0] and selected:
                widened = True
            if len(selected) >= min_shortlist or window == TIME_WINDOWS_MINUTES[-1]:
                break
        if not selected:
            selected = list(ranked)

    used_temporal_fallback = False
    filtered = [item for item in selected if item.token_score >= MIN_TOKEN_OVERLAP]
    if filtered:
        selected = filtered
    elif selected:
        used_temporal_fallback = True

    selected.sort(key=lambda item: (item.delta_minutes, -item.token_score, item.event.id))
    shortlisted = [item.event for item in selected[:top_k]]
    return ShortlistResult(
        events=shortlisted,
        pool_size=pool_size,
        shortlist_size=len(shortlisted),
        widened_time_window=widened,
        used_temporal_fallback=used_temporal_fallback,
    )
