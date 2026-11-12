"""Evidence for a final and interpretation of its decisive score."""

import re
from dataclasses import dataclass, field
from datetime import datetime

from .competition_identity import CompetitionIdentity


@dataclass(frozen=True, slots=True)
class ParticipantIdentity:
    source: str
    source_participant_id: int
    source_participant_name: str | None = field(default=None, compare=False)


@dataclass(frozen=True, slots=True)
class CupFinalEvidence:
    competition: CompetitionIdentity
    season_year: int | None
    status: str
    event_id: int | None = None
    season_id: int | None = None
    starts_at: datetime | None = None
    observed_at: datetime | None = None
    observation_id: str | None = None
    evidence_source: str | None = None
    home: ParticipantIdentity | None = None
    away: ParticipantIdentity | None = None
    result: dict | None = None
    winner: ParticipantIdentity | None = None
    decision: str | None = None
    candidate_event_ids: tuple[int, ...] = ()

    @property
    def loser(self) -> ParticipantIdentity | None:
        if self.winner is None:
            return None
        return self.away if self.winner == self.home else self.home


def decisive_winner(result: dict) -> tuple[str | None, str]:
    """Return the champion's side, leaving the match-result winner untouched."""
    home, away = result.get("home_score"), result.get("away_score")
    if any(isinstance(score, bool) or not isinstance(score, int) or score < 0 for score in (home, away)):
        return None, "invalid_score"
    sets = [str(result.get(f"{side}_sets") or "") for side in ("home", "away")]
    extra_time = [re.fullmatch(r"(\d+(?:-\d+)*)-\((\d+)\)(?:\+\d+)?", value) for value in sets]
    if any(extra_time):
        if not all(extra_time):
            return None, "contradictory_extra_time"
        totals = []
        for score, match in zip((home, away), extra_time):
            regulation = sum(int(part) for part in match.group(1).split("-"))
            total = regulation + int(match.group(2))
            if score not in (regulation, total):
                return None, "contradictory_extra_time"
            totals.append(total)
        home, away = totals
    penalties = [re.search(r"\+(\d+)$", value) for value in sets]
    if any("+" in value for value in sets):
        if not all(penalties) or home != away:
            return None, "contradictory_penalties"
        scores = [int(match.group(1)) for match in penalties]
        if scores[0] == scores[1]:
            return None, "undecided_penalties"
        side = "1" if scores[0] > scores[1] else "2"
        decision = "penalties"
    elif home != away:
        side = "1" if home > away else "2"
        decision = "extra_time" if any("(" in value for value in sets) else "regulation"
    else:
        return None, "no_decisive_result"
    recorded = result.get("winner")
    if recorded not in (None, side, "X" if decision in {"penalties", "extra_time"} else side):
        return None, "contradictory_winner"
    return side, decision
