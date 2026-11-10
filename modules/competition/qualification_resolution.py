"""Per-evaluation LaLiga/Copa allocation. No database access or catalogue mutation."""

from dataclasses import dataclass, replace
from datetime import datetime
import re

from modules.competition import competition_ids as ids
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.competitive_rules import CompetitionRules, ObjectiveRule
from modules.competition.rule_terms import ObjectiveType
from shared.temporal import as_utc


@dataclass(frozen=True, slots=True)
class ParticipantIdentity:
    source: str
    source_participant_id: int


def participant_identity(row) -> ParticipantIdentity | None:
    value = row.get("participant_identity") if isinstance(row, dict) else None
    if isinstance(value, ParticipantIdentity):
        return value
    if isinstance(value, dict) and value.get("source") and value.get("source_participant_id") is not None:
        return ParticipantIdentity(value["source"], int(value["source_participant_id"]))
    return None


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


@dataclass(frozen=True, slots=True)
class QualificationResolution:
    season_year: int | None
    destination_season: str | None
    as_of: datetime
    status: str
    reason: str
    effective_rules: CompetitionRules
    cup: CupFinalEvidence
    champion_rank: int | None = None
    champion_destination: CompetitionIdentity | None = None
    # A cup champion does not contest the league's remaining UEL/UECL berths.
    eligible_participants: tuple[ParticipantIdentity, ...] = ()
    league_assignments: tuple[tuple[int, CompetitionIdentity], ...] = ()
    policy_sources: tuple[str, ...] = ()

    @property
    def applied(self) -> bool:
        return self.status in {"PROVISIONAL", "FINAL"}

    def protects(self, standing, rule: ObjectiveRule | None) -> bool:
        return (self.applied and participant_identity(standing) == self.cup.winner
                and (rule is None or rule.destination_competition_identity in {
                    ids.UEFA_EUROPA_LEAGUE, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION}))

    def cutoff(self, rule, rank, standings):
        """Select an actual eligible opponent; never approximate a missing rank."""
        if not self.applied or rule is None or rule.destination_competition_identity not in {
                ids.UEFA_EUROPA_LEAGUE, ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION}:
            return None
        rows = standings.values() if isinstance(standings, dict) else standings
        eligible = [row for row in rows if participant_identity(row) in self.eligible_participants]
        target = rule.reference_rank
        if rank == target:
            return min((row for row in eligible if row["rank"] > target),
                       key=lambda row: row["rank"], default=None)
        return next((row for row in eligible if row["rank"] == target), None)


def resolve_laliga_qualification(base: CompetitionRules, table, cup: CupFinalEvidence,
                               season_year: int | None, as_of: datetime,
                               *, policy_sources: tuple[str, ...] = ()) -> QualificationResolution:
    """Allocate the audited four-CL/two-EL/one-Conference baseline for 2026/27."""
    as_of = as_utc(as_of)
    result = QualificationResolution(
        season_year, f"{season_year + 1}/{str(season_year + 2)[-2:]}" if season_year else None,
        as_of, "PENDING", "cup_pending", base, cup, policy_sources=policy_sources)
    if season_year != 2026 or base.reference_season != "2026/27":
        return replace(result, reason="unsupported_cycle")
    if cup.competition != ids.COPA_DEL_REY or cup.season_year != season_year:
        return replace(result, reason="wrong_cup_edition")
    if (cup.status != "DECIDED" or cup.winner is None or cup.starts_at is None
            or cup.observed_at is None):
        return replace(result, reason=cup.status)
    if as_utc(cup.starts_at) >= as_of or as_utc(cup.observed_at) > as_of:
        return replace(result, reason="cup_evidence_after_cutoff")
    rows = list(table.values() if isinstance(table, dict) else table or [])
    if any(not isinstance(row, dict) or not isinstance(row.get("rank"), int) for row in rows):
        return replace(result, reason="incomplete_participant_table")
    rows.sort(key=lambda row: row["rank"])
    identities = [participant_identity(row) for row in rows]
    if (len(rows) != base.number_of_teams or [row.get("rank") for row in rows] != list(range(1, len(rows) + 1))
            or None in identities or len(set(identities)) != len(rows)):
        return replace(result, reason="incomplete_participant_table")
    external = {rule.destination_competition_identity: rule for rule in base.objectives
                if rule.objective_type == ObjectiveType.COMPETITION_QUALIFICATION}
    cl = external.get(ids.UEFA_CHAMPIONS_LEAGUE)
    el = external.get(ids.UEFA_EUROPA_LEAGUE)
    conference = external.get(ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION)
    if not cl or not el or not conference or (cl.rank_start, cl.rank_end, el.rank_start, el.rank_end,
                                            conference.rank_start, conference.rank_end) != (1, 4, 5, 5, 6, 6):
        return replace(result, reason="unsupported_base_allocation")
    champion_rank = next((row["rank"] for row in rows if participant_identity(row) == cup.winner), None)
    champion_in_cl = champion_rank is not None and champion_rank <= cl.rank_end
    eligible = [row for row in rows if row["rank"] > cl.rank_end and participant_identity(row) != cup.winner]
    el_rows = eligible[:2 if champion_in_cl else 1]
    conference_row = eligible[len(el_rows)]
    objectives = []
    for rule in base.objectives:
        if rule is el:
            objectives.append(replace(rule, rank_start=el_rows[0]["rank"], rank_end=el_rows[-1]["rank"],
                                      reference_rank=el_rows[-1]["rank"]))
        elif rule is conference:
            objectives.append(replace(rule, rank_start=conference_row["rank"], rank_end=conference_row["rank"],
                                      reference_rank=conference_row["rank"]))
        else:
            objectives.append(rule)
    assignments = [(row["rank"], ids.UEFA_CHAMPIONS_LEAGUE) for row in rows[:cl.rank_end]]
    assignments.extend((row["rank"], ids.UEFA_EUROPA_LEAGUE) for row in el_rows)
    if champion_rank is not None and not champion_in_cl:
        assignments.append((champion_rank, ids.UEFA_EUROPA_LEAGUE))
    assignments.append((conference_row["rank"], ids.UEFA_CONFERENCE_LEAGUE_QUALIFICATION))
    complete = all(row.get("games_played", 0) >= base.total_regular_season_games for row in rows)
    return replace(result, status="FINAL" if complete else "PROVISIONAL",
                   reason="cup_winner_in_champions" if champion_in_cl else "cup_winner_uses_europa_berth",
                   effective_rules=replace(base, objectives=tuple(objectives)), champion_rank=champion_rank,
                   champion_destination=ids.UEFA_CHAMPIONS_LEAGUE if champion_in_cl else ids.UEFA_EUROPA_LEAGUE,
                   eligible_participants=tuple(identity for identity in identities if identity != cup.winner),
                   league_assignments=tuple(sorted(assignments)))
