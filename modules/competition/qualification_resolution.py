"""Allocate configured external and league berths from a profile and evidence."""

from collections import Counter
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime

from shared.temporal import as_utc

from .competition_identity import CompetitionIdentity
from .competitive_rules import CompetitionRules, ObjectiveRule
from .final_outcome import CupFinalEvidence, ParticipantIdentity
from .rule_terms import AggregationScope, ObjectiveType, RankingScope, RuleStatus, SelectionMethod


def _standing_identity(row: dict | None) -> ParticipantIdentity | None:
    value = row.get("participant_identity") if row is not None else None
    if isinstance(value, ParticipantIdentity):
        return value
    if isinstance(value, dict) and value.get("source") and value.get("source_participant_id") is not None:
        return ParticipantIdentity(value["source"], int(value["source_participant_id"]))
    return None


@dataclass(frozen=True, slots=True)
class CupQualificationAllocation:
    """Resolution of one configured cup berth against the league table."""

    evidence: CupFinalEvidence
    champion_rank: int | None
    champion_destination: CompetitionIdentity
    berth_awarded: bool
    configured_competition_name: str | None = None
    configured_destination_competition_name: str | None = None
    policy_sources: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class QualificationResolution:
    season_year: int | None
    destination_season: str | None
    as_of: datetime
    status: str
    reason: str
    effective_rules: CompetitionRules
    cups: tuple[CupFinalEvidence, ...]
    eligible_participants: tuple[ParticipantIdentity, ...] = ()
    league_assignments: tuple[tuple[int, CompetitionIdentity], ...] = ()
    policy_sources: tuple[str, ...] = ()
    cup_berth_awarded: bool = False
    affected_objectives: tuple[ObjectiveRule, ...] = ()
    cup_allocations: tuple[CupQualificationAllocation, ...] = ()

    @property
    def applied(self) -> bool:
        return self.status in {"AS_OF_TABLE", "FINAL"}

    @property
    def cup(self) -> CupFinalEvidence | None:
        """First evidence item, retained as a convenience for single-cup callers."""
        return self.cups[0] if self.cups else None

    def protects(self, standing: dict | None, rule: ObjectiveRule | None) -> bool:
        if not self.applied or (rule is not None and rule not in self.affected_objectives):
            return False
        identity = _standing_identity(standing)
        return any(
            allocation.berth_awarded and allocation.evidence.winner == identity
            for allocation in self.cup_allocations
        )

    def affects_cutoff(self, rule: ObjectiveRule | None) -> bool:
        return self.applied and rule in self.affected_objectives

    def can_contest(self, standing: dict) -> bool:
        return _standing_identity(standing) in self.eligible_participants


def _ranked_league_objectives(profile: CompetitionRules) -> list[ObjectiveRule]:
    return sorted(
        (rule for rule in profile.objectives or ()
         if rule.objective_type == ObjectiveType.COMPETITION_QUALIFICATION
         and rule.status == RuleStatus.CONFIGURED
         and rule.selection_method == SelectionMethod.RANK_RANGE
         and rule.ranking_scope == RankingScope.GLOBAL
         and rule.aggregation_scope == AggregationScope.TOURNAMENT),
        key=lambda rule: rule.rank_start,
    )


def _select_ranks(
    start: int,
    places: int,
    identity_by_rank: Mapping[int, ParticipantIdentity],
    assigned: Collection[ParticipantIdentity],
) -> list[int] | None:
    """Fill the configured slots in rank order, skipping participants with a berth."""
    ranks = []
    rank = start
    while len(ranks) < places:
        identity = identity_by_rank.get(rank)
        if identity is None:
            return None
        if identity not in assigned:
            ranks.append(rank)
        rank += 1
    return ranks


def _effective_ranges(rule: ObjectiveRule, ranks: list[int]) -> tuple[ObjectiveRule, ...]:
    """Represent holes left by already qualified participants as separate intervals."""
    start = previous = ranks[0]
    ranges = []
    for rank in ranks[1:]:
        if rank != previous + 1:
            ranges.append((start, previous))
            start = rank
        previous = rank
    ranges.append((start, previous))
    return tuple(
        replace(rule, rank_start=start, rank_end=end, reference_rank=ranks[-1])
        for start, end in ranges
    )


def _normalize_evidence(cups: CupFinalEvidence | Sequence[CupFinalEvidence]) -> tuple[CupFinalEvidence, ...]:
    if isinstance(cups, CupFinalEvidence):
        return (cups,)
    return tuple(cups)


def resolve_qualification(
    profile: CompetitionRules,
    table: Mapping[str, dict] | Sequence[dict] | None,
    cups: CupFinalEvidence | Sequence[CupFinalEvidence],
    season_year: int | None,
    as_of: datetime,
    *,
    target_event_starts_at: datetime | None = None,
) -> QualificationResolution | None:
    """Resolve every configured cup berth alongside the league's ranked berths."""
    policies = profile.cup_qualifications
    if not policies:
        return None

    evidence = _normalize_evidence(cups)
    as_of = as_utc(as_of)
    destination_year = None
    first_offset = policies[0].destination_season_offset
    if season_year is not None and first_offset is not None:
        destination_year = season_year + first_offset
    pending = QualificationResolution(
        season_year=season_year,
        destination_season=f"{destination_year}/{(destination_year + 1) % 100:02d}"
        if destination_year is not None else None,
        as_of=as_of,
        status="PENDING",
        reason="cup_evidence_count_mismatch",
        effective_rules=profile,
        cups=evidence,
        policy_sources=tuple(source for policy in policies for source in policy.sources),
    )
    if len(evidence) != len(policies):
        return pending

    # Evidence follows profile order. Destination objective rank decides across
    # destinations; profile order decides between cups targeting the same one.
    for policy, cup in zip(policies, evidence):
        if cup.competition != policy.competition_identity or cup.season_year != season_year:
            return replace(pending, reason="wrong_cup_edition")
        if cup.status != "DECIDED":
            return replace(pending, reason=cup.status)
        if cup.winner is None or cup.starts_at is None or cup.observed_at is None:
            return replace(pending, reason="incomplete_cup_evidence")
        if (target_event_starts_at is not None
                and as_utc(cup.starts_at) >= as_utc(target_event_starts_at)):
            return replace(pending, reason="cup_final_after_target_event")
        if as_utc(cup.starts_at) >= as_of or as_utc(cup.observed_at) > as_of:
            return replace(pending, reason="cup_evidence_after_cutoff")

    league_objectives = _ranked_league_objectives(profile)
    for policy in policies:
        matches = [rule for rule in league_objectives
                   if rule.destination_competition_identity == policy.destination_competition_identity]
        if len(matches) != 1:
            return replace(pending, reason="unresolved_cup_destination")

    rows = list(table.values() if isinstance(table, Mapping) else table or ())
    if any(not isinstance(row, dict) for row in rows):
        return replace(pending, reason="incomplete_participant_table")
    ranked_participants = [(row.get("rank"), _standing_identity(row)) for row in rows]
    rank_counts = Counter(rank for rank, _ in ranked_participants)
    identity_counts = Counter(identity for _, identity in ranked_participants)
    identity_by_rank = {
        rank: identity for rank, identity in ranked_participants
        if identity is not None and isinstance(rank, int)
        and rank_counts[rank] == 1 and identity_counts[identity] == 1
    }
    champion_ranks = []
    for cup in evidence:
        champion_rank = next((rank for rank, identity in identity_by_rank.items()
                              if identity == cup.winner), None)
        if champion_rank is None and (
            identity_counts[cup.winner] or any(identity is None for _, identity in ranked_participants)
        ):
            return replace(pending, reason="incomplete_participant_table")
        champion_ranks.append(champion_rank)

    assignments: dict[ParticipantIdentity, CompetitionIdentity] = {}
    league_assignments = []
    replacements = {}
    affected_objectives = []
    cup_berth_awarded = [False] * len(policies)
    cup_destinations = {policy.destination_competition_identity for policy in policies}
    reached_cup_destination = False

    for rule in league_objectives:
        destination = rule.destination_competition_identity
        places = rule.rank_end - rule.rank_start + 1
        if destination in cup_destinations:
            reached_cup_destination = True
            for index, (policy, cup) in enumerate(zip(policies, evidence)):
                if policy.destination_competition_identity != destination:
                    continue
                champion = cup.winner
                if champion in assignments:
                    places += int(policy.reallocate_to_league)
                else:
                    assignments[champion] = destination
                    cup_berth_awarded[index] = True
                    if champion_ranks[index] is not None:
                        league_assignments.append((champion_ranks[index], destination))

        ranks = _select_ranks(rule.rank_start, places, identity_by_rank, assignments)
        if ranks is None:
            return replace(pending, reason="incomplete_participant_table")
        for rank in ranks:
            assignments[identity_by_rank[rank]] = destination
            league_assignments.append((rank, destination))
        effective = _effective_ranges(rule, ranks)
        replacements[rule] = effective
        if reached_cup_destination:
            affected_objectives.extend(effective)

    objectives = tuple(
        effective
        for rule in profile.objectives
        for effective in replacements.get(rule, (rule,))
    )
    season_complete = (
        len(rows) == profile.number_of_teams
        and profile.total_regular_season_games is not None
        and len(identity_by_rank) == len(rows)
        and all((row.get("games_played") or 0) >= profile.total_regular_season_games for row in rows)
    )
    cup_winners = {cup.winner for cup in evidence}
    eligible = tuple(
        identity for _, identity in sorted(identity_by_rank.items())
        if identity not in cup_winners
    )
    allocations = tuple(
        CupQualificationAllocation(
            evidence=cup,
            champion_rank=champion_ranks[index],
            champion_destination=assignments[cup.winner],
            berth_awarded=cup_berth_awarded[index],
            configured_competition_name=policies[index].competition_name,
            configured_destination_competition_name=policies[index].destination_competition_name,
            policy_sources=policies[index].sources,
        )
        for index, cup in enumerate(evidence)
    )
    return replace(
        pending,
        status="FINAL" if season_complete else "AS_OF_TABLE",
        reason="cup_winner_uses_berth" if any(cup_berth_awarded) else "cup_winner_already_qualified",
        effective_rules=replace(profile, objectives=objectives),
        eligible_participants=eligible,
        league_assignments=tuple(sorted(league_assignments, key=lambda entry: entry[0])),
        cup_berth_awarded=any(cup_berth_awarded),
        affected_objectives=tuple(affected_objectives),
        cup_allocations=allocations,
    )
