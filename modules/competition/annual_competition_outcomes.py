"""Allocate season berths and relegations from ordered sporting rules."""

from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction

from .competition_identity import CompetitionIdentity
from .competitive_rules import AnnualQualificationRules, FinalBerthRule, QualificationDestination, RankSelection
from .final_outcome import CupFinalEvidence, ParticipantIdentity
from .rule_terms import Finalist, SelectionMethod
from .season_aggregate import SeasonAggregateStandings


@dataclass(frozen=True, slots=True)
class AnnualBerth:
    slot: str
    destination: QualificationDestination
    participant: ParticipantIdentity | None
    source: CompetitionIdentity | None
    status: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class AnnualRelegation:
    participants: tuple[ParticipantIdentity, ...]
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class AnnualCompetitionOutcomes:
    destination_year: int
    berths: tuple[AnnualBerth, ...]
    relegation: AnnualRelegation
    finals: tuple[CupFinalEvidence, ...]


def resolve_annual_relegation(
    rules: tuple[RankSelection, ...],
    annual: SeasonAggregateStandings,
    averages: Mapping[ParticipantIdentity, Fraction] | None,
) -> AnnualRelegation:
    if rules and not annual.regular_season_complete:
        return AnnualRelegation((), "annual_table_incomplete")

    remaining = {row.participant: row.points for row in annual.standings}
    relegated = []
    for rule in rules:
        if rule.selection_method == SelectionMethod.LOWEST_POINTS_AVERAGE:
            if averages is None or not remaining.keys() <= averages.keys():
                return AnnualRelegation(tuple(relegated), "incomplete_multiseason_record")
            scores = {club: averages[club] for club in remaining}
            tie_reason = "average_relegation_decider_required"
        elif rule.selection_method == SelectionMethod.LOWEST_POINTS:
            scores = remaining
            tie_reason = "annual_relegation_decider_required"
        else:
            raise ValueError(f"Unsupported annual relegation selector: {rule.selection_method}")

        count = rule.places
        if count is None or len(scores) < count:
            return AnnualRelegation(tuple(relegated), "insufficient_relegation_candidates")
        ordered = sorted(scores, key=scores.__getitem__)
        if len(ordered) > count and scores[ordered[count - 1]] == scores[ordered[count]]:
            return AnnualRelegation(tuple(relegated), tie_reason)
        relegated.extend(ordered[:count])
        for club in ordered[:count]:
            del remaining[club]
    return AnnualRelegation(tuple(relegated))


def _final_candidates(
    rule: FinalBerthRule,
    final: CupFinalEvidence | None,
    ranks: Mapping[ParticipantIdentity, int | None],
    eligible: set[ParticipantIdentity],
    eliminated: Mapping[str, tuple[ParticipantIdentity, ...]],
    roster_complete: bool,
) -> tuple[ParticipantIdentity | None, str | None]:
    if final is None:
        return None, "final_absent"
    if final.status != "DECIDED":
        return None, final.status
    if final.winner is None:
        return None, "incomplete_cup_evidence"

    for finalist in rule.finalists:
        club = final.winner if finalist == Finalist.WINNER else final.loser
        if club is None:
            return None, "incomplete_cup_evidence"
        if club in eligible:
            return club, None
        if club not in ranks and not roster_complete:
            return None, "finalist_eligibility_unknown"

    for round_name, expected_matches in rule.replacement_rounds:
        losers = eliminated.get(round_name, ())
        if len(set(losers)) != expected_matches:
            return None, "incomplete_cup_bracket"
        candidates = set(losers) & eligible
        if len(candidates) == 1:
            return next(iter(candidates)), None
        if candidates:
            positions = {ranks.get(club) for club in candidates}
            if None in positions or len(positions) != len(candidates):
                return None, "cup_replacement_annual_tiebreak_pending"
            return min(candidates, key=ranks.__getitem__), None
    return None, None


def resolve_annual_competition_outcomes(
    profile: AnnualQualificationRules,
    annual: SeasonAggregateStandings,
    finals: tuple[CupFinalEvidence, ...],
    relegation: AnnualRelegation,
    *,
    cup_eliminations: Mapping[str, Mapping[str, tuple[ParticipantIdentity, ...]]] | None = None,
    next_season_members: frozenset[ParticipantIdentity] | None = None,
) -> AnnualCompetitionOutcomes:
    ranks = {row.participant: row.rank for row in annual.standings}
    roster_complete = next_season_members is not None
    available = set(ranks).difference(relegation.participants) if next_season_members is None else set(next_season_members)
    evidence = {final.competition: final for final in finals if final.season_year == annual.season_year}
    berths: dict[str, AnnualBerth] = {}
    table_vacancies: list[tuple[str, QualificationDestination]] = []
    eligibility_provisional = not roster_complete and relegation.reason is not None
    preceding_final_unresolved = False

    def award(slot, destination, club, *, source=None, status, reason=None):
        if club is not None:
            available.remove(club)
        berths[slot] = AnnualBerth(slot, destination, club, source, status, reason)

    for rule in profile.final_berths:
        club, reason = _final_candidates(
            rule, evidence.get(rule.competition), ranks, available,
            (cup_eliminations or {}).get(rule.slot, {}), roster_complete,
        )
        if club is not None:
            status = "PROVISIONAL" if eligibility_provisional or preceding_final_unresolved else "QUALIFIED"
        elif reason is not None:
            status = "PENDING"
            preceding_final_unresolved = True
        elif rule.reallocate_to_table:
            table_vacancies.append((rule.slot, rule.destination))
            continue
        elif rule.replacement_rounds:
            status, reason = "PENDING", "no_eligible_cup_participant"
            preceding_final_unresolved = True
        else:
            status, reason = "NOT_APPLICABLE", "winner_outside_eligible_division"
        award(rule.slot, rule.destination, club, source=rule.competition, status=status, reason=reason)

    table_berths = tuple((slot, rule.destination) for rule in profile.table_berths for slot in rule.slots)
    ranked = iter(row for row in annual.standings if row.participant in available)
    provisional = eligibility_provisional or preceding_final_unresolved or not annual.regular_season_complete
    table_status = "PROVISIONAL" if provisional else "QUALIFIED"
    ranking_pending_reason = None
    for slot, destination in (*table_vacancies, *table_berths):
        if ranking_pending_reason is None:
            row = next(ranked, None)
            if row is None:
                ranking_pending_reason = "insufficient_eligible_annual_participants"
            elif row.rank is None:
                ranking_pending_reason = row.ranking_reason or "annual_ranking_unresolved"
            else:
                award(slot, destination, row.participant, status=table_status)
                continue
        award(slot, destination, None, status="PENDING", reason=ranking_pending_reason)

    slot_order = (*(rule.slot for rule in profile.final_berths), *(slot for slot, _ in table_berths))
    return AnnualCompetitionOutcomes(
        annual.season_year + profile.destination_season_offset,
        tuple(berths[slot] for slot in slot_order),
        relegation,
        finals,
    )
