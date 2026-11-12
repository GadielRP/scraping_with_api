"""Load evidence and resolve competition outcomes before P1 captures its inputs."""

import logging
from dataclasses import replace
from datetime import datetime

from sqlalchemy.orm import Session

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.cup_final_repository import (
    read_cup_eliminations,
    read_cup_final,
    read_cup_finals,
)
from infrastructure.persistence.repositories.season_aggregate_totals_repository import read_season_members
from modules.alerts.matchup_streak_analysis.season_aggregate_standings_calculator import (
    calculate_multiseason_averages,
    calculate_season_aggregate_standings,
)
from modules.competition.annual_competition_outcomes import (
    AnnualCompetitionOutcomes,
    resolve_annual_competition_outcomes,
    resolve_annual_relegation,
)
from modules.competition.competitive_rules import AnnualQualificationRules, RankSelection
from modules.competition.final_outcome import ParticipantIdentity
from modules.competition.qualification_resolution import resolve_qualification
from modules.competition.rule_terms import AggregationScope, ObjectiveType, RuleStatus
from modules.competition.season_aggregate import SeasonAggregateStandings
from modules.pillars.context import QualificationState
from shared.temporal import as_utc, utc_now

logger = logging.getLogger(__name__)


def _resolve_annual_outcomes_from_evidence(
    session: Session,
    outcome_profile: AnnualQualificationRules,
    annual_standings: SeasonAggregateStandings,
    relegation_rules: tuple[RankSelection, ...],
    *,
    read_at: datetime,
) -> AnnualCompetitionOutcomes:
    season_year = annual_standings.season_year
    cutoff = annual_standings.kickoff_cutoff
    aggregate_profile = outcome_profile.standings_profile
    final_rules = outcome_profile.final_berths
    competitions = tuple(dict.fromkeys(rule.competition for rule in final_rules))
    finals = read_cup_finals(session, competitions, season_year, cutoff, read_at=read_at)
    average_seasons = outcome_profile.relegation_average_seasons
    averages = None
    if average_seasons is not None:
        averages = calculate_multiseason_averages(
            session, aggregate_profile, annual_standings, seasons=average_seasons,
        )
    relegation = resolve_annual_relegation(relegation_rules, annual_standings, averages)
    next_season_members = None
    if annual_standings.regular_season_complete:
        rows = read_season_members(
            session, aggregate_profile.sources, season_year=season_year + outcome_profile.destination_season_offset,
            known_at=cutoff,
        )
        roster = frozenset(ParticipantIdentity(*row) for row in rows)
        if len(roster) == aggregate_profile.number_of_teams:
            next_season_members = roster

    outcomes = resolve_annual_competition_outcomes(
        outcome_profile, annual_standings, finals, relegation, next_season_members=next_season_members,
    )
    brackets_needed = {berth.slot for berth in outcomes.berths if berth.reason == "incomplete_cup_bracket"}
    eliminations = {
        rule.slot: read_cup_eliminations(
            session, rule.competition, season_year, rule.replacement_rounds, cutoff, read_at=read_at,
        )
        for rule in final_rules if rule.slot in brackets_needed
    }
    if any(eliminations.values()):
        return resolve_annual_competition_outcomes(
            outcome_profile, annual_standings, finals, relegation,
            next_season_members=next_season_members, cup_eliminations=eliminations,
        )
    return outcomes


def prepare_qualification_resolution(event, competition, streak, *, as_of=None):
    profile = competition.competitive_rules
    policies = profile.configured_cup_qualifications if profile is not None else ()
    annual_profile = profile.annual_qualification if profile is not None else None
    season_year = event.season_year
    if not policies and (annual_profile is None or season_year is None):
        return replace(competition, qualification_state=None)
    # Freeze the evaluation instant. A retrospective evaluation may use evidence
    # already available now, even when it was persisted after the event kickoff.
    now = utc_now()
    cutoff = as_utc(as_of or now)
    if policies:
        logger.info(
            "Qualification resolution started event_id=%s competition=%s season_year=%s "
            "as_of=%s cup_policy_count=%s",
            event.event_id,
            competition.canonical_name or competition.display_name,
            event.season_year,
            cutoff.isoformat(),
            len(policies),
        )
    qualification_state: QualificationState = {}
    with db_manager.get_session() as session:
        if annual_profile is not None and season_year is not None:
            annual_standings = calculate_season_aggregate_standings(
                session,
                annual_profile.standings_profile,
                season_year=season_year,
                kickoff_cutoff=min(cutoff, as_utc(event.starts_at)),
            )
            relegation_rules = tuple(
                rule.rank_selection
                for rule in profile.objectives or ()
                if rule.objective_type == ObjectiveType.DIVISION_RETENTION
                and rule.status == RuleStatus.CONFIGURED
                and rule.rank_selection.aggregation_scope in (AggregationScope.SEASON_AGGREGATE, AggregationScope.MULTISEASON_RECORD)
            )
            qualification_state["annual_standings"] = annual_standings
            qualification_state["annual_resolution"] = _resolve_annual_outcomes_from_evidence(
                session, annual_profile, annual_standings, relegation_rules, read_at=now,
            )
        evidence = tuple(
            read_cup_final(
                session, policy.competition_identity, event.season_year, cutoff,
                final_round=policy.final_round, read_at=now,
            )
            for policy in policies
        )
    if not policies:
        return replace(competition, qualification_state=qualification_state or None)

    for policy, item in zip(policies, evidence):
        logger.info(
            "Cup final evidence loaded event_id=%s cup=%s cup_identity=%s round=%s season_year=%s "
            "status=%s final_event_id=%s winner=%s decision=%s observed_at=%s source=%s",
            event.event_id,
            policy.competition_name,
            policy.competition_identity,
            policy.final_round,
            event.season_year,
            item.status,
            item.event_id,
            item.winner,
            item.decision,
            item.observed_at.isoformat() if item.observed_at else None,
            item.evidence_source,
        )
    resolution = resolve_qualification(
        profile,
        getattr(streak, "current_standings", None),
        evidence,
        event.season_year,
        cutoff,
        target_event_starts_at=event.starts_at,
    )
    logger.info(
        "Qualification resolution completed event_id=%s status=%s reason=%s "
        "destination_season=%s cup_berth_awarded=%s allocations=%s pending_cups=%s eligible_participants=%s "
        "affected_objectives=%s",
        event.event_id,
        resolution.status,
        resolution.reason,
        resolution.destination_season,
        resolution.cup_berth_awarded,
        [
            {
                "cup": allocation.configured_competition_name,
                "winner": allocation.evidence.winner,
                "rank": allocation.champion_rank,
                "destination_competition_identity": allocation.qualification_destination.destination_competition_identity,
                "destination_competition_name": allocation.qualification_destination.destination_competition_name,
                "destination_phase": allocation.qualification_destination.destination_phase,
                "destination_round": allocation.qualification_destination.destination_round,
                "qualification_path": allocation.qualification_destination.qualification_path,
                "berth_awarded": allocation.berth_awarded,
                "berth_recipient": allocation.berth_recipient,
                "berth_recipient_rank": allocation.berth_recipient_rank,
            }
            for allocation in resolution.cup_allocations
        ],
        resolution.pending_cup_reasons,
        len(resolution.eligible_participants),
        [
            (
                rule.objective_type.value,
                rule.rank_selection.rank_start,
                rule.rank_selection.rank_end,
                (rule.qualification_destination.destination_competition_name
                 or rule.qualification_destination.destination_competition_identity)
                if rule.qualification_destination else None,
            )
            for rule in resolution.affected_objectives
        ],
    )
    qualification_state["table_qualification_resolution"] = resolution
    return replace(competition, qualification_state=qualification_state)
