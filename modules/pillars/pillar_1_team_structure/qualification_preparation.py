"""Load shared season standings and cup evidence before P1 captures its inputs."""

import logging
from dataclasses import replace

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.cup_final_repository import read_cup_final
from modules.alerts.matchup_streak_analysis.season_aggregate_standings_calculator import (
    calculate_season_aggregate_standings,
)
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.competition_profiles import get_season_aggregate_profile
from modules.competition.qualification_resolution import resolve_qualification
from shared.temporal import as_utc, utc_now

logger = logging.getLogger(__name__)


def _season_aggregate_profile(competition):
    if (not competition.source or competition.source_unique_tournament_id is None
            or competition.source_tournament_id is None):
        return None
    identity = CompetitionIdentity(
        competition.source_unique_tournament_id, competition.source_tournament_id, competition.source,
    )
    return get_season_aggregate_profile(identity)


def prepare_qualification_resolution(event, competition, streak, *, as_of=None):
    profile = competition.competitive_rules
    policies = profile.configured_cup_qualifications if profile is not None else ()
    season_year = event.season_year
    season_profile = _season_aggregate_profile(competition)
    if not policies and (season_profile is None or season_year is None):
        return replace(competition, qualification_resolution=None, season_aggregate_standings=None)
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
    season_standings = None
    with db_manager.get_session() as session:
        if season_profile is not None and season_year is not None:
            season_standings = calculate_season_aggregate_standings(
                session,
                season_profile,
                season_year=season_year,
                kickoff_cutoff=min(cutoff, as_utc(event.starts_at)),
            )
        evidence = tuple(
            read_cup_final(
                session, policy.competition_identity, event.season_year, cutoff,
                final_round=policy.final_round, read_at=now,
            )
            for policy in policies
        )
    competition = replace(competition, season_aggregate_standings=season_standings)
    if not policies:
        return replace(competition, qualification_resolution=None)

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
    return replace(competition, qualification_resolution=resolution)
