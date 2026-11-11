"""Load cup evidence after standings, before P1's existing immutable capture."""

import logging
from dataclasses import replace

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.cup_final_repository import read_cup_final
from modules.competition.qualification_resolution import resolve_qualification
from shared.temporal import as_utc, utc_now

logger = logging.getLogger(__name__)


def prepare_qualification_resolution(event, competition, streak, *, as_of=None):
    profile = competition.competitive_rules
    if profile is None or not profile.cup_qualifications:
        return replace(competition, qualification_resolution=None)
    # Freeze the evaluation instant. A retrospective evaluation may use evidence
    # already available now, even when it was persisted after the event kickoff.
    now = utc_now()
    cutoff = as_utc(as_of or now)
    logger.info(
        "Qualification resolution started event_id=%s competition=%s season_year=%s "
        "as_of=%s cup_policy_count=%s",
        event.event_id,
        competition.canonical_name or competition.display_name,
        event.season_year,
        cutoff.isoformat(),
        len(profile.cup_qualifications),
    )
    with db_manager.get_session() as session:
        evidence = tuple(
            read_cup_final(
                session, policy.competition_identity, event.season_year, cutoff,
                final_round=policy.final_round, read_at=now,
            )
            for policy in profile.cup_qualifications
        )
    for policy, item in zip(profile.cup_qualifications, evidence):
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
        "destination_season=%s cup_berth_awarded=%s allocations=%s eligible_participants=%s "
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
                "destination": allocation.configured_destination_competition_name
                or allocation.champion_destination,
                "berth_awarded": allocation.berth_awarded,
            }
            for allocation in resolution.cup_allocations
        ],
        len(resolution.eligible_participants),
        [
            (rule.objective_type.value, rule.rank_start, rule.rank_end,
             rule.destination_competition_name or rule.destination_competition_identity)
            for rule in resolution.affected_objectives
        ],
    )
    return replace(competition, qualification_resolution=resolution)
