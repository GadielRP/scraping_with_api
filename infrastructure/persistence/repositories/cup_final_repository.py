"""Read the matching cup edition and evidence known at the evaluation cutoff."""

from dataclasses import replace
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from infrastructure.persistence.models import (
    Competition, Event, EventOutcomeObservation, Participant, Result, Season,
)
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.final_outcome import (
    CupFinalEvidence, ParticipantIdentity, decisive_winner,
)
from shared.temporal import as_utc, utc_now


def _normalize_observed_result(payload):
    # Internal observations contain canonical scores; SofaScore observations
    # retain the terminal status and original score snapshot.
    if "home_score" in payload:
        return payload
    event = payload.get("event", payload)
    if event.get("status", {}).get("type") != "finished":
        return None
    home, away = event.get("homeScore", {}), event.get("awayScore", {})
    result = {
        "home_score": home.get("display") if home.get("display") is not None else home.get("current"),
        "away_score": away.get("display") if away.get("display") is not None else away.get("current"),
        "winner": {1: "1", 2: "2", 3: "X"}.get(event.get("winnerCode")),
    }
    for side, score in (("home", home), ("away", away)):
        value = "(extra_time)" if "overtime" in score else ""
        if score.get("penalties") is not None:
            value += f"+{score['penalties']}"
        result[f"{side}_sets"] = value
    return result


def _participant_identity(participant: Participant | None) -> ParticipantIdentity | None:
    if participant is None:
        return None
    return ParticipantIdentity(participant.source, participant.source_participant_id)


def read_cup_final(
    session: Session,
    competition: CompetitionIdentity,
    season_year: int | None,
    as_of: datetime | None = None,
    *,
    final_round: str,
    read_at: datetime | None = None,
) -> CupFinalEvidence:
    """No writes, including for legacy results. The caller captures this evidence.

    A legacy result is known only when read, never at its kickoff. If any outcome
    observations exist, a current Result cannot replace missing historical evidence.
    """
    historical_cutoff = as_of is not None
    as_of = as_utc(as_of or utc_now())
    home_participant, away_participant = aliased(Participant), aliased(Participant)
    query = (select(Event, Season, home_participant, away_participant, Result)
             .join(Season, Season.id == Event.season_id)
             .join(Competition, Competition.competition_id == Event.competition_id)
             .outerjoin(home_participant, home_participant.participant_id == Event.home_participant_id)
             .outerjoin(away_participant, away_participant.participant_id == Event.away_participant_id)
             .outerjoin(Result, Result.event_id == Event.id)
             .where(Competition.source == competition.source,
                    Competition.source_unique_tournament_id == competition.source_unique_tournament_id,
                    Season.year == season_year, Event.round == final_round))
    if competition.source_tournament_id is not None:
        query = query.where(Competition.source_tournament_id == competition.source_tournament_id)
    rows = session.execute(query).all()
    evidence = CupFinalEvidence(competition, season_year, "final_absent",
                                candidate_event_ids=tuple(sorted(row[0].id for row in rows)))
    if len(rows) != 1:
        return replace(evidence, status="final_ambiguous" if rows else "final_absent")
    event, season, home, away, current_result = rows[0]
    evidence = replace(evidence, event_id=event.id, season_id=season.id,
                       starts_at=event.starts_at, home=_participant_identity(home), away=_participant_identity(away))
    if as_utc(event.starts_at) >= as_of:
        return replace(evidence, status="final_future")
    observations = select(EventOutcomeObservation).where(EventOutcomeObservation.event_id == event.id)
    known = session.scalars(observations.where(EventOutcomeObservation.received_at <= as_of)
                            .order_by(EventOutcomeObservation.received_at.desc(),
                                      EventOutcomeObservation.observation_id.desc()).limit(1)).first()
    if known:
        result = _normalize_observed_result(known.payload)
        evidence = replace(evidence, observed_at=known.received_at,
                           observation_id=known.observation_id, evidence_source=known.source)
    elif session.scalars(observations.limit(1)).first() is not None:
        return replace(evidence, status="no_observation_before_cutoff")
    elif current_result is not None:
        observed_at = as_utc(read_at or utc_now())
        evidence = replace(evidence, observed_at=observed_at, evidence_source="legacy_result_read")
        if historical_cutoff and observed_at > as_of:
            return replace(evidence, status="legacy_result_not_known_at_cutoff")
        result = {key: getattr(current_result, key) for key in
                  ("home_score", "away_score", "winner", "home_sets", "away_sets")}
    else:
        return replace(evidence, status="final_unfinished")
    if result is None:
        return replace(evidence, status="final_unfinished")
    side, decision = decisive_winner(result)
    winner = {"1": evidence.home, "2": evidence.away}.get(side)
    if side is None:
        status = decision
    elif winner is None:
        status = "missing_participant_identity"
    else:
        status = "DECIDED"
    return replace(evidence, status=status,
                   result=dict(result), winner=winner, decision=decision)
