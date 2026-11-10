"""Read the matching cup edition and evidence known at the evaluation cutoff."""

from dataclasses import replace
from sqlalchemy import select
from sqlalchemy.orm import aliased

from infrastructure.persistence.models import (
    Competition, Event, EventOutcomeObservation, Participant, Result, Season,
)
from modules.competition.qualification_resolution import (
    CupFinalEvidence, ParticipantIdentity, decisive_winner,
)
from shared.temporal import as_utc, utc_now


def _observation_result(payload):
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


def read_cup_final(session, competition, season_year, as_of=None, *, read_at=None):
    """No writes, including for legacy results. The caller captures this evidence.

    A legacy result is known only when read, never at its kickoff. If any outcome
    observations exist, a current Result cannot replace missing historical evidence.
    """
    historical_cutoff = as_of is not None
    as_of = as_utc(as_of or utc_now())
    home, away = aliased(Participant), aliased(Participant)
    query = (select(Event, Season, home, away, Result)
             .join(Season, Season.id == Event.season_id)
             .join(Competition, Competition.competition_id == Event.competition_id)
             .outerjoin(home, home.participant_id == Event.home_participant_id)
             .outerjoin(away, away.participant_id == Event.away_participant_id)
             .outerjoin(Result, Result.event_id == Event.id)
             .where(Competition.source == competition.source,
                    Competition.source_unique_tournament_id == competition.source_unique_tournament_id,
                    Competition.source_tournament_id == competition.source_tournament_id,
                    Season.year == season_year, Event.round == "final"))
    rows = session.execute(query).all()
    evidence = CupFinalEvidence(competition, season_year, "final_absent",
                                candidate_event_ids=tuple(sorted(row[0].id for row in rows)))
    if len(rows) != 1:
        return replace(evidence, status="final_ambiguous" if rows else "final_absent")
    event, season, home, away, current = rows[0]
    def identity(participant):
        return (ParticipantIdentity(participant.source, participant.source_participant_id)
                if participant else None)
    evidence = replace(evidence, event_id=event.id, season_id=season.id,
                       starts_at=event.starts_at, home=identity(home), away=identity(away))
    if as_utc(event.starts_at) >= as_of:
        return replace(evidence, status="final_future")
    observations = select(EventOutcomeObservation).where(EventOutcomeObservation.event_id == event.id)
    known = session.scalars(observations.where(EventOutcomeObservation.received_at <= as_of)
                            .order_by(EventOutcomeObservation.received_at.desc(),
                                      EventOutcomeObservation.observation_id.desc()).limit(1)).first()
    if known:
        result = _observation_result(known.payload)
        evidence = replace(evidence, observed_at=known.received_at,
                           observation_id=known.observation_id, evidence_source=known.source)
    elif session.scalars(observations.limit(1)).first() is not None:
        return replace(evidence, status="no_observation_before_cutoff")
    elif current:
        observed_at = as_utc(read_at or utc_now())
        evidence = replace(evidence, observed_at=observed_at, evidence_source="legacy_result_read")
        if historical_cutoff and observed_at > as_of:
            return replace(evidence, status="legacy_result_not_known_at_cutoff")
        result = {key: getattr(current, key) for key in
                  ("home_score", "away_score", "winner", "home_sets", "away_sets")}
    else:
        return replace(evidence, status="final_unfinished")
    if result is None:
        return replace(evidence, status="final_unfinished")
    side, decision = decisive_winner(result)
    winner = evidence.home if side == "1" else evidence.away if side == "2" else None
    return replace(evidence, status="DECIDED" if winner else decision if not side else "missing_participant_identity",
                   result=dict(result), winner=winner, decision=decision)
