"""Read the matching cup edition and evidence known at the evaluation cutoff."""

from dataclasses import replace
from datetime import datetime

from sqlalchemy import and_, or_, select
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
    return ParticipantIdentity(
        participant.source,
        participant.source_participant_id,
        participant.name,
    )


def _cup_matches(session, competitions, season_year, rounds, cutoff):
    home_participant, away_participant = aliased(Participant), aliased(Participant)
    observation = aliased(EventOutcomeObservation)
    observations = select(EventOutcomeObservation.observation_id).where(EventOutcomeObservation.event_id == Event.id)
    known_id = (
        observations.where(EventOutcomeObservation.received_at <= cutoff)
        .order_by(EventOutcomeObservation.received_at.desc(), EventOutcomeObservation.observation_id.desc())
        .limit(1)
        .correlate(Event)
        .scalar_subquery()
    )
    identities = []
    for competition in competitions:
        filters = [Competition.source == competition.source,
                   Competition.source_unique_tournament_id == competition.source_unique_tournament_id]
        if competition.source_tournament_id is not None:
            filters.append(Competition.source_tournament_id == competition.source_tournament_id)
        identities.append(and_(*filters))
    query = (select(Event, Season, home_participant, away_participant, Result, Competition,
                    observation, observations.correlate(Event).exists())
             .select_from(Event)
             .join(Season, Season.id == Event.season_id)
             .join(Competition, Competition.competition_id == Event.competition_id)
             .outerjoin(home_participant, home_participant.participant_id == Event.home_participant_id)
             .outerjoin(away_participant, away_participant.participant_id == Event.away_participant_id)
             .outerjoin(Result, Result.event_id == Event.id)
             .outerjoin(observation, observation.observation_id == known_id)
             .where(or_(*identities), Season.year == season_year, Event.round.in_(rounds)))
    return session.execute(query).all()


def _read_match_outcome(row, evidence, as_of, *, historical_cutoff, read_at):
    event, season, home, away, current_result, _, known, has_observations = row
    evidence = replace(evidence, event_id=event.id, season_id=season.id,
                       starts_at=event.starts_at, home=_participant_identity(home), away=_participant_identity(away))
    if as_utc(event.starts_at) >= as_of:
        return replace(evidence, status="final_future")
    if known:
        result = _normalize_observed_result(known.payload)
        evidence = replace(evidence, observed_at=known.received_at,
                           observation_id=known.observation_id, evidence_source=known.source)
    elif has_observations:
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


def read_cup_finals(
    session: Session,
    competitions: tuple[CompetitionIdentity, ...],
    season_year: int | None,
    as_of: datetime | None = None,
    *,
    read_at: datetime | None = None,
) -> tuple[CupFinalEvidence, ...]:
    """Find the finals in one query, preserving each competition's evidence status."""
    if not competitions:
        return ()
    historical_cutoff = as_of is not None
    cutoff = as_utc(as_of or utc_now())
    rows = _cup_matches(session, competitions, season_year, ("final",), cutoff)
    evidence = []
    for identity in competitions:
        matches = [
            row for row in rows
            if row[5].source == identity.source
            and row[5].source_unique_tournament_id == identity.source_unique_tournament_id
            and (identity.source_tournament_id is None
                 or row[5].source_tournament_id == identity.source_tournament_id)
        ]
        item = CupFinalEvidence(
            identity, season_year, "final_ambiguous" if len(matches) > 1 else "final_absent",
            candidate_event_ids=tuple(sorted(row[0].id for row in matches)),
        )
        if len(matches) == 1:
            item = _read_match_outcome(
                matches[0], item, cutoff, historical_cutoff=historical_cutoff, read_at=read_at,
            )
        evidence.append(item)
    return tuple(evidence)


def read_cup_eliminations(
    session: Session,
    competition: CompetitionIdentity,
    season_year: int,
    rounds: tuple[tuple[str, int], ...],
    as_of: datetime,
    *,
    read_at: datetime,
) -> dict[str, tuple[ParticipantIdentity, ...]]:
    """Read complete single-leg rounds in sporting order for a cup replacement."""
    rows = _cup_matches(session, (competition,), season_year, tuple(name for name, _ in rounds), as_of)
    eliminations = {}
    for name, expected in rounds:
        matches = [row for row in rows if row[0].round == name]
        if len(matches) != expected:
            break
        losers = []
        for row in matches:
            item = _read_match_outcome(
                row, CupFinalEvidence(competition, season_year, "final_absent"),
                as_of, historical_cutoff=True, read_at=read_at,
            )
            loser = item.loser
            if item.status == "DECIDED" and loser is not None:
                losers.append(loser)
        if len(set(losers)) != expected:
            break
        eliminations[name] = tuple(losers)
    return eliminations
