"""Read the matching tournament edition and evidence known at the evaluation cutoff."""

from dataclasses import replace
from datetime import datetime

from modules.competition import competition_ids
from modules.competition.rule_terms import TournamentFinalStatus, TournamentRound

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, aliased

from infrastructure.persistence.models import (
    Competition, Event, EventOutcomeObservation, Participant, Result, Season,
)
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.final_outcome import (
    TournamentFinalEvidence, ParticipantIdentity, decisive_winner,
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
        value = ""
        if score.get("overtime") is not None and score.get("normaltime") is not None:
            value = f"{score['normaltime']}-({score['overtime']})"
        elif "overtime" in score:
            value = "(extra_time)"
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


def _tournament_matches(session, competitions, season_year, rounds, cutoff):
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


def _read_match_outcome(row, evidence, as_of, *, historical_cutoff, read_at, decide_match=True):
    event, season, home, away, current_result, _, known, has_observations = row
    evidence = replace(evidence, event_id=event.id, season_id=season.id,
                       starts_at=event.starts_at, home=_participant_identity(home), away=_participant_identity(away))
    if as_utc(event.starts_at) >= as_of:
        return replace(evidence, status=TournamentFinalStatus.FINAL_FUTURE)
    if known:
        result = _normalize_observed_result(known.payload)
    elif has_observations:
        return replace(evidence, status=TournamentFinalStatus.NO_OBSERVATION_BEFORE_CUTOFF)
    elif current_result is not None:
        observed_at = as_utc(read_at or utc_now())
        if historical_cutoff and observed_at > as_of:
            return replace(evidence, status=TournamentFinalStatus.LEGACY_RESULT_NOT_KNOWN_AT_CUTOFF)
        result = {key: getattr(current_result, key) for key in
                  ("home_score", "away_score", "winner", "home_sets", "away_sets")}
    else:
        return replace(evidence, status=TournamentFinalStatus.FINAL_UNFINISHED)
    if result is None:
        return replace(evidence, status=TournamentFinalStatus.FINAL_UNFINISHED)
    if not decide_match:
        status = (TournamentFinalStatus.DECIDED if evidence.home and evidence.away
                  else TournamentFinalStatus.MISSING_PARTICIPANT_IDENTITY)
        return replace(evidence, status=status, result=dict(result))
    side, decision = decisive_winner(result)
    winner = {"1": evidence.home, "2": evidence.away}.get(side)
    if side is None:
        status = decision
    elif winner is None:
        status = TournamentFinalStatus.MISSING_PARTICIPANT_IDENTITY
    else:
        status = TournamentFinalStatus.DECIDED
    return replace(evidence, status=status,
                   result=dict(result), winner=winner, decision=decision)


def _read_two_leg_final(matches, evidence, cutoff, *, historical_cutoff, read_at):
    """Resolve both legs with the same cutoff and score evidence as single games."""
    if len(matches) < 2:
        return replace(evidence, status=TournamentFinalStatus.FINAL_UNFINISHED)
    if len(matches) > 2:
        return replace(evidence, status=TournamentFinalStatus.FINAL_AMBIGUOUS)
    games = [
        _read_match_outcome(row, evidence, cutoff, historical_cutoff=historical_cutoff,
                            read_at=read_at, decide_match=False)
        for row in sorted(matches, key=lambda row: as_utc(row[0].starts_at))
    ]
    finalists = {games[0].home, games[0].away}
    if None in finalists:
        return replace(evidence, status=TournamentFinalStatus.MISSING_PARTICIPANT_IDENTITY)
    if len(finalists) != 2 or any({game.home, game.away} != finalists for game in games):
        return replace(evidence, status=TournamentFinalStatus.FINAL_AMBIGUOUS)
    first_leg, second_leg = games
    for game in games:
        if game.status != TournamentFinalStatus.DECIDED:
            return game
    if first_leg.home != second_leg.away or first_leg.away != second_leg.home:
        return replace(evidence, status=TournamentFinalStatus.FINAL_AMBIGUOUS)
    # Align both legs to the second leg's venue before applying extra time and penalties.
    aggregate = {}
    for side, first_side in (("home", "away"), ("away", "home")):
        score_key = f"{side}_score"
        first_score = first_leg.result.get(f"{first_side}_score")
        second_score = second_leg.result.get(score_key)
        if any(type(score) is not int or score < 0 for score in (first_score, second_score)):
            return replace(second_leg, status=TournamentFinalStatus.INVALID_SCORE)
        aggregate[score_key] = first_score + second_score
        sets_key = f"{side}_sets"
        sets = second_leg.result.get(sets_key) or ""
        if "(extra_time)" in sets:
            return replace(second_leg, status=TournamentFinalStatus.CONTRADICTORY_EXTRA_TIME)
        # Preserve period details used by decisive_winner to reconcile regulation and extra time.
        aggregate[sets_key] = f"{first_score}-{sets}" if sets[:1].isdigit() else sets
    side, decision = decisive_winner(aggregate)
    winner = {"1": second_leg.home, "2": second_leg.away}.get(side)
    if winner is None:
        return replace(second_leg, status=decision)
    return replace(second_leg, result=aggregate, winner=winner, decision=decision)


def read_tournament_finals(
    session: Session,
    competitions: tuple[CompetitionIdentity, ...],
    season_year: int | None,
    as_of: datetime | None = None,
    *,
    read_at: datetime | None = None,
) -> tuple[TournamentFinalEvidence, ...]:
    """Find the finals in one query, preserving each competition's evidence status."""
    if not competitions:
        return ()
    historical_cutoff = as_of is not None
    cutoff = as_utc(as_of or utc_now())
    rows = _tournament_matches(session, competitions, season_year, (TournamentRound.FINAL,), cutoff)
    evidence = []
    for identity in competitions:
        matches = [
            row for row in rows
            if row[5].source == identity.source
            and row[5].source_unique_tournament_id == identity.source_unique_tournament_id
            and (identity.source_tournament_id is None
                 or row[5].source_tournament_id == identity.source_tournament_id)
        ]
        item = TournamentFinalEvidence(
            identity, season_year, TournamentFinalStatus.FINAL_AMBIGUOUS if len(matches) > 1 else TournamentFinalStatus.FINAL_ABSENT,
            candidate_event_ids=tuple(sorted(row[0].id for row in matches)),
        )
        if matches and identity in (
            competition_ids.LIGA_MX_APERTURA_PLAYOFFS, competition_ids.LIGA_MX_CLAUSURA_PLAYOFFS,
        ):
            item = _read_two_leg_final(
                matches, item, cutoff, historical_cutoff=historical_cutoff, read_at=read_at,
            )
        elif len(matches) == 1:
            item = _read_match_outcome(
                matches[0], item, cutoff, historical_cutoff=historical_cutoff, read_at=read_at,
            )
        evidence.append(item)
    return tuple(evidence)


def read_tournament_eliminations(
    session: Session,
    competition: CompetitionIdentity,
    season_year: int,
    rounds: tuple[tuple[TournamentRound, int], ...],
    as_of: datetime,
    *,
    read_at: datetime,
) -> dict[str, tuple[ParticipantIdentity, ...]]:
    """Read complete single-leg rounds in sporting order for tournament berth replacement."""
    rows = _tournament_matches(session, (competition,), season_year, tuple(name for name, _ in rounds), as_of)
    eliminations = {}
    for name, expected in rounds:
        matches = [row for row in rows if row[0].round == name]
        if len(matches) != expected:
            break
        losers = []
        for row in matches:
            item = _read_match_outcome(
                row, TournamentFinalEvidence(competition, season_year, TournamentFinalStatus.FINAL_ABSENT),
                as_of, historical_cutoff=True, read_at=read_at,
            )
            loser = item.loser
            if item.status == TournamentFinalStatus.DECIDED and loser is not None:
                losers.append(loser)
        if len(set(losers)) != expected:
            break
        eliminations[name] = tuple(losers)
    return eliminations
