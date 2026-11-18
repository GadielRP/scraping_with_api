"""Load evidence, allocate qualification berths and re-cut objectives before P1 captures its inputs."""


import logging
from dataclasses import replace

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.tournament_final_repository import (
    read_tournament_eliminations,
    read_tournament_finals,
)
from infrastructure.persistence.repositories.season_aggregate_totals_repository import read_season_members
from modules.alerts.matchup_streak_analysis.season_aggregate_standings_calculator import (
    calculate_multiseason_averages,
    calculate_season_aggregate_standings,
)
from modules.competition.berth_allocation_engine.berth_allocation_engine import allocate_berths
from modules.competition.berth_allocation_engine.relegation_engine import resolve_relegation
from modules.competition.final_outcome import ParticipantIdentity
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.qualification_rules import build_qualification_rules
from modules.competition.rule_terms import (
    AggregationScope,
    BerthType,
    ObjectiveType,
    RuleStatus,
    SelectionMethod,
    TournamentFinalStatus,
)
from modules.competition.standings_rows import annual_table_rows, table_rows
from shared.temporal import as_utc, utc_now

logger = logging.getLogger(__name__)


def _usable_finals(finals, event_starts_at, cutoff):
    """Discard decided finals played after the event began."""
    return tuple(
        item for item in finals
        if item.status != TournamentFinalStatus.DECIDED or (
            item.starts_at is not None
            and as_utc(item.starts_at) < min(cutoff, as_utc(event_starts_at))
        )
    )


def _aggregate_inputs(session, profile, annual):
    """Relegated clubs and next season's roster for a table shared by several tournaments."""
    config = profile.qualification_sources
    relegation_rules = tuple(
        rule.rank_selection for rule in profile.objectives or ()
        if rule.objective_type == ObjectiveType.DIVISION_RETENTION
        and rule.status == RuleStatus.CONFIGURED
        and rule.rank_selection.aggregation_scope in (
            AggregationScope.SEASON_AGGREGATE, AggregationScope.MULTISEASON_RECORD)
    )
    average_seasons = next(
        (rule.seasons for rule in relegation_rules
         if rule.selection_method == SelectionMethod.LOWEST_POINTS_AVERAGE and rule.seasons is not None),
        None,
    )
    averages = None
    if average_seasons is not None:
        averages = calculate_multiseason_averages(
            session, config.shared_table, annual, seasons=average_seasons,
        )
    relegation = resolve_relegation(relegation_rules, annual.standings, multiseason_averages=averages)
    if not relegation_rules:
        return relegation, None, False
    members = frozenset(ParticipantIdentity(*row) for row in read_season_members(
        session, config.shared_table.sources,
        season_year=annual.season_year + config.destination_season_offset,
        known_at=annual.kickoff_cutoff,
    ))
    roster = members if len(members) == config.shared_table.number_of_teams else None
    return relegation, roster, roster is None


def prepare_qualification(event, competition, streak, *, as_of=None):
    profile = competition.competitive_rules
    rules = build_qualification_rules(profile) if profile is not None else None
    season_year = event.season_year
    if rules is None or season_year is None:
        return replace(competition, berth_allocation=None)

    # Freeze the evaluation instant. A retrospective evaluation may use evidence
    # already available now, even when it was persisted after the event kickoff.
    now = utc_now()
    cutoff = as_utc(as_of or now)
    config = profile.qualification_sources
    table = None
    relegated, roster, eligibility_unconfirmed = frozenset(), None, False
    with db_manager.get_session() as session:
        if config.shared_table is not None:
            annual = calculate_season_aggregate_standings(
                session, config.shared_table, season_year=season_year,
                kickoff_cutoff=min(cutoff, as_utc(event.starts_at)),
            )
            # The berth engine reads the annual table under the league-table contract; the original
            # aggregate still feeds relegation and the report.
            table = annual.standings
            standings = annual_table_rows(table)
            relegation, roster, eligibility_unconfirmed = _aggregate_inputs(session, profile, annual)
            relegated = frozenset(relegation.participants)
        else:
            standings = table_rows(getattr(streak, "current_standings", None))
        if standings is None and (config.shared_table is None or table):
            logger.warning(
                "Qualification skipped: standings lack participant identity, ordering or group membership "
                "event_id=%s competition=%s", event.event_id, competition.canonical_name or competition.display_name,
            )
        if not standings:
            return replace(competition, berth_allocation=None)
        competitions = tuple(dict.fromkeys(rule.competition for rule in rules.tournament_berths))
        finals = _usable_finals(
            read_tournament_finals(session, competitions, season_year, cutoff, read_at=now),
            event.starts_at, cutoff,
        )
        eliminations = {
            rule.competition: read_tournament_eliminations(
                session, rule.competition, season_year, rule.replacement_rounds, cutoff, read_at=now,
            )
            for rule in rules.tournament_berths if rule.replacement_rounds
        }

    profile_identity = CompetitionIdentity(
        source_unique_tournament_id=competition.source_unique_tournament_id,
        source_tournament_id=competition.source_tournament_id,
        source=competition.source,
        competition_name=competition.display_name or competition.canonical_name,
    )
    outcome = allocate_berths(
        rules, standings,
        season_year=season_year,
        profile_identity=profile_identity,
        tournament_evidence=finals,
        bracket_eliminations=eliminations,
        excluded_participants=relegated,
        next_season_roster=roster,
        eligibility_unconfirmed=eligibility_unconfirmed,
    )
    allocation = replace(outcome, table=table)
    logger.info(
        "Qualification allocated event_id=%s competition=%s season_year=%s as_of=%s destination_season_year=%s berths=%s",
        event.event_id,
        competition.canonical_name or competition.display_name,
        season_year,
        cutoff.isoformat(),
        allocation.destination_season_year,
        [(b.source, b.participant, b.table_rank, b.resolution.value, b.issue)
         for b in allocation.berths if b.berth_type is BerthType.TOURNAMENT or b.issue],
    )
    return replace(competition, berth_allocation=allocation)
