"""Bind current sporting rules to P1's existing competition context."""

import logging
from dataclasses import replace

from modules.competition.competition_profiles import get_competition_profile
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.rule_terms import GroupingMethod
from modules.pillars.context import CompetitionContext, EventIdentity

logger = logging.getLogger(__name__)


def _objective_summary(rules):
    return [
        {
            "type": rule.objective_type.value,
            "ranks": (rule.rank_selection.rank_start, rule.rank_selection.rank_end),
            "destination_competition_identity": (
                rule.qualification_destination.destination_competition_identity
                if rule.qualification_destination else None
            ),
            "destination_competition_name": (
                rule.qualification_destination.destination_competition_name
                if rule.qualification_destination else None
            ),
            "destination_phase": (
                rule.qualification_destination.destination_phase
                if rule.qualification_destination else None
            ),
            "destination_round": (
                rule.qualification_destination.destination_round
                if rule.qualification_destination else None
            ),
            "qualification_path": (
                rule.qualification_destination.qualification_path
                if rule.qualification_destination else None
            ),
            "status": rule.status.value,
        }
        for rule in rules.objectives or ()
    ]


def resolve_p1_competition_context(
    event_identity: EventIdentity,
    competition_context: CompetitionContext,
) -> CompetitionContext:
    """Resolve current facts by provider identity, with no I/O or season-name parsing.

    Unknown IDs have no sporting configuration; cached DB values are not substitutes.
    """
    identity = None
    if (competition_context.source and competition_context.source_unique_tournament_id is not None
            and competition_context.source_tournament_id is not None):
        identity = CompetitionIdentity(
            source=competition_context.source,
            source_unique_tournament_id=competition_context.source_unique_tournament_id,
            source_tournament_id=competition_context.source_tournament_id,
        )
    if identity is None:
        logger.warning(
            "Competition profile lookup skipped: provider identity incomplete "
            "event_id=%s competition_id=%s source=%s unique_tournament_id=%s tournament_id=%s",
            event_identity.event_id,
            competition_context.competition_id,
            competition_context.source,
            competition_context.source_unique_tournament_id,
            competition_context.source_tournament_id,
        )
    rules = get_competition_profile(identity)
    if rules is None:
        if identity is not None:
            logger.warning(
                "Competition profile not found event_id=%s identity=%s; "
                "competition-dependent calculations will have no profile",
                event_identity.event_id,
                identity,
            )
        return replace(competition_context, competitive_rules=None, number_of_teams=None,
                       berth_allocation=None,
                       total_regular_season_games=None, standings_grouping=None,
                       number_of_teams_source="missing", competition_config_source="missing")
    logger.info(
        "Competition profile selected event_id=%s identity=%s profile_name=%s "
        "reference_season=%s teams=%s regular_season_games=%s standings_policy=%s "
        "standings_criteria=%s objectives=%s tournament_berths=%s",
        event_identity.event_id,
        identity,
        competition_context.canonical_name or competition_context.display_name,
        rules.reference_season,
        rules.number_of_teams,
        rules.total_regular_season_games,
        rules.standings.name,
        len(rules.standings.criteria),
        _objective_summary(rules),
        [
            {
                "competition": rule.competition_name,
                "competition_identity": rule.competition,
                "before_table_slot": rule.before_table_slot,
                "reallocate_to_table": rule.reallocate_to_table,
                "destination_competition_identity": rule.destination.destination_competition_identity,
                "destination_phase": rule.destination.destination_phase,
            }
            for rule in (rules.qualification_sources.tournament_berths if rules.qualification_sources else ())
        ],
    )
    return replace(
        competition_context,
        competitive_rules=rules,
        berth_allocation=None,
        number_of_teams=rules.number_of_teams,
        number_of_teams_source="competition_rules",
        total_regular_season_games=rules.total_regular_season_games,
        standings_grouping="single_table" if rules.grouping_method == GroupingMethod.LEAGUE_WIDE else "split_tables",
        competition_config_source="competition_rules",
    )
