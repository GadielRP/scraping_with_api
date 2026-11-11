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
            "ranks": (rule.rank_start, rule.rank_end),
            "destination": rule.destination_competition_name
            or rule.destination_competition_identity,
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
                       qualification_resolution=None,
                       total_regular_season_games=None, standings_grouping=None,
                       number_of_teams_source="missing", competition_config_source="missing")
    logger.info(
        "Competition profile selected event_id=%s identity=%s profile_name=%s "
        "reference_season=%s teams=%s regular_season_games=%s standings_policy=%s "
        "standings_criteria=%s objectives=%s cup_qualification_rules=%s",
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
                "competition": policy.competition_name,
                "competition_identity": policy.competition_identity,
                "final_round": policy.final_round,
                "destination": policy.destination_competition_name
                or policy.destination_competition_identity,
                "reallocate_to_league": policy.reallocate_to_league,
            }
            for policy in rules.cup_qualifications
        ],
    )
    return replace(
        competition_context,
        competitive_rules=rules,
        qualification_resolution=None,
        number_of_teams=rules.number_of_teams,
        number_of_teams_source="competition_rules",
        total_regular_season_games=rules.total_regular_season_games,
        standings_grouping="single_table" if rules.grouping_method == GroupingMethod.LEAGUE_WIDE else "split_tables",
        competition_config_source="competition_rules",
    )
