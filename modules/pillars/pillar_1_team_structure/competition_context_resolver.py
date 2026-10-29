"""Bind current sporting rules to P1's existing competition context."""

from dataclasses import replace

from modules.competition.competition_profiles import get_competition_profile
from modules.pillars.context import CompetitionContext, EventIdentity


def resolve_p1_competition_context(
    event_identity: EventIdentity,
    competition_context: CompetitionContext,
) -> CompetitionContext:
    """Resolve current facts by canonical ID, with no I/O or season-name parsing.

    Unknown IDs have no sporting configuration; cached DB values are not substitutes.
    """
    rules = get_competition_profile(event_identity.competition_id)
    if rules is None:
        return replace(competition_context, competitive_rules=None, number_of_teams=None,
                       total_regular_season_games=None, standings_grouping=None,
                       number_of_teams_source="missing", competition_config_source="missing")
    return replace(
        competition_context,
        competitive_rules=rules,
        number_of_teams=rules.number_of_teams,
        number_of_teams_source="competition_rules",
        total_regular_season_games=rules.total_regular_season_games,
        standings_grouping=rules.standings_grouping,
        competition_config_source="competition_rules",
    )
