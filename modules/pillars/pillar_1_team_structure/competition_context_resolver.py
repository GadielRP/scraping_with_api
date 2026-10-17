"""Bind season-specific sporting rules to P1's existing competition context."""

from dataclasses import replace

from modules.competition.competitive_rules import season_key_from_name
from modules.competition.league_config import LEAGUE_CONFIGS
from modules.pillars.context import CompetitionContext, EventIdentity


def resolve_p1_competition_context(
    event_identity: EventIdentity,
    competition_context: CompetitionContext,
) -> CompetitionContext:
    """Pure resolution: exact provider scope and explicit season, with no I/O.

    Legacy metadata remains available to other modules when the rule season is
    unknown. Never reuse a current-season profile for a historical event.
    """
    season_key = season_key_from_name(event_identity.season_name)
    config = None
    if competition_context.source == "sofascore":
        config = next((
            candidate for candidate in LEAGUE_CONFIGS
            if candidate.primary_identity.matches(
                competition_context.source_unique_tournament_id,
                competition_context.source_tournament_id,
            )
        ), None)
    rules = next((
        profile for profile in config.competitive_rules
        if profile.season_key == season_key
    ), None) if config else None
    if rules is None:
        return replace(competition_context, competitive_rules=None)
    return replace(
        competition_context,
        competitive_rules=rules,
        number_of_teams=rules.number_of_teams,
        number_of_teams_source="competition_rules",
        total_regular_season_games=rules.total_regular_season_games,
        standings_grouping=rules.standings_grouping,
        league_config_source="competition_rules",
    )
