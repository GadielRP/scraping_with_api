"""Bind current sporting rules to P1's existing competition context."""

from dataclasses import replace

from modules.competition.competition_profiles import get_competition_profile
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.rule_terms import GroupingMethod
from modules.pillars.context import CompetitionContext, EventIdentity


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
    rules = get_competition_profile(identity)
    if rules is None:
        return replace(competition_context, competitive_rules=None, number_of_teams=None,
                       qualification_resolution=None,
                       total_regular_season_games=None, standings_grouping=None,
                       number_of_teams_source="missing", competition_config_source="missing")
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
