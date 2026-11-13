"""Turn a profile's objectives and tournament berths into engine rules.

Table berths are never declared twice: every configured competition
qualification objective that selects clubs by table position contributes one
berth per place, in profile order. The profile only adds berths won elsewhere.
"""

from modules.competition.berth_allocation_engine.models import QualificationRules, TableBerthRule
from modules.competition.competitive_rules import CompetitionRules, ObjectiveRule
from modules.competition.rule_terms import (
    AggregationScope, ObjectiveType, RankingScope, RuleStatus, SelectionMethod,
)


def _table_places(rule: ObjectiveRule) -> int:
    """Berths this objective takes from the table; 0 if it is not a table berth."""
    selection = rule.rank_selection
    if rule.objective_type != ObjectiveType.COMPETITION_QUALIFICATION or rule.status != RuleStatus.CONFIGURED:
        return 0
    if (selection.selection_method == SelectionMethod.RANK_RANGE
            and selection.ranking_scope == RankingScope.GLOBAL
            and selection.aggregation_scope == AggregationScope.TOURNAMENT):
        return selection.rank_end - selection.rank_start + 1
    if (selection.selection_method == SelectionMethod.BEST_REMAINING
            and selection.aggregation_scope == AggregationScope.SEASON_AGGREGATE):
        return selection.places or 0
    return 0


def build_qualification_rules(profile: CompetitionRules) -> QualificationRules | None:
    """Engine rules for the profile, or None when it has no tournament berths."""
    config = profile.qualification_sources
    if config is None:
        return None
    return QualificationRules(
        destination_season_offset=config.destination_season_offset,
        tournament_berths=config.tournament_berths,
        table_berths=tuple(
            TableBerthRule(destination=rule.qualification_destination, places=places)
            for rule in profile.objectives or ()
            if (places := _table_places(rule))
        ),
    )
