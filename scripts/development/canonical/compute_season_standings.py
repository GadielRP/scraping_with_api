"""Send full-season standings to the configured personal Telegram chat.

Examples:
    python -m scripts.development.compute_season_standings --season-id 86668 --competition MLS
    python -m scripts.development.compute_season_standings --season-id 87913 --competition 310 --annual

The calculators require a cutoff. A far-future bound includes every stored result
for the selected season; it does not force final rankings or invent missing games.
The tie-break sequence is chosen from the stored results: the closing sequence applies
only when every game of the season is stored (100 %); otherwise the in-season one does.
The report title shows the policy used and the season completion ratio.
A second message lists which clubs reach which competition. It comes from the same
competition context the pre-start pipeline builds: the profile bound by
resolve_p1_competition_context and the berths allocated by prepare_qualification.
"""

import argparse
import os
from dataclasses import asdict
from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import func, or_, select

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Season
from infrastructure.persistence.repositories.competition_repository import get_competition_profile_for_local_id
from modules.standings.telegram_report import (
    format_annual_standings_table_for_telegram,
    format_berth_allocation_for_telegram,
    format_standings_table_for_telegram,
    format_fair_play_table_for_telegram,
    debug_standings_enabled,
)
from infrastructure.network.telegram import send_telegram_message
from modules.standings.season_aggregate import calculate_season_aggregate_standings
from modules.standings.calculator import standings_calculator
from modules.competition.rule_terms import RankingMetric
from modules.pillars.context import EventIdentity, build_competition_context
from modules.pillars.pillar_1_team_structure.competition_context_resolver import resolve_p1_competition_context
from modules.pillars.pillar_1_team_structure.qualification_preparation import prepare_qualification


def _policy_label(policy, ratio):
    return f"policy={policy} completion={'n/a' if ratio is None else f'{ratio:.1%}'}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season-id", required=True, type=int)
    parser.add_argument("--competition", "--competition-id", required=True,
                        help="Local competition ID or exact display/canonical name (case insensitive).")
    parser.add_argument("--annual", action="store_true", help="Use the profile's shared season aggregate table.")
    arguments = parser.parse_args()
    if not debug_standings_enabled():
        parser.error("Configure TELEGRAM_BOT_TOKEN and PERSONAL_CHAT_ID to send standings.")
    with db_manager.get_session() as session:
        competition_filter = (
            Competition.competition_id == int(arguments.competition)
            if arguments.competition.isdigit() else or_(
                func.lower(Competition.display_name) == arguments.competition.lower(),
                func.lower(Competition.canonical_name) == arguments.competition.lower(),
            )
        )
        competitions = session.scalars(select(Competition).where(competition_filter)).all()
        display_matches = [item for item in competitions if item.display_name.lower() == arguments.competition.lower()]
        if display_matches:
            competitions = display_matches
        if len(competitions) != 1:
            matches = ", ".join(f"{item.competition_id}: {item.display_name}" for item in competitions)
            parser.error(f"Specify exactly one competition. Matches: {matches or 'none'}")
        competition = competitions[0]
        season = session.get(Season, arguments.season_id)
        if season is None:
            parser.error(f"Unknown season_id={arguments.season_id}")
        profile = get_competition_profile_for_local_id(competition.competition_id)
        if profile is None:
            parser.error(f"No sporting profile for competition_id={competition.competition_id}")
        # Year 3000 is also representable by Windows' Unix timestamp conversion.
        all_results_bound = datetime(3000, 1, 1, tzinfo=timezone.utc)
        title = f"{competition.display_name} | season_id={season.id} | year={season.year}"
        # The pre-start pipeline's inputs: a competition context and the standings its streak carries.
        # Aggregate tables are computed by prepare_qualification itself, so no current standings are needed.
        competition_context = build_competition_context(competition)
        event_identity = EventIdentity(
            event_id=0, participants_label=title, starts_at=all_results_bound, minutes_until_start=None,
            sport="unknown", competition_id=competition.competition_id,
            competition_name=competition.display_name, season_id=season.id, season_year=season.year,
        )
        streak = SimpleNamespace(current_standings=None, league_totals_context=None)
        if arguments.annual:
            sources = profile.qualification_sources
            if sources is None or sources.shared_table is None or season.year is None:
                parser.error("Annual mode requires a shared table in the profile and a season year.")
            result = calculate_season_aggregate_standings(
                session, sources.shared_table, season_year=season.year, kickoff_cutoff=all_results_bound,
            )
            policy_name = result.standings_policy
            title += f" | {_policy_label(policy_name, result.season_completion_ratio)}"
            report = format_annual_standings_table_for_telegram(result.standings, title)
            fair_play_standings = {row.name: asdict(row) for row in result.standings}
            standings_rules = sources.shared_table.standings
        else:
            result = standings_calculator.calculate_standings_bundle_at(
                season_id=season.id, cutoff_timestamp=all_results_bound.timestamp(),
                competition_id=competition.competition_id,
                source_unique_tournament_id=competition.source_unique_tournament_id,
                source_tournament_id=competition.source_tournament_id,
            )
            if result.get("status") == "ERROR":
                parser.error(result["error"])
            fair_play_standings = result["standings"]
            streak = SimpleNamespace(
                current_standings=result["standings"], league_totals_context=result.get("league_totals_context"),
            )
            first_row = next(iter(fair_play_standings.values()), {})
            policy_name = first_row.get("standings_policy", "in_season")
            title += f" | {_policy_label(policy_name, first_row.get('season_completion_ratio'))}"
            report = format_standings_table_for_telegram(result["standings"], title)
            standings_rules = profile.standings
    send_telegram_message(report, os.environ["PERSONAL_CHAT_ID"])
    criteria = getattr(standings_rules, f"{policy_name}_criteria")
    metrics = {metric for criterion in criteria for metric in criterion.metrics}
    if standings_rules.fair_play_weights and metrics.intersection({
        RankingMetric.NEGATIVE_FAIR_PLAY_POINTS,
        RankingMetric.NEGATIVE_FAIR_PLAY_POINTS_PER_GAME,
        RankingMetric.NEGATIVE_FAIR_PLAY_RED_CARDS,
    }):
        fair_play_report = format_fair_play_table_for_telegram(
            fair_play_standings, title,
            break_ties_on_red_cards=RankingMetric.NEGATIVE_FAIR_PLAY_RED_CARDS in metrics,
        )
        if fair_play_report:
            send_telegram_message(fair_play_report, os.environ["PERSONAL_CHAT_ID"])

    competition_context = resolve_p1_competition_context(event_identity, competition_context)
    competition_context = prepare_qualification(
        event_identity, competition_context, streak, as_of=all_results_bound,
    )
    if competition_context.berth_allocation is None:
        print("No berth allocation for this competition and season.")
        return
    send_telegram_message(
        format_berth_allocation_for_telegram(competition_context.berth_allocation, title),
        os.environ["PERSONAL_CHAT_ID"],
    )


if __name__ == "__main__":
    main()
