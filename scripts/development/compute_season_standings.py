"""Send full-season standings to the configured personal Telegram chat.

Examples:
    python -m scripts.development.compute_season_standings --season-id 86668 --competition MLS
    python -m scripts.development.compute_season_standings --season-id 87913 --competition 310 --annual

The calculators require a cutoff. A far-future bound includes every stored result
for the selected season; it does not force final rankings or invent missing games.
"""

import argparse
import os
from dataclasses import asdict
from datetime import datetime, timezone

from sqlalchemy import func, or_, select

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Season
from infrastructure.persistence.repositories.competition_repository import get_competition_profile_for_local_id
from modules.alerts.matchup_streak_analysis.historical_form_reporting import (
    format_annual_standings_table_for_telegram,
    format_standings_table_for_telegram,
    format_fair_play_table_for_telegram,
    debug_standings_enabled,
    send_debug_telegram,
)
from modules.alerts.matchup_streak_analysis.season_aggregate_standings_calculator import calculate_season_aggregate_standings
from modules.alerts.matchup_streak_analysis.standings_engine import standings_calculator
from modules.competition.rule_terms import RankingMetric


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
        if arguments.annual:
            sources = profile.qualification_sources
            if sources is None or sources.shared_table is None or season.year is None:
                parser.error("Annual mode requires a shared table in the profile and a season year.")
            result = calculate_season_aggregate_standings(
                session, sources.shared_table, season_year=season.year, kickoff_cutoff=all_results_bound,
            )
            report = format_annual_standings_table_for_telegram(result.standings, title)
            fair_play_standings = {row.name: asdict(row) for row in result.standings}
            standings_rules = sources.shared_table.standings
            policy_name = result.standings_policy
        else:
            result = standings_calculator.calculate_standings_bundle_at(
                season_id=season.id, cutoff_timestamp=all_results_bound.timestamp(),
                competition_id=competition.competition_id,
                source_unique_tournament_id=competition.source_unique_tournament_id,
                source_tournament_id=competition.source_tournament_id,
            )
            if result.get("status") == "ERROR":
                parser.error(result["error"])
            report = format_standings_table_for_telegram(result["standings"], title)
            fair_play_standings = result["standings"]
            standings_rules = profile.standings
            policy_name = next(iter(fair_play_standings.values()), {}).get("standings_policy", "in_season")
    send_debug_telegram(report, os.environ["PERSONAL_CHAT_ID"])
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
            send_debug_telegram(fair_play_report, os.environ["PERSONAL_CHAT_ID"])


if __name__ == "__main__":
    main()
