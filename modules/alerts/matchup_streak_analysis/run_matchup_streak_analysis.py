"""
Matchup Streak Analysis Orchestrator.

This module is the public entry point for matchup streak analysis.
It:
  - Re-exports the MatchupStreakContext dataclass
  - Exposes build_matchup_streak_context() and should_send_streak_alert()
  - Wires together head_to_head, historical_form, winning_odds, and constants
    into a single analysis pipeline.

Replaces: streak_alerts.StreakAlertEngine and the top-level streak_alert_engine
instance.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from modules.sofascore import api_client
from infrastructure.persistence.repositories import SeasonRepository
from infrastructure.settings import Config

from .head_to_head import analyze_head_to_head_events, detect_streak
from .historical_form import (
    get_team_last_results_by_id,
    process_team_results_into_batches,
    _calculate_final_real_ranking,
    _get_filtering_criteria,
)
from .winning_odds import get_winning_odds_data

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MatchupStreakContext dataclass (previously in streak_alerts.py)
# ---------------------------------------------------------------------------

@dataclass
class MatchupStreakContext:
    """Represents Matchup streak analysis between two teams (relative to upcoming event)"""
    event_id: int
    custom_id: str
    participants: str
    discovery_source: str
    competition_name: str
    competition_slug: str
    season_id: Optional[int]
    season_name: Optional[str]
    observations: Optional[List[Dict]]
    sport: str
    home_team_name: str  # Upcoming event home team
    away_team_name: str  # Upcoming event away team
    sofascores_snapshot_home_team_ranking: Optional[int]  # Ranking of upcoming event's home team
    sofascores_snapshot_away_team_ranking: Optional[int]  # Ranking of upcoming event's away team
    raw_h2h_matchup_event_count: int
    h2h_matchup_matches_analyzed: int  # Matches within 2-year window
    h2h_matchup_home_wins: int  # Wins for upcoming event's home team
    h2h_matchup_away_wins: int  # Wins for upcoming event's away team
    h2h_matchup_draws: int
    h2h_matchup_home_win_rate: float  # Percentage
    h2h_matchup_away_win_rate: float  # Percentage
    h2h_matchup_draw_rate: float  # Percentage
    h2h_matchup_matches: List[Dict]  # All H2H matches with detailed information (most recent first)
    h2h_matchup_home_net_points: int  # Net points when upcoming home team was home
    h2h_matchup_away_net_points: int  # Net points when upcoming home team was away
    h2h_matchup_streak_summary: str  # e.g., "Home team won last 3 matches"
    h2h_matchup_streak_count: int
    h2h_matchup_avg_home_score: float  # Avg score for upcoming home team
    h2h_matchup_avg_away_score: float  # Avg score for upcoming away team
    minutes_until_start: int
    # Team results data
    home_team_results: List[Dict]  # Last 10 results for home team
    away_team_results: List[Dict]  # Last 10 results for away team
    home_team_wins: int  # Wins in last 10 games for home team
    away_team_wins: int  # Wins in last 10 games for away team
    home_team_losses: int  # Losses in last 10 games for home team
    away_team_losses: int  # Losses in last 10 games for away team
    home_team_draws: int  # Draws in last 10 games for home team
    away_team_draws: int  # Draws in last 10 games for away team
    # Batched team form data
    home_team_batches: List[Dict]  # Home team results in batches of 5
    away_team_batches: List[Dict]  # Away team results in batches of 5
    # Final real rankings (average of batch real rankings)
    home_team_final_real_ranking: float = 0  # Final real ranking for home team
    away_team_final_real_ranking: float = 0  # Final real ranking for away team
    # Standings snapshots
    home_team_standing: Optional[Dict] = None
    away_team_standing: Optional[Dict] = None
    # Winning odds data
    winning_odds_data: Optional[Dict] = None  # Winning odds response data
    # Current event odds (for display in H2H streak messages)
    one_open: Optional[float] = None
    x_open: Optional[float] = None
    two_open: Optional[float] = None
    one_final: Optional[float] = None
    x_final: Optional[float] = None
    two_final: Optional[float] = None
    # Overall win streaks (unfiltered)
    home_current_win_streak: int = 0
    away_current_win_streak: int = 0
    standings_response: Optional[List[Dict]] = None


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def build_matchup_streak_context(
    event_id: int,
    event_custom_id: str,
    event_start_time: datetime,
    sport: str,
    discovery_source: str,
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    competition_name: str,
    competition_slug: str,
    season_id: int,
    season_name: str,
    participants: str,
    home_team_name: str,
    away_team_name: str,
    matchup_events: List[Dict],
    minutes_until_start: int,
    season_year: Optional[int] = None,
    observations: Optional[List[Dict]] = None,
    home_team_id: int = None,
    away_team_id: int = None,
    event_odds: Optional[Any] = None,
    standings_response: Optional[List[Dict]] = None,
    debug_mode: bool = False,
) -> Optional[MatchupStreakContext]:
    """
    Analyze H2H events and build a MatchupStreakContext for an upcoming event.

    Tracks wins relative to ACTUAL TEAMS (not home/away positions which change historically).

    Args:
        event_id: Upcoming event ID
        event_custom_id: Custom identifier for the event
        event_start_time: datetime of the upcoming event
        sport: Sport type (e.g. 'Basketball', 'Tennis')
        discovery_source: How the event was discovered
        source_unique_tournament_id: SofaScore unique tournament id for standings lookup
        source_tournament_id: SofaScore source tournament id for DB/identity filtering
        competition_name: Human-readable competition name
        competition_slug: URL slug for the competition
        season_id: Current season ID
        season_name: Human-readable season name
        participants: Human-readable matchup string
        home_team_name: Upcoming event home team name
        away_team_name: Upcoming event away team name
        matchup_events: Raw H2H event list from the API
        minutes_until_start: Minutes until the event starts
        season_year: Optional season year for fallback filtering
        observations: Optional list of observation dicts
        home_team_id: Home team ID for form retrieval
        away_team_id: Away team ID for form retrieval
        event_odds: Optional odds object with open/final odds fields

    Returns:
        MatchupStreakContext or None on error
    """
    try:
        # -----------------------------------------------------------------
        # H2H analysis
        # -----------------------------------------------------------------
        results = analyze_head_to_head_events(
            matchup_events=matchup_events,
            event_id=event_id,
            event_start_time=event_start_time,
            home_team_name=home_team_name,
            away_team_name=away_team_name,
            sport=sport,
            competition_slug=competition_slug,
            participants=participants,
            observations=observations,
        )

        # Log H2H filtering results
        filter_type, filter_value = _get_filtering_criteria(sport, competition_slug, observations)
        if filter_value:
            match_type_info = ""
            if sport in ('Tennis', 'Tennis Doubles'):
                match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                match_type_info = f" and match_type '{match_type}'"
            logger.info(
                f"📊 Matchup analysis: Found {len(results)} matches in {filter_type} "
                f"'{filter_value}'{match_type_info} (filtered by {filter_type})"
            )
        else:
            logger.info(f"📊 Matchup analysis: Found {len(results)} matches (no filtering)")

        # Calculate H2H statistics relative to upcoming event teams
        h2h_matchup_home_wins = sum(1 for r in results if r['winner'] == '1')
        h2h_matchup_away_wins = sum(1 for r in results if r['winner'] == '2')
        h2h_matchup_draws = sum(1 for r in results if r['winner'] == 'X')
        total = len(results)

        h2h_matchup_matches = results  # Already in most recent first order

        # Extract results list for streak detection
        all_h2h_results = [match['winner'] for match in h2h_matchup_matches]
        streak_text, streak_count = detect_streak(all_h2h_results, home_team_name, away_team_name)

        # Calculate averages relative to upcoming event teams
        avg_home = sum(r['home_score'] for r in results) / total if total > 0 else 0
        avg_away = sum(r['away_score'] for r in results) / total if total > 0 else 0

        # Calculate H2H net points by role
        h2h_matchup_home_net_points = 0
        h2h_matchup_away_net_points = 0
        for r in results:
            if r.get('upcoming_home_role') == 'home':
                h2h_matchup_home_net_points += (r['home_score'] - r['away_score'])
            else:
                h2h_matchup_away_net_points += (r['home_score'] - r['away_score'])

        # -----------------------------------------------------------------
        # Team form retrieval (parallel)
        # -----------------------------------------------------------------
        home_team_results: List[Dict] = []
        away_team_results: List[Dict] = []
        home_team_wins = 0
        away_team_wins = 0
        home_team_losses = 0
        away_team_losses = 0
        home_team_draws = 0
        away_team_draws = 0
        home_team_batches: List[Dict] = []
        away_team_batches: List[Dict] = []
        home_overall_win_streak = 0
        away_overall_win_streak = 0

        # Use provided season_year, or fallback to parsing season_name if not provided
        target_season_year = season_year
        if target_season_year is None and season_name:
            target_season_year = SeasonRepository._parse_year(season_name)

        from .historical_form import ENABLE_SEASON_YEAR_FILTERING
        if ENABLE_SEASON_YEAR_FILTERING and target_season_year:
            logger.info(f"📅 Using season year filtering: {target_season_year} (fallback enabled: {ENABLE_SEASON_YEAR_FILTERING})")

        event_start_ts = event_start_time.timestamp() if event_start_time else None

        with ThreadPoolExecutor(max_workers=2) as team_executor:
            home_future = None
            away_future = None

            if home_team_id:
                home_future = team_executor.submit(
                    get_team_last_results_by_id,
                    home_team_id,
                    home_team_name,
                    competition_slug,
                    sport,
                    season_id=season_id,
                    source_unique_tournament_id=source_unique_tournament_id,
                    source_tournament_id=source_tournament_id,
                    season_year=target_season_year,
                    observations=observations,
                    exclude_event_id=event_id,
                    event_start_timestamp=event_start_ts,
                    debug_mode=debug_mode,
                )
            else:
                logger.debug(f"No home_team_id provided for {home_team_name}")

            if away_team_id:
                away_future = team_executor.submit(
                    get_team_last_results_by_id,
                    away_team_id,
                    away_team_name,
                    competition_slug,
                    sport,
                    season_id=season_id,
                    source_unique_tournament_id=source_unique_tournament_id,
                    source_tournament_id=source_tournament_id,
                    season_year=target_season_year,
                    observations=observations,
                    exclude_event_id=event_id,
                    event_start_timestamp=event_start_ts,
                    debug_mode=debug_mode,
                )
            else:
                logger.debug(f"No away_team_id provided for {away_team_name}")

        # Collect results from parallel execution
        if home_future:
            home_team_results, home_overall_win_streak = home_future.result()
        if away_future:
            away_team_results, away_overall_win_streak = away_future.result()

        # Trim results to match the player with fewer results (for fair ranking comparison)
        if home_team_results and away_team_results:
            home_count = len(home_team_results)
            away_count = len(away_team_results)
            min_count = min(home_count, away_count)

            if home_count != away_count:
                logger.info(f"📊 Trimming results to {min_count} matches (home had {home_count}, away had {away_count})")
                home_team_results = home_team_results[:min_count]
                away_team_results = away_team_results[:min_count]

        # Calculate stats and process batches for home team
        home_team_final_real_ranking = 0
        if home_team_results:
            home_team_wins = sum(1 for r in home_team_results if r['team_result_code'] == '1')
            home_team_losses = sum(1 for r in home_team_results if r['team_result_code'] == '2')
            home_team_draws = sum(1 for r in home_team_results if r['team_result_code'] == 'X')
            home_team_batches = process_team_results_into_batches(home_team_results, home_team_name)
            home_team_final_real_ranking = _calculate_final_real_ranking(home_team_batches)
            logger.info(
                f"📊 Home team {home_team_name} form: "
                f"{home_team_wins}W-{home_team_losses}L-{home_team_draws}D "
                f"({len(home_team_batches)} batches, final ranking: {home_team_final_real_ranking})"
            )

        # Calculate stats and process batches for away team
        away_team_final_real_ranking = 0
        if away_team_results:
            away_team_wins = sum(1 for r in away_team_results if r['team_result_code'] == '1')
            away_team_losses = sum(1 for r in away_team_results if r['team_result_code'] == '2')
            away_team_draws = sum(1 for r in away_team_results if r['team_result_code'] == 'X')
            away_team_batches = process_team_results_into_batches(away_team_results, away_team_name)
            away_team_final_real_ranking = _calculate_final_real_ranking(away_team_batches)
            logger.info(
                f"📊 Away team {away_team_name} form: "
                f"{away_team_wins}W-{away_team_losses}L-{away_team_draws}D "
                f"({len(away_team_batches)} batches, final ranking: {away_team_final_real_ranking})"
            )

        # -----------------------------------------------------------------
        # Winning odds
        # -----------------------------------------------------------------
        winning_odds_data = get_winning_odds_data(event_id)

        # Log summary of what data we have
        data_summary = []
        if total > 0:
            data_summary.append(f"Matchup: {total} matches")
        if home_team_wins + home_team_losses + home_team_draws > 0:
            data_summary.append(f"Home form: {home_team_wins}W-{home_team_losses}L-{home_team_draws}D")
        if away_team_wins + away_team_losses + away_team_draws > 0:
            data_summary.append(f"Away form: {away_team_wins}W-{away_team_losses}L-{away_team_draws}D")
        if winning_odds_data:
            data_summary.append("Winning odds: available")

        if data_summary:
            logger.info(f"📊 Matchup analysis for {participants}: {', '.join(data_summary)}")
        else:
            logger.info(
                f"📊 Matchup analysis for {participants}: "
                "No data available (no H2H matches, no team form, no winning odds)"
            )

        # -----------------------------------------------------------------
        # Standings snapshot
        # -----------------------------------------------------------------
        sofascores_snapshot_home_team_ranking = None
        sofascores_snapshot_away_team_ranking = None
        home_team_standing = None
        away_team_standing = None
        resolved_standings_response = standings_response

        standings_api_unique_tournament_id = source_unique_tournament_id or source_tournament_id
        if sport not in ['Tennis', 'Tennis Doubles'] and season_id and standings_api_unique_tournament_id and (home_team_id or away_team_id):
            raw_standings = standings_response
            if raw_standings is None:
                logger.info(
                    "No preloaded standings response for event %s; falling back to SofaScore API (season_id=%s, source_unique_tournament_id=%s, source_tournament_id=%s)",
                    event_id,
                    season_id,
                    source_unique_tournament_id,
                    source_tournament_id,
                )
                raw_standings = api_client.get_standings_response(season_id, standings_api_unique_tournament_id)
            else:
                logger.info(
                    "Using preloaded standings response for event %s from pre-start payload",
                    event_id,
                )
            resolved_standings_response = raw_standings
            home_team_standing, away_team_standing = api_client.process_standings_response(
                raw_standings,
                home_team_id,
                away_team_id
            )

            if home_team_standing and sofascores_snapshot_home_team_ranking is None:
                sofascores_snapshot_home_team_ranking = home_team_standing.get('position')
            if away_team_standing and sofascores_snapshot_away_team_ranking is None:
                sofascores_snapshot_away_team_ranking = away_team_standing.get('position')

            if home_team_standing or away_team_standing:
                logger.debug(
                    f"Extracted standings snapshots for event {event_id}: "
                    f"home={home_team_standing}, away={away_team_standing}"
                )
            else:
                logger.debug(f"No standings found for event {event_id}")

        if observations:
            rankings_obs = next(
                (obs for obs in observations if isinstance(obs, dict) and obs.get('type') == 'rankings'),
                None
            )
            if rankings_obs:
                sofascores_snapshot_home_team_ranking = rankings_obs.get('home_ranking')
                sofascores_snapshot_away_team_ranking = rankings_obs.get('away_ranking')
                logger.debug(f"Extracted rankings from observations: home={sofascores_snapshot_home_team_ranking}, away={sofascores_snapshot_away_team_ranking}")
            else:
                logger.debug(f"No rankings found in observations for event {event_id}")

        # -----------------------------------------------------------------
        # Event odds extraction
        # -----------------------------------------------------------------
        one_open = None
        x_open = None
        two_open = None
        one_final = None
        x_final = None
        two_final = None

        if event_odds:
            try:
                one_open = float(event_odds.one_open) if event_odds.one_open is not None else None
                x_open = float(event_odds.x_open) if event_odds.x_open is not None else None
                two_open = float(event_odds.two_open) if event_odds.two_open is not None else None
                one_final = float(event_odds.one_final) if event_odds.one_final is not None else None
                x_final = float(event_odds.x_final) if event_odds.x_final is not None else None
                two_final = float(event_odds.two_final) if event_odds.two_final is not None else None
                logger.debug(
                    f"Extracted odds for event {event_id}: "
                    f"1={one_open}→{one_final}, X={x_open}→{x_final}, 2={two_open}→{two_final}"
                )
            except (AttributeError, ValueError, TypeError) as e:
                logger.warning(f"Error extracting odds from event_odds for event {event_id}: {e}")

        # -----------------------------------------------------------------
        # Assemble and return context
        # -----------------------------------------------------------------
        return MatchupStreakContext(
            event_id=event_id,
            custom_id=event_custom_id,
            participants=participants,
            discovery_source=discovery_source,
            competition_name=competition_name,
            competition_slug=competition_slug,
            season_id=season_id,
            season_name=season_name,
            observations=observations,
            sport=sport,
            home_team_name=home_team_name,
            away_team_name=away_team_name,
            sofascores_snapshot_home_team_ranking=sofascores_snapshot_home_team_ranking,
            sofascores_snapshot_away_team_ranking=sofascores_snapshot_away_team_ranking,
            raw_h2h_matchup_event_count=len(matchup_events),
            h2h_matchup_matches_analyzed=total,
            h2h_matchup_home_wins=h2h_matchup_home_wins,
            h2h_matchup_away_wins=h2h_matchup_away_wins,
            h2h_matchup_draws=h2h_matchup_draws,
            h2h_matchup_home_win_rate=round(h2h_matchup_home_wins / total * 100, 1) if total > 0 else 0,
            h2h_matchup_away_win_rate=round(h2h_matchup_away_wins / total * 100, 1) if total > 0 else 0,
            h2h_matchup_draw_rate=round(h2h_matchup_draws / total * 100, 1) if total > 0 else 0,
            h2h_matchup_matches=h2h_matchup_matches,
            h2h_matchup_home_net_points=h2h_matchup_home_net_points,
            h2h_matchup_away_net_points=h2h_matchup_away_net_points,
            h2h_matchup_streak_summary=streak_text,
            h2h_matchup_streak_count=streak_count,
            h2h_matchup_avg_home_score=round(avg_home, 1),
            h2h_matchup_avg_away_score=round(avg_away, 1),
            minutes_until_start=minutes_until_start,
            # Team results data
            home_team_results=home_team_results,
            away_team_results=away_team_results,
            home_team_wins=home_team_wins,
            away_team_wins=away_team_wins,
            home_team_losses=home_team_losses,
            away_team_losses=away_team_losses,
            home_team_draws=home_team_draws,
            away_team_draws=away_team_draws,
            # Batched team form data
            home_team_batches=home_team_batches,
            away_team_batches=away_team_batches,
            # Final real rankings
            home_team_final_real_ranking=home_team_final_real_ranking,
            away_team_final_real_ranking=away_team_final_real_ranking,
            # Standings snapshots
            home_team_standing=home_team_standing,
            away_team_standing=away_team_standing,
            # Winning odds data
            winning_odds_data=winning_odds_data,
            # Current event odds
            one_open=one_open,
            x_open=x_open,
            two_open=two_open,
            one_final=one_final,
            x_final=x_final,
            two_final=two_final,
            # Overall win streaks
            home_current_win_streak=home_overall_win_streak,
            away_current_win_streak=away_overall_win_streak,
            standings_response=resolved_standings_response,
        )

    except Exception as e:
        logger.error(f"Error analyzing H2H events: {e}")
        return None


def should_send_streak_alert(streak: MatchupStreakContext) -> bool:
    """
    Determine if a streak alert should be sent.

    UPDATED: Now requires at least one team to have ≥STREAK_ALERT_MIN_RESULTS past results.
    This filters out events with insufficient historical data.

    Send alerts if we have sufficient data AND at least one of:
    1. Matchup data (at least 1 match analyzed), OR
    2. Team form data (at least one team has results), OR
    3. Winning odds data

    Args:
        streak: MatchupStreakContext object

    Returns:
        True if we have any meaningful data to show AND sufficient historical data
    """
    # Calculate total games for each team
    home_total_games = len(streak.home_team_results) if hasattr(streak, 'home_team_results') and streak.home_team_results else 0
    away_total_games = len(streak.away_team_results) if hasattr(streak, 'away_team_results') and streak.away_team_results else 0

    # NEW REQUIREMENT: At least one team must have ≥STREAK_ALERT_MIN_RESULTS past results
    min_results_threshold = Config.STREAK_ALERT_MIN_RESULTS
    has_sufficient_data = home_total_games >= min_results_threshold or away_total_games >= min_results_threshold

    if not has_sufficient_data:
        logger.info(
            f"⏭️ Matchup streak alert skipped for {streak.participants}: "
            f"Insufficient data (home: {home_total_games}, away: {away_total_games}, need ≥{min_results_threshold})"
        )
        return False

    # Check if we have Matchup data
    has_h2h_data = streak.h2h_matchup_matches_analyzed >= 1

    # Check if we have team form data
    has_team_form = (
        (streak.home_team_wins + streak.home_team_losses + streak.home_team_draws > 0) or
        (streak.away_team_wins + streak.away_team_losses + streak.away_team_draws > 0)
    )

    # Check if we have winning odds data
    has_winning_odds = streak.winning_odds_data is not None

    should_send = has_h2h_data or has_team_form or has_winning_odds

    if should_send:
        reasons = []
        if has_h2h_data:
            reasons.append(f"Matchup data ({streak.h2h_matchup_matches_analyzed} matches)")
        if has_team_form:
            reasons.append("team form data")
        if has_winning_odds:
            reasons.append("winning odds data")
        logger.info(
            f"✅ Matchup streak alert will send for {streak.participants}: "
            f"{', '.join(reasons)} (data: home={home_total_games}, away={away_total_games})"
        )
    else:
        logger.info(
            f"⏭️ Matchup streak alert skipped for {streak.participants}: "
            f"No meaningful data (Matchup: {streak.h2h_matchup_matches_analyzed}, "
            f"Home form: {streak.home_team_wins + streak.home_team_losses + streak.home_team_draws}, "
            f"Away form: {streak.away_team_wins + streak.away_team_losses + streak.away_team_draws}, "
            f"Winning odds: {has_winning_odds})"
        )

    return should_send
