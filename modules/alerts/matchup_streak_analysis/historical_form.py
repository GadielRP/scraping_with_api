"""
Historical Form processing for Matchup Streak Analysis.

Handles fetching and processing team form data from the API (Route 2)
and from the local database for collected seasons (Route 1).
Extracted from streak_alerts.py (StreakAlertEngine methods).
"""

import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime

from modules.sofascore import api_client
from infrastructure.persistence.repositories import SeasonRepository

from modules.competition.league_config import (
    get_grouping_method,
    get_included_season_ids,
    get_standings_method,
    is_collected_competition_scope,
)
from .historical_form_service import historical_form_processor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants (previously class-level on StreakAlertEngine)
# ---------------------------------------------------------------------------

DEFAULT_MIN_RESULTS = 10
ENABLE_SEASON_YEAR_FILTERING = True


# ---------------------------------------------------------------------------
# Calculation helpers (previously private methods of StreakAlertEngine)
# ---------------------------------------------------------------------------

def _calculate_tennis_total_points(game: Dict, is_team: bool) -> int:
    """
    Calculate total tennis points from all periods (sets) for a game.

    Args:
        game: Game dictionary with period point data
        is_team: True to calculate for team, False for opponent

    Returns:
        Total points across all periods
    """
    prefix = 'team' if is_team else 'opponent'

    period1 = game.get(f'{prefix}_period1', 0) or 0
    period2 = game.get(f'{prefix}_period2', 0) or 0
    period3 = game.get(f'{prefix}_period3', 0) or 0  # May be 0 for 2-set matches

    return period1 + period2 + period3


def _calculate_single_ranking(result: str, own_ranking: int, opponent_ranking: int) -> int:
    """
    Calculate the single ranking for a game based on result and rankings.

    Logic:
    - Win + opponent lower ranked (higher number): use opponent's ranking
    - Win + opponent higher ranked (lower number): use player's ranking
    - Loss + opponent lower ranked (higher number): use player's ranking
    - Loss + opponent higher ranked (lower number): use opponent's ranking

    Simplified: Win = min(own, opp), Loss = max(own, opp)

    Args:
        result: '1' for win, '2' for loss, 'X' for draw
        own_ranking: Player's ranking
        opponent_ranking: Opponent's ranking

    Returns:
        Single ranking for this game (0 if rankings not available)
    """
    if own_ranking == 0 or opponent_ranking == 0:
        return 0

    if result == '1':  # Win
        return min(own_ranking, opponent_ranking)
    elif result == '2':  # Loss
        return max(own_ranking, opponent_ranking)
    else:  # Draw
        # For draws, use average of both rankings
        return (own_ranking + opponent_ranking) // 2


def _calculate_final_real_ranking(batches: List[Dict]) -> float:
    """
    Calculate final real ranking as average of all batch real rankings.
    Excludes batches with 0 real ranking.

    Args:
        batches: List of batch dictionaries with batch_real_ranking

    Returns:
        Final real ranking (0 if no valid rankings)
    """
    valid_rankings = [b['batch_real_ranking'] for b in batches if b.get('batch_real_ranking', 0) > 0]
    return float(sum(valid_rankings) / len(valid_rankings)) if valid_rankings else 0


def _calculate_current_win_streak(results: List[Dict]) -> int:
    """
    Calculate the current win streak (no filters) from a chronological list of results.

    Args:
        results: List of result dictionaries sorted by most recent first.

    Returns:
        Count of consecutive wins from the most recent result.
    """
    if not results:
        return 0

    streak_count = 0
    for result in results:
        if result.get('team_result_code') == '1':
            streak_count += 1
        else:
            break

    return streak_count


def _get_filtering_criteria(sport: str, competition_slug: str, observations: Optional[List[Dict]] = None) -> Tuple[str, str]:
    """
    Determine the filtering criteria and value for logging purposes.

    Args:
        sport: Sport type
        competition_slug: Competition slug for non-tennis sports
        observations: Observations for sport-specific filtering

    Returns:
        Tuple of (filter_type, filter_value) for logging
    """
    if (sport == 'Tennis' or sport == 'Tennis Doubles') and observations:
        # For tennis, search for ground_type anywhere in observations (not just first item)
        ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None)
        if ground_type_obs:
            return ('ground_type', ground_type_obs.get('value', 'unknown'))

    # For other sports, use competition filtering
    return ('competition', competition_slug)


def _extract_ranking_from_team(team_data: Dict) -> int:
    """
    Extract ranking from team data, handling both singles and doubles teams.

    Args:
        team_data: Team data dictionary from event

    Returns:
        Ranking as integer (0 if not found)
    """
    # First check if team has direct ranking (singles)
    ranking = team_data.get('ranking', None)
    if ranking is not None:
        return ranking

    # If no direct ranking, check subTeams for doubles teams
    sub_teams = team_data.get('subTeams', [])
    if sub_teams:
        # Get all rankings from subTeams and return the best (lowest) ranking
        rankings = [sub.get('ranking') for sub in sub_teams if sub.get('ranking') is not None]
        if rankings:
            return min(rankings)

    return 0


# ---------------------------------------------------------------------------
# Batch processing (previously StreakAlertEngine.process_team_results_into_batches)
# ---------------------------------------------------------------------------

def process_team_results_into_batches(team_results: List[Dict], team_name: str) -> List[Dict]:
    """
    Process team results into batches of 5 matches with individual game results and batch totals.

    For tennis, uses period points instead of sets for net differential calculation.

    Args:
        team_results: List of team results from get_team_last_results_by_id
        team_name: Name of the team for context

    Returns:
        List of batch dictionaries, each containing:
        - batch_number: Sequential batch number (1, 2, 3, etc.)
        - games: List of individual game results in this batch
        - batch_wins: Number of wins in this batch
        - batch_losses: Number of losses in this batch
        - batch_draws: Number of draws in this batch
        - batch_points_for: Total points scored by team in this batch (sets or total game points)
        - batch_points_against: Total points scored against team in this batch
        - batch_net_points: Net points (for - against) in this batch
    """
    if not team_results:
        return []

    batches = []
    batch_size = 5

    # Check if this is tennis data (has period points)
    is_tennis = any('team_period1' in game for game in team_results)

    # Process results in batches of 5 (most recent first)
    for i in range(0, len(team_results), batch_size):
        batch_games = team_results[i:i + batch_size]
        batch_number = (i // batch_size) + 1

        # Calculate batch statistics
        batch_wins = sum(1 for game in batch_games if game["team_result_code"] == '1')
        batch_losses = sum(1 for game in batch_games if game["team_result_code"] == '2')
        batch_draws = sum(1 for game in batch_games if game["team_result_code"] == 'X')

        # Calculate points totals
        if is_tennis:
            # For tennis, use total points from all periods instead of sets
            batch_points_for = 0
            batch_points_against = 0
            for game in batch_games:
                team_total = _calculate_tennis_total_points(game, is_team=True)
                opponent_total = _calculate_tennis_total_points(game, is_team=False)
                batch_points_for += team_total
                batch_points_against += opponent_total
            batch_net_points = batch_points_for - batch_points_against
        else:
            # For other sports, use regular scores
            batch_points_for = sum(game["team_score"] for game in batch_games)
            batch_points_against = sum(game["opponent_score"] for game in batch_games)
            batch_net_points = batch_points_for - batch_points_against

        # Calculate net points by role
        batch_home_net_points = 0
        batch_away_net_points = 0
        for game in batch_games:
            if is_tennis:
                # For tennis, calculate net points from period points
                team_total = _calculate_tennis_total_points(game, is_team=True)
                opponent_total = _calculate_tennis_total_points(game, is_team=False)
                net = team_total - opponent_total
            else:
                # For other sports, use regular scores
                net = game["team_score"] - game["opponent_score"]

            if game["team_role"] == 'home':
                batch_home_net_points += net
            elif game["team_role"] == 'away':
                batch_away_net_points += net

        # Calculate real ranking (average of single rankings)
        single_rankings = []
        for game in batch_games:
            own_ranking = game["own_ranking"]
            opponent_ranking = game["opponent_ranking"]
            result = game["team_result_code"]
            single_ranking = _calculate_single_ranking(result, own_ranking, opponent_ranking)
            if single_ranking > 0:
                single_rankings.append(single_ranking)

        # Calculate average real ranking for this batch
        batch_real_ranking = round(int(sum(single_rankings)) / len(single_rankings)) if single_rankings else 0

        # Format individual games for display
        formatted_games = []
        for game in batch_games:
            result_code = game["team_result_code"]

            # Convert winner to display format
            if result_code == '1':
                result_symbol = 'W'
            elif result_code == '2':
                result_symbol = 'L'
            else:  # 'X'
                result_symbol = 'D'

            # Format opponent name (truncate if too long)
            opponent = game["opponent_name"]
            if len(opponent) > 15:
                opponent = opponent[:12] + "..."

            # Get role for this game
            team_role = game["team_role"]
            team_score = game["team_score"]
            opponent_score = game["opponent_score"]
            team_standing = game["team_standing"] if isinstance(game["team_standing"], dict) else {}
            opponent_standing = game["opponent_standing"] if isinstance(game["opponent_standing"], dict) else {}

            own_ranking = game["own_ranking"]
            opponent_ranking = game["opponent_ranking"]

            # Calculate score_for and score_against based on sport
            if is_tennis:
                # For tennis, use total points from all periods
                score_for = _calculate_tennis_total_points(game, is_team=True)
                score_against = _calculate_tennis_total_points(game, is_team=False)
            else:
                # For other sports, use regular scores
                score_for = team_score
                score_against = opponent_score

            # Calculate single ranking for this game
            single_ranking = _calculate_single_ranking(
                result_code,
                own_ranking,
                opponent_ranking
            )

            formatted_games.append({
                'result': result_symbol,
                'team_result_code': result_code,
                'team_result': result_symbol,
                'opponent': opponent,
                'team_name': game['team_name'],
                'opponent_name': game['opponent_name'],
                'score_for': score_for,
                'score_against': score_against,
                'net_score': score_for - score_against,
                'startTimestamp': game['startTimestamp'],  # Include timestamp for date display
                'team_role': team_role,
                'opponent_ranking': opponent_ranking,
                'own_ranking': own_ranking,
                'team_score': team_score,
                'opponent_score': opponent_score,
                'team_standing': team_standing,
                'opponent_standing': opponent_standing,
                'single_ranking': single_ranking,  # Single ranking for this game
            })

        batches.append({
            'batch_number': batch_number,
            'games': formatted_games,
            'batch_wins': batch_wins,
            'batch_losses': batch_losses,
            'batch_draws': batch_draws,
            'batch_points_for': batch_points_for,
            'batch_points_against': batch_points_against,
            'batch_net_points': batch_net_points,
            'batch_home_net_points': batch_home_net_points,
            'batch_away_net_points': batch_away_net_points,
            'batch_real_ranking': batch_real_ranking
        })

    return batches


# ---------------------------------------------------------------------------
# Event processing (previously StreakAlertEngine._process_events_into_results)
# ---------------------------------------------------------------------------

def _process_events_into_results(
    events: List[Dict],
    team_id: int,
    team_name: str,
    competition_slug: str,
    sport: str,
    season_id: Optional[str] = None,
    season_year: Optional[int] = None,
    observations: Optional[List[Dict]] = None,
    exclude_event_id: Optional[int] = None,
    apply_filters: bool = True,
    overall_results: Optional[List[Dict]] = None,
    overall_seen_event_ids: Optional[Set] = None
) -> List[Dict]:
    """
    Process events into results list with filtering applied.
    Helper to avoid code duplication between first and second fetch.

    Args:
        events: List of event dictionaries from API
        team_id: ID of the team
        team_name: Name of the team for context
        competition_slug: Slug of the competition
        sport: Sport type for filtering logic
        season_id: Season ID for filtering (non-tennis sports)
        season_year: Season Year for fallback filtering (e.g. 2023)
        observations: List of dicts containing filtering observations
        exclude_event_id: Optional event ID to exclude from results
        apply_filters: Whether to apply sport-specific filters
        overall_results: Optional list to collect unfiltered results
        overall_seen_event_ids: Optional set of seen event IDs for overall collection

    Returns:
        List of processed result dictionaries
    """
    results = []
    for event in events:
        try:
            # Exclude current event if exclude_event_id is provided
            event_id_from_api = event.get('id')
            if exclude_event_id and event_id_from_api == exclude_event_id:
                logger.debug(f"Skipping event {event_id_from_api} for {team_name} - this is the current/upcoming event being analyzed")
                continue

            # Extract result using proven logic
            # For tennis, also extract period points for points-based tracking
            extract_points = sport in ['Tennis', 'Tennis Doubles']
            result_data = api_client.extract_results_from_response({'event': event}, extract_tennis_points=extract_points, for_streaks=True)
            if not result_data:
                continue
            # Skip canceled/postponed events
            if result_data.get('_canceled'):
                continue

            # Get team names and data
            home_team_data = event.get('homeTeam', {})
            away_team_data = event.get('awayTeam', {})
            home_team_name = home_team_data.get('name', '')
            away_team_name = away_team_data.get('name', '')

            if home_team_name != team_name and away_team_name != team_name:
                logger.debug(f"Skipping event {event_id_from_api} for {team_name} - team not found among participants")
                continue

            is_team_home = home_team_name == team_name
            opponent_name = away_team_name if is_team_home else home_team_name
            opponent_ranking = _extract_ranking_from_team(away_team_data if is_team_home else home_team_data)
            own_ranking = _extract_ranking_from_team(home_team_data if is_team_home else away_team_data)

            raw_winner_code = event.get('winnerCode')
            winner_position = result_data.get('winner')

            if raw_winner_code == 1:
                team_result_code = '1' if is_team_home else '2'
            elif raw_winner_code == 2:
                team_result_code = '1' if not is_team_home else '2'
            else:
                if winner_position == '1':
                    team_result_code = '1' if is_team_home else '2'
                elif winner_position == '2':
                    team_result_code = '1' if not is_team_home else '2'
                else:
                    team_result_code = 'X'

            if is_team_home:
                team_score = result_data['home_score']
                opponent_score = result_data['away_score']
                team_role = 'home'
                opponent_role = 'away'
            else:
                team_score = result_data['away_score']
                opponent_score = result_data['home_score']
                team_role = 'away'
                opponent_role = 'home'

            team_result = 'W' if team_result_code == '1' else 'L' if team_result_code == '2' else 'D'

            passes_filters = True
            if apply_filters:
                if sport in ['Tennis', 'Tennis Doubles']:
                    if observations:
                        ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None)
                        if ground_type_obs:
                            ground_type_value = ground_type_obs.get('value')
                            if ground_type_value != event.get('groundType'):
                                logger.debug(f"Skipping event for {team_name} - different ground type: {ground_type_value} vs {event.get('groundType')}")
                                passes_filters = False
                    if passes_filters:
                        home_team_type = home_team_data.get('type', 1)
                        away_team_type = away_team_data.get('type', 1)
                        event_is_doubles = home_team_type == 2 and away_team_type == 2
                        sport_is_doubles = sport == 'Tennis Doubles'
                        if event_is_doubles != sport_is_doubles:
                            logger.debug(f"Skipping event for {team_name} - different match type (event is {'doubles' if event_is_doubles else 'singles'}, sport is {'doubles' if sport_is_doubles else 'singles'})")
                            passes_filters = False
                else:
                    # For non-tennis sports: filter by competition slug AND season_id
                    if competition_slug:
                        event_competition_slug = event.get('tournament', {}).get('uniqueTournament', {}).get('slug', '')
                        if event_competition_slug != competition_slug:
                            logger.debug(f"Skipping event for {team_name} - different competition: {event_competition_slug} vs {competition_slug}")
                            passes_filters = False

                    # Also filter by season_id if provided
                    if passes_filters and season_id:
                        event_season_id = str(event.get('season', {}).get('id', ''))

                        # ID Check (Primary)
                        id_match = event_season_id and event_season_id == season_id

                        # Year Check (Fallback)
                        year_match = False
                        event_year = None
                        if not id_match and ENABLE_SEASON_YEAR_FILTERING and season_year:
                            raw_year = event.get('season', {}).get('year')
                            event_year = SeasonRepository._parse_year(raw_year)
                            if event_year and event_year == season_year:
                                year_match = True
                                logger.debug(f"✅ Year match for {team_name}: event_year={event_year} == target_season_year={season_year} (ID mismatch: {event_season_id} != {season_id})")

                        # Apply filter if neither ID nor Year matches
                        if not (id_match or year_match):
                            if ENABLE_SEASON_YEAR_FILTERING and season_year:
                                logger.debug(f"Skipping event for {team_name} - different season: ID {event_season_id} vs {season_id}, Year {event_year} vs {season_year}")
                            else:
                                logger.debug(f"Skipping event for {team_name} - different season: ID {event_season_id} vs {season_id} (year filtering disabled or no year provided)")
                            passes_filters = False

            result_dict = {
                'event_id': event_id_from_api,
                'team_name': team_name,
                'team_role': team_role,
                'opponent_name': opponent_name,
                'opponent_role': opponent_role,
                'team_score': team_score,
                'opponent_score': opponent_score,
                'team_result_code': team_result_code,
                'team_result': team_result,
                'opponent_ranking': opponent_ranking,
                'own_ranking': own_ranking,
                'startTimestamp': event.get('startTimestamp', 0),
                'team_standing': {},
                'opponent_standing': {}
            }

            # Add tennis period points if available (for points-based tracking)
            if sport in ['Tennis', 'Tennis Doubles'] and 'home_period1' in result_data:
                if is_team_home:
                    result_dict['team_period1'] = result_data.get('home_period1')
                    result_dict['team_period2'] = result_data.get('home_period2')
                    result_dict['team_period3'] = result_data.get('home_period3')
                    result_dict['opponent_period1'] = result_data.get('away_period1')
                    result_dict['opponent_period2'] = result_data.get('away_period2')
                    result_dict['opponent_period3'] = result_data.get('away_period3')
                else:
                    result_dict['team_period1'] = result_data.get('away_period1')
                    result_dict['team_period2'] = result_data.get('away_period2')
                    result_dict['team_period3'] = result_data.get('away_period3')
                    result_dict['opponent_period1'] = result_data.get('home_period1')
                    result_dict['opponent_period2'] = result_data.get('home_period2')
                    result_dict['opponent_period3'] = result_data.get('home_period3')

            if overall_results is not None and overall_seen_event_ids is not None:
                if event_id_from_api not in overall_seen_event_ids:
                    overall_results.append(dict(result_dict))
                    overall_seen_event_ids.add(event_id_from_api)

            if apply_filters and not passes_filters:
                continue

            results.append(result_dict)

        except Exception as e:
            logger.debug(f"Error processing team result event: {e}")
            continue

    # Sort by startTimestamp to get most recent first (API returns inverted order)
    results.sort(key=lambda x: x.get('startTimestamp', 0), reverse=True)

    return results


# ---------------------------------------------------------------------------
# Main public function (previously StreakAlertEngine.get_team_last_results_by_id)
# ---------------------------------------------------------------------------

def get_team_last_results_by_id(
    team_id: int,
    team_name: str,
    competition_slug: str,
    sport: str,
    season_id: Optional[int] = None,
    source_unique_tournament_id: Optional[int] = None,
    source_tournament_id: Optional[int] = None,
    season_year: Optional[int] = None,
    observations: Optional[List[Dict]] = None,
    exclude_event_id: Optional[int] = None,
    min_results: int = None,
    event_start_timestamp: float = None,
    debug_mode: bool = False
) -> Tuple[List[Dict], int]:
    """
    Get team results using team_id with flexible filtering.

    Season-based filtering (non-tennis):
    - When season_id is provided, fetches ALL games from that season (no minimum)
    - Stops fetching when encountering events from a different season

    Legacy filtering (tennis or no season):
    - Uses min_results to fetch a minimum number of games
    - Applies ground_type filtering for tennis

    Args:
        team_id: ID of the team
        team_name: Name of the team for context
        competition_slug: Slug of the competition
        sport: Sport type for filtering logic
        season_id: Season ID for filtering (non-tennis sports only)
        season_year: Season Year for fallback filtering (e.g. 2023)
        observations: List of dicts containing filtering observations
        exclude_event_id: Optional event ID to exclude from results (current/upcoming event)
        min_results: Minimum number of results to fetch (ignored when season_id is provided)
        event_start_timestamp: Timestamp of the current event (for DB-based filtering)

    Returns:
        Tuple where:
            - List of dicts with canonical form-result keys (team_result_code, team_score, opponent_score, team_name, opponent_name)
            - Integer representing the overall (unfiltered) current win streak
    """
    # =====================================================================
    # ROUTE 1: DB-based form retrieval for collected seasons
    # Uses the DB-backed historical form service instead of API calls
    # =====================================================================
    season_id_int = int(season_id) if season_id is not None else None
    if is_collected_competition_scope(
        source_unique_tournament_id=source_unique_tournament_id,
        source_tournament_id=source_tournament_id,
        season_id=season_id_int,
    ):
        included_season_ids = get_included_season_ids(
            source_unique_tournament_id,
            source_tournament_id,
            season_id_int,
        )
        standings_method = get_standings_method(
            source_unique_tournament_id,
            source_tournament_id,
            sport,
        )
        grouping_method = get_grouping_method(
            source_unique_tournament_id,
            source_tournament_id,
        )
        logger.info(
            "📊 Using DB-based form retrieval for %s (season_id=%s, included_season_ids=%s, source_unique_tournament_id=%s, source_tournament_id=%s, standings_method=%s, grouping_method=%s)",
            team_name,
            season_id,
            included_season_ids,
            source_unique_tournament_id,
            source_tournament_id,
            standings_method,
            grouping_method,
        )
        return historical_form_processor.get_team_form_from_db(
            team_name=team_name,
            season_id=season_id_int,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
            exclude_event_id=exclude_event_id,
            current_event_timestamp=event_start_timestamp,
            send_debug_standings=debug_mode,  # Toggle: via .env global_debug_mode
        )

    # =====================================================================
    # ROUTE 2: API-based form retrieval (existing logic - UNCHANGED)
    # =====================================================================

    # Season-based filtering: fetch all games from current season (no minimum)
    use_season_filtering = season_id and sport not in ['Tennis', 'Tennis Doubles']

    # Use module constant if min_results not provided and not using season filtering
    if min_results is None and not use_season_filtering:
        min_results = DEFAULT_MIN_RESULTS
    elif use_season_filtering:
        # For season filtering, we don't have a minimum - we fetch all available
        min_results = 0  # Will be ignored in the loop logic

    try:
        # Log filtering criteria at the start
        filter_type, filter_value = _get_filtering_criteria(sport, competition_slug, observations)
        # debugging line:
        # logger.info(f"📊 Team {team_name} - Starting to fetch last results with filter: {filter_type}='{filter_value}' (season filtering: {'enabled' if use_season_filtering else 'disabled'}, min_results={min_results if not use_season_filtering else 'N/A'})")
        match_type_info = ""
        if (sport == 'Tennis' or sport == 'Tennis Doubles'):
            match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
            match_type_info = f" and match_type '{match_type}'"

        # Log detailed filtering information
        if (sport == 'Tennis' or sport == 'Tennis Doubles'):
            ground_type_obs = next((obs for obs in observations if obs.get('type') == 'ground_type'), None) if observations else None
            if ground_type_obs:
                logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: ground_type='{ground_type_obs.get('value')}'{match_type_info}")
            else:
                logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: NO ground_type filter (ground_type not found in observations){match_type_info}")
        else:
            if use_season_filtering:
                year_filter_info = ""
                if ENABLE_SEASON_YEAR_FILTERING and season_year:
                    year_filter_info = f" OR season_year='{season_year}' (fallback)"
                logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: competition='{competition_slug}' AND season_id='{season_id}'{year_filter_info} (fetching all season games)")
            else:
                logger.info(f"🔍 FILTERING: Team {team_name} - Applying filters: competition='{competition_slug}'")

        # Determine tennis parameters
        is_tennis_singles = sport == 'Tennis'
        is_tennis_doubles = sport == 'Tennis Doubles'

        # First fetch
        logger.debug(f"TEST DEBUG: Sport-> {sport}")
        full_response = api_client.get_team_last_results_response(
            team_id,
            is_tennis_singles=is_tennis_singles,
            is_tennis_doubles=is_tennis_doubles,
            fetch_index=0
        )

        all_results = []
        seen_event_ids = set()
        overall_results: List[Dict] = []
        overall_seen_event_ids: Set[int] = set()

        if full_response and 'events' in full_response:
            events = full_response['events']
            if events:
                logger.info(f"🔍 FILTERING: Processing {len(events)} total events from API (first fetch), filtering by {filter_type} '{filter_value}'{match_type_info}")
                first_fetch_results = _process_events_into_results(
                    events,
                    team_id,
                    team_name,
                    competition_slug,
                    sport,
                    season_id=season_id,
                    season_year=season_year,
                    observations=observations,
                    exclude_event_id=exclude_event_id,
                    apply_filters=True,
                    overall_results=overall_results,
                    overall_seen_event_ids=overall_seen_event_ids
                )

                # Track seen event IDs to avoid duplicates
                for result in first_fetch_results:
                    result_key = result.get('event_id')
                    if result_key not in seen_event_ids:
                        all_results.append(result)
                        seen_event_ids.add(result_key)

        # Check if we need additional fetches
        # Season filtering: keep fetching until we exhaust the season (no minimum)
        # Legacy filtering: fetch until we reach min_results
        fetch_index = 1
        MAX_FETCH_INDEX = 3

        # For season filtering, we continue fetching until we run out of events
        # For min_results filtering, we stop when we have enough
        should_continue_fetching = use_season_filtering or len(all_results) < min_results

        while should_continue_fetching and fetch_index < MAX_FETCH_INDEX:
            if use_season_filtering:
                logger.info(f"📊 Team {team_name} - Collected {len(all_results)} season results so far, fetching more (index {fetch_index})")
            else:
                logger.info(f"📊 Team {team_name} - Only {len(all_results)} results after fetch {fetch_index}, requesting additional batch (index {fetch_index}) to reach minimum of {min_results}")

            additional_response = api_client.get_team_last_results_response(
                team_id,
                is_tennis_singles=is_tennis_singles,
                is_tennis_doubles=is_tennis_doubles,
                fetch_index=fetch_index
            )

            if additional_response and 'events' in additional_response:
                additional_events = additional_response['events']
                if additional_events:
                    logger.info(f"🔍 FILTERING: Processing {len(additional_events)} total events from API (fetch index {fetch_index}), filtering by {filter_type} '{filter_value}'{match_type_info}")
                    additional_fetch_results = _process_events_into_results(
                        additional_events,
                        team_id,
                        team_name,
                        competition_slug,
                        sport,
                        season_id=season_id,
                        season_year=season_year,
                        observations=observations,
                        exclude_event_id=exclude_event_id,
                        apply_filters=True,
                        overall_results=overall_results,
                        overall_seen_event_ids=overall_seen_event_ids
                    )

                    # Add results from additional fetch, avoiding duplicates
                    added_count = 0
                    for result in additional_fetch_results:
                        result_key = result.get('event_id')
                        if result_key not in seen_event_ids:
                            all_results.append(result)
                            seen_event_ids.add(result_key)
                            added_count += 1

                    if added_count == 0:
                        logger.info(f"📊 Team {team_name} - No new results added in fetch index {fetch_index}, continuing to next fetch (max {MAX_FETCH_INDEX})")
                else:
                    logger.info(f"📊 Team {team_name} - Fetch index {fetch_index} returned no events, continuing to next fetch (max {MAX_FETCH_INDEX})")
            else:
                logger.info(f"📊 Team {team_name} - Fetch index {fetch_index} returned no response, continuing to next fetch (max {MAX_FETCH_INDEX})")

            fetch_index += 1

            # Update loop condition
            should_continue_fetching = use_season_filtering or len(all_results) < min_results

        # Sort results by timestamp (most recent first)
        all_results.sort(key=lambda x: x.get('startTimestamp', 0), reverse=True)
        overall_results.sort(key=lambda x: x.get('startTimestamp', 0), reverse=True)

        # Calculate overall (unfiltered) win streak
        current_win_streak = _calculate_current_win_streak(overall_results)
        logger.info(f"📈 Overall win streak for {team_name} (ID: {team_id}): {current_win_streak}")

        # Debug: Log the results to verify processing
        if all_results:
            filter_type, filter_value = _get_filtering_criteria(sport, competition_slug, observations)
            match_type_info = ""
            if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                match_type_info = f" and match_type '{match_type}'"
            logger.info(f"📊 Team {team_name} processed {len(all_results)} total results from {filter_type} '{filter_value}'{match_type_info}:")
            for i, result in enumerate(all_results[:5]):  # Show first 5
                timestamp = result.get('startTimestamp', 0)
                date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M') if timestamp > 0 else 'Invalid'
                own_rank = result.get('own_ranking', 0)
                opp_rank = result.get('opponent_ranking', 0)
                logger.info(f"  {i+1}. {date_str} - Winner: {result.get('winner', 'N/A')} ~{own_rank} vs ~{opp_rank} {result.get('opponent_name', 'N/A')}")
        else:
            filter_type, filter_value = _get_filtering_criteria(sport, competition_slug, observations)
            match_type_info = ""
            if (sport == 'Tennis' or sport == 'Tennis Doubles'):
                match_type = "doubles" if sport == 'Tennis Doubles' else "singles"
                match_type_info = f" and match_type '{match_type}'"
            logger.info(f"📊 Team {team_name} - no results found in {filter_type} '{filter_value}'{match_type_info}")

        # Return filtered results:
        # - Season filtering: all collected season results (no slicing)
        # - Legacy/min_results filtering: only the most recent min_results entries
        results_to_return = all_results if use_season_filtering else all_results[:min_results]
        return results_to_return, current_win_streak

    except Exception as e:
        logger.error(f"Error getting team last results for {team_name} (ID: {team_id}): {e}")
        return [], 0
