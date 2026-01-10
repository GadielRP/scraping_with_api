"""
Optimization utilities for SofaScore discovery jobs.

This module contains all optimization-related functions for parallel processing,
batch operations, and performance improvements in the discovery system.

Performance Strategy:
- Parallel API calls when endpoints are independent
- Batch database operations to reduce transaction overhead
- Smart processing decisions based on data availability
"""

import logging
from typing import List, Optional, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from sofascore_api import api_client
from repository import EventRepository, OddsRepository

logger = logging.getLogger(__name__)


# ============================================================================
# EVENT FILTERING UTILITIES
# ============================================================================

def filter_upcoming_events(events: List[Dict], min_minutes_away: int = 10) -> List[Dict]:
    """
    Filter events to only include those that haven't started yet and are at least min_minutes_away from starting.
    
    This is a shared utility used across all discovery jobs to ensure we only process
    future events with sufficient lead time for odds extraction and alerts.
    
    Args:
        events: List of event objects with startTimestamp field
        min_minutes_away: Minimum minutes away from current time (default: 10)
    
    Returns:
        List of upcoming event objects
    """
    from timezone_utils import get_local_now_aware
    
    if not events:
        return []
    
    try:
        # Get current time (timezone-aware)
        current_time = get_local_now_aware()
        current_timestamp = int(current_time.timestamp())
        
        # Calculate minimum start timestamp (current time + min_minutes_away)
        min_start_timestamp = current_timestamp + (min_minutes_away * 60)
        
        upcoming_events = []
        filtered_count = 0
        
        for event in events:
            start_timestamp = event.get('startTimestamp')
            
            if not start_timestamp:
                logger.debug(f"Event {event.get('id', 'unknown')} has no startTimestamp, skipping")
                filtered_count += 1
                continue
            
            # Filter: keep only events starting at least min_minutes_away from now
            if start_timestamp >= min_start_timestamp:
                upcoming_events.append(event)
            else:
                event_id = event.get('id', 'unknown')
                time_diff_minutes = (start_timestamp - current_timestamp) / 60
                if time_diff_minutes < 0:
                    logger.debug(f"Filtered out event {event_id}: already started ({abs(time_diff_minutes):.1f} minutes ago)")
                else:
                    logger.debug(f"Filtered out event {event_id}: starts in {time_diff_minutes:.1f} minutes (< {min_minutes_away} min threshold)")
                filtered_count += 1
        
        if filtered_count > 0:
            logger.info(f"Filtered {len(upcoming_events)} upcoming events (excluded {filtered_count} events that already started or are starting soon)")
        
        return upcoming_events
        
    except Exception as e:
        logger.error(f"Error filtering upcoming events: {e}")
        return events  # Return original list on error


# ============================================================================
# PARALLEL FETCHING UTILITIES
# ============================================================================

def parallel_team_event_fetching(team_ids: List[int], max_workers: int = 5) -> List[Dict]:
    """
    Fetch nearest events for multiple teams in parallel.
    
    This is a major optimization for team streaks source which requires
    2 API calls per team (get_nearest_event_for_team + get_event_details).
    
    Args:
        team_ids: List of team IDs to fetch events for
        max_workers: Number of parallel workers (default: 5)
        
    Returns:
        List of event data dictionaries
        
    Performance:
        Sequential: 20 teams × 2 calls × 1s = 40 seconds
        Parallel (5 workers): (20 ÷ 5) × 2 × 1s = 8 seconds
    """
    def fetch_team_event(team_id: int) -> Optional[Dict]:
        """Fetch nearest event for a single team."""
        try:
            nearest_event_id = api_client.get_nearest_event_for_team(team_id)
            if nearest_event_id:
                event_response = api_client.get_event_details(nearest_event_id)
                if event_response:
                    event_data = api_client.get_event_information(
                        event_response, 
                        discovery_source='team_streaks'
                    )
                    if event_data:
                        logger.debug(f"Fetched event {nearest_event_id} for team {team_id}")
                        return event_data
                    else:
                        logger.debug(f"Failed to structure event data for event {nearest_event_id}")
                else:
                    logger.debug(f"Failed to get event details for event {nearest_event_id}")
            else:
                logger.debug(f"No nearest event found for team {team_id}")
        except Exception as e:
            logger.debug(f"Error processing team {team_id}: {e}")
        return None
    
    team_events = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_team = {
            executor.submit(fetch_team_event, team_id): team_id 
            for team_id in team_ids
        }
        for future in as_completed(future_to_team):
            event_data = future.result()
            if event_data:
                team_events.append(event_data)
    
    return team_events


def parallel_odds_checking(events: List[Dict], max_workers: int = 5, no_retry_on_404: bool = True) -> Tuple[Dict[str, Dict], List[int]]:
    """
    Check odds availability for multiple events in parallel.
    
    Returns both events with odds and event IDs without odds for batch cleanup.
    This is used for sources with high failure rates (team streaks: 95% failure).
    
    Args:
        events: List of event data dictionaries
        max_workers: Number of parallel workers (default: 5)
        no_retry_on_404: Skip retries on 404 errors (default: True)
        
    Returns:
        Tuple of (events_with_odds dict, events_to_delete list)
        
    Performance:
        Sequential: 20 events × 1s = 20 seconds
        Parallel (5 workers): (20 ÷ 5) × 1s = 4 seconds
    """
    def check_event_odds(event_data: Dict) -> Tuple[str, Optional[Dict]]:
        """Check if event has odds available."""
        event_id = str(event_data['id'])
        odds_data = api_client.get_event_final_odds(event_id, no_retry_on_404=no_retry_on_404)
        if not odds_data:
            return (event_id, None)
        
        processed_odds_data = api_client.extract_final_odds_from_response(
            odds_data, 
            initial_odds_extraction=True
        )
        return (event_id, processed_odds_data)
    
    events_with_odds = {}
    events_to_delete = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_event = {
            executor.submit(check_event_odds, event_data): event_data 
            for event_data in events
        }
        for future in as_completed(future_to_event):
            try:
                event_id, odds_data = future.result()
                if odds_data is None:
                    events_to_delete.append(int(event_id))
                else:
                    events_with_odds[event_id] = odds_data
            except Exception as e:
                event_data = future_to_event[future]
                logger.debug(f"Error checking odds for event {event_data.get('id')}: {e}")
                events_to_delete.append(int(event_data['id']))
    
    return events_with_odds, events_to_delete


# ============================================================================
# BATCH PROCESSING UTILITIES
# ============================================================================

def batch_upsert_events(events: List[Dict]) -> int:
    """
    Upsert multiple events efficiently.
    
    Args:
        events: List of event data dictionaries
        
    Returns:
        Number of successfully upserted events
    """
    upserted_count = 0
    for event_data in events:
        try:
            event = EventRepository.upsert_event(event_data)
            if event:
                upserted_count += 1
        except Exception as e:
            logger.debug(f"Error upserting event {event_data.get('id')}: {e}")
    
    return upserted_count


def batch_process_odds(events_with_odds: Dict[str, Dict], events: List[Dict]) -> Tuple[int, int]:
    """
    Process odds data for multiple events efficiently.
    
    Reason: Processes only events that have valid odds data, avoiding
    wasted operations on events without odds.
    
    Args:
        events_with_odds: Dictionary mapping event_id to odds_data
        events: Original list of event data dictionaries
        
    Returns:
        Tuple of (processed_count, skipped_count)
    """
    processed_count = 0
    skipped_count = 0
    
    for event_data in events:
        event_id = str(event_data['id'])
        if event_id in events_with_odds:
            try:
                odds_data = events_with_odds[event_id]
                
                # Create snapshot
                snapshot = OddsRepository.create_odds_snapshot(int(event_id), odds_data)
                if not snapshot:
                    logger.debug(f"Failed to create odds snapshot for event {event_id}")
                    skipped_count += 1
                    continue
                
                # Upsert event odds
                upserted_id = OddsRepository.upsert_event_odds(int(event_id), odds_data)
                if not upserted_id:
                    logger.debug(f"Failed to upsert event odds for event {event_id}")
                    skipped_count += 1
                    continue
                
                processed_count += 1
            except Exception as e:
                logger.debug(f"Error processing event {event_id}: {e}")
                skipped_count += 1
    
    return processed_count, skipped_count


# ============================================================================
# COMPLETE PIPELINE FUNCTIONS
# ============================================================================

def process_with_batch_cleanup(
    events: List[Dict], 
    discovery_source: str = None,
    max_workers: int = 5
) -> Tuple[int, int]:
    """
    Complete pipeline for processing events with batch deletion optimization.
    
    This is the main optimization function that combines:
    1. Batch event upsert
    2. Parallel odds checking
    3. Batch deletion of failed events
    4. Processing of successful events
    
    Use this for sources with high failure rates (team streaks, high value streaks).
    
    Args:
        events: List of event data dictionaries
        discovery_source: Source identifier for logging
        max_workers: Number of parallel workers (default: 5)
        
    Returns:
        Tuple of (processed_count, skipped_count)
        
    Performance Example (20 events, 95% failure rate):
        Sequential: 20s check + 19s delete one-by-one = 39 seconds
        Batch: 4s check parallel + 1s batch delete = 5 seconds
        Improvement: 87% faster (34 seconds saved)
    """
    if not events:
        return 0, 0
    
    # Step 1: Upsert all events first
    batch_upsert_events(events)
    
    # Step 2: Check odds availability in parallel
    events_with_odds, events_to_delete = parallel_odds_checking(events, max_workers=max_workers)
    
    # Step 3: Batch delete all events without odds
    if events_to_delete:
        deleted_count = EventRepository.batch_delete_events(events_to_delete)
        logger.info(f"Batch deleted {deleted_count} {discovery_source} events without odds")
    
    # Step 4: Process events with odds
    processed_count, skipped_count = batch_process_odds(events_with_odds, events)
    
    # Add deleted events to skipped count
    skipped_count += len(events_to_delete)
    
    return processed_count, skipped_count


def process_with_parallel_db_ops(
    events: List[Dict], 
    odds_map: Dict,
    discovery_source: str = None,
    max_workers: int = 5
) -> Tuple[int, int]:
    """
    Process events with pre-fetched odds using parallel database operations.
    
    Use this for sources where odds are already fetched in the API response
    (winning_odds, dropping_odds) - no API rate limiting bottleneck.
    
    Args:
        events: List of event data dictionaries
        odds_map: Dictionary mapping event_id to odds data
        discovery_source: Source identifier for logging
        max_workers: Number of parallel workers (default: 5)
        
    Returns:
        Tuple of (processed_count, skipped_count)
        
    Performance Example (20 events):
        Sequential: 20s database operations = 20 seconds
        Parallel: (20 ÷ 5) × 1s = 4 seconds
        Improvement: 80% faster (16 seconds saved)
    """
    from odds_utils import process_event_odds_from_dropping_odds
    
    def process_single_event(event_data: Dict) -> Tuple[bool, str]:
        """Process a single event with pre-fetched odds."""
        try:
            event_id = str(event_data['id'])
            
            # Upsert event
            event = EventRepository.upsert_event(event_data)
            if not event:
                return False, f"Failed to upsert event {event_id}"
            
            # Process odds data from pre-fetched map
            odds_data = process_event_odds_from_dropping_odds(event_id, odds_map)
            if not odds_data:
                return False, f"No odds data found for event {event_id}"
            
            # Create snapshot
            snapshot = OddsRepository.create_odds_snapshot(int(event_id), odds_data)
            if not snapshot:
                return False, f"Failed to create odds snapshot for event {event_id}"
            
            # Upsert event odds
            upserted_id = OddsRepository.upsert_event_odds(int(event_id), odds_data)
            if not upserted_id:
                return False, f"Failed to upsert event odds for event {event_id}"
            
            return True, f"Successfully processed event {event_id}"
            
        except Exception as e:
            return False, f"Error processing event {event_data.get('id')}: {e}"
    
    processed_count = 0
    skipped_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_event = {
            executor.submit(process_single_event, event_data): event_data 
            for event_data in events
        }
        
        for future in as_completed(future_to_event):
            try:
                success, reason = future.result()
                if success:
                    processed_count += 1
                else:
                    logger.debug(reason)
                    skipped_count += 1
            except Exception as e:
                event_data = future_to_event[future]
                logger.error(f"Exception processing event {event_data.get('id')}: {e}")
                skipped_count += 1
    
    return processed_count, skipped_count


# ============================================================================
# OPTIMIZATION RECOMMENDATIONS
# ============================================================================

def should_skip_source(source_name: str, failure_rate: float = 0.9) -> bool:
    """
    Determine if a discovery source should be skipped based on historical failure rate.
    
    Reason: Sources with >90% failure rate waste API calls and processing time.
    Consider disabling them or caching their "bad event" lists.
    
    Args:
        source_name: Name of the discovery source
        failure_rate: Threshold for skipping (default: 0.9 = 90%)
        
    Returns:
        True if source should be skipped
        
    Note: This is a recommendation helper. Actual skipping logic should be
    implemented in the scheduler based on configuration.
    """
    # Team streaks has ~95% failure rate (19/20 events deleted)
    high_failure_sources = {
        'team_streaks': 0.95,
    }
    
    if source_name in high_failure_sources:
        if high_failure_sources[source_name] >= failure_rate:
            logger.info(f"⚠️ Source '{source_name}' has {high_failure_sources[source_name]*100}% failure rate - consider disabling")
            return True
    
    return False


# ============================================================================
# PERFORMANCE ANALYSIS & RECOMMENDATIONS
# ============================================================================

def analyze_discovery_performance(
    high_value_time: float,
    team_streaks_time: float,
    h2h_time: float,
    winning_odds_time: float
) -> Dict[str, any]:
    """
    Analyze discovery job performance and provide optimization recommendations.
    
    Args:
        high_value_time: Time taken for high value streaks (seconds)
        team_streaks_time: Time taken for team streaks (seconds)
        h2h_time: Time taken for h2h events (seconds)
        winning_odds_time: Time taken for winning odds (seconds)
        
    Returns:
        Dictionary with analysis and recommendations
    """
    total_time = high_value_time + team_streaks_time + h2h_time + winning_odds_time
    
    recommendations = []
    
    # Identify slowest source
    sources = {
        'high_value_streaks': high_value_time,
        'team_streaks': team_streaks_time,
        'h2h': h2h_time,
        'winning_odds': winning_odds_time
    }
    slowest = max(sources.items(), key=lambda x: x[1])
    
    if slowest[1] > 30:
        recommendations.append(f"🐌 Slowest source: {slowest[0]} ({slowest[1]}s) - Consider optimization")
    
    # Check if team streaks is worth it (95% failure rate)
    if team_streaks_time > 20 and sources['team_streaks'] / total_time > 0.3:
        recommendations.append("⚠️ Team streaks takes >30% of time with 95% failure - Consider disabling")
    
    return {
        'total_time': total_time,
        'slowest_source': slowest[0],
        'slowest_time': slowest[1],
        'recommendations': recommendations
    }


# ============================================================================
# ADVANCED OPTIMIZATION STRATEGIES
# ============================================================================

def get_optimization_config() -> Dict:
    """
    Get recommended optimization configuration based on current system performance.
    
    Returns:
        Dictionary with optimization settings
        
    Configuration Guide:
        - max_workers: Number of parallel threads (5-10)
        - rate_limit: Seconds between API calls (0.5-1.0)
        - skip_low_value_sources: Skip sources with >90% failure
        - batch_size: Events to process in one batch (10-50)
    """
    return {
        'max_workers': 5,  # Conservative: 5 workers
        'aggressive_max_workers': 10,  # Aggressive: 10 workers (test carefully)
        'rate_limit_seconds': 1.0,  # Current: 1 second
        'aggressive_rate_limit': 0.5,  # Aggressive: 0.5 seconds (RISKY - may trigger 429)
        'skip_team_streaks': False,  # Set True to skip team streaks entirely
        'batch_deletion_enabled': True,  # Always use batch deletion
        'parallel_odds_check_enabled': True,  # Always use parallel odds checking
    }


def calculate_expected_speedup(current_time: float, workers_from: int, workers_to: int) -> float:
    """
    Calculate expected speedup from changing number of workers.
    
    Reason: Helps estimate performance improvement before deploying changes.
    
    Args:
        current_time: Current execution time in seconds
        workers_from: Current number of workers
        workers_to: New number of workers
        
    Returns:
        Expected new execution time in seconds
        
    Example:
        20 events, 5 workers: 20/5 = 4 batches × 1s = 4 seconds
        20 events, 10 workers: 20/10 = 2 batches × 1s = 2 seconds
        Speedup: 2x faster
    """
    # Assuming linear scaling (not always true due to rate limiting)
    speedup_factor = workers_to / workers_from
    expected_time = current_time / speedup_factor
    return expected_time


# ============================================================================
# AGGRESSIVE OPTIMIZATION MODE (Use with caution)
# ============================================================================

def process_events_only(
    events: List[Dict], 
    discovery_source: str = None,
    max_workers: int = 10
) -> Tuple[int, int]:
    """
    Process events without fetching odds - pre-start checks handle odds at optimal times.
    
    This is the standard processing for Discovery2:
    - No API calls for odds (saves 30+ seconds)
    - No rate limiting delays
    - No proxy retry issues
    - Odds fetched by pre-start checks at 30min and 5min before start
    
    Args:
        events: List of event data dictionaries
        discovery_source: Source identifier for logging
        max_workers: Number of parallel workers (unused but kept for consistency)
        
    Returns:
        Tuple of (processed_count, skipped_count)
    """
    if not events:
        return 0, 0
    
    # Step 1: Upsert all events (no odds processing)
    upserted_count = batch_upsert_events(events)
    
    logger.info(f"✅ {discovery_source} events processed: {upserted_count}/{len(events)} events upserted")
    
    return upserted_count, len(events) - upserted_count


def process_with_aggressive_parallel(
    events: List[Dict],
    discovery_source: str = None,
    max_workers: int = 10  # AGGRESSIVE: 10 workers instead of 5
) -> Tuple[int, int]:
    """
    AGGRESSIVE optimization with 10 parallel workers.
    
    ⚠️ WARNING: Use this only if your API can handle high concurrency.
    May trigger rate limiting (429 errors) if REQUEST_DELAY_SECONDS is too low.
    
    Performance Comparison (30 events):
        5 workers: (30 ÷ 5) × 1s = 6 seconds
        10 workers: (30 ÷ 10) × 1s = 3 seconds
        Speedup: 2x faster
    
    Args:
        events: List of event data dictionaries
        discovery_source: Source identifier for logging
        max_workers: Number of parallel workers (default: 10)
        
    Returns:
        Tuple of (processed_count, skipped_count)
    """
    logger.warning(f"🚀 AGGRESSIVE MODE: Using {max_workers} workers for {discovery_source}")
    return process_with_batch_cleanup(events, discovery_source, max_workers)


# ============================================================================
# OPTIMIZATION SUMMARY
# ============================================================================

OPTIMIZATION_STRATEGIES = {
    'conservative': {
        'description': 'Safe and stable',
        'max_workers': 5,
        'expected_job_b_time': 69,  # seconds
        'risk': 'Low',
    },
    'moderate': {
        'description': 'Balanced performance and stability',
        'max_workers': 7,
        'expected_job_b_time': 55,  # seconds
        'risk': 'Low-Medium',
    },
    'aggressive': {
        'description': 'Maximum speed (may trigger rate limits)',
        'max_workers': 10,
        'expected_job_b_time': 45,  # seconds
        'risk': 'Medium-High',
    },
}


def get_recommended_strategy(current_time: float) -> str:
    """
    Get recommended optimization strategy based on current performance.
    
    Args:
        current_time: Current Job B execution time (seconds)
        
    Returns:
        Recommended strategy name
    """
    if current_time > 120:
        return 'aggressive'  # Definitely need aggressive optimization
    elif current_time > 90:
        return 'moderate'  # Moderate optimization sufficient
    else:
        return 'conservative'  # Already fast enough


