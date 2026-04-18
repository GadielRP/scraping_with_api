"""
Winning Odds extraction for Matchup Streak Analysis.

Fetches and processes winning odds data from the API for a given event.
Extracted from streak_alerts.py (StreakAlertEngine.get_winning_odds_data).
"""

import logging
from typing import Dict, Optional

from modules.sofascore import api_client
from shared.odds_utils import fractional_to_decimal

logger = logging.getLogger(__name__)


def get_winning_odds_data(event_id: int) -> Optional[Dict]:
    """
    Fetch winning odds data for an event using the new API endpoint.

    Args:
        event_id: ID of the event

    Returns:
        Dict with winning odds data or None if error
    """
    try:
        logger.debug(f"🎯 Fetching winning odds for event {event_id}")
        response = api_client.get_winning_odds_response(event_id)

        if not response:
            logger.debug(f"No winning odds data found for event {event_id}")
            return None

        # Check if response is an error or empty
        if isinstance(response, dict) and 'error' in response:
            logger.debug(f"API returned error for event {event_id}: {response.get('error')}")
            return None

        # Process the response to include decimal odds
        processed_data = {}

        # Check if response has the expected structure
        if not isinstance(response, dict):
            logger.debug(f"Invalid response format for event {event_id}: {type(response)}")
            return None

        for team_key in ['home', 'away']:
            if team_key in response and response[team_key] is not None:
                team_data = response[team_key]

                # Validate that team_data is a dictionary with expected fields
                if isinstance(team_data, dict) and 'fractionalValue' in team_data:
                    fractional_value = team_data.get('fractionalValue', '')

                    # Convert fractional to decimal
                    decimal_odds = fractional_to_decimal(fractional_value)

                    processed_data[team_key] = {
                        'fractionalValue': fractional_value,
                        'decimalValue': decimal_odds,
                        'expected': team_data.get('expected', 0),
                        'actual': team_data.get('actual', 0),
                        'id': team_data.get('id', 0)
                    }

                    logger.debug(
                        f"📊 {team_key.title()} team odds: {fractional_value} → {decimal_odds} "
                        f"(Expected: {team_data.get('expected')}%, Actual: {team_data.get('actual')}%)"
                    )
                else:
                    logger.debug(f"📊 {team_key.title()} team: Invalid odds data structure")
            else:
                logger.debug(f"📊 {team_key.title()} team: No odds data available (null or missing)")

        # Only return data if we have at least one valid team's odds
        if processed_data:
            logger.info(f"✅ Winning odds processed for event {event_id}: {list(processed_data.keys())}")
            return processed_data
        else:
            logger.info(f"ℹ️ No valid winning odds data found for event {event_id}")
            return None

    except Exception as e:
        logger.error(f"Error fetching winning odds for event {event_id}: {e}")
        return None
