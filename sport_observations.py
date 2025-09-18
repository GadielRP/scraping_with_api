#!/usr/bin/env python3
"""
Sport Observations Module - Handles sport-specific data extraction and display

MODULARITY BOUNDARIES:
=====================
START: This file contains all sport-specific observation functionality
END: Sport observation functionality ends at the end of this file

RESPONSIBILITIES:
- Extract sport-specific observations from API responses
- Store observations in database
- Format sport-specific information for notifications
- Handle ground type extraction for tennis events

FAIL-SAFE DESIGN:
All functions are designed to be completely fail-safe and not break main application flow.
"""

import logging
from typing import Dict, List, Optional
from repository import ObservationRepository

logger = logging.getLogger(__name__)


class SportObservationsManager:
    """Manages sport-specific observations and data extraction"""
    
    def __init__(self):
        self.observation_repo = ObservationRepository()
    
    def extract_tennis_ground_type(self, event_id: int, api_response: Dict) -> None:
        """
        Extract ground type for tennis events during pre-start check.
        FAIL-SAFE: Logs warnings on error but doesn't break main flow.
        
        Args:
            event_id: ID of the tennis event
            api_response: API response containing event data
        """
        try:
            logger.info(f"🎾 Extracting ground type for tennis event {event_id}")
            
            # Import here to avoid circular imports
            from sofascore_api import api_client
            
            # DEBUG: Log the API response structure to understand what we're getting
            logger.info(f"🔍 DEBUG: API response keys: {list(api_response.keys()) if api_response else 'None'}")
            if api_response and 'event' in api_response:
                event_data = api_response['event']
                logger.info(f"🔍 DEBUG: Event data keys: {list(event_data.keys()) if event_data else 'None'}")
                if 'groundType' in event_data:
                    logger.info(f"🔍 DEBUG: Found groundType in event data: {event_data['groundType']}")
                else:
                    logger.info(f"🔍 DEBUG: No groundType field in event data")
                    # Check if it's in a different location
                    if 'tournament' in event_data:
                        tournament = event_data['tournament']
                        logger.info(f"🔍 DEBUG: Tournament keys: {list(tournament.keys()) if tournament else 'None'}")
            
            # Use the existing method from sofascore_api to extract observations
            observations = api_client._extract_observations_from_response(api_response)
            
            if observations:
                logger.info(f"🎾 Found {len(observations)} observations for tennis event {event_id}")
                
                # Process each observation
                for observation in observations:
                    obs_type = observation.get('type')
                    obs_value = observation.get('value')
                    sport = observation.get('sport', 'Tennis')
                    
                    if obs_type and obs_value:
                        # Save the observation to database
                        saved_obs = self.observation_repo.upsert_observation(
                            event_id=event_id,
                            sport=sport,
                            observation_type=obs_type,
                            observation_value=obs_value
                        )
                        
                        if saved_obs:
                            logger.info(f"🎾 ✅ Saved {obs_type}: {obs_value} for event {event_id}")
                        else:
                            logger.warning(f"🎾 ❌ Failed to save {obs_type} for event {event_id}")
                    else:
                        logger.warning(f"🎾 ❌ Invalid observation data: {observation}")
            else:
                logger.info(f"🎾 No observations found for tennis event {event_id}")
                
        except Exception as e:
            logger.warning(f"🎾 Error extracting ground type for tennis event {event_id}: {e}")
            # FAIL-SAFE: Don't break the main pre-start flow
    
    def process_event_observations(self, event, result_data: Dict) -> None:
        """
        Process event observations from result data.
        COMPLETELY FAIL-SAFE: Any error here will not break the main results processing.
        
        Args:
            event: Event object
            result_data: Dictionary containing result data with observations
        """
        try:
            # Check if observations exist in result_data
            observations = result_data.get('observations')
            if not observations:
                logger.debug(f"No observations found for event {event.id}")
                return
            
            # Process each observation
            for obs_data in observations:
                try:
                    observation_type = obs_data.get('type')
                    observation_value = obs_data.get('value')
                    sport = obs_data.get('sport', event.sport)  # Fallback to event sport
                    
                    if observation_type and observation_value:
                        # Upsert the observation (fail-safe)
                        observation = self.observation_repo.upsert_observation(
                            event_id=event.id,
                            sport=sport,
                            observation_type=observation_type,
                            observation_value=observation_value
                        )
                        
                        if observation:
                            logger.info(f"📍 Saved observation for event {event.id}: {observation_type} = {observation_value}")
                        else:
                            logger.debug(f"Failed to save observation {observation_type} for event {event.id}")
                    else:
                        logger.debug(f"Invalid observation data for event {event.id}: {obs_data}")
                        
                except Exception as e:
                    logger.warning(f"Error processing individual observation for event {event.id}: {e}")
                    # Continue with next observation
                    continue
            
        except Exception as e:
            logger.warning(f"Error processing observations for event {event.id}: {e}")
            # FAIL-SAFE: Don't break main results processing
            return
    
    def get_sport_specific_info(self, event_id: int, sport: str) -> Optional[str]:
        """
        Get sport-specific information for notifications.
        COMPLETELY FAIL-SAFE: Returns None on any error, doesn't break notifications.
        
        Args:
            event_id: ID of the event
            sport: Sport type (e.g., 'Tennis', 'Football')
            
        Returns:
            Formatted sport-specific information string or None
        """
        try:
            logger.info(f"🔍 DEBUG: Getting sport info for event_id={event_id}, sport='{sport}'")
            
            if not event_id or not sport:
                logger.info(f"🔍 DEBUG: Missing event_id or sport - event_id={event_id}, sport='{sport}'")
                return None
            
            # TENNIS: Add ground type
            if sport.lower() == 'tennis':
                logger.info(f"🔍 DEBUG: Processing Tennis event {event_id}")
                ground_type_obs = self.observation_repo.get_observation(event_id, 'ground_type')
                logger.info(f"🔍 DEBUG: Ground type observation result: {ground_type_obs}")
                
                if ground_type_obs and ground_type_obs.observation_value:
                    result = f"🎾 Court: {ground_type_obs.observation_value}"
                    logger.info(f"🔍 DEBUG: Found ground type: {result}")
                    return result
                else:
                    result = "🎾 Court: Unknown"
                    logger.info(f"🔍 DEBUG: No ground type found, returning: {result}")
                    return result
            
            logger.info(f"🔍 DEBUG: Sport '{sport}' not Tennis, returning None")
            return None
                
        except Exception as e:
            logger.warning(f"Error getting sport-specific info for event {event_id}: {e}")
            # FAIL-SAFE: Return None, don't break notifications
            return None
    
    def format_sport_info_for_candidates(self, candidate_event_id: int, candidate_sport: str) -> Optional[str]:
        """
        Format sport-specific information for candidate events in notifications.
        FAIL-SAFE: Returns None if no sport info available.
        
        Args:
            candidate_event_id: ID of the candidate event
            candidate_sport: Sport of the candidate event
            
        Returns:
            Formatted sport information string or None
        """
        try:
            logger.info(f"🔍 DEBUG: Processing candidate - event_id={candidate_event_id}, sport='{candidate_sport}'")
            
            sport_info = self.get_sport_specific_info(candidate_event_id, candidate_sport)
            logger.info(f"🔍 DEBUG: Sport info result for candidate: '{sport_info}'")
            
            if sport_info:
                logger.info(f"🔍 DEBUG: Found sport info for candidate: {sport_info}")
                return sport_info
            else:
                logger.info(f"🔍 DEBUG: No sport info found for candidate")
                return None
                
        except Exception as e:
            logger.warning(f"Error formatting sport info for candidate {candidate_event_id}: {e}")
            return None


# Global instance for easy access
sport_observations_manager = SportObservationsManager()
