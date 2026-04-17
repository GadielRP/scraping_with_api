import logging
import re
from typing import Optional
from infrastructure.persistence.models import Season
from infrastructure.persistence.database import db_manager

logger = logging.getLogger(__name__)

class SeasonRepository:
    """Repository for season-related database operations"""
    
    @staticmethod
    def _parse_season_name(season_name: str, unique_tournament_name: str) -> str:
        if not season_name:
            return season_name  # or return "" if you prefer

        # Check if there is at least one alphabetical character anywhere
        has_alpha = any(ch.isalpha() for ch in season_name)

        # If there are NO alphabetical characters, prefix the unique_tournament_name
        if not has_alpha:
            return f"{unique_tournament_name} {season_name}"

        # Otherwise, leave it as is
        return season_name

    @staticmethod
    def _parse_year(year_str: str) -> int:
        if not year_str:
            return None

        try:
            year_str = str(year_str)

            # First, try to find a 4-digit year pattern (e.g., "2020", "2023", "1999")
            # Look for patterns like "2020/2021" or "2020-2021" or just "2020"
            four_digit_pattern = r'\b(19|20)\d{2}\b'
            four_digit_match = re.search(four_digit_pattern, year_str)
            
            if four_digit_match:
                year_int = int(four_digit_match.group())
                return year_int
            
            # If no 4-digit year found, look for 2-digit years (e.g., "20/21", "24/25")
            # Extract the first number before a slash or the first number in the string
            if '/' in year_str:
                # Split by '/' and get the first part (e.g., "NBA 20" from "NBA 20/21")
                year_part = year_str.split('/')[0].strip()
            else:
                year_part = year_str.strip()

            # Extract all digits from the year_part
            digits = re.findall(r'\d+', year_part)
            if digits:
                year_int = int(digits[0])
                # Convert 2-digit years to 4-digit years (e.g., 20 -> 2020, 24 -> 2024)
                if year_int < 100:
                    # Assume years 00-99 are 2000-2099
                    return 2000 + year_int
                # If already 4 digits or more, return as is
                return year_int

            # Last resort: try to convert the whole year_part to int
            year_int = int(year_part)
            if year_int < 100:
                return 2000 + year_int
            return year_int

        except (ValueError, TypeError):
            logger.warning(f"Could not parse year string: {year_str}")
            return None
    
    @staticmethod
    def get_or_create_season(season_id: int, name: str, year: int, sport: str) -> Optional[Season]:
        """
        Get existing season or create new one if it doesn't exist.
        Updates season info if it changed.
        
        Args:
            season_id: SofaScore season ID (unique identifier)
            name: Season name (e.g., "NBA 24/25")
            year: Season year (e.g., 2024)
            sport: Sport name (e.g., "Basketball")
            
        Returns:
            Season object if successful, None otherwise
        """
        if not season_id:
            return None
        
        try:
            with db_manager.get_session() as session:
                # Check if season exists
                season = session.query(Season).filter(Season.id == season_id).first()
                
                if season:
                    # Update existing season if info changed
                    updated = False
                    if season.name != name:
                        season.name = name
                        updated = True
                    if season.year != year:
                        season.year = year
                        updated = True
                    if season.sport != sport:
                        season.sport = sport
                        updated = True
                    
                    if updated:
                        logger.debug(f"Updated season {season_id}: {name}")
                    return season
                else:
                    # Create new season
                    season = Season(
                        id=season_id,
                        name=name,
                        year=year,
                        sport=sport
                    )
                    session.add(season)
                    logger.debug(f"Created new season {season_id}: {name} (Year: {year}, Sport: {sport})")
                    return season
                    
        except Exception as e:
            logger.error(f"Error getting/creating season {season_id}: {e}")
            return None
