#!/usr/bin/env python3
"""
Sport Classifier Module

Modular sport classification system following @rules.mdc principles.

Responsibilities:
- Classify sports into subcategories (Tennis -> Tennis Singles/Doubles)
- Provide database analysis and correction utilities
- Support future sport classifications

Usage:
- API Integration: classify_sport(sport, home_team, away_team)
- DB Correction: python sport_classifier.py --correct-tennis --dry-run
"""

import logging
import argparse
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Import database dependencies
from database import db_manager
from models import Event

logger = logging.getLogger(__name__)

class SportClassifier:
    """Main sport classification engine"""
    
    def __init__(self):
        self.classification_stats = {
            'tennis_singles': 0,
            'tennis_doubles': 0,
            'other_sports': 0
        }
    
    def classify_sport(self, sport: str, home_team: str, away_team: str) -> str:
        """
        Flexible sport classification function.
        
        Works for both API extraction and database correction.
        
        Args:
            sport: Original sport name (e.g., "Tennis")
            home_team: Home team/player name
            away_team: Away team/player name
            
        Returns:
            Classified sport name (e.g., "Tennis Doubles" or "Tennis")
        """
        if not sport or not home_team or not away_team:
            logger.warning(f"Missing data for classification: sport='{sport}', home='{home_team}', away='{away_team}'")
            return sport or "Unknown"
        
        # Tennis classification
        if sport.lower() == 'tennis':
            return self._classify_tennis(home_team, away_team)
        
        # Future sport classifications can be added here
        # elif sport.lower() == 'handball':
        #     return self._classify_handball(home_team, away_team)
        
        # Default: return original sport
        self.classification_stats['other_sports'] += 1
        return sport
    
    def _classify_tennis(self, home_team: str, away_team: str) -> str:
        """
        Classify tennis events as Singles or Doubles.
        
        Rules:
        - If BOTH home_team AND away_team contain '/', it's doubles
        - Otherwise, it's singles
        
        Args:
            home_team: Home player(s) name
            away_team: Away player(s) name
            
        Returns:
            "Tennis Doubles" or "Tennis"
        """
        try:
            # Check if both teams contain the "/" pattern (indicating doubles)
            home_has_slash = '/' in home_team
            away_has_slash = '/' in away_team
            
            if home_has_slash and away_has_slash:
                self.classification_stats['tennis_doubles'] += 1
                logger.debug(f"Tennis Doubles detected: '{home_team}' vs '{away_team}'")
                return "Tennis Doubles"
            else:
                self.classification_stats['tennis_singles'] += 1
                logger.debug(f"Tennis Singles detected: '{home_team}' vs '{away_team}'")
                return "Tennis"
                
        except Exception as e:
            logger.error(f"Error classifying tennis event '{home_team}' vs '{away_team}': {e}")
            # Default to singles on error
            self.classification_stats['tennis_singles'] += 1
            return "Tennis"
    
    def analyze_database_tennis(self) -> Dict:
        """
        Analyze current database for tennis events that need reclassification.
        
        Returns:
            Dictionary with analysis results
        """
        logger.info("🔍 Analyzing database for tennis events...")
        
        try:
            with db_manager.get_session() as session:
                # Get all tennis events
                tennis_events = session.query(Event).filter(
                    Event.sport == 'Tennis'
                ).all()
                
                if not tennis_events:
                    logger.info("No tennis events found in database")
                    return {
                        'total_tennis_events': 0,
                        'singles_count': 0,
                        'doubles_count': 0,
                        'needs_correction': [],
                        'analysis_timestamp': datetime.now().isoformat()
                    }
                
                logger.info(f"Found {len(tennis_events)} tennis events in database")
                
                # Analyze each event
                singles_count = 0
                doubles_count = 0
                needs_correction = []
                
                for event in tennis_events:
                    classified_sport = self._classify_tennis(event.home_team, event.away_team)
                    
                    if classified_sport == "Tennis Doubles":
                        doubles_count += 1
                        # This event needs correction from "Tennis" to "Tennis Doubles"
                        needs_correction.append({
                            'event_id': event.id,
                            'home_team': event.home_team,
                            'away_team': event.away_team,
                            'competition': event.competition,
                            'start_time': event.start_time_utc.isoformat(),
                            'current_sport': event.sport,
                            'corrected_sport': classified_sport
                        })
                    else:
                        singles_count += 1
                
                analysis_result = {
                    'total_tennis_events': len(tennis_events),
                    'singles_count': singles_count,
                    'doubles_count': doubles_count,
                    'needs_correction_count': len(needs_correction),
                    'needs_correction': needs_correction,
                    'analysis_timestamp': datetime.now().isoformat()
                }
                
                logger.info(f"📊 Analysis complete:")
                logger.info(f"  - Total tennis events: {len(tennis_events)}")
                logger.info(f"  - Singles: {singles_count}")
                logger.info(f"  - Doubles: {doubles_count}")
                logger.info(f"  - Need correction: {len(needs_correction)}")
                
                return analysis_result
                
        except Exception as e:
            logger.error(f"Error analyzing database: {e}")
            return {
                'error': str(e),
                'analysis_timestamp': datetime.now().isoformat()
            }
    
    def correct_database_tennis(self, dry_run: bool = True) -> Dict:
        """
        Correct tennis events in database from "Tennis" to "Tennis Doubles" where appropriate.
        
        Args:
            dry_run: If True, only preview changes without applying them
            
        Returns:
            Dictionary with correction results
        """
        logger.info(f"🔧 Starting tennis database correction (dry_run={dry_run})...")
        
        try:
            # First, analyze what needs to be corrected
            analysis = self.analyze_database_tennis()
            
            if 'error' in analysis:
                return analysis
            
            needs_correction = analysis['needs_correction']
            
            if not needs_correction:
                logger.info("✅ No tennis events need correction")
                return {
                    'status': 'success',
                    'message': 'No corrections needed',
                    'corrected_count': 0,
                    'dry_run': dry_run,
                    'timestamp': datetime.now().isoformat()
                }
            
            if dry_run:
                logger.info(f"🔍 DRY RUN: Would correct {len(needs_correction)} tennis events:")
                for event in needs_correction[:5]:  # Show first 5 examples
                    logger.info(f"  - Event {event['event_id']}: '{event['home_team']}' vs '{event['away_team']}'")
                    logger.info(f"    Competition: {event['competition']}")
                    logger.info(f"    Change: '{event['current_sport']}' → '{event['corrected_sport']}'")
                
                if len(needs_correction) > 5:
                    logger.info(f"  ... and {len(needs_correction) - 5} more events")
                
                return {
                    'status': 'dry_run_complete',
                    'message': f'Would correct {len(needs_correction)} events',
                    'preview_count': len(needs_correction),
                    'preview_events': needs_correction,
                    'dry_run': True,
                    'timestamp': datetime.now().isoformat()
                }
            
            # Actual correction (when dry_run=False)
            logger.info(f"🔧 Applying corrections to {len(needs_correction)} tennis events...")
            
            corrected_count = 0
            failed_corrections = []
            
            with db_manager.get_session() as session:
                for correction in needs_correction:
                    try:
                        event = session.query(Event).filter(Event.id == correction['event_id']).first()
                        if event:
                            event.sport = correction['corrected_sport']
                            event.updated_at = datetime.utcnow()
                            corrected_count += 1
                            logger.debug(f"✅ Corrected event {event.id}: {correction['corrected_sport']}")
                        else:
                            failed_corrections.append(f"Event {correction['event_id']} not found")
                    except Exception as e:
                        failed_corrections.append(f"Event {correction['event_id']}: {e}")
                        logger.error(f"Failed to correct event {correction['event_id']}: {e}")
                
                # Commit all changes
                session.commit()
                logger.info(f"✅ Successfully corrected {corrected_count} tennis events")
                
                if failed_corrections:
                    logger.warning(f"⚠️ {len(failed_corrections)} corrections failed:")
                    for failure in failed_corrections:
                        logger.warning(f"  - {failure}")
            
            return {
                'status': 'success',
                'message': f'Successfully corrected {corrected_count} events',
                'corrected_count': corrected_count,
                'failed_count': len(failed_corrections),
                'failed_corrections': failed_corrections,
                'dry_run': False,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error during tennis database correction: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'dry_run': dry_run,
                'timestamp': datetime.now().isoformat()
            }
    
    def get_classification_stats(self) -> Dict:
        """Get current classification statistics"""
        return self.classification_stats.copy()


def main():
    """Command line interface for database correction"""
    parser = argparse.ArgumentParser(description='Sport Classification and Database Correction')
    parser.add_argument('--correct-tennis', action='store_true',
                       help='Correct tennis events in database')
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Preview changes without applying them (default: True)')
    parser.add_argument('--apply', action='store_true',
                       help='Actually apply the corrections (overrides --dry-run)')
    parser.add_argument('--analyze', action='store_true',
                       help='Only analyze database without making corrections')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    classifier = SportClassifier()
    
    if args.analyze:
        print("🔍 Analyzing tennis events in database...")
        result = classifier.analyze_database_tennis()
        
        if 'error' not in result:
            print(f"\n📊 Analysis Results:")
            print(f"  - Total tennis events: {result['total_tennis_events']}")
            print(f"  - Singles: {result['singles_count']}")
            print(f"  - Doubles: {result['doubles_count']}")
            print(f"  - Need correction: {result['needs_correction_count']}")
            
            if result['needs_correction_count'] > 0:
                print(f"\n🔧 Sample events that need correction:")
                for event in result['needs_correction'][:3]:
                    print(f"  - {event['home_team']} vs {event['away_team']}")
                    print(f"    Competition: {event['competition']}")
        else:
            print(f"❌ Error during analysis: {result['error']}")
    
    elif args.correct_tennis:
        dry_run = not args.apply  # If --apply is specified, dry_run becomes False
        
        if dry_run:
            print("🔍 Running DRY RUN for tennis corrections...")
        else:
            print("🔧 Applying tennis corrections to database...")
        
        result = classifier.correct_database_tennis(dry_run=dry_run)
        
        if result['status'] == 'success':
            if dry_run:
                print(f"✅ Dry run complete: Would correct {result.get('preview_count', 0)} events")
                print("Use --apply to actually make the changes")
            else:
                print(f"✅ Corrections applied: {result['corrected_count']} events updated")
        elif result['status'] == 'dry_run_complete':
            print(f"✅ Dry run complete: Would correct {result['preview_count']} events")
            print("Use --apply to actually make the changes")
        else:
            print(f"❌ Error: {result.get('error', 'Unknown error')}")
    
    else:
        parser.print_help()


# Global classifier instance for import by other modules
sport_classifier = SportClassifier()

if __name__ == '__main__':
    main()
