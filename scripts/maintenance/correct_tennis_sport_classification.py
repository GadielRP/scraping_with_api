"""Maintenance script for tennis sport classification corrections."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import Dict, List

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from modules.sofascore.sport_classifier import SPORT_TENNIS, SPORT_TENNIS_DOUBLES, SportClassifier
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class TennisSportClassificationMaintenance:
    def __init__(self, classifier: SportClassifier | None = None):
        self.classifier = classifier or SportClassifier()

    def analyze_database_tennis(self) -> Dict:
        logger.info("Analyzing database for tennis events...")

        try:
            with db_manager.get_session() as session:
                tennis_events = session.query(Event).filter(Event.sport == SPORT_TENNIS).all()

                if not tennis_events:
                    return {
                        "total_tennis_events": 0,
                        "singles_count": 0,
                        "doubles_count": 0,
                        "needs_correction": [],
                        "analysis_timestamp": datetime.now().isoformat(),
                    }

                singles_count = 0
                doubles_count = 0
                needs_correction = []

                for event in tennis_events:
                    classified_sport = self.classifier.classify_tennis_match_format(event.home_team, event.away_team)
                    if classified_sport == SPORT_TENNIS_DOUBLES:
                        doubles_count += 1
                        needs_correction.append(
                            {
                                "event_id": event.id,
                                "home_team": event.home_team,
                                "away_team": event.away_team,
                                "competition": event.competition,
                                "start_time": event.start_time_utc.isoformat(),
                                "current_sport": event.sport,
                                "corrected_sport": classified_sport,
                            }
                        )
                    else:
                        singles_count += 1

                return {
                    "total_tennis_events": len(tennis_events),
                    "singles_count": singles_count,
                    "doubles_count": doubles_count,
                    "needs_correction_count": len(needs_correction),
                    "needs_correction": needs_correction,
                    "analysis_timestamp": datetime.now().isoformat(),
                }
        except Exception as exc:
            logger.error("Error analyzing database: %s", exc)
            return {"error": str(exc), "analysis_timestamp": datetime.now().isoformat()}

    def correct_database_tennis(self, dry_run: bool = True) -> Dict:
        logger.info("Starting tennis database correction (dry_run=%s)...", dry_run)

        try:
            analysis = self.analyze_database_tennis()
            if "error" in analysis:
                return analysis

            needs_correction = analysis["needs_correction"]
            if not needs_correction:
                return {
                    "status": "success",
                    "message": "No corrections needed",
                    "corrected_count": 0,
                    "dry_run": dry_run,
                    "timestamp": datetime.now().isoformat(),
                }

            if dry_run:
                return {
                    "status": "dry_run_complete",
                    "message": f"Would correct {len(needs_correction)} events",
                    "preview_count": len(needs_correction),
                    "preview_events": needs_correction,
                    "dry_run": True,
                    "timestamp": datetime.now().isoformat(),
                }

            corrected_count = 0
            failed_corrections: List[str] = []

            with db_manager.get_session() as session:
                for correction in needs_correction:
                    try:
                        event = session.query(Event).filter(Event.id == correction["event_id"]).first()
                        if event:
                            event.sport = correction["corrected_sport"]
                            event.updated_at = get_local_now()
                            corrected_count += 1
                        else:
                            failed_corrections.append(f"Event {correction['event_id']} not found")
                    except Exception as exc:
                        failed_corrections.append(f"Event {correction['event_id']}: {exc}")
                        logger.error("Failed to correct event %s: %s", correction["event_id"], exc)

                session.commit()

            return {
                "status": "success",
                "message": f"Successfully corrected {corrected_count} events",
                "corrected_count": corrected_count,
                "failed_count": len(failed_corrections),
                "failed_corrections": failed_corrections,
                "dry_run": False,
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as exc:
            logger.error("Error during tennis database correction: %s", exc)
            return {
                "status": "error",
                "error": str(exc),
                "dry_run": dry_run,
                "timestamp": datetime.now().isoformat(),
            }


def main() -> None:
    parser = argparse.ArgumentParser(description="Tennis sport classification maintenance")
    parser.add_argument("--correct-tennis", action="store_true", help="Correct tennis events in database")
    parser.add_argument("--dry-run", action="store_true", default=True, help="Preview changes without applying them")
    parser.add_argument("--apply", action="store_true", help="Actually apply the corrections")
    parser.add_argument("--analyze", action="store_true", help="Only analyze database without making corrections")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    maintenance = TennisSportClassificationMaintenance()

    if args.analyze:
        result = maintenance.analyze_database_tennis()
        if "error" in result:
            print(f"Error during analysis: {result['error']}")
            return

        print("Analysis Results:")
        print(f"  - Total tennis events: {result['total_tennis_events']}")
        print(f"  - Singles: {result['singles_count']}")
        print(f"  - Doubles: {result['doubles_count']}")
        print(f"  - Need correction: {result['needs_correction_count']}")
        return

    if args.correct_tennis:
        dry_run = not args.apply
        result = maintenance.correct_database_tennis(dry_run=dry_run)
        if result.get("status") in {"success", "dry_run_complete"}:
            print(result.get("message", "Done"))
        else:
            print(f"Error: {result.get('error', 'Unknown error')}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
