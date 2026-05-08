import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from infrastructure.persistence.repositories import DualProcessOddsRepository


def show_events(limit: int = 10):
    """Show recent events."""
    logger = logging.getLogger(__name__)

    try:
        with db_manager.get_session() as session:
            events = (
                session.query(Event)
                .order_by(Event.start_time_utc.desc())
                .limit(limit)
                .all()
            )

        print(f"\n=== Recent Events (showing {len(events)}) ===")
        for event in events:
            odds = DualProcessOddsRepository.get_event_odds(event.id)
            print(f"\nEvent ID: {event.id}")
            print(f"Teams: {event.home_team} vs {event.away_team}")
            print(f"Competition: {event.competition}")
            print(f"Start Time: {event.start_time_utc}")

            if odds:
                print(f"Market: {odds.market_name} / {odds.market_group} / {odds.market_period}")
                print(f"Odds - Open: 1={odds.one_open}, X={odds.x_open}, 2={odds.two_open}")
                print(f"Odds - Final: 1={odds.one_final}, X={odds.x_final}, 2={odds.two_final}")
            else:
                print("No dual-process market odds available")

        print("\n" + "=" * 40)
    except Exception as exc:
        logger.error(f"Error showing events: {exc}")

