"""Formatting and sending helpers for time correction alerts."""

import logging
from datetime import datetime

from infrastructure.persistence.repositories import EventRepository

logger = logging.getLogger(__name__)


def create_time_correction_message(
    event_id: int,
    current_starting_time: datetime,
    new_starting_time: datetime,
) -> str:
    """Build the Telegram message for a time correction alert."""
    event = EventRepository.get_event_by_id(event_id)
    if not event or not event.home_participant or not event.away_participant or not event.competition_ref:
        logger.warning("Could not find event %s for time correction message", event_id)
        participants = f"Missing normalized participants/competition for event_id {event_id}"
    else:
        participants = f"{event.home_participant.name} vs {event.away_participant.name}"

    current_time_str = current_starting_time.strftime("%H:%M")
    new_time_str = new_starting_time.strftime("%H:%M")

    time_diff = new_starting_time - current_starting_time
    if time_diff.total_seconds() > 0:
        diff_str = f"+{int(time_diff.total_seconds() / 60)} min"
    else:
        diff_str = f"{int(time_diff.total_seconds() / 60)} min"

    now = datetime.now()
    if new_starting_time > now:
        footer = "Starting time corrected during pre-start check"
    else:
        footer = "Starting time corrected during late timestamp check"

    message = "🕐 <b>Time Correction Alert</b>\n\n"
    message += f"🏆 <b>{participants}</b>\n"
    message += f"📅 Event ID: {event_id}\n\n"
    message += "⏰ <b>Time Change:</b>\n"
    message += f"Original: {current_time_str}\n"
    message += f"Updated: {new_time_str}\n"
    message += f"Difference: {diff_str}\n\n"
    message += f"🔄 <i>{footer}</i>"
    return message


def send_time_correction_message(
    notifier,
    event_id: int,
    current_starting_time: datetime,
    new_starting_time: datetime,
) -> bool:
    """Create and send a time correction alert through the provided notifier."""
    try:
        if not notifier.telegram_enabled:
            logger.warning("Telegram notifications not configured - cannot send time correction message")
            return False

        message = create_time_correction_message(event_id, current_starting_time, new_starting_time)
        if message.startswith("Missing normalized participants/competition for event_id"):
            logger.warning("Skipping time correction alert for event %s because normalized context is missing", event_id)
            return False
        logger.info("Sending time correction message for event %s", event_id)
        return notifier.send_telegram_message(message)
    except Exception as e:
        logger.error("Error creating time correction message for event %s: %s", event_id, e)
        message = f"🕐 Time correction message for event {event_id}\n\n"
        message += f"Current starting time: {current_starting_time}\n"
        message += f"New starting time: {new_starting_time}\n"
        return notifier.send_telegram_message(message)


__all__ = [
    "create_time_correction_message",
    "send_time_correction_message",
]
