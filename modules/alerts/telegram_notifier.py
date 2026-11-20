"""Where alert messages go: the alerts chat, or the personal chat in test-only mode."""

import logging
import os

from infrastructure.network.telegram import send_telegram_message
from infrastructure.settings import Config

logger = logging.getLogger(__name__)


class PreStartNotification:
    """Alert destination settings; delivery goes through infrastructure.network.telegram."""

    def __init__(self):
        self.telegram_chat_id = Config.TELEGRAM_CHAT_ID
        self.telegram_enabled = bool(Config.TELEGRAM_BOT_TOKEN and self.telegram_chat_id)
        if os.getenv("TEST_ONLY_MODE", "false").lower() == "true":
            self.telegram_chat_id = Config.PERSONAL_CHAT_ID
        logger.info("Telegram notification: %s", "Enabled" if self.telegram_enabled else "Disabled")
        if not self.telegram_enabled:
            logger.warning("Telegram not configured. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env file")

    def send_telegram_message(self, message: str) -> bool:
        """Send an alert message."""
        if not self.telegram_enabled:
            logger.warning("Telegram notifications not configured - cannot send alert message")
            return False
        return send_telegram_message(message, self.telegram_chat_id)


pre_start_notifier = PreStartNotification()
