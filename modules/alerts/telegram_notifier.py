"""Telegram transport for alert messages."""

import logging

import requests

logger = logging.getLogger(__name__)


class PreStartNotification:
    """Telegram notification transport used by the alert system."""

    def __init__(self):
        self.notification_enabled = True
        self.telegram_enabled = False
        self.telegram_bot_token = ""
        self.telegram_chat_id = ""
        self.telegram_test_only = False
        self.personal_chat_id = ""
        self._load_notification_settings()

    def _load_notification_settings(self):
        """Load notification settings from environment variables."""
        import os

        from dotenv import load_dotenv

        load_dotenv()

        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self.telegram_enabled = bool(self.telegram_bot_token and self.telegram_chat_id)
        self.telegram_test_only = os.getenv("TEST_ONLY_MODE", "false").lower() == "true"
        self.personal_chat_id = os.getenv("PERSONAL_CHAT_ID", "")

        logger.info("Telegram notification: %s", "Enabled" if self.telegram_enabled else "Disabled")
        if not self.telegram_enabled:
            logger.warning(
                "Telegram not configured. Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to .env file"
            )

    def _split_message(self, message: str, limit: int = 4000):
        """Split a message into chunks of approximately `limit` characters."""
        if len(message) <= limit:
            return [message]

        chunks = []
        while message:
            if len(message) <= limit:
                chunks.append(message)
                break

            split_at = message.rfind("\n", 0, limit)
            if split_at == -1:
                split_at = limit

            last_open = message.rfind("<", 0, split_at)
            last_close = message.rfind(">", 0, split_at)
            if last_open > last_close:
                split_at = last_open

            if split_at == 0:
                split_at = limit

            chunks.append(message[:split_at].strip())
            message = message[split_at:].strip()

        return chunks

    def _send_telegram_notification(self, message: str) -> bool:
        """Send a message through Telegram, splitting it when needed."""
        if not message:
            return False

        try:
            chunks = self._split_message(message)
            all_success = True

            for chunk in chunks:
                url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
                data = {
                    "chat_id": self.telegram_chat_id,
                    "text": chunk,
                    "parse_mode": "HTML",
                }

                if self.telegram_test_only:
                    data["chat_id"] = self.personal_chat_id

                response = requests.post(url, data=data, timeout=10)
                if response.status_code == 200:
                    logger.info("Telegram notification chunk sent successfully (%s chars)", len(chunk))
                else:
                    logger.error(
                        "Telegram notification failed: %s - %s",
                        response.status_code,
                        response.text,
                    )
                    all_success = False

            return all_success
        except Exception as e:
            logger.error("Error sending Telegram notification: %s", e)
            return False

    def send_telegram_message(self, message: str) -> bool:
        """Send a custom Telegram message."""
        if not self.telegram_enabled:
            logger.warning("Telegram notifications not configured - cannot send alert message")
            return False

        logger.info("Sending alert message via Telegram...")
        return self._send_telegram_notification(message)


pre_start_notifier = PreStartNotification()

__all__ = ["PreStartNotification", "pre_start_notifier"]
