"""The only client of the Telegram Bot API: sends HTML messages, split to fit Telegram's size limit."""

import logging

import requests

from infrastructure.settings import Config

logger = logging.getLogger(__name__)

# Telegram rejects messages above 4096 characters; the margin keeps HTML tags whole.
MESSAGE_LIMIT = 4000


def _split_message(message: str, limit: int = MESSAGE_LIMIT) -> list[str]:
    """Split at line breaks, never inside an HTML tag."""
    chunks = []
    while len(message) > limit:
        split_at = message.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = limit
        last_open = message.rfind("<", 0, split_at)
        if last_open > message.rfind(">", 0, split_at):
            split_at = last_open
        if split_at == 0:
            split_at = limit
        chunks.append(message[:split_at].strip())
        message = message[split_at:].strip()
    if message:
        chunks.append(message)
    return chunks


def send_telegram_message(message: str, chat_id: str) -> bool:
    """Send ``message`` to ``chat_id``. True only if every part was delivered."""
    if not message or not chat_id or not Config.TELEGRAM_BOT_TOKEN:
        logger.warning("Telegram message not sent: missing message, chat id or bot token")
        return False
    url = f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage"
    delivered = True
    try:
        for chunk in _split_message(message):
            response = requests.post(
                url, data={"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}, timeout=10,
            )
            if response.status_code == 200:
                logger.info("📲 Telegram message sent (%s chars)", len(chunk))
            else:
                logger.error("Telegram message failed: %s - %s", response.status_code, response.text)
                delivered = False
    except requests.RequestException as exc:
        logger.error("Error sending Telegram message: %s", exc)
        return False
    return delivered
