import logging
import os
import sys

from infrastructure.settings import Config
from shared.timezone_utils import get_local_now_aware


_MONTH_NAMES = [
    "",
    "01_January",
    "02_February",
    "03_March",
    "04_April",
    "05_May",
    "06_June",
    "07_July",
    "08_August",
    "09_September",
    "10_October",
    "11_November",
    "12_December",
]


def _get_log_path() -> str:
    """Build the dynamic log file path based on current local date."""
    now = get_local_now_aware()
    month_folder = _MONTH_NAMES[now.month]
    week_number = min((now.day - 1) // 7 + 1, 4)

    log_dir = os.path.join("logs", month_folder, f"week_{week_number}")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "sofascore_odds.log")


def _get_oddsportal_log_path() -> str:
    """Build the OddsPortal-specific log file path."""
    now = get_local_now_aware()
    month_folder = _MONTH_NAMES[now.month]
    week_number = min((now.day - 1) // 7 + 1, 4)

    log_dir = os.path.join("logs", "oddsportal", month_folder, f"week_{week_number}")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "oddsportal.log")


class _WeeklyRotatingFileHandler(logging.FileHandler):
    """File handler that rotates to a new weekly/monthly path when needed."""

    def __init__(self, path_fn=None, **kwargs):
        self._path_fn = path_fn or _get_log_path
        self._current_path = self._path_fn()
        super().__init__(self._current_path, **kwargs)

    def emit(self, record):
        new_path = self._path_fn()
        if new_path != self._current_path:
            self.close()
            self._current_path = new_path
            self.baseFilename = os.path.abspath(new_path)
            self.stream = self._open()
        super().emit(record)


def setup_logging():
    """Setup logging configuration with weekly-rotated log files."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    formatter = logging.Formatter(Config.LOG_FORMAT)
    level = getattr(logging, Config.LOG_LEVEL)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    file_handler = _WeeklyRotatingFileHandler(mode="a", encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    op_logger = logging.getLogger("oddsportal_scraper")
    for handler in op_logger.handlers[:]:
        op_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    op_logger.propagate = True

    op_file_handler = _WeeklyRotatingFileHandler(
        path_fn=_get_oddsportal_log_path,
        mode="a",
        encoding="utf-8",
    )
    op_file_handler.setLevel(level)
    op_file_handler.setFormatter(formatter)
    op_logger.addHandler(op_file_handler)

    console_handler.flush()
    file_handler.flush()

    logging.info("Logging system initialized successfully")


__all__ = ["setup_logging"]

