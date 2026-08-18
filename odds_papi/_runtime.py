"""Shared runtime configuration for the standalone Oddspapi scripts."""

from __future__ import annotations

import os
from pathlib import Path
import sys

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

project_root_text = str(PROJECT_ROOT)
if project_root_text not in sys.path:
    sys.path.insert(0, project_root_text)


from infrastructure.settings import Config
from modules.oddspapi.api_keys import configured_api_keys


def get_api_key() -> str:
    """Select one configured key for a standalone script invocation.

    Scripts use the first key by default. Set ODDSPAPI_SCRIPT_KEY_INDEX=2 to
    diagnose the second key without copying a secret into a command.
    """
    api_keys = configured_api_keys()
    if not api_keys:
        raise RuntimeError(
            "ODDSPAPI_FREE_KEYS is not configured. Expected one or more "
            "comma-separated API keys."
        )

    raw_index = str(os.getenv("ODDSPAPI_SCRIPT_KEY_INDEX", "1")).strip()
    try:
        key_index = int(raw_index)
    except ValueError as exc:
        raise RuntimeError(
            "ODDSPAPI_SCRIPT_KEY_INDEX must be a positive integer."
        ) from exc
    if key_index < 1 or key_index > len(api_keys):
        raise RuntimeError(
            "ODDSPAPI_SCRIPT_KEY_INDEX is outside the configured key range "
            f"(configured keys: {len(api_keys)})."
        )
    return api_keys[key_index - 1]


def oddspapi_url(endpoint: str) -> str:
    base_url = Config.ODDSPAPI_BASE_URL.rstrip("/")
    normalized_endpoint = str(endpoint or "").strip().lstrip("/")
    return f"{base_url}/{normalized_endpoint}"
