"""Capture Oddspapi fixture responses that cannot persist both participants."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import unicodedata
import uuid
from typing import Any


logger = logging.getLogger(__name__)


class OddspapiFixtureResponseDebugWriter:
    """Save raw fixture objects with participant data rejected by persistence.

    Files are written below ``debug/oddspapi_fixture_responses/empty_participant``
    by default. Set ``ODDSPAPI_EMPTY_PARTICIPANT_DEBUG_DIR`` to override the root.
    Debug capture is best-effort and must never fail fixture discovery.
    """

    OUTPUT_DIRECTORY = (
        Path("debug") / "oddspapi_fixture_responses" / "empty_participant"
    )
    PARTICIPANT_FIELDS = ("Id", "Name", "ShortName", "Abbr")

    @classmethod
    def issues_for_payload(cls, payload: Any) -> list[str]:
        """Report participant fields that prevent a source Participant row."""
        if not isinstance(payload, dict):
            return ["invalid_fixture_payload"]

        issues: list[str] = []
        for number in (1, 2):
            prefix = f"participant{number}"
            participant_id = payload.get(f"{prefix}Id")
            if participant_id is None or not str(participant_id).strip():
                issues.append(f"{prefix}_missing_id")
            else:
                try:
                    int(str(participant_id).strip())
                except (TypeError, ValueError):
                    issues.append(f"{prefix}_invalid_id")

            # ParticipantRepository requires `name`; short name or abbreviation
            # alone currently does not create a Participant row.
            name = payload.get(f"{prefix}Name")
            if name is None or not str(name).strip():
                issues.append(f"{prefix}_missing_name")

        return issues

    @staticmethod
    def _filename_token(value: object, *, fallback: str) -> str:
        normalized = (
            unicodedata.normalize("NFKD", str(value or ""))
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        token = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized).strip("._-")
        return (token[:100] or fallback)

    @classmethod
    def _output_directory(cls) -> Path:
        configured = os.getenv("ODDSPAPI_EMPTY_PARTICIPANT_DEBUG_DIR")
        return Path(configured).expanduser() if configured else cls.OUTPUT_DIRECTORY

    @classmethod
    def save_if_incomplete(cls, payload: Any) -> Path | None:
        """Persist one raw fixture response when its participant data is incomplete."""
        if not isinstance(payload, dict):
            return None
        issues = cls.issues_for_payload(payload)
        if not issues:
            return None

        captured_at = datetime.now(timezone.utc)
        fixture_token = cls._filename_token(
            payload.get("fixtureId"),
            fallback=f"unknown-fixture-{uuid.uuid4().hex[:12]}",
        )
        date_directory = cls._output_directory() / captured_at.strftime("%Y-%m-%d")
        filename = f"{captured_at.strftime('%H%M%S_%f')}_{fixture_token}.json"
        path = date_directory / filename
        temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")

        record = {
            "captured_at_utc": captured_at.isoformat(),
            "fixture_id": payload.get("fixtureId"),
            "issues": issues,
            "raw_response": payload,
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with temporary_path.open("w", encoding="utf-8") as file_handle:
                json.dump(record, file_handle, ensure_ascii=False, indent=2, default=str)
                file_handle.write("\n")
            temporary_path.replace(path)
        except (OSError, TypeError, ValueError) as exc:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
            logger.warning(
                "Could not save Oddspapi fixture response with incomplete participants "
                "fixture_id=%s issues=%s: %s",
                payload.get("fixtureId"),
                ",".join(issues),
                exc,
            )
            return None

        logger.info(
            "Captured Oddspapi fixture with incomplete participant data "
            "fixture_id=%s issues=%s debug_path=%s",
            payload.get("fixtureId"),
            ",".join(issues),
            path,
        )
        return path
