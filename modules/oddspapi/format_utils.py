"""Shared formatting helpers for OddsPapi catalog and adapter code."""

from __future__ import annotations


def format_line(value) -> str | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        normalized = str(value).strip()
        return normalized or None
    if number == 0:
        return "0"
    if number.is_integer():
        return str(int(number))
    return str(number).rstrip("0").rstrip(".")


def normalize_source_id(value) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_name(value) -> str:
    return str(value or "").strip()


def normalize_source(source) -> str:
    return str(source or "").strip().lower()


def normalized_token(value) -> str:
    return str(value or "").strip().lower()


def normalized_compact(value) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("_", "")
        .replace("-", "")
        .replace(" ", "")
    )
