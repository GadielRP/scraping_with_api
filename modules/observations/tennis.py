"""Tennis observation formatting helpers."""

from __future__ import annotations


def normalize_tennis_ground_type_label(value: str) -> str:
    normalized = (value or "").strip().lower()
    if normalized == "hardcourt indoor":
        return "Indoor"
    if normalized == "hardcourt outdoor":
        return "Outdoor"
    if "clay" in normalized:
        return "Clay"
    if "grass" in normalized:
        return "Grass"
    if "carpet" in normalized:
        return "Carpet"
    if "synthetic" in normalized:
        return "Synthetic"
    return "Unknown"


def format_tennis_ground_type(value: str | None) -> str:
    return f"🎾: {normalize_tennis_ground_type_label(value or '')}"
