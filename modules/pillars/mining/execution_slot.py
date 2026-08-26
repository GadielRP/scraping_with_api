"""Deterministic identity policy for mining executions."""

from __future__ import annotations


def build_execution_slot(
    evaluation_minute: int | None,
    target_minute: int | None,
) -> str:
    if evaluation_minute is not None:
        return f"evaluation:{evaluation_minute}"
    if target_minute is not None:
        return f"target:{target_minute}"
    return "event"
