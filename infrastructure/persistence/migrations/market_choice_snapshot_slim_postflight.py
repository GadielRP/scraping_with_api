"""Reader probes for the Phase 6 snapshot migration.

This module verifies database-facing contracts after the schema migrator has
finished. It owns no DDL and contains no application alert decisions.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

from sqlalchemy import bindparam, text

REQUIRED_EVENT_READERS = {
    "v_dual_process_event_odds": "event_id",
    # event_all_odds intentionally exposes no identifier; its contract is a
    # report projection, so the probe executes it with a cheap LIMIT.
    "event_all_odds": None,
    "v_pre_start_odds_trajectory": "event_id",
    "mv_alert_events": "event_id",
}


@dataclass(frozen=True, slots=True)
class SnapshotSlimReaderPostflight:
    event_scope: tuple[int, ...]
    row_counts: dict[str, int]
    quote_reader_targets: dict[str, str]
    errors: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["ok"] = self.ok
        return payload


class MarketChoiceSnapshotSlimPostflight:
    """Probe all persisted readers that rely on snapshot quote lineage."""

    @staticmethod
    def run(
        engine,
        *,
        event_ids: Iterable[int] = (158955, 169158),
    ) -> SnapshotSlimReaderPostflight:
        scope = tuple(sorted({int(item) for item in event_ids}))
        row_counts: dict[str, int] = {}
        quote_reader_targets: dict[str, str] = {}
        errors: list[str] = []

        for reader, event_column in REQUIRED_EVENT_READERS.items():
            with engine.connect() as connection:
                if event_column is None:
                    statement = text(
                        f'SELECT * FROM public."{reader}" LIMIT 0'
                    )
                    params = {}
                else:
                    statement = text(
                        f'SELECT COUNT(*) FROM public."{reader}" '
                        f'WHERE "{event_column}" IN :event_ids'
                    ).bindparams(bindparam("event_ids", expanding=True))
                    params = {"event_ids": list(scope)}
                try:
                    result = connection.execute(statement, params).scalar()
                    row_counts[reader] = int(result or 0)
                except Exception as exc:
                    errors.append(f"reader_query_failed:{reader}:{exc}")

        with engine.connect() as connection:
            if connection.dialect.name == "postgresql":
                expected_targets = {
                    "v_dual_process_event_odds": "market_choice_quotes",
                    "v_pre_start_odds_trajectory": "eligible_quotes",
                }
                for reader, expected_target in expected_targets.items():
                    try:
                        definition = str(
                            connection.execute(
                                text("SELECT pg_get_viewdef(:view_name, true)"),
                                {"view_name": f"public.{reader}"},
                            ).scalar()
                            or ""
                        ).casefold()
                    except Exception as exc:
                        errors.append(
                            f"reader_definition_failed:{reader}:{exc}"
                        )
                        continue
                    quote_reader_targets[reader] = expected_target
                    if expected_target not in definition:
                        errors.append(
                            f"reader_not_quotes:{reader}:{expected_target}"
                        )
        return SnapshotSlimReaderPostflight(
            event_scope=scope,
            row_counts=row_counts,
            quote_reader_targets=quote_reader_targets,
            errors=tuple(errors),
        )


__all__ = [
    "MarketChoiceSnapshotSlimPostflight",
    "REQUIRED_EVENT_READERS",
    "SnapshotSlimReaderPostflight",
]
