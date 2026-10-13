"""Pillar pipeline for the pre-start job.

Runs pillar/module calculations for events at key moments.
Parallel to, but independent of, the existing alert pipeline.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import fields, replace
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from uuid import uuid4
from typing import Any

from .pillar_result_logging import log_pillar_results

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from infrastructure.settings import Config
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    Pillar5PriceMemoryRepository,
)
from infrastructure.persistence.repositories.pillar_mining_repository import (
    PillarMiningRepository,
)
from modules.competition.tracked_competitions import is_tracked_competition
from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.market_evaluation import prepare_event_markets
from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult
from modules.pillars.trajectory_selection import select_target_minute
from modules.pillars.competition_metadata_resolver import (
    apply_competition_metadata_resolution,
    resolve_competition_metadata,
)
from modules.pillars.streak_analysis_resolver import (
    resolve_matchup_streak_analysis,
)
from modules.pillars.pillar_1_team_structure.side import calculate_p1_side
from modules.pillars.pillar_1_team_structure.totals import calculate_p1_totals
from modules.pillars.pillar_2_side_market.run_pillar_2 import (
    ENGINE_VERSION as P2_ENGINE_VERSION,
    calculate_pillar_2,
)
from modules.pillars.pillar_3_totals_market_context.run_pillar_3 import (
    ENGINE_VERSION as P3_ENGINE_VERSION,
    calculate_pillar_3,
)
from modules.pillars.mining.adapters import (
    P1SideMiningAdapter,
    P1TotalsMiningAdapter,
    P2MiningAdapter,
    P3MiningAdapter,
    P4MiningAdapter,
    P5MiningAdapter,
)
from modules.pillars.mining.service import PillarMiningService
from modules.pillars.pillar_4.run_pillar_4 import (
    ENGINE_VERSION as P4_ENGINE_VERSION,
    calculate_pillar_4,
)
from modules.pillars.pillar_5.run_pillar_5 import (
    ENGINE_VERSION as P5_ENGINE_VERSION,
    calculate_pillar_5,
    prepare_pillar_5,
)

from modules.pillars.pillar_5.ports import MemorySamplePersistenceError
from shared.temporal import utc_now
from modules.pillars.instrumentation import (
    capture_calculations,
    calculation_scope,
)
from modules.pillars.mining.contracts import EvaluationSnapshot
from modules.pillars.mining.serialization import to_json_value
from modules.pillars.mining.versioning import calculation_versions
from modules.pillars.mining.fingerprint import content_hash

from .debug_exports.odds_trajectory_excel import export_odds_trajectory_context_xlsx
from .providers.debug_paths import sport_folder_name

logger = logging.getLogger(__name__)

CANONICAL_SIGNAL_FLOW_ID = "pre_start_signal_profile"


def _is_pillar_competition_in_scope(competition_id) -> bool:
    """Return whether the competition is inside the configured pillar scope."""
    return (
        not Config.FILTER_PIPELINES_BY_TRACKED_COMPETITIONS
        or is_tracked_competition(competition_id)
    )


def _build_market_error_result(event_context, pillar, evaluation, exc):
    identifiers = {
        2: ("pillar_2_side_market", P2_ENGINE_VERSION),
        3: ("pillar_3_totals_market_context", P3_ENGINE_VERSION),
        4: ("pillar_4_temporal_market_drift", P4_ENGINE_VERSION),
        5: ("pillar_5", P5_ENGINE_VERSION),
    }
    pillar_id, version = identifiers[pillar]
    return EvaluationResult(
        event_context.event_id,
        pillar_id,
        version,
        evaluation.target_selection.target_minute,
        evaluation.selection,
        (
            SignalResult(
                "execution",
                "ERROR",
                reason="CALCULATION_ERROR",
                evidence={"error_class": type(exc).__name__, "message": str(exc)},
            ),
        ),
        evaluation.coverage(pillar),
        contracts=evaluation.contracts(pillar),
    ).to_dict()


def _safe_debug_name(value: Any) -> str:
    try:
        safe_value = str(value)
        safe_value = re.sub(r"[^A-Za-z0-9 _-]+", "_", safe_value)
        safe_value = safe_value.replace(" ", "_")
        safe_value = re.sub(r"_+", "_", safe_value)
        safe_value = safe_value.strip("_")
        return safe_value or "unknown"
    except Exception:
        return "unknown"


def _write_debug_json(filepath: Path, payload: Any) -> None:
    with filepath.open("w", encoding="utf-8") as handle:
        json.dump(
            to_json_value(payload), handle, ensure_ascii=False, indent=2, sort_keys=True
        )


def _save_pillar_debug_snapshots(
    *,
    event_context: Any,
    odds_trajectory_context: Any,
    streak_analysis: Any = None,
    competition_slug: str | None = None,
) -> None:
    try:
        event_id = getattr(event_context, "event_id", "unknown_event")
        participants = getattr(event_context, "participants_label", "unknown_matchup")

        safe_participants = _safe_debug_name(participants)
        competition_folder = (
            re.sub(r"[^a-z0-9]+", "_", (competition_slug or "").lower()).strip("_")
            or "unknown_competition"
        )
        debug_dir = (
            Path("debug")
            / "pillar_pipeline_objects"
            / sport_folder_name(getattr(event_context, "sport", None))
            / competition_folder
            / f"{event_id}_{safe_participants}"
        )
        debug_dir.mkdir(parents=True, exist_ok=True)

        _write_debug_json(debug_dir / f"{event_id}_event_context.json", event_context)
        if odds_trajectory_context is not None:
            _write_debug_json(
                debug_dir / f"{event_id}_odds_trajectory_context.json",
                odds_trajectory_context,
            )
            workbook_path = debug_dir / f"{event_id}_odds_trajectory_context.xlsx"
            try:
                saved_workbook_path = (
                    export_odds_trajectory_context_xlsx(
                        odds_trajectory_context,
                        workbook_path,
                        event_context=event_context,
                    )
                    or workbook_path
                )
                if saved_workbook_path != workbook_path:
                    logger.warning(
                        "Odds trajectory debug workbook target was locked; saved alternate file "
                        "event_id=%s path=%s",
                        event_id,
                        saved_workbook_path,
                    )
                else:
                    logger.info(
                        "Odds trajectory debug workbook saved event_id=%s path=%s",
                        event_id,
                        saved_workbook_path,
                    )
            except Exception as exc:
                logger.exception(
                    "Failed to save odds trajectory debug workbook event_id=%s path=%s exception_class=%s: %s",
                    event_id,
                    workbook_path,
                    type(exc).__name__,
                    exc,
                )

        resolved_streak = (
            streak_analysis
            if streak_analysis is not None
            else getattr(event_context, "streak_analysis", None)
        )
        if resolved_streak is not None:
            _write_debug_json(
                debug_dir / f"{event_id}_streak_analysis.json", resolved_streak
            )

        logger.info(
            "💾 Pillar debug snapshots saved for event %s at %s (streak_analysis_saved=%s)",
            event_id,
            debug_dir,
            resolved_streak is not None,
        )
    except Exception:
        logger.exception(
            "Failed to save pillar debug snapshots for event %s",
            event_id if "event_id" in locals() else "unknown_event",
        )


PILLAR_DEFINITIONS = {
    2: ("pillar_2_side_market", P2_ENGINE_VERSION),
    3: ("pillar_3_totals_market_context", P3_ENGINE_VERSION),
    4: ("pillar_4_temporal_market_drift", P4_ENGINE_VERSION),
    5: ("pillar_5", P5_ENGINE_VERSION),
}


def _registered_mining_adapters():
    return {
        "pillar_1_team_structure_side": P1SideMiningAdapter(),
        "pillar_1_team_structure_totals": P1TotalsMiningAdapter(),
        "pillar_2_side_market": P2MiningAdapter(),
        "pillar_3_totals_market_context": P3MiningAdapter(),
        "pillar_4_temporal_market_drift": P4MiningAdapter(),
        "pillar_5": P5MiningAdapter(),
    }


def _p1_unavailable(status, reason, **evidence):
    return {
        "status": status,
        "reason": reason,
        "evidence": evidence,
        "engine_version": "p1-input-contract-v1",
    }


class EventPillarProcessor:
    """Acquire first, freeze knowledge, calculate independently, commit once."""

    def __init__(
        self,
        *,
        debug_mode=False,
        enabled_pillars=None,
        mining_service=None,
        versions=None,
    ):
        self.debug_mode = debug_mode
        self.enabled_pillars = (
            Config.PILLAR_PIPELINE_ENABLED_PILLARS
            if enabled_pillars is None
            else enabled_pillars
        )
        self.mining_service = mining_service or PillarMiningService(
            PillarMiningRepository(), adapters=_registered_mining_adapters()
        )
        self.configuration, self.versions = versions or calculation_versions()

    def _enabled(self, number):
        return bool(self.enabled_pillars.get(f"pillar_{number}", True))

    def _prepare_p1(self, context):
        try:
            competition = context.competition
            if not context.competition_metadata_resolved and any(
                getattr(competition, name, None) is None
                for name in (
                    "number_of_teams",
                    "total_regular_season_games",
                    "standings_grouping",
                )
            ):
                apply_competition_metadata_resolution(
                    context, resolve_competition_metadata(context)
                )
            streak, _ = resolve_matchup_streak_analysis(
                context, debug_mode=self.debug_mode
            )
            context.streak_analysis = streak
            return None
        except Exception as exc:
            logger.exception(
                "P1 input preparation failed event_id=%s", context.event_id
            )
            return {"error_class": type(exc).__name__, "message": str(exc)}

    def _calculate_p1(self, context, skipped, preparation_error, traces):
        outputs = {}
        for scope, calculator in (
            ("side", calculate_p1_side),
            ("totals", calculate_p1_totals),
        ):
            trace_key = f"p1_{scope}"
            if skipped or not self._enabled(1):
                result = _p1_unavailable("SKIPPED", skipped or "DISABLED")
            elif preparation_error:
                result = _p1_unavailable(
                    "ERROR", "CONTEXT_PREPARATION_ERROR", **preparation_error
                )
            elif context.streak_analysis is None:
                result = _p1_unavailable(
                    "INSUFFICIENT_DATA", "MISSING_INPUT", missing=["streak_analysis"]
                )
            else:
                try:
                    with calculation_scope(trace_key):
                        native = calculator(
                            context.streak_analysis,
                            event_context=context,
                            debug_mode=self.debug_mode,
                        )
                    if native is None:
                        aborts = [
                            step
                            for step in traces.get(trace_key, [])
                            if step["operation"] == "totals_abort"
                        ]
                        abort = aborts[-1] if aborts else {}
                        code = abort.get("reason", "MISSING_INPUT")
                        reason = (
                            "ZERO_DENOMINATOR"
                            if "SCALE_ZERO" in code or "WEIGHT_SUM_ZERO" in code
                            else (
                                "INSUFFICIENT_OBSERVATIONS"
                                if "N_AVAIL_ZERO" in code or "GAMES_USED_ZERO" in code
                                else "MISSING_INPUT"
                            )
                        )
                        result = _p1_unavailable(
                            "INSUFFICIENT_DATA",
                            reason,
                            native_reason=code,
                            **abort.get("evidence", {}),
                        )
                    elif isinstance(native, dict):
                        result = native
                    else:
                        # Keep numbers typed until the mining adapter projects them.
                        result = {
                            f.name: getattr(native, f.name) for f in fields(native)
                        }
                        for name in ("active_layers", "ignored_layers"):
                            result[name] = [
                                {f.name: getattr(layer, f.name) for f in fields(layer)}
                                for layer in result.get(name, [])
                            ]
                except Exception as exc:
                    logger.exception(
                        "P1 %s calculation failed event_id=%s", scope, context.event_id
                    )
                    result = _p1_unavailable(
                        "ERROR",
                        "CALCULATION_ERROR",
                        error_class=type(exc).__name__,
                        message=str(exc),
                    )
            stages = traces.get(trace_key, [])
            completed_modules = [
                step["result"] for step in stages if step["operation"] == "side_module"
            ]
            if (
                scope == "side"
                and result.get("status") == "ERROR"
                and completed_modules
            ):
                result["modules"] = completed_modules
            # Completed totals expose these components in their native RAW payload.
            result["calculation_trace"] = (
                []
                if scope == "totals" and result.get("status") == "OK"
                else [step for step in stages if step["operation"] != "side_module"]
            )
            result["input_refs"] = ["evaluation.inputs.p1"]
            outputs[scope] = result
        return outputs

    def process_event(self, event_context, trajectory_points=None):
        if not isinstance(event_context, EventContext):
            raise TypeError("pillar evaluation requires EventContext")
        if not _is_pillar_competition_in_scope(
            event_context.competition.competition_id
        ):
            return None
        preparation_started_at = utc_now()
        skipped = (
            "EVENT_UNAVAILABLE"
            if not event_context.success
            else (
                "OUTSIDE_REGULAR_SEASON"
                if event_context.round != "regular_season"
                else None
            )
        )
        preparation_error = (
            self._prepare_p1(event_context)
            if self._enabled(1) and not skipped
            else None
        )
        identity = event_context.to_identity()
        raw_odds = (
            trajectory_points
            if trajectory_points is not None
            else event_context.odds_trajectory
        )
        # All network enrichment has completed before opening the audit transaction.
        odds = build_odds_trajectory_context(
            raw_odds,
            evaluation_minute=identity.minutes_until_start,
            event_starts_at=identity.starts_at,
            evaluation_as_of=utc_now(),
        )
        selection = select_target_minute(
            odds,
            flow_id=CANONICAL_SIGNAL_FLOW_ID,
            expected_event_id=identity.event_id,
            allowed_target_minutes=Config.PRE_START_ODDS_MOMENTS,
            evaluation_minute=identity.minutes_until_start,
        )
        markets = prepare_event_markets(odds, selection, identity)
        evaluation_id = str(uuid4())
        configuration = {**self.configuration, "enabled_pillars": self.enabled_pillars}
        versions = {
            **self.versions,
            "configuration_sha256": content_hash(configuration),
        }
        results = {}
        with db_manager.get_session() as session:
            # A single MVCC population is used by every P5 bookmaker query and its COPY capture.
            if session.get_bind().dialect.name == "postgresql":
                session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            prepared_p5 = None
            p5_preparation_error = None
            if self._enabled(5) and not skipped:
                try:
                    prepared_p5 = prepare_pillar_5(
                        identity,
                        odds,
                        target_selection=selection,
                        memory_repository=Pillar5PriceMemoryRepository(
                            session=session, capture=True
                        ),
                        debug_mode=self.debug_mode,
                        market_evaluation=markets,
                    )
                except (SQLAlchemyError, MemorySamplePersistenceError):
                    raise
                except Exception as exc:
                    p5_preparation_error = exc
            evaluation_as_of = utc_now()
            odds = replace(odds, evaluation_as_of=evaluation_as_of)
            selected = {
                **markets.selection,
                "checkpoint": {
                    **markets.selection.get("checkpoint", {}),
                    "evaluation_as_of": evaluation_as_of.isoformat(),
                },
            }
            markets = replace(markets, context=odds, selection=selected)
            if prepared_p5 is not None:
                prepared_p5 = replace(prepared_p5, selection=selected)
            # Serialize shared material inputs once, before calculations can release their buffers.
            p1_context = {
                f.name: getattr(event_context, f.name)
                for f in fields(event_context)
                if f.name
                not in {
                    "odds_trajectory",
                    "odds_trajectory_context",
                    "ft_1x2_odds_trajectory_context",
                    "dual_report",
                }
            }
            snapshot = EvaluationSnapshot(
                evaluation_id,
                identity.event_id,
                evaluation_as_of,
                context=to_json_value(identity),
                inputs=to_json_value(
                    {
                        "p1": p1_context,
                        "odds_observations": raw_odds,
                        "odds_selection": selection,
                        "odds_availability": odds.availability_diagnostics,
                        "market_contracts": {
                            str(n): markets.contracts(n) for n in range(2, 6)
                        },
                        "preparation_started_at": preparation_started_at,
                        "inputs_captured_at": evaluation_as_of,
                        "p1_preparation_error": preparation_error,
                        "p5_samples": prepared_p5.profiles if prepared_p5 else {},
                    }
                ),
                configuration=configuration,
                versions=versions,
            )
            with capture_calculations() as traces:
                p1 = self._calculate_p1(
                    event_context, skipped, preparation_error, traces
                )
                for scope, result in p1.items():
                    result["evaluation_as_of"] = evaluation_as_of
                    results["pillar_1" if scope == "side" else "pillar_1_totals"] = (
                        result
                    )
                for number, (pillar_id, version) in PILLAR_DEFINITIONS.items():
                    calculator = {
                        2: calculate_pillar_2,
                        3: calculate_pillar_3,
                        4: calculate_pillar_4,
                        5: calculate_pillar_5,
                    }[number]
                    if skipped or not self._enabled(number):
                        result = EvaluationResult(
                            identity.event_id,
                            pillar_id,
                            version,
                            selection.target_minute,
                            markets.selection,
                            (),
                            markets.coverage(number),
                            contracts=markets.contracts(number),
                            skipped=True,
                            diagnostics=({"reason": skipped or "DISABLED"},),
                        ).to_dict()
                    else:
                        try:
                            if number == 5 and p5_preparation_error is not None:
                                raise p5_preparation_error
                            with calculation_scope(f"p{number}"):
                                kwargs = {
                                    "event_context": identity,
                                    "target_selection": selection,
                                    "debug_mode": self.debug_mode,
                                    "market_evaluation": markets,
                                }
                                if number == 5:
                                    kwargs["prepared_inputs"] = prepared_p5
                                else:
                                    kwargs["odds_trajectory_context"] = odds
                                result = calculator(**kwargs)
                        except Exception as exc:
                            logger.exception(
                                "P%s calculation failed event_id=%s",
                                number,
                                identity.event_id,
                            )
                            result = _build_market_error_result(
                                identity, number, markets, exc
                            )
                    result["evaluation_as_of"] = evaluation_as_of
                    result["calculation_trace"] = {
                        key: steps
                        for key, steps in traces.items()
                        if key == f"p{number}" or key.startswith(f"p{number}.")
                    }
                    result["shared_input_refs"] = [
                        "evaluation.inputs.odds_observations",
                        "evaluation.inputs.market_contracts",
                    ]
                    results[f"pillar_{number}"] = result
                # Selected inputs shared by several scopes have one stored payload.
                selected_inputs = {}
                for result in results.values():
                    for point in result.get("inputs", {}).values():
                        selected_inputs.setdefault(
                            content_hash(point), to_json_value(point)
                        )
                snapshot.inputs["selected_inputs"] = selected_inputs
                self.mining_service.writer.append_evaluation(snapshot, session=session)
                registrations = {
                    "pillar_1": "pillar_1_team_structure_side",
                    "pillar_1_totals": "pillar_1_team_structure_totals",
                    **{
                        f"pillar_{n}": definition[0]
                        for n, definition in PILLAR_DEFINITIONS.items()
                    },
                }
                for key, result in results.items():
                    run = self.mining_service.persist(
                        registrations[key],
                        identity,
                        result,
                        evaluation_id=evaluation_id,
                        session=session,
                    )
                    if key in {"pillar_1", "pillar_1_totals"}:
                        results[key] = {
                            **run.output_payload,
                            "diagnostics": run.diagnostics,
                        }
                self.mining_service.writer.require_complete_evaluation(
                    evaluation_id, session=session
                )
        # No persistence exception is converted into a pillar signal or swallowed.
        if self.debug_mode:
            _save_pillar_debug_snapshots(
                event_context=identity,
                odds_trajectory_context=odds,
                streak_analysis=event_context.streak_analysis,
                competition_slug=event_context.competition.slug,
            )
        logger.info(
            "Immutable pillar evaluation committed evaluation_id=%s event_id=%s as_of=%s",
            evaluation_id,
            identity.event_id,
            evaluation_as_of.isoformat(),
        )
        log_pillar_results(
            logger,
            evaluation_id=evaluation_id,
            event_id=identity.event_id,
            evaluation_as_of=evaluation_as_of,
            participants=identity.participants_label,
            results=results,
        )
        return {
            "evaluation_id": evaluation_id,
            "evaluation_as_of": evaluation_as_of,
            "event_id": identity.event_id,
            "participants": identity.participants_label,
            **results,
        }


def evaluate_and_calculate_pillars_batch(
    events_for_pillars,
    *,
    debug_mode=False,
    enabled_pillars=None,
    trajectories_by_event_id=None,
):
    """Complete all eligible evaluations; propagate infrastructure failures to the coordinator."""
    events = [
        ctx
        for ctx in events_for_pillars
        if _is_pillar_competition_in_scope(ctx.competition.competition_id)
    ]
    if not events:
        return
    processor = EventPillarProcessor(
        debug_mode=debug_mode,
        enabled_pillars=(
            Config.PILLAR_PIPELINE_ENABLED_PILLARS
            if enabled_pillars is None
            else enabled_pillars
        ),
    )

    def execute(context):
        points = (
            trajectories_by_event_id.pop(context.event_id, None)
            if trajectories_by_event_id is not None
            else None
        )
        processor.process_event(context, trajectory_points=points)

    failures = []
    workers = min(Config.PILLAR_PIPELINE_WORKERS, len(events))

    def failed(event_id, exc):
        logger.exception("Pillar evaluation failed event_id=%s", event_id)
        failures.append((event_id, exc))

    if workers == 1:
        for context in events:
            try:
                execute(context)
            except Exception as exc:
                failed(context.event_id, exc)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(execute, context): context.event_id
                for context in events
            }
            for future in as_completed(futures):
                event_id = futures.pop(future)
                try:
                    future.result()
                except Exception as exc:
                    failed(event_id, exc)
    if failures:
        raise RuntimeError(
            f"{len(failures)} pillar evaluations could not be persisted: "
            f"{[event_id for event_id, _ in failures]}"
        ) from failures[0][1]
