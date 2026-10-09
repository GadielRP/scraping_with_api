"""Pillar pipeline for the pre-start job.

Runs pillar/module calculations for events at key moments.
Parallel to, but independent of, the existing alert pipeline.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, is_dataclass
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any, Optional

from collections import Counter
from infrastructure.settings import Config
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    Pillar5PriceMemoryRepository,
)
from infrastructure.persistence.repositories.pillar_mining_repository import (
    PillarMiningRepository,
)
from modules.competition.tracked_competitions import is_tracked_competition
from modules.pillars.context import (
    EventContext,
    EventIdentity,
    build_event_context,
    summarize_number_of_teams_from_streak_analysis,
)
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.market_evaluation import prepare_event_markets
from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult
from modules.pillars.trajectory_selection import (
    TargetMinuteSelection,
    select_target_minute,
)
from modules.pillars.competition_metadata_resolver import (
    apply_competition_metadata_resolution,
    resolve_competition_metadata,
)
from modules.pillars.streak_analysis_resolver import (
    resolve_matchup_streak_analysis,
)
from modules.pillars.pillar_1_team_structure.run_pillar_1_team_structure import (
    calculate_pillar_1_team_structure,
)
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
)
from modules.pillars.pillar_1_team_structure.totals import (
    P1TotalsOutput,
)

from .providers.debug_paths import sport_folder_name

logger = logging.getLogger(__name__)

CANONICAL_SIGNAL_FLOW_ID = "pre_start_signal_profile"


def _serialize_p1_totals_output(output: P1TotalsOutput) -> dict:
    return asdict(output)


def _resolve_pillar_competition_id(event_context: Any):
    """Resolve the canonical competition ID used by the pillar scope policy."""
    if hasattr(event_context, "competition"):
        competition = getattr(event_context, "competition", None)
        if competition is not None:
            return getattr(competition, "competition_id", None)

    if isinstance(event_context, dict):
        ctx = event_context.get("event_context")
        if ctx and hasattr(ctx, "competition"):
            return getattr(ctx.competition, "competition_id", None)
        competition_id = event_context.get("competition_id")
        if competition_id is not None:
            return competition_id
        event_data = event_context.get("event_data")
        if isinstance(event_data, dict):
            return event_data.get("competition_id")

    return None


def _is_pillar_competition_in_scope(competition_id) -> bool:
    """Return whether the competition is inside the configured pillar scope."""
    return (
        not Config.FILTER_PIPELINES_BY_TRACKED_COMPETITIONS
        or is_tracked_competition(competition_id)
    )


def _log_market_result(participants, result):
    logger.info(
        "Market result pillar=%s participants=%s status=%s target=%s FT=%s reason=%s computed=%s blocked=%s",
        result.get("pillar_id"),
        participants,
        result.get("status"),
        result.get("target_minute"),
        result.get("selected_full_time_period"),
        result.get("selection", {}).get("reason"),
        result.get("evidence", {}).get("computed_signals"),
        dict(
            Counter(
                signal.get("reason")
                for signal in result.get("signals", [])
                if signal.get("status") in ("BLOCKED", "ERROR")
            )
        ),
    )


def _clear_unpersisted_samples(result):
    for profile in result.get("analysis", {}).values():
        profile["sample_id"] = None
    for signal in result.get("signals", []):
        signal["evidence"].pop("sample_id", None)
    for diagnostic in result.get("diagnostics", []):
        if "audit_persisted" in diagnostic:
            diagnostic["audit_persisted"] = False


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
                evidence={"error_class": type(exc).__name__},
            ),
        ),
        evaluation.coverage(pillar),
        contracts=evaluation.contracts(pillar),
    ).to_dict()


def _to_json_safe(value: Any):
    try:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value

        if isinstance(value, Decimal):
            return str(value)

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        to_dict = getattr(value, "to_dict", None)
        if callable(to_dict):
            try:
                return _to_json_safe(to_dict())
            except Exception:
                pass

        if is_dataclass(value) and not isinstance(value, type):
            return _to_json_safe(asdict(value))

        if isinstance(value, dict):
            return {str(key): _to_json_safe(item) for key, item in value.items()}

        if isinstance(value, (list, tuple)):
            return [_to_json_safe(item) for item in value]

        if isinstance(value, set):
            try:
                return [_to_json_safe(item) for item in sorted(value, key=lambda item: str(item))]
            except Exception:
                return [_to_json_safe(item) for item in list(value)]

        if hasattr(value, "__dict__"):
            return {
                str(key): _to_json_safe(item)
                for key, item in vars(value).items()
                if not str(key).startswith("_")
            }

        return str(value)
    except Exception:
        return str(value)


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
        json.dump(_to_json_safe(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)


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
        competition_folder = re.sub(
            r"[^a-z0-9]+", "_", (competition_slug or "").lower()
        ).strip("_") or "unknown_competition"
        debug_dir = (
            Path("debug") / "pillar_pipeline_objects"
            / sport_folder_name(getattr(event_context, "sport", None))
            / competition_folder / f"{event_id}_{safe_participants}"
        )
        debug_dir.mkdir(parents=True, exist_ok=True)

        _write_debug_json(debug_dir / f"{event_id}_event_context.json", event_context)
        _write_debug_json(debug_dir / f"{event_id}_odds_trajectory_context.json", odds_trajectory_context)

        resolved_streak = (
            streak_analysis
            if streak_analysis is not None
            else getattr(event_context, "streak_analysis", None)
        )
        if resolved_streak is not None:
            _write_debug_json(debug_dir / f"{event_id}_streak_analysis.json", resolved_streak)

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


class EventPillarProcessor:
    """Processes a single event through the pillar/module architecture."""

    def __init__(
        self,
        event_repo,
        debug_mode: bool = False,
        enabled_pillars: Optional[dict[str, bool]] = None,
        mining_service: PillarMiningService | None = None,
        evaluation_as_of: datetime | None = None,
    ):
        self.event_repo = event_repo
        self.debug_mode = debug_mode
        self.enabled_pillars = enabled_pillars or {}
        self.mining_service = mining_service
        self.evaluation_as_of = evaluation_as_of

    def _is_pillar_enabled(self, pillar_key: str) -> bool:
        if not self.enabled_pillars:
            return True
        return bool(self.enabled_pillars.get(pillar_key, True))

    def _persist_mining_result(
        self,
        pillar_id: str,
        event_context: EventIdentity | EventContext,
        result: dict[str, Any],
    ) -> None:
        if self.mining_service is None:
            return

        target_minute = result.get("target_minute", result.get("P1_TARGET_MINUTE"))
        engine_version = result.get("engine_version") or result.get("raw", {}).get(
            "engine_version"
        )

        started = perf_counter()
        try:
            persisted = self.mining_service.persist(
                pillar_id,
                event_context,
                result,
            )
            if persisted:
                logger.info(
                    "Pillar mining run persisted pillar_id=%s event_id=%s status=%s target_minute=%s engine_version=%s duration_ms=%.1f",
                    pillar_id,
                    event_context.event_id,
                    result.get("status"),
                    target_minute,
                    engine_version,
                    (perf_counter() - started) * 1000,
                )
        except Exception:
            # Mining is an analytical side effect. Its failure must be visible,
            # but must not prevent P4/P5 or the rest of the event from running.
            logger.exception(
                "Pillar mining persistence failed pillar_id=%s event_id=%s status=%s target_minute=%s engine_version=%s",
                pillar_id,
                event_context.event_id,
                result.get("status"),
                target_minute,
                engine_version,
            )

    def process_event(
        self,
        event_context: EventContext,
        trajectory_points: Optional[list[Any]] = None,
    ) -> Optional[dict]:
        """Calculate pillar modules for a single event context.

        Returns a dictionary with pillar results or ``None`` on failure.
        """
        # The pillar pipeline has one canonical input contract.  Do not
        # silently reconstruct it from the legacy event payload here: doing
        # so would bring back the duplicate objects this context refactor is
        # intended to remove.
        if event_context is not None and not isinstance(event_context, EventContext):
            logger.warning(
                "Pillar pipeline received a non-canonical event context (%s); skipping event",
                type(event_context).__name__,
            )
            return None

        if event_context is None or not event_context.success:
            event_id = event_context.event_id if event_context is not None else "?"
            logger.warning(f"☢️ Pillar pipeline: success is false for event {event_id}, skipping pillar calculation")
            return None

        event_id = event_context.event_id

        competition_id = event_context.competition.competition_id
        if not _is_pillar_competition_in_scope(competition_id):
            logger.info(
                "🚫 Pillar pipeline: competition_id=%s is outside the configured scope for event %s; skipping pillar calculation",
                competition_id,
                event_id,
            )
            return None

        logger.info(f"🏛️ Started pillars processing for event {event_id}")
        round_value = event_context.round
        if round_value != "regular_season":
            logger.info(
                "🚫 Pillar pipeline: round is %s for event_id %s, skipping pillar calculation",
                round_value,
                event_id,
            )
            return None

        evaluation_minute = event_context.minutes_until_start
        odds_trajectory = (
            trajectory_points
            if trajectory_points is not None
            else event_context.odds_trajectory
        )
        odds_trajectory_context = build_odds_trajectory_context(
            odds_trajectory,
            evaluation_minute=evaluation_minute,
            event_starts_at=event_context.starts_at,
            evaluation_as_of=self.evaluation_as_of,
        )
        target_selection = select_target_minute(
            odds_trajectory_context,
            flow_id=CANONICAL_SIGNAL_FLOW_ID,
            expected_event_id=event_context.event_id,
            allowed_target_minutes=Config.PRE_START_ODDS_MOMENTS,
            evaluation_minute=evaluation_minute,
        )
        event_identity = event_context.to_identity()
        market_evaluation = prepare_event_markets(
            odds_trajectory_context, target_selection, event_identity
        )

        logger.info(
            "Pillar odds trajectory context for event %s: available=%s market_groups=%s present_minutes=%s missing_minutes=%s",
            event_id,
            odds_trajectory_context.available,
            len(odds_trajectory_context.markets),
            odds_trajectory_context.target_minutes_present,
            odds_trajectory_context.missing_target_minutes,
        )
        logger.info(
            "Canonical structural target selected for event %s: target_minute=%s reason=%s diagnostics=%s",
            event_id,
            target_selection.target_minute,
            target_selection.reason,
            target_selection.diagnostics,
        )
        if self.debug_mode and odds_trajectory_context.available:
            trajectory_keys = []
            for market_group, periods in odds_trajectory_context.markets.items():
                for market_period in periods:
                    trajectory_keys.append(f"{market_group}/{market_period}")
            logger.info(
                "P4 pre-check trajectory sample for event %s: %s",
                event_id,
                trajectory_keys[:10],
            )

        if self.debug_mode:
            _save_pillar_debug_snapshots(
                event_context=event_identity,
                odds_trajectory_context=odds_trajectory_context,
                competition_slug=event_context.competition.slug,
            )

        p2_result = None
        if self._is_pillar_enabled("pillar_2"):
            try:
                p2_result = calculate_pillar_2(
                    event_context=event_identity,
                    odds_trajectory_context=odds_trajectory_context,
                    target_selection=target_selection,
                    debug_mode=self.debug_mode,
                    market_evaluation=market_evaluation,
                )
            except Exception as exc:
                logger.exception(
                    "Error calculating P2 for event %s (%s): %s",
                    event_id,
                    event_context.participants_label,
                    exc,
                )
                p2_result = _build_market_error_result(
                    event_identity, 2, market_evaluation, exc
                )

            _log_market_result(
                event_context.participants_label,
                p2_result,
            )
            self._persist_mining_result(
                "pillar_2_side_market",
                event_identity,
                p2_result,
            )
        else:
            p2_result = EvaluationResult(
                event_identity.event_id,
                "pillar_2_side_market",
                P2_ENGINE_VERSION,
                target_selection.target_minute,
                market_evaluation.selection,
                (),
                market_evaluation.coverage(2),
                skipped=True,
            ).to_dict()
            logger.info(
                "Pillar 2 (Side Market Signal Engine) skipped for %s (disabled by toggle)",
                event_context.participants_label,
            )

        p3_result = None
        if self._is_pillar_enabled("pillar_3"):
            try:
                p3_result = calculate_pillar_3(
                    event_context=event_identity,
                    odds_trajectory_context=odds_trajectory_context,
                    target_selection=target_selection,
                    debug_mode=self.debug_mode,
                    market_evaluation=market_evaluation,
                )
            except Exception as exc:
                logger.exception(
                    "Error calculating P3 for event %s (%s): %s",
                    event_id,
                    event_context.participants_label,
                    exc,
                )
                p3_result = _build_market_error_result(
                    event_identity, 3, market_evaluation, exc
                )

            _log_market_result(
                event_context.participants_label,
                p3_result,
            )
            self._persist_mining_result(
                "pillar_3_totals_market_context",
                event_identity,
                p3_result,
            )
        else:
            p3_result = EvaluationResult(
                event_identity.event_id,
                "pillar_3_totals_market_context",
                P3_ENGINE_VERSION,
                target_selection.target_minute,
                market_evaluation.selection,
                (),
                market_evaluation.coverage(3),
                skipped=True,
            ).to_dict()
            logger.info(
                "Pillar 3 (Over/Under Market Signal Engine) skipped for %s (disabled by toggle)",
                event_context.participants_label,
            )

        p4_result = None
        if self._is_pillar_enabled("pillar_4"):
            try:
                # calculate pillar 4 (p4)
                p4_result = calculate_pillar_4(
                    event_context=event_identity,
                    odds_trajectory_context=odds_trajectory_context,
                    target_selection=target_selection,
                    debug_mode=self.debug_mode,
                    market_evaluation=market_evaluation,
                )
            except Exception as exc:
                logger.exception(
                    "Error calculating P4 for event %s (%s): %s",
                    event_id,
                    event_context.participants_label,
                    exc,
                )
                p4_result = _build_market_error_result(
                    event_identity, 4, market_evaluation, exc
                )

            _log_market_result(event_context.participants_label, p4_result)
            self._persist_mining_result(
                "pillar_4_temporal_market_drift",
                event_identity,
                p4_result,
            )
        else:
            p4_result = EvaluationResult(
                event_identity.event_id,
                "pillar_4_temporal_market_drift",
                P4_ENGINE_VERSION,
                target_selection.target_minute,
                market_evaluation.selection,
                (),
                market_evaluation.coverage(4),
                skipped=True,
            ).to_dict()
            logger.info(
                "Pillar 4 (Temporal Market Drift) skipped for %s (disabled by toggle)",
                event_context.participants_label,
            )

        p5_result = None
        if self._is_pillar_enabled("pillar_5"):
            try:
                if self.mining_service is not None and self.mining_service.enabled:
                    with db_manager.get_session() as session:
                        p5_result = calculate_pillar_5(
                            event_identity,
                            odds_trajectory_context,
                            target_selection=target_selection,
                            debug_mode=self.debug_mode,
                            market_evaluation=market_evaluation,
                            memory_repository=Pillar5PriceMemoryRepository(
                                session=session, capture=True
                            ),
                        )
                        if not self.mining_service.persist(
                            "pillar_5", event_identity, p5_result, session=session
                        ):
                            session.rollback()
                            _clear_unpersisted_samples(p5_result)
                else:
                    p5_result = calculate_pillar_5(
                        event_identity,
                        odds_trajectory_context,
                        target_selection=target_selection,
                        debug_mode=self.debug_mode,
                        market_evaluation=market_evaluation,
                        memory_repository=Pillar5PriceMemoryRepository(
                            db_manager.SessionLocal
                        ),
                    )
            except Exception as exc:
                logger.exception(
                    "Error calculating P5 for event %s (%s): %s",
                    event_id,
                    event_context.participants_label,
                    exc,
                )
                if p5_result is None:
                    p5_result = _build_market_error_result(
                        event_identity, 5, market_evaluation, exc
                    )
                else:
                    _clear_unpersisted_samples(p5_result)
                    p5_result["diagnostics"].append(
                        {
                            "reason": "PERSISTENCE_ERROR",
                            "error_class": type(exc).__name__,
                        }
                    )
                    p5_result["signals"].append(
                        SignalResult(
                            "persistence",
                            "ERROR",
                            reason="PERSISTENCE_ERROR",
                            evidence={"error_class": type(exc).__name__},
                        ).to_dict()
                    )
                    computed = any(
                        signal["status"] == "COMPUTED"
                        for signal in p5_result["signals"]
                    )
                    p5_result["status"] = "ACTIVE" if computed else "ERROR"
                    p5_result["execution_status"] = (
                        "COMPLETED_WITH_ERRORS" if computed else "FAILED"
                    )

            _log_market_result(event_context.participants_label, p5_result)
        else:
            p5_result = EvaluationResult(
                event_identity.event_id,
                "pillar_5",
                P5_ENGINE_VERSION,
                target_selection.target_minute,
                market_evaluation.selection,
                (),
                market_evaluation.coverage(5),
                skipped=True,
            ).to_dict()
            logger.info(
                "Pillar 5 (Exact Price Memory) skipped for %s (disabled by toggle)",
                event_context.participants_label,
            )

        # Release raw odds before Pillar 1; contexts must not retain a trajectory.
        market_evaluation = None
        odds_trajectory = None
        odds_trajectory_context = None
        trajectory_points = None
        if event_context.odds_trajectory:
            event_context.odds_trajectory = []

        logger.info(
            "Pillar pipeline metadata check for event %s: competition_id=%s source_unique_tournament_id=%s season_id=%s number_of_teams=%s total_regular_season_games=%s standings_grouping=%s league_config_source=%s",
            event_id,
            getattr(event_context.competition, "competition_id", None),
            getattr(event_context.competition, "source_unique_tournament_id", None),
            getattr(event_context, "season_id", None),
            getattr(event_context.competition, "number_of_teams", None),
            getattr(event_context.competition, "total_regular_season_games", None),
            getattr(event_context.competition, "standings_grouping", None),
            getattr(event_context.competition, "league_config_source", None),
        )

        missing_fields = []
        if getattr(event_context.competition, "number_of_teams", None) is None:
            missing_fields.append("number_of_teams")
        if getattr(event_context.competition, "total_regular_season_games", None) is None:
            missing_fields.append("total_regular_season_games")
        if getattr(event_context.competition, "standings_grouping", None) is None:
            missing_fields.append("standings_grouping")

        if missing_fields and getattr(event_context, "competition_metadata_resolved", False):
            logger.info(
                "Pillar pipeline metadata enrichment skipped for event %s; resolver already ran during payload build (missing fields: %s)",
                event_id,
                ", ".join(missing_fields),
            )
        elif missing_fields:
            logger.info(
                "Pillar pipeline metadata enrichment needed for event %s; missing fields: %s; calling competition metadata resolver",
                event_id,
                ", ".join(missing_fields),
            )
            resolution = resolve_competition_metadata(event_context)
            apply_competition_metadata_resolution(event_context, resolution)
            logger.info(
                "Pillar pipeline metadata enrichment result for event %s: source=%s standings_called=%s should_persist=%s number_of_teams=%s total_regular_season_games=%s standings_grouping=%s",
                event_id,
                resolution.league_config_source,
                resolution.standings_called,
                resolution.should_persist,
                resolution.number_of_teams,
                resolution.total_regular_season_games,
                resolution.standings_grouping,
            )

        season_id = event_context.season_id
        participants = event_context.participants_label

        # If Pillar 1 is disabled, return results calculated so far
        if not self._is_pillar_enabled("pillar_1"):
            logger.info(
                "Pillar 1 (Team Structure) skipped for %s (disabled by toggle)",
                participants,
            )
            return {
                "event_id": event_id,
                "participants": participants,
                "pillar_1": None,
                "pillar_1_totals": None,
                "pillar_2": p2_result,
                "pillar_3": p3_result,
                "pillar_4": p4_result,
                "pillar_5": p5_result,
            }

        # --- Resolve streak analysis (shared with alert pipeline) ---
        streak_analysis, _should_send = resolve_matchup_streak_analysis(
            event_context=event_context,
            debug_mode=self.debug_mode,
        )

        if streak_analysis and self.debug_mode:
            _save_pillar_debug_snapshots(
                event_context=event_identity,
                odds_trajectory_context=None,
                streak_analysis=streak_analysis,
                competition_slug=event_context.competition.slug,
            )

        if streak_analysis is None:
            logger.info(
                "Pillar pipeline: no streak_analysis for event %s (%s), returning P2, P3, P4 and P5 only",
                event_id,
                participants,
            )
            return {
                "event_id": event_id,
                "participants": participants,
                "pillar_1": None,
                "pillar_1_totals": None,
                "pillar_2": p2_result,
                "pillar_3": p3_result,
                "pillar_4": p4_result,
                "pillar_5": p5_result,
            }

        number_of_teams_summary = summarize_number_of_teams_from_streak_analysis(
            streak_analysis,
            event_context,
        )
        inferred_number_of_teams = number_of_teams_summary.inferred_number_of_teams
        unique_team_count = number_of_teams_summary.unique_team_count
        inferred_number_of_teams_used = False
        competition_id = event_context.competition.competition_id

        logger.info(
            "Pillar context for %s: context_status=%s, event_context_present=%s, competition_id=%s, competition_number_of_teams=%s, number_of_teams_source=%s, inferred_number_of_teams=%s, unique_team_count=%s, inferred_used=%s, total_regular_season_games=%s",
            participants,
            event_context.context_status,
            True,
            competition_id,
            event_context.competition.number_of_teams,
            event_context.competition.number_of_teams_source,
            inferred_number_of_teams,
            unique_team_count,
            inferred_number_of_teams_used,
            event_context.competition.total_regular_season_games,
        )

        # --- Calculate Pillar 1 (Orchestrated) ---
        try:
            p1_output = calculate_pillar_1_team_structure(
                event_context=event_context,
                debug_mode=self.debug_mode,
            )
            p1_result = p1_output["side"]
            p1_totals_result = p1_output["totals"]
        except Exception as exc:
            logger.error(
                "Error calculating P1 for event %s (%s): %s",
                event_id,
                participants,
                exc,
            )
            return {
                "event_id": event_id,
                "participants": participants,
                "pillar_1": None,
                "pillar_1_totals": None,
                "pillar_2": p2_result,
                "pillar_3": p3_result,
                "pillar_4": p4_result,
                "pillar_5": p5_result,
            }
        # Log the M1 result.
        m1 = p1_result.get("modules", [{}])[0] if p1_result.get("modules") else {}
        logger.info(
            "P1/M1 Base Strength calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m1.get("value", 0),
            m1.get("bias", "N/A"),
            m1.get("strength", "N/A"),
        )

        for comp in m1.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        # Log the M2 result.
        modules = p1_result.get("modules", [])
        m2 = modules[1] if len(modules) > 1 else {}
        logger.info(
            "P1/M2 Offensive Profile Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m2.get("value", 0),
            m2.get("bias", "N/A"),
            m2.get("strength", "N/A"),
        )

        for comp in m2.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        # Log the M3 result.
        m3 = modules[2] if len(modules) > 2 else {}
        logger.info(
            "P1/M3 Direct Matchup Profile calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m3.get("value", 0),
            m3.get("bias", "N/A"),
            m3.get("strength", "N/A"),
        )

        for comp in m3.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m4 = modules[3] if len(modules) > 3 else {}
        logger.info(
            "P1/M4 Quality-Adjusted Immediate State Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m4.get("value", 0),
            m4.get("bias", "N/A"),
            m4.get("strength", "N/A"),
        )

        for comp in m4.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m5 = modules[4] if len(modules) > 4 else {}
        logger.info(
            "P1/M5 Contextual Competitive Cost Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m5.get("value", 0),
            m5.get("bias", "N/A"),
            m5.get("strength", "N/A"),
        )

        for comp in m5.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m6 = modules[5] if len(modules) > 5 else {}
        logger.info(
            "P1/M6 Structural Drift Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m6.get("value", 0),
            m6.get("bias", "N/A"),
            m6.get("strength", "N/A"),
        )

        for comp in m6.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m7 = modules[6] if len(modules) > 6 else {}
        logger.info(
            "P1/M7 Opponent Expectation Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m7.get("value", 0),
            m7.get("bias", "N/A"),
            m7.get("strength", "N/A"),
        )

        for comp in m7.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        p1_final_raw = p1_result.get("raw", {}).get("final", {})
        logger.info(
            "P1/SIDE calculated for %s: value=%.3f, bias=%s, strength=%s, context_state=%s",
            participants,
            p1_result.get("value", 0),
            p1_result.get("raw", {}).get("final", {}).get("p1_final_bias", p1_result.get("bias", "N/A")),
            p1_result.get("raw", {}).get("final", {}).get("p1_final_strength", p1_result.get("strength", "N/A")),
            p1_final_raw.get("p1_context_state", "N/A"),
        )

        if p1_totals_result is not None:
            logger.info(
                "P1/P1_TOTALS Totals calculated for %s: directional_score=%.3f, direction=%s, strength=%s, variance_state=%s, status=%s",
                participants,
                p1_totals_result.P1_TOTALS_DIRECTIONAL_SCORE,
                p1_totals_result.P1_TOTALS_DIRECTION,
                p1_totals_result.P1_TOTALS_STRENGTH,
                p1_totals_result.P1_TOTALS_VARIANCE_STATE,
                p1_totals_result.status,
            )
            for layer in p1_totals_result.active_layers:
                logger.info(
                    "   - active layer: %s raw_signal=%s final_signal=%s weighted=%s",
                    layer.layer,
                    layer.raw_signal,
                    layer.final_signal,
                    layer.weighted_signal,
                )
            for layer in p1_totals_result.ignored_layers:
                logger.info(
                    "   - ignored layer: %s raw_signal=%s final_signal=%s reason=%s",
                    layer.layer,
                    layer.raw_signal,
                    layer.final_signal,
                    layer.ignored_reason,
                )
        else:
            logger.info(
                "P1/P1_TOTALS Totals skipped for %s: unavailable",
                participants,
            )

        p1_result.setdefault("raw", {}).update(
            {
                "event_context_present": True,
                "context_status": event_context.context_status,
                "competition_id": competition_id,
                "competition_display_name": event_context.competition.display_name,
                "competition_number_of_teams": event_context.competition.number_of_teams,
                "competition_number_of_teams_source": event_context.competition.number_of_teams_source,
                "total_regular_season_games": event_context.competition.total_regular_season_games,
                "standings_grouping": event_context.competition.standings_grouping,
                "league_config_source": event_context.competition.league_config_source,
                "inferred_number_of_teams": inferred_number_of_teams,
                "inferred_number_of_teams_from_streak_analysis": inferred_number_of_teams,
                "inferred_number_of_teams_source": "streak_analysis_team_results",
                "inferred_number_of_teams_used": inferred_number_of_teams_used,
                "unique_team_count": unique_team_count,
                "persisted_number_of_teams": False,
            }
        )

        p1_totals_serialized = (
            _serialize_p1_totals_output(p1_totals_result)
            if p1_totals_result is not None
            else None
        )
        self._persist_mining_result(
            "pillar_1_team_structure_side",
            event_identity,
            p1_result,
        )
        if p1_totals_serialized is not None:
            self._persist_mining_result(
                "pillar_1_team_structure_totals",
                event_identity,
                p1_totals_serialized,
            )

        return {
            "event_id": event_id,
            "participants": participants,
            "pillar_1": p1_result,
            "pillar_1_totals": p1_totals_serialized,
            "pillar_2": p2_result,
            "pillar_3": p3_result,
            "pillar_4": p4_result,
            "pillar_5": p5_result,
        }


def _registered_mining_adapters() -> dict[
    str,
    P1SideMiningAdapter
    | P1TotalsMiningAdapter
    | P2MiningAdapter
    | P3MiningAdapter
    | P4MiningAdapter
    | P5MiningAdapter,
]:
    """Composition root for structural signal-profile mining writers."""
    return {
        "pillar_1_team_structure_side": P1SideMiningAdapter(),
        "pillar_1_team_structure_totals": P1TotalsMiningAdapter(),
        "pillar_2_side_market": P2MiningAdapter(),
        "pillar_3_totals_market_context": P3MiningAdapter(),
        "pillar_4_temporal_market_drift": P4MiningAdapter(),
        "pillar_5": P5MiningAdapter(),
    }


def evaluate_and_calculate_pillars_batch(
    events_for_pillars: list[EventContext],
    event_repo=None,
    key_moments: Optional[list] = None,
    *,
    debug_mode: bool = False,
    enabled_pillars: Optional[dict[str, bool]] = None,
    trajectories_by_event_id: Optional[dict[int, list[Any]]] = None,
    evaluation_as_of: datetime | None = None,
    **_legacy_kwargs: Any,
) -> None:
    """Entry point to evaluate and calculate pillar modules for a batch of events."""
    # Older callers pass (events, key_moments, event_repo).
    if isinstance(event_repo, (list, tuple)) and key_moments is not None:
        event_repo, key_moments = key_moments, event_repo
    if not events_for_pillars:
        return

    allowed_events = [
        event_context
        for event_context in events_for_pillars
        if _is_pillar_competition_in_scope(
            _resolve_pillar_competition_id(event_context)
        )
    ]
    skipped_count = len(events_for_pillars) - len(allowed_events)
    if skipped_count:
        logger.info(
            "🚫 Pillar pipeline competition filter skipped %s/%s events",
            skipped_count,
            len(events_for_pillars),
        )
    if not allowed_events:
        return

    logger.info(
        "Evaluating pillar modules for %d events...",
        len(allowed_events),
    )

    mining_service = PillarMiningService(
        PillarMiningRepository(),
        adapters=_registered_mining_adapters(),
        enabled=Config.PILLAR_MINING_ENABLED,
        status_mode=Config.PILLAR_MINING_STATUS_MODE,
    )
    processor = EventPillarProcessor(
        event_repo=event_repo,
        debug_mode=debug_mode,
        enabled_pillars=(
            Config.PILLAR_PIPELINE_ENABLED_PILLARS
            if enabled_pillars is None
            else enabled_pillars
        ),
        mining_service=mining_service,
        evaluation_as_of=evaluation_as_of,
    )

    def _resolve_batch_event_id(ctx: Any) -> Optional[int]:
        raw_id = getattr(ctx, "event_id", None)
        if raw_id is None and isinstance(ctx, dict):
            raw_id = ctx.get("event_id") or ctx.get("id")
        if raw_id is not None:
            try:
                return int(raw_id)
            except (ValueError, TypeError):
                return None
        return None

    def _execute_process_event(
        proc: Any,
        ctx: Any,
    ) -> Any:
        if trajectories_by_event_id is not None:
            event_id = _resolve_batch_event_id(ctx)
            if event_id is not None and event_id in trajectories_by_event_id:
                try:
                    # Consume the entry as the worker starts so the batch
                    # cannot retain this event's raw odds after it completes.
                    return proc.process_event(
                        ctx,
                        trajectory_points=trajectories_by_event_id.pop(event_id),
                    )
                except TypeError:
                    return proc.process_event(ctx)
        return proc.process_event(ctx)

    max_workers = min(Config.PILLAR_PIPELINE_WORKERS, len(allowed_events))
    logger.info(
        "Pillar pipeline concurrency events=%s workers=%s",
        len(allowed_events),
        max_workers,
    )
    if max_workers == 1:
        for event_context in allowed_events:
            try:
                _execute_process_event(
                    processor,
                    event_context,
                )
            except Exception as exc:
                logger.error("Critical failure in pillar processing: %s", exc)
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _execute_process_event,
                processor,
                event_context,
            )
            for event_context in allowed_events
        ]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                logger.error("Critical failure in pillar processing thread: %s", exc)
