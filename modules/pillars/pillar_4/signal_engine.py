"""Assemble temporal series; global state belongs to EvaluationResult."""

from .semantic_metrics import build_checkpoint_semantic_series
from .trajectory_engine import build_trajectory_features

ENGINE_VERSION = "p4-signal-profile-v3"


def build_p4_series(extraction, *, debug_mode=False):
    if not extraction.usable:
        raise ValueError("P4 extraction is not usable")
    return (
        *build_trajectory_features(
            extraction.adaptive_series,
            apply_relations=False,
            debug_mode=debug_mode,
        ),
        *build_trajectory_features(
            (*extraction.checkpoint_series,
             *build_checkpoint_semantic_series(
                 extraction.checkpoint_series, debug_mode=debug_mode
             )),
            apply_relations=True,
            debug_mode=debug_mode,
        ),
    )
