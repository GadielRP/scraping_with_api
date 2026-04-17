"""Basketball 4Q alert package."""

from importlib import import_module

from .predictor import Basketball4QPredictor, map_season_stage_to_db_round, predictor_4q

__all__ = [
    "Basketball4QPredictor",
    "Basketball4QMonitor",
    "basketball_4q_monitor",
    "map_season_stage_to_db_round",
    "predictor_4q",
]

def __getattr__(name):
    if name in {"Basketball4QMonitor", "basketball_4q_monitor"}:
        module = import_module(".run_basketball_4q", __name__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
