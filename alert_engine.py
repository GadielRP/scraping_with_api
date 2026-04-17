"""Backward-compatible shim for the Process 1 alert engine."""

from modules.alerts.dual_process.process_1 import AlertEngine, alert_engine

__all__ = ["AlertEngine", "alert_engine"]
