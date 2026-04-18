"""In-game checks for the pre-start job."""

from __future__ import annotations

from modules.alerts.basketball_4q import basketball_4q_monitor


def run_in_game_checks() -> None:
    basketball_4q_monitor.check_nba_4th_quarter()
