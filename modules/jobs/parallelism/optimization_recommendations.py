"""Optimization recommendations and configuration helpers."""

from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

OPTIMIZATION_STRATEGIES = {
    "conservative": {
        "description": "Safe and stable",
        "max_workers": 5,
        "expected_job_b_time": 69,
        "risk": "Low",
    },
    "moderate": {
        "description": "Balanced performance and stability",
        "max_workers": 7,
        "expected_job_b_time": 55,
        "risk": "Low-Medium",
    },
    "aggressive": {
        "description": "Maximum speed (may trigger rate limits)",
        "max_workers": 10,
        "expected_job_b_time": 45,
        "risk": "Medium-High",
    },
}


def should_skip_source(source_name: str, failure_rate: float = 0.9) -> bool:
    """Return True when a source is known to be too wasteful to keep enabled."""
    high_failure_sources = {
        "team_streaks": 0.95,
    }

    if source_name in high_failure_sources and high_failure_sources[source_name] >= failure_rate:
        logger.info(
            "Source '%s' has %.0f%% failure rate - consider disabling",
            source_name,
            high_failure_sources[source_name] * 100,
        )
        return True

    return False


def analyze_discovery_performance(
    high_value_time: float,
    team_streaks_time: float,
    h2h_time: float,
    winning_odds_time: float,
) -> Dict[str, Any]:
    """Analyze discovery job performance and provide recommendations."""
    total_time = high_value_time + team_streaks_time + h2h_time + winning_odds_time
    sources = {
        "high_value_streaks": high_value_time,
        "team_streaks": team_streaks_time,
        "h2h": h2h_time,
        "winning_odds": winning_odds_time,
    }
    slowest = max(sources.items(), key=lambda item: item[1])

    recommendations = []
    if slowest[1] > 30:
        recommendations.append(f"Slowest source: {slowest[0]} ({slowest[1]}s) - consider optimization")
    if team_streaks_time > 20 and total_time > 0 and sources["team_streaks"] / total_time > 0.3:
        recommendations.append("Team streaks takes >30% of time with 95% failure - consider disabling")

    return {
        "total_time": total_time,
        "slowest_source": slowest[0],
        "slowest_time": slowest[1],
        "recommendations": recommendations,
    }


def get_optimization_config() -> Dict[str, Any]:
    """Return the default optimization settings used by discovery jobs."""
    return {
        "max_workers": 5,
        "aggressive_max_workers": 10,
        "rate_limit_seconds": 1.0,
        "aggressive_rate_limit": 0.5,
        "skip_team_streaks": False,
        "batch_deletion_enabled": True,
        "parallel_odds_check_enabled": True,
    }


def calculate_expected_speedup(current_time: float, workers_from: int, workers_to: int) -> float:
    """Estimate the runtime after changing the worker count."""
    speedup_factor = workers_to / workers_from
    return current_time / speedup_factor


def get_recommended_strategy(current_time: float) -> str:
    """Return the strategy name that best matches the current runtime."""
    if current_time > 120:
        return "aggressive"
    if current_time > 90:
        return "moderate"
    return "conservative"

