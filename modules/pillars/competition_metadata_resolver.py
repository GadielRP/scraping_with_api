"""Resolve operational competition metadata for EventContext."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from infrastructure.settings import Config
from modules.competition.league_config import get_league_config
from modules.pillars.context import EventContext
from modules.sofascore import api_client
from modules.sofascore.standings import parse_competition_metadata_from_standings

logger = logging.getLogger(__name__)

VALID_STANDINGS_GROUPINGS = {"single_table", "split_tables", "unknown"}
DISALLOWED_COMPETITION_TERMS = (
    "playoff",
    "playoffs",
    "play-in",
    "play in",
    "cup",
    "copa",
    "relegation",
    "releg",
    "promotion",
    "knockout",
    "knockouts",
)

_COMPETITION_METADATA_REFRESH_ATTEMPTED: set[int] = set()


@dataclass
class CompetitionMetadataResolution:
    number_of_teams: Optional[int]
    total_regular_season_games: Optional[int]
    standings_grouping: Optional[str]
    league_config_source: str
    source_detail: str
    standings_called: bool
    standings_team_count: Optional[int]
    standings_table_count: Optional[int]
    standings_conflict: bool
    should_persist: bool
    raw: Dict[str, Any]


def mark_competition_metadata_refresh_attempted(competition_id: Optional[int]) -> None:
    if competition_id is not None:
        _COMPETITION_METADATA_REFRESH_ATTEMPTED.add(int(competition_id))


def competition_metadata_refresh_already_attempted(competition_id: Optional[int]) -> bool:
    return competition_id is not None and int(competition_id) in _COMPETITION_METADATA_REFRESH_ATTEMPTED


def apply_competition_metadata_resolution(
    event_context: EventContext,
    resolution: CompetitionMetadataResolution,
) -> EventContext:
    event_context.competition.number_of_teams = resolution.number_of_teams
    event_context.competition.total_regular_season_games = resolution.total_regular_season_games
    event_context.competition.standings_grouping = resolution.standings_grouping
    event_context.competition.league_config_source = resolution.league_config_source
    event_context.competition.number_of_teams_source = resolution.league_config_source
    return event_context


def _clean_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _is_regular_competition_scope(event_context: EventContext, event_obj=None) -> bool:
    round_value = _clean_text(getattr(event_obj, "round", None))
    if round_value and round_value != "regular_season":
        return False

    competition = event_context.competition
    text = " ".join(
        _clean_text(value)
        for value in (
            competition.display_name,
            competition.canonical_name,
            competition.slug,
            competition.unique_slug,
        )
    )
    return not any(term in text for term in DISALLOWED_COMPETITION_TERMS)


def _valid_grouping(value: Optional[str]) -> Optional[str]:
    if value in VALID_STANDINGS_GROUPINGS:
        return value
    return None


def _valid_number(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 1 else None


def _valid_total_games(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _merge_number(
    current_value: Optional[int],
    current_source: str,
    new_value: Optional[int],
    new_source: str,
    raw: Dict[str, Any],
) -> tuple[Optional[int], str]:
    new_value = _valid_number(new_value)
    if new_value is None:
        return current_value, current_source
    if current_value is None or new_value > current_value:
        if current_value is not None and new_value != current_value:
            raw.setdefault("conflicts", []).append(
                {
                    "field": "number_of_teams",
                    "existing_value": current_value,
                    "new_value": new_value,
                    "source": new_source,
                    "resolution": "used_greater_value",
                }
            )
        return new_value, new_source
    if new_value < current_value:
        raw.setdefault("conflicts", []).append(
            {
                "field": "number_of_teams",
                "existing_value": current_value,
                "new_value": new_value,
                "source": new_source,
                "resolution": "kept_greater_value",
            }
        )
    return current_value, current_source


def _has_critical_metadata_gap(
    number_of_teams: Optional[int],
    total_regular_season_games: Optional[int],
    standings_grouping: Optional[str],
    manual_config: Optional[Dict[str, Any]],
    db_total_regular_season_games: Optional[int],
) -> bool:
    if number_of_teams is None:
        return True
    if standings_grouping is None:
        return True
    if total_regular_season_games is None and not manual_config and db_total_regular_season_games is None:
        return True
    return False


def resolve_competition_metadata(
    event_context: EventContext,
    event_obj=None,
    standings_endpoint_missing_competition_ids: Optional[set[int]] = None,
) -> CompetitionMetadataResolution:
    competition = event_context.competition
    competition_id = getattr(competition, "competition_id", None)
    db_number_of_teams = _valid_number(getattr(competition, "number_of_teams", None))
    db_total_regular_season_games = _valid_total_games(getattr(competition, "total_regular_season_games", None))
    db_standings_grouping = _valid_grouping(getattr(competition, "standings_grouping", None))
    league_config_source = getattr(competition, "league_config_source", None)
    existing_source = None if league_config_source == "missing" else league_config_source
    db_source = existing_source or (
        "db_cache"
        if any(value is not None for value in (db_number_of_teams, db_total_regular_season_games, db_standings_grouping))
        else "missing"
    )

    raw: Dict[str, Any] = {
        "competition_id": competition_id,
        "source": competition.source,
        "source_unique_tournament_id": competition.source_unique_tournament_id,
        "season_id": event_context.season_id,
        "has_standings_source_endpoint": getattr(competition, "has_standings_source_endpoint", None),
        "db_cache": {
            "number_of_teams": db_number_of_teams,
            "total_regular_season_games": db_total_regular_season_games,
            "standings_grouping": db_standings_grouping,
            "league_config_source": league_config_source,
        },
    }

    if competition.source != "sofascore":
        raw["skip_reason"] = "unsupported_source"
        return CompetitionMetadataResolution(
            number_of_teams=db_number_of_teams,
            total_regular_season_games=db_total_regular_season_games,
            standings_grouping=db_standings_grouping,
            league_config_source=db_source,
            source_detail="unsupported_source",
            standings_called=False,
            standings_team_count=None,
            standings_table_count=None,
            standings_conflict=False,
            should_persist=False,
            raw=raw,
        )

    sport = _clean_text(event_context.sport)
    if sport in {"tennis", "tennis doubles"}:
        raw["skip_reason"] = "tennis_no_standings_metadata_persist"
        return CompetitionMetadataResolution(
            number_of_teams=db_number_of_teams,
            total_regular_season_games=db_total_regular_season_games,
            standings_grouping=db_standings_grouping,
            league_config_source=db_source,
            source_detail="tennis_skipped",
            standings_called=False,
            standings_team_count=None,
            standings_table_count=None,
            standings_conflict=False,
            should_persist=False,
            raw=raw,
        )

    regular_scope = _is_regular_competition_scope(event_context, event_obj=event_obj)
    raw["regular_competition_scope"] = regular_scope
    league_config = get_league_config(competition.source_unique_tournament_id, competition.source_tournament_id) if regular_scope else None
    raw["manual_config"] = asdict(league_config) if league_config else None

    number_of_teams = db_number_of_teams
    total_regular_season_games = db_total_regular_season_games
    standings_grouping = db_standings_grouping
    source = db_source
    source_detail = "db_cache" if source != "missing" else "missing"
    should_persist = False

    if league_config:
        number_of_teams, source = _merge_number(
            number_of_teams,
            source,
            league_config.number_of_teams,
            "manual_config",
            raw,
        )
        manual_total = _valid_total_games(league_config.total_regular_season_games)
        if manual_total is not None and manual_total != total_regular_season_games:
            total_regular_season_games = manual_total
            source = "manual_config"
        if standings_grouping is None:
            standings_grouping = _valid_grouping(league_config.standings_grouping)
            if standings_grouping is not None:
                source = "manual_config"
        elif league_config.standings_grouping and league_config.standings_grouping != standings_grouping:
            raw.setdefault("conflicts", []).append(
                {
                    "field": "standings_grouping",
                    "db_value": standings_grouping,
                    "new_value": league_config.standings_grouping,
                    "source": "manual_config",
                    "resolution": "kept_db_value",
                }
            )
        source_detail = "manual_config"
        should_persist = competition.competition_id is not None

    if not regular_scope:
        raw["skip_reason"] = "non_regular_competition_scope"
        logger.info(
            "Competition metadata resolver skipping standings for event_id=%s competition_id=%s source_unique_tournament_id=%s season_id=%s skip_reason=%s",
            event_context.event_id,
            competition.competition_id,
            competition.source_unique_tournament_id,
            event_context.season_id,
            raw["skip_reason"],
        )
        return CompetitionMetadataResolution(
            number_of_teams=number_of_teams,
            total_regular_season_games=total_regular_season_games,
            standings_grouping=standings_grouping,
            league_config_source=source,
            source_detail="non_regular_scope_db_cache_only",
            standings_called=False,
            standings_team_count=None,
            standings_table_count=None,
            standings_conflict=bool(raw.get("conflicts")),
            should_persist=False,
            raw=raw,
        )

    force_refresh = bool(Config.FORCE_STANDINGS_COMPETITION_METADATA_REFRESH)
    run_cache_hit = (
        standings_endpoint_missing_competition_ids is not None
        and competition_id is not None
        and int(competition_id) in standings_endpoint_missing_competition_ids
    )
    known_missing_standings_endpoint = getattr(competition, "has_standings_source_endpoint", None) is False or run_cache_hit
    if known_missing_standings_endpoint and getattr(competition, "has_standings_source_endpoint", None) is not False:
        competition.has_standings_source_endpoint = False
        raw["has_standings_source_endpoint"] = False
    standings_needed = _has_critical_metadata_gap(
        number_of_teams,
        total_regular_season_games,
        standings_grouping,
        league_config,
        db_total_regular_season_games,
    )
    already_attempted = competition_metadata_refresh_already_attempted(competition.competition_id)
    raw["standings_needed"] = standings_needed
    raw["force_refresh"] = force_refresh
    raw["refresh_already_attempted"] = already_attempted

    should_call_standings = (
        Config.ENABLE_STANDINGS_COMPETITION_METADATA_ENRICHMENT
        and (standings_needed or force_refresh)
        and event_context.season_id is not None
        and competition.source_unique_tournament_id is not None
        and (force_refresh or not already_attempted)
        and not known_missing_standings_endpoint
    )

    standings_called = False
    standings_team_count = None
    standings_table_count = None
    standings_conflict = False

    if should_call_standings:
        logger.info(
            "Competition metadata resolver calling standings for event_id=%s competition_id=%s season_id=%s source_unique_tournament_id=%s force_refresh=%s standings_needed=%s already_attempted=%s",
            event_context.event_id,
            competition_id,
            event_context.season_id,
            competition.source_unique_tournament_id,
            force_refresh,
            standings_needed,
            already_attempted,
        )
        standings_called = True
        mark_competition_metadata_refresh_attempted(competition_id)
        raw_standings = api_client.get_standings_response(
            int(event_context.season_id),
            int(competition.source_unique_tournament_id),
            competition_context=competition,
            standings_endpoint_missing_competition_ids=standings_endpoint_missing_competition_ids,
        )
        raw["standings_response_raw"] = raw_standings
        parsed = parse_competition_metadata_from_standings(raw_standings)
        raw["standings_response"] = asdict(parsed)
        standings_team_count = parsed.unique_team_count
        standings_table_count = parsed.table_count

        if parsed.valid:
            previous_number = number_of_teams
            number_of_teams, source = _merge_number(
                number_of_teams,
                source,
                parsed.number_of_teams,
                "standings_response",
                raw,
            )
            if previous_number is not None and parsed.number_of_teams not in (None, previous_number):
                standings_conflict = True

            parsed_grouping = _valid_grouping(parsed.standings_grouping)
            if standings_grouping is None:
                standings_grouping = parsed_grouping
                source = "standings_response"
            elif parsed_grouping is not None and parsed_grouping != standings_grouping:
                standings_conflict = True
                raw.setdefault("conflicts", []).append(
                    {
                        "field": "standings_grouping",
                        "db_value": standings_grouping,
                        "new_value": parsed_grouping,
                        "source": "standings_response",
                        "resolution": "used_standings_value" if force_refresh else "kept_existing_value",
                    }
                )
                if force_refresh:
                    standings_grouping = parsed_grouping
                    source = "standings_response"

            source_detail = "standings_response"
            should_persist = competition.competition_id is not None
        else:
            source_detail = f"standings_response_invalid:{parsed.reason}"
    elif not Config.ENABLE_STANDINGS_COMPETITION_METADATA_ENRICHMENT:
        raw["skip_reason"] = "standings_enrichment_disabled"
    elif not standings_needed and not force_refresh:
        raw["skip_reason"] = "not_needed_due_to_metadata_being_complete_already"
    elif event_context.season_id is None:
        raw["skip_reason"] = "missing_season_id"
    elif competition.source_unique_tournament_id is None:
        raw["skip_reason"] = "missing_source_unique_tournament_id"
    elif known_missing_standings_endpoint:
        raw["skip_reason"] = "known_missing_standings_source_endpoint"
    elif already_attempted and not force_refresh:
        raw["skip_reason"] = "competition_refresh_already_attempted"

    if already_attempted and not force_refresh and not standings_called:
        should_persist = False

    if not standings_called:
        logger.info(
            "Competition metadata resolver did not call standings for competition_id=%s season_id=%s skip_reason=%s",
            competition_id,
            event_context.season_id,
            raw.get("skip_reason"),
        )

    return CompetitionMetadataResolution(
        number_of_teams=number_of_teams,
        total_regular_season_games=total_regular_season_games,
        standings_grouping=standings_grouping,
        league_config_source=source,
        source_detail=source_detail,
        standings_called=standings_called,
        standings_team_count=standings_team_count,
        standings_table_count=standings_table_count,
        standings_conflict=standings_conflict,
        should_persist=should_persist,
        raw=raw,
    )
