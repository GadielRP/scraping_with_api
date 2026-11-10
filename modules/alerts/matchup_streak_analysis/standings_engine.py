"""Historical standings calculation engine.

This layer owns the DB-backed standings computation and the cache used by
historical form retrieval.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.disciplinary_incident_repository import DisciplinaryIncidentRepository
from modules.competition.rule_terms import GroupingMethod
from shared.temporal import from_unix_timestamp

from infrastructure.persistence.repositories.competition_repository import get_competition_profile_for_local_id as get_competition_profile
from modules.competition.competitive_rules import CompetitionRules
from .historical_form_reporting import format_standings_table_for_telegram
from .constants import get_team_group
from .standings_rules import normalize_result_subtype, rank_standings

logger = logging.getLogger(__name__)
FOOTBALL_CARD_STATS = (("yellow_cards", "yellow_card"), ("red_cards", "red_card"))


def _create_team_stats(group: Optional[str]) -> Dict[str, object]:
    return {
        "wins": 0,
        "losses": 0,
        "draws": 0,
        "ties": 0,
        "ot_losses": 0,
        "regulation_wins": 0,
        "regulation_overtime_wins": 0,
        "ot_so_wins": 0,
        "goals_for": 0,
        "goals_against": 0,
        "games_played": 0,
        "away_goals": 0,
        "home_goals": 0,
        "away_goal_diff": 0,
        "home_goal_diff": 0,
        "away_wins": 0,
        "group": group,
        "points": 0,
        "pct": None,
    }


def _finalize_team_stats(stats: Dict[str, object], profile: CompetitionRules) -> None:
    stats["goal_diff"] = stats["goals_for"] - stats["goals_against"]
    stats["points"] = profile.scoring.points(stats)
    games = stats["games_played"]
    stats["pct"] = (
        stats["points"] / (games * profile.scoring.maximum_per_game) if games else 0.0
    ) if profile.standings.record_percentage else None
    if profile.standings.fair_play_weights:
        stats["fair_play_points"] = profile.standings.fair_play_points(stats)


def _build_team_standing_payload(
    stats: Dict[str, object],
    position_meta: Dict[str, object],
    profile: CompetitionRules,
    group_name: Optional[str] = None,
) -> Dict[str, object]:
    payload = dict(stats)
    payload.update(position_meta)
    position = position_meta.get("position")

    # Engine-level aliases kept for downstream callers that still expect them.
    payload["rank"] = position
    payload["gp"] = stats.get("games_played")
    payload["diff"] = stats.get("goal_diff")
    payload["standings_method"] = profile.standings.name
    payload["ranking_metric"] = profile.standings.primary_metric.name
    payload["group"] = group_name
    payload["conference"] = group_name
    if profile.grouping_method == GroupingMethod.AHL_DIVISION:
        payload["division"] = group_name
    return payload


def _apply_game_result(
    team_stats, home_team, away_team, home_score, away_score, winner, result_subtype=None,
) -> None:
    """Count outcomes once; scoring rules assign points when a snapshot is built."""
    home, away = team_stats[home_team], team_stats[away_team]
    home["home_goals"] += home_score
    home["home_goal_diff"] += home_score - away_score
    away["away_goals"] += away_score
    away["away_goal_diff"] += away_score - home_score
    for own, scored, conceded in ((home, home_score, away_score), (away, away_score, home_score)):
        own["goals_for"] += scored
        own["goals_against"] += conceded
        own["games_played"] += 1
    if winner == "X":
        for stats in (home, away):
            stats["draws"] += 1
            stats["ties"] += 1
    elif winner in {"1", "2"}:
        won, lost = (home, away) if winner == "1" else (away, home)
        won["wins"] += 1
        lost["losses"] += 1
        subtype = normalize_result_subtype(result_subtype, winner)
        if winner == "2":
            won["away_wins"] += 1
        if subtype != "SO":
            won["regulation_overtime_wins"] += 1
        if subtype in {"OT", "SO"}:
            won["ot_so_wins"] += 1
            lost["ot_losses"] += 1
        else:
            won["regulation_wins"] += 1


def _register_match_record(
    record: Dict[str, Any],
    team_stats: Dict[str, Dict[str, object]],
    profile: CompetitionRules,
    season_id: int,
    warned_unknown_group_teams: Set[str],
) -> None:
    """Ensure both teams exist in team_stats and apply one match result."""
    home_team = record["home_team"]
    away_team = record["away_team"]

    for team in (home_team, away_team):
        if team not in team_stats:
            group = None
            if profile.grouping_method != GroupingMethod.LEAGUE_WIDE:
                group = get_team_group(team, profile.grouping_method)
                if not group:
                    if team not in warned_unknown_group_teams:
                        logger.warning(
                            "Standings group mapping missing for season %s team '%s'; keeping in UNKNOWN group",
                            season_id,
                            team,
                        )
                        warned_unknown_group_teams.add(team)
                    group = "UNKNOWN"

            team_stats[team] = _create_team_stats(group)

    _apply_game_result(
        team_stats=team_stats,
        home_team=home_team,
        away_team=away_team,
        home_score=record["home_score"],
        away_score=record["away_score"],
        winner=record["winner"],
        result_subtype=record.get("result_subtype"),
    )
    for side, team in (("home", home_team), ("away", away_team)):
        stats = team_stats[team]
        identity = record.get(f"{side}_participant_identity")
        if identity is not None:
            # A reused display name cannot silently merge provider participants.
            if "participant_identity" not in stats:
                stats["participant_identity"] = identity
            elif stats["participant_identity"] != identity:
                stats["participant_identity"] = None
        for stat, _ in FOOTBALL_CARD_STATS:
            key = f"{side}_{stat}"
            if key in record:
                stats[stat] = stats.get(stat, 0) + (record[key] or 0)


def _finalize_standings_snapshot(
    team_stats: Dict[str, Dict[str, object]],
    profile: CompetitionRules,
    match_records: Optional[List[Dict[str, object]]],
) -> Dict[str, Dict]:
    """Build a standings payload from running stats without mutating them."""
    finalized_stats = {team: dict(stats) for team, stats in team_stats.items()}
    for stats in finalized_stats.values():
        _finalize_team_stats(stats, profile)
    return _build_standings_payload(finalized_stats, profile, match_records)


def _build_standings_payload(
    team_stats: Dict[str, Dict[str, object]],
    profile: CompetitionRules,
    match_records: Optional[List[Dict[str, object]]] = None,
) -> Dict[str, Dict]:
    standings: Dict[str, Dict] = {}

    season_complete = bool(
        profile.total_regular_season_games and profile.number_of_teams
        and len(team_stats) == profile.number_of_teams
        and all(stats["games_played"] >= profile.total_regular_season_games for stats in team_stats.values())
    )
    grouped_teams: Dict[Optional[str], List[Tuple[str, Dict[str, object]]]] = {}
    for team_name, stats in team_stats.items():
        group_name = None if profile.grouping_method == GroupingMethod.LEAGUE_WIDE else stats.get("group") or "UNKNOWN"
        grouped_teams.setdefault(group_name, []).append((team_name, stats))

    for group_name, teams in grouped_teams.items():
        sorted_teams, positions = rank_standings(
            teams, profile, match_records, season_complete=season_complete,
        )

        for team_name, stats in sorted_teams:
            pos_meta = positions[team_name]
            if group_name == "UNKNOWN":
                pos_meta.update(position=None, ranking_status="UNRESOLVED", ranking_reason="MISSING_GROUP_MEMBERSHIP")
            standings[team_name] = _build_team_standing_payload(stats, pos_meta, profile, group_name)

    return standings


def _build_league_totals_context(
    *,
    season_id: int,
    cutoff_timestamp: float,
    sport: Optional[str],
    source_unique_tournament_id: Optional[int],
    source_tournament_id: Optional[int],
    match_records: List[Dict[str, object]],
    team_stats: Dict[str, Dict[str, object]],
    standings: Dict[str, Dict],
) -> Dict[str, Any]:
    teams: Dict[str, Dict[str, Any]] = {}

    for team_name, stats in team_stats.items():
        games_played = int(stats.get("games_played", 0) or 0)
        goals_for = int(stats.get("goals_for", 0) or 0)
        goals_against = int(stats.get("goals_against", 0) or 0)
        teams[team_name] = {
            "team_name": team_name,
            "games_played": games_played,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "team_total_per_game": (
                (goals_for + goals_against) / games_played if games_played > 0 else None
            ),
            "game_totals": [],
            "results": [],
            "standing": standings.get(team_name, {}),
        }

    matches: List[Dict[str, Any]] = []
    for record in match_records:
        game_total = int(record["home_score"]) + int(record["away_score"])
        start_timestamp = int(record["startTimestamp"])
        match_payload = {
            "event_id": int(record["event_id"]),
            "startTimestamp": start_timestamp,
            "home_team": record["home_team"],
            "away_team": record["away_team"],
            "home_score": int(record["home_score"]),
            "away_score": int(record["away_score"]),
            "winner": record["winner"],
            "result_subtype": record.get("result_subtype"),
            "game_total": game_total,
        }
        matches.append(match_payload)

        home_team_name = record["home_team"]
        away_team_name = record["away_team"]
        winner = record["winner"]

        if winner == "X":
            home_result_code = "X"
            away_result_code = "X"
        elif winner == "1":
            home_result_code = "1"
            away_result_code = "2"
        elif winner == "2":
            home_result_code = "2"
            away_result_code = "1"
        else:
            home_result_code = "X"
            away_result_code = "X"

        home_entry = teams.get(home_team_name)
        if home_entry is not None:
            home_entry["game_totals"].append(game_total)
            home_entry["results"].append({
                "event_id": int(record["event_id"]),
                "startTimestamp": start_timestamp,
                "team_role": "home",
                "opponent_name": away_team_name,
                "team_score": int(record["home_score"]),
                "opponent_score": int(record["away_score"]),
                "game_total": game_total,
                "team_result_code": home_result_code,
            })

        away_entry = teams.get(away_team_name)
        if away_entry is not None:
            away_entry["game_totals"].append(game_total)
            away_entry["results"].append({
                "event_id": int(record["event_id"]),
                "startTimestamp": start_timestamp,
                "team_role": "away",
                "opponent_name": home_team_name,
                "team_score": int(record["away_score"]),
                "opponent_score": int(record["home_score"]),
                "game_total": game_total,
                "team_result_code": away_result_code,
            })

    return {
        "source": "db_standings_calculator",
        "season_id": season_id,
        "cutoff_timestamp": cutoff_timestamp,
        "cutoff_rule": "starts_at < cutoff_dt",
        "sport": sport,
        "source_unique_tournament_id": source_unique_tournament_id,
        "source_tournament_id": source_tournament_id,
        "match_count": len(match_records),
        "team_count": len(teams),
        "matches": matches,
        "teams": teams,
    }


def _serialize_match_records(match_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return a public-safe copy of match records without datetime objects."""
    serialized_records: List[Dict[str, Any]] = []
    for record in match_records:
        serialized_records.append({
            "event_id": record["event_id"],
            "startTimestamp": record["startTimestamp"],
            "home_team": record["home_team"],
            "away_team": record["away_team"],
            "home_score": record["home_score"],
            "away_score": record["away_score"],
            "winner": record["winner"],
            "result_subtype": record.get("result_subtype"),
            "game_total": record["game_total"],
            **{f"{side}_participant_identity": record[f"{side}_participant_identity"]
               for side in ("home", "away") if f"{side}_participant_identity" in record},
            **{f"{side}_{stat}": record[f"{side}_{stat}"]
               for side in ("home", "away") for stat, _ in FOOTBALL_CARD_STATS
               if f"{side}_{stat}" in record},
        })
    return serialized_records


class HistoricalStandingsCalculator:
    """Compute standings at any point in time for a collected season."""

    def __init__(self):
        self._cache: Dict[Tuple[int, float, str, Optional[int], Optional[int], Optional[int], Optional[int]], Dict[str, Any]] = {}

    def _fetch_match_records_before_cutoff(
        self,
        season_id: int,
        cutoff_timestamp: float,
        source_unique_tournament_id: Optional[int],
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        cutoff_dt = from_unix_timestamp(cutoff_timestamp)
        query_sql = """
            SELECT
                v.event_id, v.starts_at, v.home_team, v.away_team,
                v.home_score, v.away_score, v.winner, v.result_subtype, v.sport,
                hp.source AS home_participant_source,
                hp.source_participant_id AS home_source_participant_id,
                ap.source AS away_participant_source,
                ap.source_participant_id AS away_source_participant_id
            FROM season_events_with_results v
            JOIN events e ON e.id = v.event_id
            LEFT JOIN participants hp ON hp.participant_id = e.home_participant_id
            LEFT JOIN participants ap ON ap.participant_id = e.away_participant_id
            WHERE v.round = 'regular_season'
              AND v.starts_at < :cutoff_dt
        """
        query_params: Dict[str, Any] = {
            "cutoff_dt": cutoff_dt,
        }
        if competition_id is not None:
            query_sql += " AND v.competition_id = :competition_id"
            query_params["competition_id"] = int(competition_id)
        elif source_unique_tournament_id is not None:
            query_sql += " AND v.source_unique_tournament_id = :source_unique_tournament_id"
            query_params["source_unique_tournament_id"] = source_unique_tournament_id

        if season_year is not None:
            query_sql += " AND v.season_year = :season_year"
            query_params["season_year"] = int(season_year)
        else:
            query_sql += " AND v.season_id = :season_id"
            query_params["season_id"] = int(season_id)

        query_sql += "\n            ORDER BY v.starts_at\n            "

        query = text(query_sql)

        match_records: List[Dict[str, Any]] = []
        football_records: List[Dict[str, Any]] = []
        with db_manager.get_session() as session:
            result = session.execute(query, query_params)
            all_rows = result.fetchall()

            for row in all_rows:
                home_score = int(row.home_score)
                away_score = int(row.away_score)
                winner = row.winner
                result_subtype = normalize_result_subtype(getattr(row, "result_subtype", None), winner)
                starts_at = row.starts_at

                record = {
                    "event_id": int(row.event_id),
                    "starts_at": starts_at,
                    "startTimestamp": int(starts_at.timestamp()),
                    "home_team": row.home_team,
                    "away_team": row.away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "winner": winner,
                    "result_subtype": result_subtype,
                    "game_total": home_score + away_score,
                }
                if getattr(row, "sport", None) == "Football":
                    football_records.append(record)
                for side in ("home", "away"):
                    source = getattr(row, f"{side}_participant_source", None)
                    source_id = getattr(row, f"{side}_source_participant_id", None)
                    if source and source_id is not None:
                        record[f"{side}_participant_identity"] = {
                            "source": source, "source_participant_id": int(source_id),
                        }
                match_records.append(record)

        football_ids = [record["event_id"] for record in football_records]
        totals = DisciplinaryIncidentRepository(db_manager).get_event_card_totals("sofascore", football_ids)
        for record in football_records:
            for side in ("home", "away"):
                for stat, kind in FOOTBALL_CARD_STATS:
                    record[f"{side}_{stat}"] = totals.get((record["event_id"], side, kind))

        return match_records

    def calculate_standings_at(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str = None,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
    ) -> Dict[str, Dict]:
        bundle = self.calculate_standings_bundle_at(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
            competition_id=competition_id,
            season_year=season_year,
        )
        return bundle.get("standings", {})

    def calculate_standings_bundle_at(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str = None,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
        send_debug_standings: bool = False,
    ) -> Dict[str, Any]:
        cache_key = (
            season_id,
            cutoff_timestamp,
            sport or "",
            source_unique_tournament_id,
            source_tournament_id,
            competition_id,
            season_year,
        )
        if cache_key in self._cache:
            cached_bundle = self._cache[cache_key]
            # Cached bundles are always stored normalized; return a copy so
            # callers cannot mutate the cached payload.
            if send_debug_standings:
                logger.info(format_standings_table_for_telegram(cached_bundle["standings"], "Current standings"))
            return {
                **cached_bundle,
                "match_records": _serialize_match_records(cached_bundle.get("match_records", [])),
            }

        try:
            bundle = self._calculate_standings_bundle_internal(
                season_id=season_id,
                cutoff_timestamp=cutoff_timestamp,
                sport=sport,
                source_unique_tournament_id=source_unique_tournament_id,
                source_tournament_id=source_tournament_id,
                competition_id=competition_id,
                season_year=season_year,
            )
            bundle = {
                **bundle,
                "match_records": _serialize_match_records(bundle.get("match_records", [])),
            }
            if send_debug_standings:
                logger.info(format_standings_table_for_telegram(bundle["standings"], "Current standings"))
            self._cache[cache_key] = bundle
            return bundle
        except Exception as exc:
            logger.error("Error computing standings for season %s: %s", season_id, exc)
            return {
                "standings": {},
                "match_records": [],
                "league_totals_context": None,
                "status": "ERROR",
                "error": str(exc),
            }


    def calculate_standings_timeline(
        self,
        season_id: int,
        cutoff_timestamps: Iterable[float],
        source_unique_tournament_id: Optional[int] = None,
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
    ) -> Dict[float, Dict[str, Dict]]:
        """Compute standings snapshots for many cutoffs with one league fetch.

        Returns a mapping of each requested cutoff timestamp to the same
        standings payload that calculate_standings_at() would produce for that
        cutoff, but performing a single league-wide SQL query instead of one
        per cutoff. On failure every cutoff maps to an empty dict, mirroring
        the per-cutoff error behavior.
        """
        unique_cutoffs = sorted({float(cutoff) for cutoff in cutoff_timestamps})
        if not unique_cutoffs:
            return {}

        try:
            return self._calculate_standings_timeline_internal(
                season_id=season_id,
                unique_cutoffs=unique_cutoffs,
                source_unique_tournament_id=source_unique_tournament_id,
                competition_id=competition_id,
                season_year=season_year,
            )
        except Exception as exc:
            logger.error("Error computing standings timeline for season %s: %s", season_id, exc)
            return {cutoff: {} for cutoff in unique_cutoffs}

    def _calculate_standings_timeline_internal(
        self,
        season_id: int,
        unique_cutoffs: List[float],
        source_unique_tournament_id: Optional[int],
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
    ) -> Dict[float, Dict[str, Dict]]:
        profile = get_competition_profile(competition_id)
        if profile is None:
            raise ValueError(f"No competition profile configured for competition_id={competition_id}")

        # One league-wide fetch bounded by the latest cutoff covers every
        # earlier snapshot: records are re-filtered in memory per cutoff.
        match_records = self._fetch_match_records_before_cutoff(
            season_id=season_id,
            cutoff_timestamp=unique_cutoffs[-1],
            source_unique_tournament_id=source_unique_tournament_id,
            competition_id=competition_id,
            season_year=season_year,
        )
        record_timestamps = [record["starts_at"].timestamp() for record in match_records]

        snapshots: Dict[float, Dict[str, Dict]] = {}
        team_stats: Dict[str, Dict[str, object]] = {}
        warned_unknown_group_teams: Set[str] = set()
        applied_count = 0

        for cutoff in unique_cutoffs:
            while applied_count < len(match_records) and record_timestamps[applied_count] < cutoff:
                _register_match_record(
                    match_records[applied_count],
                    team_stats,
                    profile,
                    season_id,
                    warned_unknown_group_teams,
                )
                applied_count += 1

            snapshots[cutoff] = _finalize_standings_snapshot(
                team_stats,
                profile,
                match_records[:applied_count],
            )

        return snapshots

    def _calculate_standings_bundle_internal(
        self,
        season_id: int,
        cutoff_timestamp: float,
        sport: str,
        source_unique_tournament_id: Optional[int] = None,
        source_tournament_id: Optional[int] = None,
        competition_id: Optional[int] = None,
        season_year: Optional[int] = None,
    ) -> Dict[str, Any]:
        profile = get_competition_profile(competition_id)
        if profile is None:
            raise ValueError(f"No competition profile configured for competition_id={competition_id}")

        match_records = self._fetch_match_records_before_cutoff(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            source_unique_tournament_id=source_unique_tournament_id,
            competition_id=competition_id,
            season_year=season_year,
        )
        team_stats: Dict[str, Dict[str, object]] = {}
        warned_unknown_group_teams: Set[str] = set()
        for row in match_records:
            _register_match_record(
                row,
                team_stats,
                profile,
                season_id,
                warned_unknown_group_teams,
            )

        for stats in team_stats.values():
            _finalize_team_stats(stats, profile)

        standings = _build_standings_payload(team_stats, profile, match_records)
        league_totals_context = _build_league_totals_context(
            season_id=season_id,
            cutoff_timestamp=cutoff_timestamp,
            sport=sport,
            source_unique_tournament_id=source_unique_tournament_id,
            source_tournament_id=source_tournament_id,
            match_records=match_records,
            team_stats=team_stats,
            standings=standings,
        )

        return {
            "standings": standings,
            "match_records": match_records,
            "league_totals_context": league_totals_context,
            "status": "ACTIVE",
            "error": None,
        }


    def clear_cache(self):
        """Clear the internal cache."""
        self._cache.clear()


standings_calculator = HistoricalStandingsCalculator()


__all__ = [
    "HistoricalStandingsCalculator",
    "standings_calculator",
]
