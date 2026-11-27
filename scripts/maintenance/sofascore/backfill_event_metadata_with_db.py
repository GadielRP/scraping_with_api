#!/usr/bin/env python3
"""
Backfill Event Metadata — DB-only (no API calls)
=================================================

Infers the three normalised FK columns that may still be NULL on older event rows:
  - home_participant_id
  - away_participant_id
  - competition_id

Three complementary modes
--------------------------

1. GENERAL MODE (default)
   Donors = any event in the DB that already has all three FK columns set.
   Matching keys (in priority order):
     a. sport + competition text + home_team text  → home_participant_id
     b. sport + competition text + away_team text  → away_participant_id
     c. sport + competition text                   → competition_id
     d. sport + home_team text  (cross-comp)       → home_participant_id (fallback)
     e. sport + away_team text  (cross-comp)       → away_participant_id (fallback)

2. BLUEPRINT MODE  (--blueprint-season / --target-season)
   Donors = events from the specified blueprint season_id.
   Targets = events from the specified target season_id.
   Matching keys are tournament-scoped so competition_id is inferred by
   tournament membership, not text match:
     a. tournament_id + home_team text → home_participant_id
     b. tournament_id + away_team text → away_participant_id
     c. tournament_id                  → competition_id (single competition per tournament)

3. TOURNAMENT AUTO MODE  (--tournament-auto)
   For every tournament_id group defined in `season_to_process`, uses
   already-backfilled events in that group as donors for incomplete events
   in the same group.  Equivalent to running blueprint mode for every
   combination of seasons within each tournament.

All modes are fully idempotent — re-running is safe.

Usage
-----
    # Dry-run all modes before committing
    python scripts/backfill/backfill_event_metadata_with_db.py --test

    # Blueprint mode: use NBA 25/26 data to fill NBA 24/25
    python scripts/backfill/backfill_event_metadata_with_db.py \\
        --blueprint-season 80229 --target-season 65360

    # Tournament auto: fill all seasons for every known tournament
    python scripts/backfill/backfill_event_metadata_with_db.py --tournament-auto

    # General fallback for a specific sport
    python scripts/backfill/backfill_event_metadata_with_db.py --sport Basketball

    # Verbose output
    python scripts/backfill/backfill_event_metadata_with_db.py --tournament-auto --verbose
"""

import argparse
import logging
import os
import sys
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import and_, func, or_, text
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# Project bootstrap
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Event

# Import the season map — tournament_id here is actually source_unique_tournament_id
from scripts.sport_seasons_processing import season_to_process

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "backfill_event_metadata_with_db.log")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("backfill_event_metadata_with_db")


# ---------------------------------------------------------------------------
# Season/unique_tournament helpers
# ---------------------------------------------------------------------------

def _build_season_to_unique_tournament() -> Dict[int, int]:
    """season_id → unique_tournament_id."""
    mapping: Dict[int, int] = {}
    for entry in season_to_process:
        sid = entry.get("season_id")
        tid = entry.get("tournament_id")  # Actually unique_tournament_id
        if sid and tid:
            mapping[sid] = tid
    return mapping


def _build_unique_tournament_to_seasons() -> Dict[int, List[int]]:
    """unique_tournament_id → [season_id, ...]  (deduped, ordered)."""
    mapping: Dict[int, Set[int]] = defaultdict(set)
    for entry in season_to_process:
        sid = entry.get("season_id")
        tid = entry.get("tournament_id")  # Actually unique_tournament_id
        if sid and tid:
            mapping[tid].add(sid)
    return {tid: sorted(sids) for tid, sids in mapping.items()}


SEASON_TO_UNIQUE_TOURNAMENT: Dict[int, int] = _build_season_to_unique_tournament()
UNIQUE_TOURNAMENT_TO_SEASONS: Dict[int, List[int]] = _build_unique_tournament_to_seasons()

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class EventRow:
    id: int
    sport: str
    competition: str           # legacy text column (lowercased)
    home_team: str             # legacy text column (lowercased)
    away_team: str             # legacy text column (lowercased)
    season_id: Optional[int]
    home_participant_id: Optional[int]
    away_participant_id: Optional[int]
    competition_id: Optional[int]

    @property
    def needs_home(self) -> bool:
        return self.home_participant_id is None

    @property
    def needs_away(self) -> bool:
        return self.away_participant_id is None

    @property
    def needs_competition(self) -> bool:
        return self.competition_id is None


@dataclass
class BackfillStats:
    events_seen: int = 0
    events_updated: int = 0
    home_participant_filled: int = 0
    away_participant_filled: int = 0
    competition_filled: int = 0
    events_still_missing: int = 0
    errors: int = 0

    def add(self, other: "BackfillStats") -> None:
        self.events_seen += other.events_seen
        self.events_updated += other.events_updated
        self.home_participant_filled += other.home_participant_filled
        self.away_participant_filled += other.away_participant_filled
        self.competition_filled += other.competition_filled
        self.events_still_missing += other.events_still_missing
        self.errors += other.errors

    def summary(self, label: str = "") -> str:
        header = f" {label} " if label else ""
        sep = "=" * 60
        return (
            f"\n{sep}\n"
            f"{header.center(60)}\n"
            f"{sep}\n"
            f"  Events seen              : {self.events_seen}\n"
            f"  Updated                  : {self.events_updated}\n"
            f"    home_participant_id    : {self.home_participant_filled}\n"
            f"    away_participant_id    : {self.away_participant_filled}\n"
            f"    competition_id         : {self.competition_filled}\n"
            f"  Still missing after run  : {self.events_still_missing}\n"
            f"  Errors                   : {self.errors}\n"
            f"{sep}"
        )


# ---------------------------------------------------------------------------
# Lookup-map types
# ---------------------------------------------------------------------------

# General mode maps
HomeBySportCompTeam = Dict[Tuple[str, str, str], int]
AwayBySportCompTeam = Dict[Tuple[str, str, str], int]
CompBySportComp     = Dict[Tuple[str, str], int]
HomeBySportTeam     = Dict[Tuple[str, str], int]
AwayBySportTeam     = Dict[Tuple[str, str], int]

# Season/tournament mode maps
HomeByTournamentTeam = Dict[Tuple[int, str], int]         # (tournament_id, team_lower) → home_participant_id
AwayByTournamentTeam = Dict[Tuple[int, str], int]         # (tournament_id, team_lower) → away_participant_id
CompByTournament     = Dict[Tuple[int, frozenset], int]   # (tournament_id, frozen_word_set) → competition_id

def _normalize_competition_text(text: str) -> frozenset:
    """
    Convert legacy text like 'usa, nba, playoffs, nba' to a set of words {'usa', 'nba', 'playoffs'}.
    This handles minor cross-season inconsistencies where redundant words are added/removed,
    while still keeping true variants (like 'playoffs' vs 'regular season') distinct.
    """
    return frozenset(re.findall(r'[a-z0-9]+', text.lower()))


# ---------------------------------------------------------------------------
# DB helpers — donor query
# ---------------------------------------------------------------------------

def _query_donor_rows(
    session: Session,
    season_ids: Optional[List[int]] = None,
    sport_filter: Optional[str] = None,
):
    """
    Return rows from events that already have all three FK columns set.
    Optionally filtered to a set of season_ids or a specific sport.
    """
    query = session.query(
        func.lower(Event.sport).label("sport"),
        func.lower(Event.competition).label("competition"),
        func.lower(Event.home_team).label("home_team"),
        func.lower(Event.away_team).label("away_team"),
        Event.season_id,
        Event.home_participant_id,
        Event.away_participant_id,
        Event.competition_id,
    ).filter(
        Event.home_participant_id.isnot(None),
        Event.away_participant_id.isnot(None),
        Event.competition_id.isnot(None),
        Event.sport.isnot(None),
        Event.competition.isnot(None),
        Event.home_team.isnot(None),
        Event.away_team.isnot(None),
        Event.sport != "",
        Event.competition != "",
        Event.home_team != "",
        Event.away_team != "",
    )

    if season_ids is not None:
        query = query.filter(Event.season_id.in_(season_ids))
    if sport_filter:
        query = query.filter(func.lower(Event.sport) == sport_filter.lower())

    return query.all()


# ---------------------------------------------------------------------------
# Map builders
# ---------------------------------------------------------------------------

def build_general_maps(
    session: Session,
    sport_filter: Optional[str],
) -> Tuple[HomeBySportCompTeam, AwayBySportCompTeam, CompBySportComp, HomeBySportTeam, AwayBySportTeam]:
    """Build lookup maps scoped only by sport + team/competition text."""
    logger.info("📖 [General] Building lookup maps from all backfilled events…")
    rows = _query_donor_rows(session, sport_filter=sport_filter)
    logger.info("  Found %d donor rows", len(rows))

    home_by_sport_comp_team: HomeBySportCompTeam = {}
    away_by_sport_comp_team: AwayBySportCompTeam = {}
    comp_by_sport_comp:      CompBySportComp      = {}
    home_by_sport_team:      HomeBySportTeam      = {}
    away_by_sport_team:      AwayBySportTeam      = {}

    for row in rows:
        sport     = row.sport or ""
        comp      = row.competition or ""
        home_team = row.home_team or ""
        away_team = row.away_team or ""
        home_pid  = row.home_participant_id
        away_pid  = row.away_participant_id
        comp_id   = row.competition_id

        hk = (sport, comp, home_team)
        ak = (sport, comp, away_team)
        ck = (sport, comp)
        fhk = (sport, home_team)
        fak = (sport, away_team)

        if hk not in home_by_sport_comp_team:
            home_by_sport_comp_team[hk] = home_pid
        if ak not in away_by_sport_comp_team:
            away_by_sport_comp_team[ak] = away_pid
        if ck not in comp_by_sport_comp:
            comp_by_sport_comp[ck] = comp_id
        if fhk not in home_by_sport_team:
            home_by_sport_team[fhk] = home_pid
        if fak not in away_by_sport_team:
            away_by_sport_team[fak] = away_pid

    logger.info(
        "  Maps: home_comp=%d, away_comp=%d, comp=%d, home_fallback=%d, away_fallback=%d",
        len(home_by_sport_comp_team), len(away_by_sport_comp_team), len(comp_by_sport_comp),
        len(home_by_sport_team), len(away_by_sport_team),
    )
    return (
        home_by_sport_comp_team, away_by_sport_comp_team, comp_by_sport_comp,
        home_by_sport_team, away_by_sport_team,
    )


def build_tournament_maps(
    session: Session,
    donor_season_ids: List[int],
    tournament_id: int,
) -> Tuple[HomeByTournamentTeam, AwayByTournamentTeam, CompByTournament]:
    """
    Build tightly-scoped maps using only events from `donor_season_ids`.

    Because all seasons in a tournament share the same set of teams, we key on
    (tournament_id, team_name) → participant_id. Since unique tournaments can
    have multiple variants (e.g. Regular Season, Playoffs), we key on
    (tournament_id, competition_text) → competition_id.
    """
    rows = _query_donor_rows(session, season_ids=donor_season_ids)

    home_map: HomeByTournamentTeam = {}
    away_map: AwayByTournamentTeam = {}
    comp_map: CompByTournament     = {}

    for row in rows:
        home_team = row.home_team or ""
        away_team = row.away_team or ""
        comp_text = row.competition or ""
        home_pid  = row.home_participant_id
        away_pid  = row.away_participant_id
        comp_id   = row.competition_id

        hk = (tournament_id, home_team)
        ak = (tournament_id, away_team)
        ck = (tournament_id, _normalize_competition_text(comp_text))

        if hk not in home_map:
            home_map[hk] = home_pid
        if ak not in away_map:
            away_map[ak] = away_pid
        if ck not in comp_map:
            comp_map[ck] = comp_id

    return home_map, away_map, comp_map


# ---------------------------------------------------------------------------
# Inference engines
# ---------------------------------------------------------------------------

def infer_general(
    ev: EventRow,
    home_by_sport_comp_team: HomeBySportCompTeam,
    away_by_sport_comp_team: AwayBySportCompTeam,
    comp_by_sport_comp:      CompBySportComp,
    home_by_sport_team:      HomeBySportTeam,
    away_by_sport_team:      AwayBySportTeam,
) -> Dict[str, int]:
    updates: Dict[str, int] = {}
    sport = ev.sport
    comp  = ev.competition
    home  = ev.home_team
    away  = ev.away_team

    if ev.needs_home:
        val = home_by_sport_comp_team.get((sport, comp, home))
        if val is None:
            val = home_by_sport_team.get((sport, home))
        if val is not None:
            updates["home_participant_id"] = val

    if ev.needs_away:
        val = away_by_sport_comp_team.get((sport, comp, away))
        if val is None:
            val = away_by_sport_team.get((sport, away))
        if val is not None:
            updates["away_participant_id"] = val

    if ev.needs_competition:
        val = comp_by_sport_comp.get((sport, comp))
        if val is not None:
            updates["competition_id"] = val

    return updates


def infer_tournament(
    ev: EventRow,
    tournament_id: int,
    home_map: HomeByTournamentTeam,
    away_map: AwayByTournamentTeam,
    comp_map: CompByTournament,
) -> Dict[str, int]:
    updates: Dict[str, int] = {}
    home = ev.home_team
    away = ev.away_team
    comp_text = ev.competition

    if ev.needs_home:
        val = home_map.get((tournament_id, home))
        if val is not None:
            updates["home_participant_id"] = val

    if ev.needs_away:
        val = away_map.get((tournament_id, away))
        if val is not None:
            updates["away_participant_id"] = val

    if ev.needs_competition:
        val = comp_map.get((tournament_id, _normalize_competition_text(comp_text)))
        if val is not None:
            updates["competition_id"] = val

    return updates


# ---------------------------------------------------------------------------
# Target-event loader
# ---------------------------------------------------------------------------

def load_incomplete_events(
    session: Session,
    season_ids: Optional[List[int]] = None,
    sport_filter: Optional[str] = None,
    batch_size: int = 500,
    offset: int = 0,
) -> List[EventRow]:
    query = session.query(
        Event.id,
        Event.sport,
        Event.competition,
        Event.home_team,
        Event.away_team,
        Event.season_id,
        Event.home_participant_id,
        Event.away_participant_id,
        Event.competition_id,
    ).filter(
        or_(
            Event.home_participant_id.is_(None),
            Event.away_participant_id.is_(None),
            Event.competition_id.is_(None),
        ),
        Event.sport.isnot(None),
        Event.home_team.isnot(None),
        Event.away_team.isnot(None),
        Event.competition.isnot(None),
        Event.sport != "",
        Event.home_team != "",
        Event.away_team != "",
        Event.competition != "",
    )

    if season_ids is not None:
        query = query.filter(Event.season_id.in_(season_ids))
    if sport_filter:
        query = query.filter(func.lower(Event.sport) == sport_filter.lower())

    rows = query.order_by(Event.id).offset(offset).limit(batch_size).all()

    return [
        EventRow(
            id=r.id,
            sport=(r.sport or "").lower(),
            competition=(r.competition or "").lower(),
            home_team=(r.home_team or "").lower(),
            away_team=(r.away_team or "").lower(),
            season_id=r.season_id,
            home_participant_id=r.home_participant_id,
            away_participant_id=r.away_participant_id,
            competition_id=r.competition_id,
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# DB update
# ---------------------------------------------------------------------------

def apply_updates(
    session: Session,
    event_id: int,
    updates: Dict[str, int],
    dry_run: bool,
    verbose: bool,
) -> bool:
    if not updates:
        return False
    if dry_run:
        if verbose:
            logger.info("  [DRY-RUN] Event %d — would set: %s", event_id, updates)
        return True
    try:
        set_clauses = ", ".join(f"{col} = :{col}" for col in updates)
        params = {"event_id": event_id, **updates}
        session.execute(
            text(f"UPDATE events SET {set_clauses} WHERE id = :event_id"),
            params,
        )
        if verbose:
            logger.info("  ✅ Event %d — updated: %s", event_id, updates)
        return True
    except Exception as exc:
        logger.error("  ❌ Event %d — update failed: %s", event_id, exc)
        return False


# ---------------------------------------------------------------------------
# Core batch-loop (shared by all modes)
# ---------------------------------------------------------------------------

def run_batch_loop(
    *,
    label: str,
    target_season_ids: Optional[List[int]],
    sport_filter: Optional[str],
    batch_size: int,
    dry_run: bool,
    verbose: bool,
    infer_fn,          # callable(ev: EventRow) -> Dict[str, int]
) -> BackfillStats:
    """
    Generic batch loop.  `infer_fn` is a closure that already holds the lookup
    maps and the tournament_id (if any) for this particular run.
    """
    stats = BackfillStats()
    offset = 0
    batch_num = 0

    while True:
        with db_manager.get_session() as session:
            batch = load_incomplete_events(
                session,
                season_ids=target_season_ids,
                sport_filter=sport_filter,
                batch_size=batch_size,
                offset=offset,
            )

            if not batch:
                break

            batch_num += 1
            logger.info(
                "  [%s] Batch %d — %d events (offset=%d)",
                label, batch_num, len(batch), offset,
            )

            batch_updated = 0
            batch_still_missing = 0

            for ev in batch:
                stats.events_seen += 1
                updates = infer_fn(ev)

                if not updates:
                    batch_still_missing += 1
                    stats.events_still_missing += 1
                    if verbose:
                        missing = [c for c in ("home_participant_id", "away_participant_id", "competition_id")
                                   if getattr(ev, f"needs_{c.split('_')[0]}" if c != "competition_id" else "needs_competition")]
                        logger.debug(
                            "    ⚠️  Event %d — cannot infer %s "
                            "(home=%r, away=%r, comp=%r, season=%s)",
                            ev.id, missing, ev.home_team, ev.away_team, ev.competition, ev.season_id,
                        )
                    continue

                if "home_participant_id" in updates:
                    stats.home_participant_filled += 1
                if "away_participant_id" in updates:
                    stats.away_participant_filled += 1
                if "competition_id" in updates:
                    stats.competition_filled += 1

                applied = apply_updates(session, ev.id, updates, dry_run, verbose)
                if applied:
                    stats.events_updated += 1
                    batch_updated += 1
                else:
                    stats.errors += 1

            logger.info(
                "  [%s] Batch %d — updated: %d, still missing: %d",
                label, batch_num, batch_updated, batch_still_missing,
            )

        if dry_run:
            logger.info("  [%s] DRY-RUN — stopping after first batch preview.", label)
            break

        # If no progress was made, remaining events are unresolvable — stop.
        if batch_updated == 0:
            logger.info(
                "  [%s] No updates in last batch — %d event(s) remain unresolvable.",
                label, batch_still_missing,
            )
            break

        # Offset stays at 0 because updated events drop out of the WHERE filter.

    return stats


# ---------------------------------------------------------------------------
# Mode: Blueprint
# ---------------------------------------------------------------------------

def run_blueprint_mode(
    args: argparse.Namespace,
    blueprint_season_id: int,
    target_season_id: int,
) -> BackfillStats:
    """
    Use events from `blueprint_season_id` as donors to fill events in
    `target_season_id`.  Both seasons must belong to the same tournament.
    """
    bp_tid = SEASON_TO_UNIQUE_TOURNAMENT.get(blueprint_season_id)
    tg_tid = SEASON_TO_UNIQUE_TOURNAMENT.get(target_season_id)

    if bp_tid is None or tg_tid is None:
        logger.warning(
            "  ⚠️  Blueprint or target season not found in season_to_process map "
            "(blueprint=%s → unique_tid=%s, target=%s → unique_tid=%s). Skipping.",
            blueprint_season_id, bp_tid, target_season_id, tg_tid,
        )
        return BackfillStats()

    if bp_tid != tg_tid:
        logger.warning(
            "  ⚠️  Blueprint season %d (unique_tournament %d) and target season %d (unique_tournament %d) "
            "belong to different tournaments. Skipping — use general mode for cross-tournament fills.",
            blueprint_season_id, bp_tid, target_season_id, tg_tid,
        )
        return BackfillStats()

    label = f"blueprint {blueprint_season_id}→{target_season_id} (tid={bp_tid})"
    logger.info("🎯 %s", label)

    with db_manager.get_session() as session:
        home_map, away_map, comp_map = build_tournament_maps(
            session,
            donor_season_ids=[blueprint_season_id],
            tournament_id=bp_tid,
        )

    if not home_map and not away_map and not comp_map:
        logger.warning(
            "  ⚠️  Blueprint season %d has no backfilled events to donate from. Skipping.",
            blueprint_season_id,
        )
        return BackfillStats()

    logger.info(
        "  Maps — home: %d teams, away: %d teams, competition: %d variants",
        len(home_map), len(away_map), len(comp_map),
    )

    def infer_fn(ev: EventRow) -> Dict[str, int]:
        return infer_tournament(ev, bp_tid, home_map, away_map, comp_map)

    return run_batch_loop(
        label=label,
        target_season_ids=[target_season_id],
        sport_filter=None,
        batch_size=args.batch_size,
        dry_run=args.test,
        verbose=args.verbose,
        infer_fn=infer_fn,
    )


# ---------------------------------------------------------------------------
# Mode: Tournament Auto
# ---------------------------------------------------------------------------

def run_tournament_auto_mode(args: argparse.Namespace) -> BackfillStats:
    """
    For every known unique_tournament_id, use already-backfilled events from any
    season in the tournament as donors for incomplete events in the same
    tournament.
    """
    total_stats = BackfillStats()

    for tournament_id, season_ids in sorted(UNIQUE_TOURNAMENT_TO_SEASONS.items()):
        label = f"tournament_auto unique_tid={tournament_id} seasons={season_ids}"
        logger.info("🏆 Processing %s", label)

        # Donors = all seasons in tournament that have complete events
        with db_manager.get_session() as session:
            home_map, away_map, comp_map = build_tournament_maps(
                session,
                donor_season_ids=season_ids,
                tournament_id=tournament_id,
            )

        if not home_map and not away_map:
            logger.info(
                "  ℹ️  No backfilled donor events for tournament %d — skipping.", tournament_id
            )
            continue

        logger.info(
            "  Maps — home: %d teams, away: %d teams, competition: %d variants",
            len(home_map), len(away_map), len(comp_map),
        )

        def infer_fn(ev: EventRow, _tid=tournament_id, _hm=home_map, _am=away_map, _cm=comp_map) -> Dict[str, int]:
            return infer_tournament(ev, _tid, _hm, _am, _cm)

        stats = run_batch_loop(
            label=label,
            target_season_ids=season_ids,
            sport_filter=None,
            batch_size=args.batch_size,
            dry_run=args.test,
            verbose=args.verbose,
            infer_fn=infer_fn,
        )
        total_stats.add(stats)
        logger.info(stats.summary(label=f"tid={tournament_id}"))

    return total_stats


# ---------------------------------------------------------------------------
# Mode: General
# ---------------------------------------------------------------------------

def run_general_mode(args: argparse.Namespace) -> BackfillStats:
    """Sport + team-text matching — no tournament awareness."""
    logger.info("🔍 Running general mode (sport=%s)", args.sport or "all")

    with db_manager.get_session() as session:
        (
            home_by_sport_comp_team,
            away_by_sport_comp_team,
            comp_by_sport_comp,
            home_by_sport_team,
            away_by_sport_team,
        ) = build_general_maps(session, sport_filter=args.sport)

    if not home_by_sport_comp_team and not home_by_sport_team:
        logger.warning("⚠️  No donor events found — nothing can be inferred.")
        return BackfillStats()

    def infer_fn(ev: EventRow) -> Dict[str, int]:
        return infer_general(
            ev,
            home_by_sport_comp_team,
            away_by_sport_comp_team,
            comp_by_sport_comp,
            home_by_sport_team,
            away_by_sport_team,
        )

    return run_batch_loop(
        label=f"general sport={args.sport or 'all'}",
        target_season_ids=None,
        sport_filter=args.sport,
        batch_size=args.batch_size,
        dry_run=args.test,
        verbose=args.verbose,
        infer_fn=infer_fn,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill event FK columns using data already in the DB (no API calls).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Mode selection ────────────────────────────────────────────────────
    mode_group = parser.add_argument_group(
        "Mode",
        "Select one mode. Defaults to general mode if none is specified.",
    )
    mode_group.add_argument(
        "--blueprint-season",
        type=int,
        metavar="SEASON_ID",
        help=(
            "Donor season_id. Use events from this season to fill another season. "
            "Must be used together with --target-season."
        ),
    )
    mode_group.add_argument(
        "--target-season",
        type=int,
        metavar="SEASON_ID",
        help=(
            "Receiving season_id. Events in this season will be backfilled "
            "from the blueprint season. Must be used together with --blueprint-season."
        ),
    )
    mode_group.add_argument(
        "--tournament-auto",
        action="store_true",
        help=(
            "Automatically backfill every tournament group defined in season_to_process. "
            "For each tournament, uses backfilled events from any season in the group "
            "as donors for incomplete events in the same group."
        ),
    )

    # ── General mode options ──────────────────────────────────────────────
    gen_group = parser.add_argument_group(
        "General mode options",
        "Options that apply when running in general (default) mode.",
    )
    gen_group.add_argument(
        "--sport",
        type=str,
        default=None,
        metavar="SPORT",
        help="Restrict general mode to events of this sport (e.g. Basketball, Football).",
    )

    # ── Common options ────────────────────────────────────────────────────
    common_group = parser.add_argument_group("Common options")
    common_group.add_argument(
        "--test",
        action="store_true",
        help="Dry-run: compute inferences and log them but do NOT write to DB.",
    )
    common_group.add_argument(
        "--batch-size",
        type=int,
        default=500,
        metavar="N",
        help="Number of incomplete events to fetch per DB query (default: 500).",
    )
    common_group.add_argument(
        "--verbose",
        action="store_true",
        help="Log every event update/skip individually.",
    )
    common_group.add_argument(
        "--list-seasons",
        action="store_true",
        help="Print the known season→tournament mapping and exit.",
    )

    args = parser.parse_args()

    # Validation
    bp = args.blueprint_season
    tg = args.target_season
    ta = args.tournament_auto

    if bp is not None and tg is None:
        parser.error("--blueprint-season requires --target-season")
    if tg is not None and bp is None:
        parser.error("--target-season requires --blueprint-season")
    if (bp is not None) and ta:
        parser.error("--blueprint-season/--target-season and --tournament-auto are mutually exclusive")

    return args


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    # ── --list-seasons helper ─────────────────────────────────────────────
    if args.list_seasons:
        print("\nKnown season → tournament_id mapping:\n")
        print(f"  {'season_id':>10}  {'tournament_id':>14}  season_name")
        print(f"  {'-'*10}  {'-'*14}  {'-'*30}")
        for entry in season_to_process:
            print(
                f"  {entry['season_id']:>10}  {entry['tournament_id']:>14}  {entry['season_name']}"
            )
        print()
        return

    logger.info("=" * 60)
    logger.info("BACKFILL EVENT METADATA — DB-ONLY (no API calls)")
    if args.test:
        logger.info("🔍 DRY-RUN — no changes will be written to the DB")

    # ── Connection test ───────────────────────────────────────────────────
    if not db_manager.test_connection():
        logger.error("❌ Database connection failed. Exiting.")
        sys.exit(1)
    logger.info("✅ Database connection OK")

    # ── Dispatch to mode ──────────────────────────────────────────────────
    if args.blueprint_season is not None:
        logger.info("Mode: BLUEPRINT  %d → %d", args.blueprint_season, args.target_season)
        logger.info("=" * 60)
        stats = run_blueprint_mode(args, args.blueprint_season, args.target_season)

    elif args.tournament_auto:
        logger.info("Mode: TOURNAMENT AUTO  (%d unique_tournament groups)", len(UNIQUE_TOURNAMENT_TO_SEASONS))
        logger.info("=" * 60)
        stats = run_tournament_auto_mode(args)

    else:
        logger.info("Mode: GENERAL  (sport=%s)", args.sport or "all")
        logger.info("=" * 60)
        stats = run_general_mode(args)

    logger.info(stats.summary(label="FINAL TOTALS"))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interrupted by user (Ctrl+C)")
        sys.exit(130)
    except Exception as exc:
        logger.exception("Unhandled error: %s", exc)
        sys.exit(1)
