"""CLI entry point for the Oddspapi fixture discovery job."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import logging

from modules.oddspapi.client import OddsPapiClient

from .constants import (
    DEFAULT_LOOKAHEAD_DAYS,
    DEFAULT_PERSIST_QUEUE,
    DEFAULT_STATUS_ID,
    DISCOVERY_SPORT_IDS,
)
from .fixture_discovery_job import OddspapiFixtureDiscoveryJob
from .response_utils import as_utc_datetime


def _parse_datetime_argument(value: str) -> datetime:
    try:
        return as_utc_datetime(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(f"invalid UTC date/time: {value}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover and map Oddspapi fixtures.")
    parser.add_argument("--from-date", type=_parse_datetime_argument)
    parser.add_argument("--to-date", type=_parse_datetime_argument)
    parser.add_argument("--date", type=str, help="UTC calendar day, YYYY-MM-DD")
    parser.add_argument("--lookahead-days", type=int, default=DEFAULT_LOOKAHEAD_DAYS)
    parser.add_argument("--sports", type=str, help="Comma-separated sport slugs")
    parser.add_argument("--status-id", type=int, default=DEFAULT_STATUS_ID)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Do not write mappings or queue rows")
    mode.add_argument("--commit", action="store_true", help="Persist successful mappings")
    parser.add_argument("--persist-queue", action="store_true")
    parser.add_argument("--max-fixtures-per-sport", type=int)
    parser.add_argument("--log-json", action="store_true")
    return parser


def _resolve_window(args: argparse.Namespace) -> tuple[datetime, datetime]:
    if args.date and (args.from_date or args.to_date):
        raise ValueError("--date cannot be combined with --from-date/--to-date")
    if args.lookahead_days <= 0:
        raise ValueError("--lookahead-days must be positive")

    if args.date:
        try:
            day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError as exc:
            raise ValueError("--date must use YYYY-MM-DD") from exc
        return day, day + timedelta(days=args.lookahead_days)

    if args.from_date or args.to_date:
        if not args.from_date or not args.to_date:
            raise ValueError("--from-date and --to-date must be provided together")
        return args.from_date, args.to_date

    return current_utc_day_window(lookahead_days=args.lookahead_days)


def _resolve_sports(value: str | None) -> dict[str, int]:
    if not value:
        return dict(DISCOVERY_SPORT_IDS)
    requested = [slug.strip().casefold() for slug in value.split(",") if slug.strip()]
    unknown = [slug for slug in requested if slug not in DISCOVERY_SPORT_IDS]
    if unknown:
        raise ValueError(
            f"unknown sport slug(s): {', '.join(unknown)}; "
            f"supported: {', '.join(DISCOVERY_SPORT_IDS)}"
        )
    if not requested:
        raise ValueError("--sports must contain at least one sport slug")
    return {slug: DISCOVERY_SPORT_IDS[slug] for slug in requested}


def current_utc_day_window(
    *,
    now: datetime | None = None,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
) -> tuple[datetime, datetime]:
    """Return current UTC time through the requested lookahead."""
    if lookahead_days <= 0:
        raise ValueError("lookahead_days must be positive")
    start = as_utc_datetime(now or datetime.now(timezone.utc))
    return start, start + timedelta(days=lookahead_days)


def run_fixture_discovery_job(
    *,
    target_date: date | str | None = None,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
    sports: dict[str, int] | None = None,
    create_mappings: bool = True,
    persist_queue: bool = DEFAULT_PERSIST_QUEUE,
    status_id: int = DEFAULT_STATUS_ID,
    max_fixtures_per_sport: int | None = None,
    client: OddsPapiClient | None = None,
    now: datetime | None = None,
):
    """Run discovery for a UTC calendar day using all configured sports by default."""
    if target_date is None:
        from_date, to_date = current_utc_day_window(
            now=now,
            lookahead_days=lookahead_days,
        )
    else:
        if isinstance(target_date, str):
            target_date = date.fromisoformat(target_date)
        if lookahead_days <= 0:
            raise ValueError("lookahead_days must be positive")
        from_date = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
        to_date = from_date + timedelta(days=lookahead_days)

    return OddspapiFixtureDiscoveryJob(
        client=client,
        sports=sports or dict(DISCOVERY_SPORT_IDS),
        create_mappings=create_mappings,
        persist_queue=persist_queue,
        status_id=status_id,
        max_fixtures_per_sport=max_fixtures_per_sport,
    ).run(from_date, to_date)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        from_date, to_date = _resolve_window(args)
        sports = _resolve_sports(args.sports)
        if args.max_fixtures_per_sport is not None and args.max_fixtures_per_sport <= 0:
            raise ValueError("--max-fixtures-per-sport must be positive")
        job = OddspapiFixtureDiscoveryJob(
            sports=sports,
            create_mappings=bool(args.commit and not args.dry_run),
            persist_queue=bool(args.persist_queue and args.commit and not args.dry_run),
            status_id=args.status_id,
            max_fixtures_per_sport=args.max_fixtures_per_sport,
        )
        summary = job.run(from_date, to_date)
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))

    if args.log_json:
        print(json.dumps(summary.to_dict(), default=lambda value: value.isoformat()))
    else:
        print(
            f"Oddspapi discovery complete: fixtures={summary.total_fixtures_fetched} "
            f"mappings_created={summary.total_mappings_created}"
        )
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(main())
