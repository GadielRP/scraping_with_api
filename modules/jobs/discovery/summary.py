"""Readable discovery configuration, admission counts and committed calendar audit."""

from dataclasses import fields
from textwrap import fill

from infrastructure.settings import Config
from infrastructure.settings import discovery as settings
from modules.sports.catalog import configured_sport_ids


EVENT_FILTER_REASONS = (
    "unsupported_sport", "untracked_competition", "missing_source_competition_ids",
    "excluded_sport", "excluded_category", "excluded_competition",
    "tennis_ranking_excluded", "missing_start_timestamp", "invalid_start_timestamp",
    "start_too_soon_or_started", "round_not_allowed", "invalid_event_payload",
    "missing_required_fields", "normalization_error",
)
TOURNAMENT_FILTER_REASONS = (
    "untracked_competition", "missing_source_competition_ids",
    "excluded_category", "excluded_competition",
)


def log_discovery_configuration(logger, *, job, tracked_competitions):
    """Snapshot the loaded policy, including disabled switches and exact denylists."""
    lines = [
        f"  timezone={Config.TIMEZONE}",
        f"  configured_sports={','.join(sorted(configured_sport_ids()))}",
        "  tracked_competition_mappings=" + (
            "disabled" if tracked_competitions is None else str(len(tracked_competitions))
        ),
    ]
    for group in (settings.SOFASCORE.filters, settings.SOFASCORE):
        for item in fields(group):
            if item.name == "filters":
                continue
            value = getattr(group, item.name)
            if isinstance(value, frozenset):
                value = sorted(value)
            lines.append(fill(
                f"{item.name}={value}", width=110,
                initial_indent="  ", subsequent_indent="    ",
                break_long_words=False, break_on_hyphens=False,
            ))
    lines.extend((
        f"  dropping_times={','.join(settings.SOFASCORE.dropping_times)}",
        f"  secondary_times={','.join(settings.SOFASCORE.secondary_times)}",
        "  filter_counts=first_rejection_per_occurrence; tournaments_are_not_events",
        "  tennis_missing_ranking=allowed; known_rank_at_or_above_cutoff=rejected",
    ))
    logger.info("Discovery configuration job=%s source=sofascore\n%s", job, "\n".join(lines))


def log_filter_counts(logger, *, source, stage, received, accepted, reasons, unit="events"):
    """Show every admission reason, including zeros, without retaining payloads."""
    defaults = TOURNAMENT_FILTER_REASONS if unit == "tournaments" else EVENT_FILTER_REASONS
    names = (*defaults, *sorted(set(reasons) - set(defaults)))
    rows = "\n".join(f"  {name:<36} {reasons.get(name, 0):>7}" for name in names)
    logger.info(
        "Discovery filters source=%s stage=%s unit=%s received=%s accepted=%s filtered=%s\n%s",
        source, stage, unit, received, accepted, sum(reasons.values()), rows,
    )


def log_daily_filter_summary(logger, stats, *, date, run_slot):
    context = f"daily_discovery requested_date={date} slot={run_slot}"
    log_filter_counts(
        logger, source=context, stage="source", received=stats["events_received"],
        accepted=stats["events_admitted"], reasons=stats["source_filter_reasons"],
    )
    log_filter_counts(
        logger, source=context, stage="before_write", received=stats["events_processed"],
        accepted=stats["events_processed"] - stats["events_filtered_before_write"],
        reasons=stats["write_filter_reasons"],
    )
    log_filter_counts(
        logger, source=context, stage="calendar", unit="tournaments",
        received=stats["tournaments_received"],
        accepted=stats["tournaments_received"] - stats["tournaments_filtered"],
        reasons=stats["tournament_filter_reasons"],
    )
    logger.info(
        "Daily discovery summary requested_date=%s slot=%s\n"
        "  events: received=%s admitted=%s filtered=%s unprocessed=%s\n"
        "  writes: processed=%s persisted=%s inserted=%s updated=%s "
        "discard_memory=%s failed=%s\n"
        "  tournaments: received=%s filtered=%s skipped=%s skip_reasons=%s\n"
        "  events_in_filtered_tournaments=unknown (not_downloaded)\n"
        "  odds_inserted=%s sports_failed=%s",
        date, run_slot, stats["events_received"], stats["events_admitted"],
        stats["events_filtered"], stats["events_unprocessed"], stats["events_processed"],
        stats["events_persisted"], stats["events_inserted"], stats["events_updated"],
        stats["events_blocked_by_discard_memory"], stats["events_failed"],
        stats["tournaments_received"], stats["tournaments_filtered"],
        stats["tournaments_skipped"], stats["tournament_skip_reasons"],
        stats["odds_inserted"], stats["sports_failed"],
    )


def log_discovery_summary(store, logger, *, job, requested_date=None, run_slot=None):
    total = 0
    for sport, event_date, count in store.counts():
        total += count
        logger.info(
            "Discovery calendar job=%s requested_date=%s slot=%s timezone=%s "
            "sport=%s event_date=%s persisted_unique=%s",
            job,
            requested_date,
            run_slot,
            Config.TIMEZONE,
            sport,
            event_date,
            count,
        )
    logger.info("Discovery persistence summary job=%s persisted_unique=%s", job, total)
