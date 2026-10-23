# Discovery persistence logging

Daily Discovery, Dropping Odds, and Secondary Sources emit a calendar summary at the end of their discovery run. Committed identities and calendar metadata are stored in `infrastructure/persistence/transient/discovery_run_store.py`; `modules/jobs/discovery/summary.py` formats the final audit. The coordinator owns the store through a context manager and closes it even when acquisition, persistence or budget checks interrupt the run.

## Calendar and scope

The summary records only events returned by successful committed event writes. It uses the persisted `Event.starts_at`, converted to `Config.TIMEZONE`, to determine `event_date`, and the persisted `Event.sport` for the sport grouping. This is the same local calendar basis used by date-based Results Collection.

Daily Discovery includes `requested_date` and `slot` as separate context fields. Dropping Odds and Secondary Sources do not query one calendar date and record `none` for these fields. All summaries include `job` and `timezone`.

## Counts

`persisted_unique` counts canonical event IDs once per job invocation, including both created and updated events. Repeated writes through different batches, tournament routes, or secondary sources do not increase this count. If an event is written again with a different kickoff or sport within the same run, its latest persisted values determine the final grouping.

The log emits one `Discovery calendar` record per sport/local-date group, followed by a `Discovery persistence summary` record with the unique run total. Groups are ordered by sport/date. An empty run emits the total of zero. A missing or invalid aware kickoff is grouped under the date `unknown`; a missing sport is grouped under `Unknown`.

Filtered events, discard-memory exclusions, and failed event writes are excluded. A committed event remains included if optional odds persistence subsequently fails. The existing operation and odds counters remain separate from the calendar summary.

Daily also logs each write batch with its source-admission counts and a final
`Daily discovery summary` after the calendar audit. `events_filtered` includes
source and pre-write filter rejections, with combined `event_filter_reasons`
and separate stage counts. `events_blocked_by_discard_memory` counts prior
discard-memory matches independently. Tournament filters and other calendar
skips have their own counts and reasons; they are never added to event totals.
An admission-only trailing batch is logged even when no event reaches a write,
and interrupted partial batches report their admitted `events_unprocessed`.
These counts describe received occurrences, whereas `persisted_unique` describes
canonical identities. The logging counters are bounded by the number of reasons
and add no database queries or payload retention.

## Loaded policy and admission audit

Each Daily, Dropping Odds and Secondary Sources run logs `Discovery configuration`
with the loaded SofaScore settings, every shared filter switch and exact denylist,
configured sports, mapped competition scope, timezone, worker limits and schedules.
This snapshot reflects the running process; editing `infrastructure/settings/discovery.py`
requires an application restart before new settings take effect.

`Discovery filters` records have a readable row for every rejection reason,
including zeros. The header identifies the source, stage, count unit, received,
accepted and filtered occurrences. Feed extraction includes metadata, policy and
normalization rejections in the same total. Nearest team event lookups now report
policy rejections at INFO; missing responses and request errors are separate from
received event counts. Persistence revalidation names the originating feed and
logs discard-memory blocks separately. Revalidation counts must not be added to
feed received counts: these are the same events checked at a later stage.

Daily's final audit separates source, pre-write and tournament filters, followed
by a compact multiline summary of event writes, skips, failures and odds.
Accepted means passing that stage's filters, not necessarily persisted.
Each occurrence is assigned only its first rejection reason, so reason totals
are disjoint within one stage, not independent evaluations of every filter.
Tournaments rejected before download have an unknown number of filtered events;
their event count is explicitly `unknown (not_downloaded)` and is never estimated
or added to the received event rejection total.

## Interpretation

Counts describe the events written during one invocation; they are not a snapshot of all events in the database. Totals from separate invocations overlap and cannot be summed to count unique events across a day. Subsequent rescheduling or deletion can change the set selected by Results Collection.

The summary stores committed metadata in a disk-backed temporary SQLite table, with a bounded page cache, and groups it in SQL. It makes no extra queries against PostgreSQL and no provider requests.

Membership and calendar metadata are committed together in one SQLite transaction per recorded mapping batch. No unused kickoff timestamp or second in-memory accumulator is retained.
