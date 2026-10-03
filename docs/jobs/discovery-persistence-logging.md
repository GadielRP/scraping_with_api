# Discovery persistence logging

Daily Discovery, Dropping Odds, and Secondary Sources emit a calendar summary at the end of their discovery run. The shared accumulator is implemented in `modules/jobs/discovery_persistence_summary.py`.

## Calendar and scope

The summary records only events returned by successful committed event writes. It uses the persisted `Event.starts_at`, converted to `Config.TIMEZONE`, to determine `event_date`, and the persisted `Event.sport` for the sport grouping. This is the same local calendar basis used by date-based Results Collection.

Daily Discovery includes `requested_date` and `slot` as separate context fields. Dropping Odds and Secondary Sources do not query one calendar date and record `none` for these fields. All summaries include `job` and `timezone`.

## Counts

`persisted_unique` counts canonical event IDs once per job invocation, including both created and updated events. Repeated writes through different batches, tournament routes, or secondary sources do not increase this count. If an event is written again with a different kickoff or sport within the same run, its latest persisted values determine the final grouping.

The log emits one multiline record with the job context and unique run total in its header. Its body groups counts by sport, with chronologically sorted local dates under each sport, followed by totals for each date. Sports are sorted alphabetically. A run with no committed events emits only the header with a unique total of zero. A missing or invalid aware kickoff is grouped under the date `unknown`; a missing sport is grouped under `Unknown`.

Filtered events, discard-memory exclusions, and failed event writes are excluded. A committed event remains included if optional odds persistence subsequently fails. The existing operation and odds counters remain separate from the calendar summary.

## Interpretation

Counts describe the events written during one invocation; they are not a snapshot of all events in the database. Totals from separate invocations overlap and cannot be summed to count unique events across a day. Subsequent rescheduling or deletion can change the set selected by Results Collection.

The summary accumulates committed event metadata in memory and performs no additional database queries or provider requests.
