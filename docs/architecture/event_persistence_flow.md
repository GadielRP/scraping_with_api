# SofaScore event and result persistence

## Responsibilities

| Owner | Responsibility |
|---|---|
| `app/runtime.py` | Compose dependencies once, bind occurrence date/deadline and dispatch the use case to its worker |
| `infrastructure/scheduler/schedules.py` | Configure the same calendar for execution and status; no provider work or persistence |
| `modules/jobs/daily_discovery/event_source.py` | Incremental tournament/event acquisition, sport/scope filtering and tournament deduplication |
| `modules/jobs/daily_discovery/persistence.py` | Normalize a filtered batch and count committed event writes |
| `modules/jobs/discovery/fetching.py` | Bounded concurrent nearest-event and odds requests |
| `modules/jobs/discovery/persistence.py` | Explicit event admission policies and bounded event/odds writes |
| `modules/jobs/discovery/summary.py` | Format the committed calendar audit without owning storage lifecycle |
| `infrastructure/persistence/transient/discovery_run_store.py` | Exact canonical calendar and provider membership on disk with a bounded SQLite cache |
| `infrastructure/persistence/repositories/event_batch_writer.py` | Canonical identities, reference preloads, discard checks and SQL writes in bounded transactions |
| `modules/jobs/results_collection_job/batch_processor.py` | Fetch/classify a result page, then commit metadata, results and guarded deletions |

## Event identity and writes

`event_normalizer.normalize_event_payload` produces `event`, `home_participant`, `away_participant` and `competition_ref`. SofaScore source IDs remain external IDs; `Event.id` is canonical. Participants and competitions carry their provider identities separately.

`EventRepository.batch_upsert_events` uses the common batch writer. It preloads source mappings, discard memory and shared references once per write chunk. Identity locks and a transactional discard recheck protect against discovery recreating an event while a result collector removes it. No HTTP calls occur inside event write transactions.

`EventWriteResult.events` contains confirmed events keyed by source ID, with separate inserted, updated, discarded and error outcomes. Result synchronization supplies expected canonical IDs, preventing a delayed response from recreating a removed or remapped parent. Single-response backfills still use the shared writer through `upsert_event`; these remaining consumers are tracked in the [legacy cleanup list](../maintenance/execution-legacy-cleanup.md).

## Discovery policies

- Daily discovery persists all filtered calendar events, including past events. It reads bounded batches, prechecks discard memory before normalization, and writes through the common writer. A separate streamed odds pass joins only committed run members and rechecks live identity/kickoff before saving future-event odds. See [daily discovery](../jobs/daily-discovery.md).
- Dropping/winning feeds use `persist_events_with_odds`: eligible events survive missing odds; available feed entries use `save_from_dropping_odds_map_entry`.
- Team streaks uses `fetch_and_persist_events_with_odds`: only successful odds responses reach the event writer. Confirmed endpoint 404s update existing source mappings; temporary failures are distinguished from missing endpoints.
- High-value streaks and H2H use `persist_events`, without an odds requirement.

Each coordinator owns a `DiscoveryRunStore` through a context manager. Confirmed canonical IDs and final local calendar metadata are recorded together in one SQLite transaction per mapping batch. The summary counts each canonical ID once, including updates. Optional odds failures do not undo a confirmed event. See [audit interpretation](../jobs/discovery-persistence-logging.md).

Secondary feeds are fetched and persisted in sequence. Daily remains incremental; some secondary provider responses are still materialized and are listed in the legacy cleanup task. Bounded futures do not make a materialized response incremental.

## Odds ingestion

`MarketOddsIngestionService.save_from_sofascore_response` accepts the normalized response; the dropping/daily entry adapters normalize their provider-specific shapes. Market/choice normalization and persistence remain in the existing odds ingestion service and repositories. All writes use the confirmed canonical event ID.

Odds persistence currently operates per event after the event batch transaction. The event batch writer is a real batch writer; it does not imply that odds snapshots are also written in one SQL batch.

## Result collection and reporting

`run_results_collection(target_date=None)` is the sole collection use case. An explicit date selects that local day; omission selects sufficiently old incomplete events. CLI `results` resolves yesterday locally, `results-date` supplies a date and `results-all` omits it. Midnight binds yesterday to its original calendar occurrence before queueing.

`ResultRepository.pending_batches` uses keyset pages and a fixed high water mark. A row needs both scores to be complete; mappings are selected without duplicating canonical candidates, and ambiguous/missing mappings are not sent to the provider. Each read session closes before HTTP work.

`collect_batch` fetches the authoritative event response, checks identity and classifies it through the existing results parser. It batches metadata writes, persists results/observations atomically and applies guarded deletion evidence. It does not fetch historical final odds. Partial confirmed work is retained when resource/deadline checks defer the remainder; re-running selects unresolved rows again.

Relevant source writes invalidate reporting in their transaction. The `view_refresh` job coalesces pending generations and refreshes the two materialized views independently. Scheduler queues and the reporting generation table solve different responsibilities. See [results collection and execution](../jobs/results-collection.md).
