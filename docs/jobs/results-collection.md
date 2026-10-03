# Results collection and maintenance scheduling

## Execution

Midnight remains scheduled at 04:00 in Config.TIMEZONE. Midnight, Daily Discovery,
Dropping Odds and Secondary Sources scheduled triggers submit to one maintenance
worker. The main scheduler can continue dispatching ordinary pre-start checks.
The worker admits at most eight distinct active/queued job keys; duplicate active
or queued triggers are suppressed and logged. The queue is in-process, not durable.
Manual CLI entrypoints remain synchronous. Other scheduler jobs have not been moved.

Shutdown rejects new submissions, cancels queued work, and signals cooperative
cancellation to active work. Current HTTP calls are allowed to return; open database
transactions finish/roll back normally. No thread is forcibly terminated.

SofaScore requests share the existing client rate limiter. Foreground requests
waiting for admission precede maintenance requests. This does not preempt requests
already on the network. Discovery's existing worker pools inherit the background
context. The limiter remains per client instance/process, not a distributed quota.
Results collection still requests one event at a time; no extra HTTP concurrency
or increase in provider rate is introduced.

## Selection and batches

`ResultRepository.pending_batches()` selects events without a Result row, with
SofaScore mappings in the same query. Date-based collection uses the configured
local day's UTC bounds. All-finished collection retains its sport-duration policy.
Missing or ambiguous SofaScore mappings remain pending and are reported as failures;
multiple mappings cannot duplicate a canonical event within a page. Response IDs
are checked before parsing or queuing deletions.

Pages use increasing canonical IDs and a fixed initial upper ID. Each read session
closes before HTTP begins. Failures are visited once per invocation, not repeatedly
within the same page loop. New events beyond the upper bound wait for the next run.
An existing incomplete Result row is still treated as present, matching prior policy.

The existing `EVENT_WRITE_BATCH_SIZE` controls page size, event writes and result
writes (default 100). The shared bounded iterator was moved to `shared/batching.py`;
there is no second independent batching framework.

Each page fetches authoritative responses and uses the shared result parser/policy.
It then updates metadata through the existing event batch writer, writes results
through SQL ON CONFLICT upserts, processes observations, and applies guarded batch
deletions. Metadata, results and deletions use separate bounded transactions: a
page is not one atomic transaction. HTTP never runs inside a write transaction.

Expected canonical IDs prevent late metadata responses from recreating deleted
or remapped events. Result writes lock existing parent rows and omit parents removed
before the lock. Classified and 404 deletion evidence is checked against identity,
newer metadata and existing results. Only configured parser kinds enter discard
memory; 404 evidence does not enter the default canceled-only memory.

## Recovery and errors

Re-running `results-date` for the same date selects only remaining events. Committed
results act as progress markers; no cursor table is required. To revisit a previous
date after process downtime, explicitly run that date again. The scheduler does
not replay missed midnight triggers or automatically reconcile older dates.

Infrastructure failures propagate rather than returning a successful zero count.
Individual provider/parsing failures remain pending and increment `failed`.
`skipped` is retained for return-shape compatibility but is zero: existing results
are filtered in SQL rather than loaded and counted individually.

Collection logs fetch/parse time, persistence time, candidates, last canonical ID
and outcome per page. Result-write logs report commits even when a later operation
in that page fails. Maintenance logs distinguish queue waiting, execution and errors.
Observation persistence retains its existing best-effort semantics; re-running
collection does not repair missing observations on already-completed results.

## Reporting refresh

Midnight keeps results -> prediction updates -> reporting refresh order. A failure
leaves a refresh pending for a later Daily Discovery heartbeat. Ordinary skipped
heartbeats do not refresh unless recovery is pending; startup marks recovery pending
once. Both materialized views still refresh synchronously inside maintenance with
the existing SQL, so database reader blocking is still possible.

## Validation

Tests cover keyset pagination while rows disappear, bounded commits, retry after a
partially committed run, discard memory, late-response identity guards, PostgreSQL
parent-row locking, duplicate/capacity admission, scheduler responsiveness,
cooperative shutdown, request priority, and refresh failure recovery.

PostgreSQL integration tests use only a disposable database supplied through
`RESULTS_TEST_DATABASE_URL` or `DISCARD_TEST_DATABASE_URL`. Their fixtures create
and drop tables; never point these variables at the application database.
