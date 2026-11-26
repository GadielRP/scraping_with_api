# Sports Data & Prediction Platform
## Overview

This repository implements a modular monolith for ingesting sports events and odds, analysing prediction signals and delivering structured alerts. This project connects to SofaScore via https requests to its API to feed the db from events
 and scrapes supplemental data from OddsPortal, persists everything in PostgreSQL, runs rule‑based algorithms (dual process, matchup streak, basketball 4th‑quarter projections and more) and dispatches notifications via Telegram.

Key capabilities include:
*   Event discovery and persistence – multiple discovery sources scan SofaScore feeds for upcoming games, deduplicate them and store events and opening odds in a unified schema.
*   Selective odds extraction – key moments (120, 30, 5, 1, 0, and −5 minutes around kick‑off) trigger odds snapshots and market persistence. Alerts are dispatched according to configured precision alert gates (e.g. 30 and 5 minutes).
*   Prediction & alerting pipeline – a multi-stage workflow synchronises supplemental OddsPortal data, evaluates dual process and matchup streak rules, and dispatches formatted notifications via Telegram. A separate basketball 4Q predictor uses historical momentum and rhythm models.
*   Pillar signal architecture – modular analytical pillars (P1 through P5) evaluate structural strengths, market contexts, and price trajectories, persisting profiles through a standardized mining layer.
*   Scheduled jobs with CLI – a built‑in scheduler runs discovery, pre‑start checks, midnight result collection and daily discovery at configurable times. All jobs can also be triggered ad‑hoc through main.py commands.
*   Extensible modules – the code is organised into clearly defined packages: alerts, jobs, observations, oddsportal scraping, prediction, infrastructure and shared utilities. Adding a new alert type or job only requires implementing a module under the appropriate package and wiring it through the CLI.

The remainder of this document describes the project structure, installation and configuration, usage patterns and core modules in detail.

## Project Structure

```text
root_dir/
  main.py                      # entrypoint and CLI integration
  app/                         # application layer: CLI parsing, initialization & commands
    cli.py
    initialize.py
    logging_setup.py
    commands/                  # individual CLI command implementations
  docs/                        # technical architecture, pillar blueprints, and provider guides
    architecture/              # contracts, canonical market catalogs, and DB ingestion context
    pillars/                   # pillar specs, inputs/lifecycle context, and mining persistence
    providers/                 # odds ingestion, SofaScore API architecture, OddsPortal scraping & snapshots
    jobs/                      # scheduled job workflows and discovery architecture
    alerts/                    # alert systems and real-time prediction engines
    operations/                # cloud deployment, Docker, and environment operations guides
    archive/                   # historical migration records and audit reports
  modules/                     # domain logic organised by feature
    alerts/                    # alert transport & formatting engines
    jobs/                      # scheduled and ad‑hoc jobs
    observations/              # observations & sport‑specific enrichments
    oddsportal/                # Playwright scraper & models for OddsPortal
    odds_ingestion/            # canonical multi-provider odds ingestion service & adapters
    oddspapi/                  # Oddspapi client, quota scheduler & candidate matching
    pillars/                   # modular prediction pillars (P1–P5), mining layer & EventContext
    prediction/                # prediction logging & utilities
    sofascore/                 # SofaScore API client & helpers
  infrastructure/              # cross‑cutting concerns: networking, persistence, scheduling, settings
  shared/                      # small helpers used across modules
  scripts/                     # one‑off scripts and maintenance tasks
```
  
## Entry Points (main.py & app/)

The application is driven by main.py, which delegates to app/cli.py. cli.py defines a command‑line parser and registers sub‑commands for each operational mode. When main.py is executed, it initialises logging, loads environment variables and invokes the appropriate handler. Available commands include:

| Command | Description |
| :--- | :--- |
| start | Checks the deployed database schema and starts the scheduler. |
| discovery | Triggers Discovery A immediately – fetches dropping odds feeds and sport‑specific dropping endpoints, deduplicates and stores events. |
| discovery2 | Runs Discovery B – pulls high‑value streaks, head‑to‑head (H2H), winning odds and other special feeds. |
| pre-start | Executes the pre‑start cycle now – captures upcoming events, applies timestamp corrections, extracts odds snapshots at key moments and evaluates alerts. |
| midnight | Performs the midnight sync job – collects results and updates prediction logs. |
| results | Collects results for the previous local day. |
| results-date --date YYYY-MM-DD | Collects results for the specified date. |
| oddspapi-fixture-discovery | Discovers Oddspapi fixture IDs for the UTC day and maps them to existing canonical events. It does not create events or ingest odds. |
| oddspapi-prematch-odds-recovery --date YYYY-MM-DD | Replays OddsPapi historical recovery for a local date and writes eligible openings and snapshots to existing quotes. |
| results-all | Attempts collection for sufficiently old events with incomplete results. |
| daily-discovery | Runs daily sports discovery – extracts today’s events and odds for each sport. |
| backfill-results --limit N | Backfills missing results history up to N events. |
| status | Prints database and scheduler status information. |
| events --limit N | Displays a summary of recently discovered events. |
| refresh-alerts | Requests and attempts refresh of the reporting views. |

Run `python main.py --help` to list supported commands and options.

## Modules
### Alerts (modules/alerts)

Alert‑related logic lives under modules/alerts/. It contains a Telegram transport for sending messages and a set of formatters used by different alert types. The telegram_notifier.py module wraps the Telegram Bot API and automatically splits long messages into safe chunks. Sub‑packages include:

alerts_formatter/ – classes responsible for constructing human‑readable alert messages. Each alert type (odds, dual process, matchup streak, Q4, time correction) implements a formatter here.
basketball_4q/ – prediction engine for basketball fourth‑quarter analysis. It uses team rhythm, momentum and statistical ranges to project performance and exposes a run_basketball_4q.py script for manual execution. See [`docs/alerts/basketball-4q.md`](docs/alerts/basketball-4q.md).
dual_process/ – implements the dual process alert strategy. The process_1 and process_2 submodules handle candidate search, evaluation and sport‑specific rules for football and other sports. The top‑level run_dual_process.py entrypoint orchestrates both phases.
matchup_streak_analysis/ – analyses head‑to‑head records, historical form and standings. It defines constants.py, head_to_head.py, historical_form.py, standings_engine.py, standings_rules.py, standings_simulator.py and winning_odds.py. The run_matchup_streak_analysis.py script triggers this pipeline and uses the materialised view mv_alert_events for candidate lookup.
### Jobs (modules/jobs)

The application runtime dispatches jobs to separate pre-start, closing and maintenance workers. Highlights include:

clean_league_cache/ – clears stale OddsPortal league cache rows before the day’s discovery.
daily_discovery/ – runs an advance pass at 17:07 and an update at 08:07, with fixed UTC target dates and per-sport retries every 30 minutes, including overnight. It maintains progress via `DailyDiscoveryRepository`. See [`docs/jobs/daily-discovery.md`](docs/jobs/daily-discovery.md).
discover_dropping_odds/ & discover_secondary_sources/ – run the A and B discovery paths. Secondary sources include high‑value streaks, team streaks, H2H, winning odds and optimisation filters.
midnight_sync_job/ – runs at 04:00 in `Config.TIMEZONE`, collects results and updates prediction logs.
oddspapi/ – maps provider fixtures to canonical events and runs separate [prematch odds recovery](docs/jobs/results-collection.md#oddspapi-prematch-odds-recovery) on the maintenance executor after scheduled Midnight Sync when configured.
pre_start_check_job/ – executes the core pre-start lifecycle, provider odds ingestion, and prediction pipelines.

#### Pre-Start Candidate Building & Lifecycle

The pre-start check runs repeatedly (every `POLL_INTERVAL_MINUTES`, default 5m) and constructs a structured `PreStartEventPlan` to govern all provider extraction and prediction evaluations:

1. **Upcoming Event Ingestion (`_load_upcoming_events`)**:
   - Queries upcoming events from the canonical database starting within the configured lookahead window.
   - `TRACKED_COMPETITIONS_ONLY=true` limits candidate loading to tracked competitions.

2. **Recently Started Maintenance & Intraday Freshness (`_maintain_recently_started_events`)**:
   - Inspects matches that recently passed kickoff to detect late-breaking timestamp delays or rain delays.
   - Triggers intraday result freshness to finalize finished events early and invalidate standings caches when necessary.
   - Filters out events stored in `scheduler.recently_rescheduled` to avoid querying stale schedules.

3. **Candidate Plan Generation (`build_pre_start_event_candidates`)**:
   - Computes exact `minutes_until_start` for each event against UTC clock time.
   - Evaluates whether the event aligns with key operational moments (`PRE_START_ODDS_MOMENTS`, e.g. `120, 30, 5, 1, 0, -5`).
   - Runs `should_extract_odds_for_event`: calls the SofaScore `/event/{id}` endpoint when timestamp correction is enabled, ingests court surface / ground observations, captures rankings into `metadata_snapshot`, and handles rescheduled events.
   - Applies the general competition gate to `should_extract_odds`. Provider phases apply their own competition gates after the candidate plan is built.
   - Yields a unified `PreStartEventPlan` containing the active candidate list and an `event_id` index lookup.

4. **Multi-Provider Ingestion & Prediction Handoff**:
   - **SofaScore & Oddspapi Odds**: Ingests fresh bookmaker odds and temporal snapshots using the candidate plan's `should_extract_odds` flags via dedicated provider runners in `modules/jobs/pre_start_check_job/providers/` coordinated by `MarketOddsIngestionService`. See [`docs/providers/pre_start_odds_ingestion.md`](docs/providers/pre_start_odds_ingestion.md) and [`docs/providers/odds-significant-change-snapshots.md`](docs/providers/odds-significant-change-snapshots.md).
   - **OddsPortal Scraping**: Concurrently queries external markets for candidates matching opening capture moments (`Config.ODDSPORTAL_OPENING_CAPTURE_MINUTES`, default 120).
   - **Alerts & Pillars**: Transforms candidate records into typed `EventContext` instances via `_build_evaluation_payloads`, executing matchup streak rules, dual process algorithms, and statistical pillars (P1–P5).

results_collection_job/ – collects finished match results and updates prediction logs.

Jobs are scheduled through the infrastructure/scheduler using the schedule
 library. The pre‑start job runs repeatedly at a configurable polling interval; discovery and midnight jobs run at specific times. All timings are configurable via environment variables and the settings/config.py file.

### Observations (modules/observations)

The observations package extracts additional metadata about events. For example, the sofascore_extractor.py module scrapes details such as court surface, player gender and venue; tennis.py adds tennis‑specific observations; and service.py coordinates persistence. These enrichments are persisted in the event_observations table and used by alert and pillar engines.

### Pillars (modules/pillars)

For a Spanish guide to the pre start check flow and all five pillars, written for non-programmers, start with [Guía funcional de los pilares](docs/pillars/guia-funcional/00-flujo-principal.md). It links to a detailed document for each pillar, including the seven P1 side modules and its totals engine.

The pillars package implements specialized analytical and statistical signal engines:

* `context.py`: Defines the typed lifecycle data model (`EventContext`). It encapsulates normalized participants, competition data, trajectory contexts, observations, and prediction reports without relying on unstructured dictionaries. See [`docs/pillars/inputs.md`](docs/pillars/inputs.md).
* `streak_analysis_resolver.py`: Centralized resolver that coordinates matchup streak evaluation and form analysis across alert and pillar pipelines.
* `competition_metadata_resolver.py`: Enriches and caches league structure details (e.g. number of teams, total regular season games, standings groupings).
* `pillar_1_team_structure/`: Multilayer side and totals engine (Base Strength, Offensive Profile, Direct Matchup, Quality-Adjusted Immediate State, Structural Drift, Opponent Expectation, etc.).
* `pillar_2_side_market/`: Evaluates side market dynamics, spreads, and market consensus.
* `pillar_3_totals_market_context/`: Evaluates totals market context, distribution expectations, and line stability.
* `pillar_4/`: Evaluates market movement over time. See the [P4 guide](docs/pillars/guia-funcional/04-pilar-4-movimiento-temporal.md).
* `pillar_5/`: Analyzes price memory, stability and exact quote distribution across bookmakers (`run_pillar_5`).
* `mining/`: Unified persistence and feature extraction layer (`PillarMiningRepository`) storing JSONB signal profiles for analytical pillars. See [`docs/pillars/mining-persistence.md`](docs/pillars/mining-persistence.md).

### OddsPortal Scraping (modules/oddsportal)

This package encapsulates a Playwright‑based scraper that captures authoritative opening odds at a configurable pre-start minute (`Config.ODDSPORTAL_OPENING_CAPTURE_MINUTES`, default 120). Core modules include:

* `scraper_impl.py`, `scraper_browser.py` and `scraper_render.py` – implement browser management, page navigation, anti-automation flags, and element extraction.
* `scraper_lookup.py` and `team_matcher.py` – resolve SofaScore events to OddsPortal URLs using league caches and fuzzy team matching.
* `scraper_data.py` and `dataclasses.py` – define typed data models for odds snapshots, markets, matches and results.
* `oddsportal_dispatcher.py` – orchestrates concurrent scraping across Chromium browser workers partitioned by season (`scrape_multiple_matches_parallel_sync`).
* `oddsportal_config.py` and `oddsportal_routes.py` – configuration and mapping for OddsPortal seasons, markets and scraping routes.

Data scraped here is canonicalized via `OddsPortalMarketAdapter` and persisted through `MarketOddsIngestionService.save_from_oddsportal_data()`. See [`docs/providers/oddsportal_scraping.md`](docs/providers/oddsportal_scraping.md) for comprehensive operational details.

#### Provider quote ownership

Markets and choices are shared across providers. OddsPortal writes its own
opening-only quote. OddsPapi and SofaScore manage their own prices and
snapshots. Alerts resolve opening and current independently using
[provider priority](config/odds_read_priority.json). See the
[ingestion guide](docs/providers/pre_start_odds_ingestion.md#canonical-writes-and-reads).

### Prediction (modules/prediction)

Prediction‑related utilities live in this small package. They include prediction_logging.py, which stores prediction attempts and their eventual outcomes. The alert engines call these helpers to record whether predicted events resulted in wins or losses.

### SofaScore Client (modules/sofascore)

This package wraps the SofaScore public API. The client.py implements a resilient HTTP client (using curl‑cffi for browser‑like TLS fingerprint impersonation) and methods for event discovery, odds retrieval, results parsing and standings. See [`docs/providers/sofascore_api_architecture.md`](docs/providers/sofascore_api_architecture.md). Additional modules include:

discovery_feeds.py and schedule_feeds.py – fetch dropping odds, high‑value streaks, H2H, winning odds and daily schedule feeds.
event_details.py and results_parser.py – normalise event details and final scores.
h2h.py and team_history.py – provide historical matchups and team form.
sport_classifier.py – distinguishes sport sub‑types (e.g., tennis singles vs doubles).
standings.py and winning_odds.py – compute standings and winning odds for ranking enrichments.

The SofaScore modules provide the primary source of events and odds for the platform.

## Infrastructure

Under infrastructure/ are cross‑cutting utilities shared by the modules:

Network & proxies – network/proxy_manager.py manages proxy rotation and failure handling for both SofaScore HTTP calls and OddsPortal scraping.
Persistence – persistence/ defines SQLAlchemy models (models.py), canonical market catalogs (see [`docs/architecture/canonical_markets_list.md`](docs/architecture/canonical_markets_list.md) and [`docs/architecture/odds_db_ingestion_context.md`](docs/architecture/odds_db_ingestion_context.md)), repository patterns for each domain (repositories/*.py) and the main database.py for creating sessions and applying migrations. The repositories perform inserts, updates and queries for events, seasons, odds, markets, results, observations and caches.
Scheduler – scheduler/job_scheduler.py wraps the schedule library to register jobs and run them on separate threads. When you call python main.py start, this scheduler initialises the job list based on configuration.
Settings – settings/config.py reads environment variables (via python‑dotenv
) and exposes typed configuration. Important options include database URL, polling intervals, discovery times, timezone, proxy toggles, and Telegram credentials.
## Shared Utilities

The shared/ package houses cross-cutting helpers that don’t belong to a specific module:

* `temporal.py` – canonical temporal primitives enforcing timezone-aware UTC instants and IANA local conversions (see [`docs/architecture/temporal-contract.md`](docs/architecture/temporal-contract.md)).
* `odds_utils.py` – convert fractional odds to decimals and compute deltas.
* `runtime_observability.py` – structured logging and diagnostic metrics for critical paths.
* `shutdown.py` – clean process termination handling.
## Scripts

One‑off scripts live under scripts/. They support administrative tasks such as:

backfill_results.py – backfill missing result rows.
backup_server.py – export and backup the PostgreSQL database.
csv_migration.py, generate_oddsportal_split.py, process_null_seasons.py – perform data migrations and cleanup.
extract_historical_results.py – download historical results into CSV.
maintenance/ – contains specialist scripts, e.g. correcting tennis classifications.

These scripts can be invoked directly with python -m scripts/<name>.py. Some are integrated into CLI commands (e.g., backfilling results).

## Installation & Setup

```bash
git clone https://github.com/GadielRP/scraping_with_api.git
cd scraping_with_api
```

Create a virtual environment and install dependencies. The project requires Python ≥ 3.9. The dependencies listed in requirements.txt include requests, psycopg2-binary, python-dotenv, schedule, sqlalchemy, alembic, curl-cffi, pydantic, rich, pytz and playwright. After installing, run playwright install to download browser binaries.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
```
Set up environment variables. Copy .env.example to .env and populate the required fields:
*   `DATABASE_URL` – PostgreSQL connection string (postgresql+psycopg2://user:pass@host/dbname).
*   `TIMEZONE` – local timezone (default America/Mexico_City).
*   `POLL_INTERVAL_MINUTES` – polling interval for the pre‑start check job.
*   `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `PERSONAL_CHAT_ID` – credentials for Telegram alerts.
*   `ODDSPAPI_FREE_KEYS` and optional `ODDSPAPI_PAID_KEY` – OddsPapi credentials. See the [provider configuration](docs/providers/pre_start_odds_ingestion.md#oddspapi-configuration) for capture and recovery settings.
*   Proxy toggles and credentials (if scraping behind a proxy).
*   Optional toggles like `ENABLE_TIMESTAMP_CORRECTION`, `ENABLE_ODDS_EXTRACTION`, `SUPPORTED_SPORTS`, `STREAK_ALERT_MIN_RESULTS`.

Configure discovery schedules and admission policy in
[`infrastructure/settings/discovery.py`](infrastructure/settings/discovery.py),
then restart. SofaScore and OddsPapi have independent switches for future events,
tracked competitions, excluded sports, excluded categories and excluded competitions.
The default accepts events at least ten minutes before kickoff and excludes the
audited lists in that module. See [discovery selection and scheduling](docs/jobs/daily-discovery.md).
The existing odds-first admission in team streaks remains enabled through
`SOFASCORE.team_streaks_require_odds`; disable it to persist admitted events
without requiring an odds response first.

Initialise the database. Run:

```bash
python main.py status
```

This will create tables and materialised views if they do not exist and verify connectivity to PostgreSQL.

## Running the System
### Full Scheduler

To start the full platform with scheduled jobs, execute:

```bash
python main.py start
```

This initialises the database, loads configuration and starts the scheduler with discovery jobs (A and B), the pre‑start polling loop, midnight sync and daily discovery. Logs are written to logs/<Month>/week_<N>/sofascore_odds.log and logs/oddsportal/<Month>/week_<N>/oddsportal.log.

### Manual Invocations

Any job can be triggered on demand using its CLI command. For example, to run the pre‑start check once:

```bash
python main.py pre-start
```

To run daily discovery immediately (useful for debugging proxy issues):

```bash
python main.py daily-discovery
```

Refer to the command table above for the full list of options.

### OddsPapi odds capture and recovery

Pre-start captures current quotes and main-line cache at T−5 in the restricted
mode. After scheduled Midnight Sync, a separate maintenance job uses that cache
to recover openings and historical observations for fixtures with results. The
[ingestion guide](docs/providers/pre_start_odds_ingestion.md) covers both modes,
bookmaker precedence, environment switches and the versioned thresholds.

```bash
python main.py oddspapi-prematch-odds-recovery --date 2026-11-25
```

The recovery command writes to the configured database. Use the
[job guide](docs/jobs/results-collection.md#oddspapi-prematch-odds-recovery) for
date selection and logs, and the
[pre-start simulator](docs/jobs/pre-start-simulator.md) to test T−5 capture.

### Oddspapi Fixture Discovery

The Oddspapi fixture discovery job finds fixtures for the configured sports and
maps their `fixtureId` values to events that already exist in the canonical
`events` table. It writes rows to `event_source_mappings` with
`source=oddspapi`; it does not create canonical events, participants,
competitions or tournaments, and it does not call any Oddspapi odds endpoint.

The default discovery window is the current UTC calendar day: midnight UTC
through midnight UTC of the following day. A requested window larger than 48
hours is split into smaller API requests. The default sports are:

```text
soccer=10, basketball=11, tennis=12, baseball=13,
american-football=14, ice-hockey=15
```

#### Job package

The runtime implementation is under `modules/jobs/oddspapi/fixture_discovery/`:

* `oddspapi/__init__.py` marks the parent Oddspapi jobs package and is kept
  intentionally free of job-specific exports.
* `fixture_discovery/__init__.py` exposes the current job, summary dataclasses
  and programmatic runner.
* `fixture_discovery/constants.py` defines the discovery sport IDs, status/language defaults,
  UTC window defaults, queue default and maximum API request window.
* `fixture_discovery/response_utils.py` extracts fixture lists from raw arrays or defensive
  wrapper objects (`fixtures`, `data` or `items`), formats UTC timestamps for
  Oddspapi and splits long windows into chunks.
* `fixture_discovery/fixture_batch_processor.py` performs efficient resolution for one response
  batch. It normalizes and deduplicates fixtures, performs bulk mapping
  lookups, loads a sport-filtered candidate pool once, and sends only
  unresolved fixtures through Layer 3 matching. It uses a one-hour candidate
  tolerance and a caller-owned SQLAlchemy session.
* `fixture_discovery/fixture_discovery_job.py` orchestrates the API calls sport by sport, handles
  empty 404 responses and per-sport failures, opens an independent transaction
  for each response batch, and aggregates per-sport and total statistics.
* `fixture_discovery/run_fixture_discovery.py` provides the standalone module runner, CLI
  argument parsing, UTC-day window resolution, sport validation and JSON
  summary output.

#### Resolution flow

For each sport and each API window, the job:

1. Calls `/v4/fixtures` sequentially with `sportId`, `from`, `to`,
   `statusId`, `language` and the configured `hasOdds` value.
2. Extracts fixture dictionaries, skips invalid entries without `fixtureId`,
   and deduplicates by normalized `fixtureId`.
3. Bulk-loads existing Oddspapi mappings and SofaScore mappings in the same
   database session.
4. Resolves Layer 1 existing Oddspapi mappings and Layer 2
   `externalProviders.sofascoreId` mappings without candidate matching.
5. Loads canonical candidate events once for the unresolved fixtures,
   filtering by sport and the one-hour start-time window. Candidates are then
   indexed and selected in memory per fixture.
6. Reuses the deterministic Layer 3 matcher. Successful matches are persisted
   with `match_method=deterministic_candidate_match` when commit mode is
   enabled.
7. Leaves unresolved fixtures out of
   `event_source_resolution_queue` by default. `--persist-queue` is opt-in,
   and pure no-candidate noise is still excluded.

The Oddspapi client enforces the documented fixtures endpoint cooldown,
currently two seconds by default, and honors retry information returned for
HTTP 429 responses. Sports are processed sequentially so one failed sport is
recorded in the summary without rolling back successful sports.

#### Manual execution

The default mode is dry-run. It performs lookups and matching but writes no
mappings or queue rows:

```bash
python -m modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery \
  --date 2026-07-15 \
  --sports soccer,basketball,baseball \
  --dry-run \
  --log-json
```

Commit successful mappings with:

```bash
python -m modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery \
  --date 2026-07-15 \
  --sports soccer,basketball,baseball \
  --commit \
  --log-json
```

The application CLI exposes the same job through `main.py` and initializes
the application/database before running it:

```bash
python main.py oddspapi-fixture-discovery \
  --date 2026-07-15 \
  --sports soccer,basketball,baseball \
  --commit \
  --log-json
```

Useful options include `--from-date` and `--to-date` for an explicit UTC
window, `--lookahead-days` for a date-based window, `--status-id`,
`--max-fixtures-per-sport` for controlled validation runs and
`--persist-queue` for opt-in review queue persistence. Unknown sport slugs
fail with a list of supported values.

#### Automatic execution

When the full scheduler is started with:

```bash
python main.py start
```

`infrastructure/scheduler/schedules.py` registers fixture discovery at
`ODDSPAPI.scheduled_times` in `infrastructure/settings/discovery.py`.
`ODDSPAPI.reconciliation_enabled` defaults to `True`: newer Daily Discovery completion allows a successful fixture run to reconcile again. Set it to `False` to disable that replay while preserving retries for missing, failed or interrupted runs.

The default schedule is `17:47` in `Config.TIMEZONE`. Edit the provider settings and restart,
for example:

```python
ODDSPAPI = OddspapiDiscoverySettings(scheduled_times=("03:00", "15:00"))
```

The scheduled evening invocation targets the next UTC day after SofaScore's daily pass,
all configured discovery sports,
commits successful mappings, and keeps queue persistence disabled unless the
job is explicitly invoked with queue persistence enabled. Use
`python main.py oddspapi-fixture-discovery --help` for immediate CLI execution.

## Developing & Extending

The modular monolith design encourages adding new features without breaking existing ones. Guidelines:

Add new alert types under modules/alerts/alerts_formatter/ and wire them into the alert_pipeline.py. Ensure the formatter produces concise, chunk‑safe output for Telegram.
Create new jobs by adding a sub‑package under modules/jobs/ with a run_<name>.py script. Register it in infrastructure/scheduler/job_scheduler.py and expose a CLI command.
Interact with SofaScore or OddsPortal by extending the clients in modules/sofascore/ and modules/oddsportal/. Use repository classes in infrastructure/persistence/ to persist data.
Keep configuration in .env and settings/config.py so that new modules remain configurable without code changes.
Write tests to cover new logic. While this project currently focuses on operational scripts, adding unit tests will help prevent regressions.
## Contribution Workflow
*   Fork the repository and create a feature branch.
*   Install pre‑commit hooks (if configured) and run linting tools locally.
*   Submit a pull request describing your changes. Include relevant docs and update this README if the structure changes.
*   The repository maintainer will review and merge after tests pass.
## License

This project is provided for educational and personal use. Consult the repository’s LICENSE file for the full license text.
