# Sports Data & Prediction Platform
## Overview

This repository implements a modular monolith for ingesting sports events and odds, analysing prediction signals and delivering structured alerts. This project connects to SofaScore via https requests to its API to feed the db from events
 and scrapes supplemental data from OddsPortal, persists everything in PostgreSQL, runs rule‑based algorithms (dual process, matchup streak, basketball 4th‑quarter projections and more) and dispatches notifications via Telegram.

Key capabilities include:

Event discovery and persistence – multiple discovery sources scan SofaScore feeds for upcoming games, deduplicate them and store events and opening odds in a unified schema.
Selective odds extraction – key moments (120, 30, 5, 0 and −5 minutes around kick‑off) trigger odds snapshots and market persistence. Alerts are only sent at the 30‑minute and −5‑minute marks.
Prediction & alerting pipeline – a three‑phase workflow synchronises supplemental OddsPortal data, evaluates dual process and matchup streak rules, and dispatches formatted notifications via Telegram. A separate basketball 4Q predictor uses historical momentum and rhythm models.
*   Event discovery and persistence – multiple discovery sources scan SofaScore feeds for upcoming games, deduplicate them and store events and opening odds in a unified schema.
*   Selective odds extraction – key moments (120, 30, 5, 0 and −5 minutes around kick‑off) trigger odds snapshots and market persistence. Alerts are only sent at the 30‑minute and −5‑minute marks.
*   Prediction & alerting pipeline – a three‑phase workflow synchronises supplemental OddsPortal data, evaluates dual process and matchup streak rules, and dispatches formatted notifications via Telegram. A separate basketball 4Q predictor uses historical momentum and rhythm models.
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
  modules/                     # domain logic organised by feature
    alerts/                    # alert transport & formatting engines
    jobs/                      # scheduled and ad‑hoc jobs
    observations/              # observations & sport‑specific enrichments
    oddsportal/                # Playwright scraper & models for OddsPortal
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
| start | Launches the full scheduler – initialises the database, creates tables, migrates schema and schedules discovery, pre‑start, midnight sync and daily discovery jobs. |
| discovery | Triggers Discovery A immediately – fetches dropping odds feeds and sport‑specific dropping endpoints, deduplicates and stores events. |
| discovery2 | Runs Discovery B – pulls high‑value streaks, head‑to‑head (H2H), winning odds and other special feeds. |
| pre-start | Executes the pre‑start cycle now – captures upcoming events, applies timestamp corrections, extracts odds snapshots at key moments and evaluates alerts. |
| midnight | Performs the midnight sync job – collects results, updates prediction logs and refreshes materialised views. |
| results | Collects previous‑day results and updates prediction logs. |
| results-date --date YYYY-MM-DD | Collects results for the specified date. |
| oddspapi-fixture-discovery | Discovers Oddspapi fixture IDs for the UTC day and maps them to existing canonical events. It does not create events or ingest odds. |
| results-all | Retrieves results for all finished events. |
| daily-discovery | Runs daily sports discovery – extracts today’s events and odds for each sport. |
| backfill-results --limit N | Backfills missing results history up to N events. |
| status | Prints database and scheduler status information. |
| events --limit N | Displays a summary of recently discovered events. |
| alerts | Evaluates and sends alerts on upcoming events. |
| refresh-alerts | Refreshes the materialised view used for alert candidate lookup. |

These commands map one‑to‑one to functions in app/commands/. See the docstrings there for further details.

## Modules
### Alerts (modules/alerts)

Alert‑related logic lives under modules/alerts/. It contains a Telegram transport for sending messages and a set of formatters used by different alert types. The telegram_notifier.py module wraps the Telegram Bot API and automatically splits long messages into safe chunks. Sub‑packages include:

alerts_formatter/ – classes responsible for constructing human‑readable alert messages. Each alert type (odds, dual process, matchup streak, Q4, time correction) implements a formatter here.
basketball_4q/ – prediction engine for basketball fourth‑quarter analysis. It uses team rhythm, momentum and statistical ranges to project performance and exposes a run_basketball_4q.py script for manual execution.
dual_process/ – implements the dual process alert strategy. The process_1 and process_2 submodules handle candidate search, evaluation and sport‑specific rules for football and other sports. The top‑level run_dual_process.py entrypoint orchestrates both phases.
matchup_streak_analysis/ – analyses head‑to‑head records, historical form and standings. It defines constants.py, head_to_head.py, historical_form.py, standings_engine.py, standings_rules.py, standings_simulator.py and winning_odds.py. The run_matchup_streak_analysis.py script triggers this pipeline and uses the materialised view mv_alert_events for candidate lookup.
### Jobs (modules/jobs)

Oddspapi jobs are grouped under `modules/jobs/oddspapi/`, with each job in its
own subpackage. `fixture_discovery/` creates mappings for known canonical
events, while `pre_start_odds/` requests and ingests odds for those existing
mappings inside the main pre-start lifecycle. This keeps discovery and odds
ingestion separate without creating a second pre-start scheduler.

Scheduled work is compartmentalised in the jobs/ package. Each job has its own sub‑directory with a run_*.py script and helper modules. Highlights include:

clean_league_cache/ – clears stale OddsPortal league cache rows before the day’s discovery.
daily_discovery/ – fetches today’s events and odds across sports. It maintains a retry queue for sports that fail due to proxies or network errors.
discover_dropping_odds/ & discover_secondary_sources/ – run the A and B discovery paths. Secondary sources include high‑value streaks, team streaks, H2H, winning odds and optimisation filters.
midnight_sync_job/ – runs after midnight to collect match results, update prediction logs and refresh materialised views.
parallelism/ – utilities for job parallelisation, event filtering and recommendation generation.
pre_start_check_job/ – executes the core alert pipeline. It orchestrates three phases: 1) synchronise with OddsPortal to fetch supplemental odds, 2) evaluate dual process and matchup streak candidates, 3) dispatch formatted alerts. Additional modules handle in‑game checks, odds extraction, rescheduled events and time correction.
results_collection_job/ – collects finished match results and updates prediction logs.

Jobs are scheduled through the infrastructure/scheduler using the schedule
 library. The pre‑start job runs repeatedly at a configurable polling interval; discovery and midnight jobs run at specific times. All timings are configurable via environment variables and the settings/config.py file.

### Observations (modules/observations)

The observations package extracts additional metadata about events. For example, the sofascore_extractor.py module scrapes details such as court surface, player gender and venue; tennis.py adds tennis‑specific observations; and service.py coordinates persistence. These enrichments are persisted in the event_observations table and used by alert engines.

### OddsPortal Scraping (modules/oddsportal)

This package encapsulates a Playwright‑based scraper to retrieve odds data from OddsPortal at the 0‑minute mark. Core modules include:

scraper_impl.py, scraper_browser.py and scraper_render.py – implement the asynchronous browser, page navigation and element extraction.
scraper_lookup.py and team_matcher.py – resolve SofaScore events to OddsPortal URLs using league caches and fuzzy team matching.
scraper_data.py and models.py – define dataclasses for odds snapshots, markets, matches and results.
oddsportal_dispatcher.py – orchestrates concurrent scraping using a dispatcher that decouples league cache seeding from event scraping. This allows sibling events to begin scraping in parallel once the league page is resolved.
oddsportal_config.py – configuration and mapping for OddsPortal seasons, markets and scraping routes.

Data scraped here is normalised into the same schema as SofaScore odds and persisted via the infrastructure layer.

### Prediction (modules/prediction)

Prediction‑related utilities live in this small package. They include prediction_logging.py, which stores prediction attempts and their eventual outcomes. The alert engines call these helpers to record whether predicted events resulted in wins or losses.

### SofaScore Client (modules/sofascore)

This package wraps the SofaScore public API. The client.py implements a resilient HTTP client (using curl‑cffi
 for browser‑like headers) and methods for event discovery, odds retrieval, results parsing and standings. Additional modules include:

discovery_feeds.py and schedule_feeds.py – fetch dropping odds, high‑value streaks, H2H, winning odds and daily schedule feeds.
event_details.py and results_parser.py – normalise event details and final scores.
h2h.py and team_history.py – provide historical matchups and team form.
sport_classifier.py – distinguishes sport sub‑types (e.g., tennis singles vs doubles).
standings.py and winning_odds.py – compute standings and winning odds for ranking enrichments.

The SofaScore modules provide the primary source of events and odds for the platform.

## Infrastructure

Under infrastructure/ are cross‑cutting utilities shared by the modules:

Network & proxies – network/proxy_manager.py manages proxy rotation and failure handling for both SofaScore HTTP calls and OddsPortal scraping.
Persistence – persistence/ defines SQLAlchemy models (models.py), repository patterns for each domain (repositories/*.py) and the main database.py for creating sessions and applying migrations. The repositories perform inserts, updates and queries for events, seasons, odds, markets, results, observations and caches.
Scheduler – scheduler/job_scheduler.py wraps the schedule library to register jobs and run them on separate threads. When you call python main.py start, this scheduler initialises the job list based on configuration.
Settings – settings/config.py reads environment variables (via python‑dotenv
) and exposes typed configuration. Important options include database URL, polling intervals, discovery times, timezone, proxy toggles, and Telegram credentials.
## Shared Utilities

The shared/ package houses small helpers that don’t belong to a specific module. For example:

odds_utils.py – convert fractional odds to decimals and compute deltas.
timezone_utils.py – provide timezone‑aware conversions between UTC and local time (default America/Mexico_City).
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
*   `DISCOVERY_INTERVAL_HOURS` and `DISCOVERY2_INTERVAL_HOURS` – intervals for discovery jobs.
*   `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `PERSONAL_CHAT_ID` – credentials for Telegram alerts.
*   `ODDSPAPI_KEY` - API key required for Oddspapi requests.
*   `ODDSPAPI_BASE_URL`, `ODDSPAPI_TIMEOUT_SECONDS` and `ODDSPAPI_DEFAULT_LANGUAGE` - Oddspapi client settings.
*   `ODDSPAPI_FIXTURE_DISCOVERY_TIMES` - comma-separated scheduler times for automatic fixture discovery.
*   `ODDSPAPI_FIXTURES_COOLDOWN_SECONDS` - minimum delay between `/v4/fixtures` requests; default `2.0` seconds.
*   `ENABLE_ODDSPAPI_PRE_START_ODDS` - enables Oddspapi odds ingestion inside the normal pre-start run.
*   `ODDSPAPI_PRE_START_BOOKMAKERS`, `ODDSPAPI_PRE_START_ALLOWED_MARKET_GROUPS`, `ODDSPAPI_PRE_START_ALLOWED_MARKET_PERIODS` and `ODDSPAPI_PRE_START_MAX_EVENTS_PER_RUN` - scope the provider's pre-start requests and persisted markets.
*   Proxy toggles and credentials (if scraping behind a proxy).
*   Optional toggles like `ENABLE_TIMESTAMP_CORRECTION`, `ENABLE_ODDS_EXTRACTION`, `EXCLUDED_SPORTS`, `STREAK_ALERT_MIN_RESULTS`.

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

### Oddspapi Pre-start Odds Ingestion

Oddspapi pre-start odds ingestion is part of `python main.py pre-start` and
the pre-start polling cycle started by `python main.py start`. It is **not** a
separate scheduled job. After the normal SofaScore odds loop, it selects only
events for which the existing pre-start timing decision set
`should_extract_odds=True`, bulk-loads their `event_source_mappings` rows with
`source=oddspapi`, requests `/v4/odds` by `fixtureId`, and sends the payload to
the existing market ingestion service. It runs before materialized views and
odds trajectories are refreshed.

The fixture-discovery job must create the mapping first. Events with no
Oddspapi mapping are skipped; this ingestion path never runs fuzzy matching,
creates events, or calls `/v4/fixtures`.

Add the following to `.env` (the same defaults are present in `.env.example`):

```dotenv
# Required for any Oddspapi request. Leave empty only to disable requests safely.
ODDSPAPI_KEY=replace_with_your_oddspapi_key

# The pre-start scheduler cadence and its shared timing moments.
POLL_INTERVAL_MINUTES=5
PRE_START_ODDS_MOMENTS=120,30,5,0,-5
PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES=3

# Oddspapi pre-start ingestion.
ENABLE_ODDSPAPI_PRE_START_ODDS=true
ODDSPAPI_PRE_START_BOOKMAKERS=pinnacle
ODDSPAPI_PRE_START_ALLOWED_MARKET_GROUPS=
ODDSPAPI_PRE_START_ALLOWED_MARKET_PERIODS=
ODDSPAPI_PRE_START_MAX_EVENTS_PER_RUN=0
```

`ODDSPAPI_KEY` is the only Oddspapi pre-start value that must contain a secret;
without it the flow logs one warning and skips eligible events. The remaining
values are optional because `infrastructure/settings/config.py` supplies the
shown defaults:

| Setting | Default | Effect |
| :--- | :--- | :--- |
| `ENABLE_ODDSPAPI_PRE_START_ODDS` | `true` | Set to `false` to disable only the Oddspapi subflow; SofaScore pre-start ingestion, views, alerts and pillars continue normally. |
| `ODDSPAPI_PRE_START_BOOKMAKERS` | `ODDSPAPI_DEFAULT_BOOKMAKERS` (normally `pinnacle`) | Comma-separated or Python-list-style bookmaker slugs sent to `/v4/odds`. It does not request every bookmaker by default. |
| `ODDSPAPI_PRE_START_ALLOWED_MARKET_GROUPS` | no filter | Optional comma-separated market groups to persist, e.g. `1X2,Home/Away,Over/Under,Asian handicap`. Blank means all mapped groups. |
| `ODDSPAPI_PRE_START_ALLOWED_MARKET_PERIODS` | no filter | Optional comma-separated periods to persist, e.g. `Full Time`. Blank means all mapped periods. |
| `ODDSPAPI_PRE_START_MAX_EVENTS_PER_RUN` | `0` | Maximum mapped events to request in one pre-start pass. `0` means unlimited; extra candidates are skipped for that pass. |
| `POLL_INTERVAL_MINUTES` | `5` | Frequency of the existing pre-start cycle. The job checks exact rounded minute values, so choose a cadence that lands on the configured key moments. |
| `PRE_START_ODDS_MOMENTS` | `120,30,5,0,-5` | Exact rounded minutes before/after kickoff at which **both** SofaScore and Oddspapi are eligible to capture odds. |
| `PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES` | `3` | Allowed distance from a configured moment when loading the downstream trajectory. |

These shared Oddspapi client settings normally need no change, but can be
overridden if required: `ODDSPAPI_BASE_URL=https://api.oddspapi.io`,
`ODDSPAPI_TIMEOUT_SECONDS=15`, `ODDSPAPI_DEFAULT_ODDS_FORMAT=decimal`,
`ODDSPAPI_DEFAULT_LANGUAGE=en`, and `ODDSPAPI_DEFAULT_VERBOSITY=3`.
`ODDSPAPI_FIXTURE_DISCOVERY_TIMES` and
`ODDSPAPI_FIXTURES_COOLDOWN_SECONDS` configure the separate mapping-discovery
job only; they do not schedule or throttle this pre-start subflow.

To validate only this path for one canonical event, without running the rest
of the pre-start job, use the manual integration harness. It prompts for an
`events.id` when none is supplied. The default performs real persistence;
`--dry-run` still requests and parses odds but writes no market data:

```bash
python -m tests.test_oddspapi_pre_start_odds_job 12345
python -m tests.test_oddspapi_pre_start_odds_job 12345 --dry-run
```

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

`infrastructure/scheduler/job_scheduler.py` registers the job once per day at
the times in `ODDSPAPI_FIXTURE_DISCOVERY_TIMES`. The default is `03:00` and
can be changed in `.env` as a comma-separated list, for example:

```text
ODDSPAPI_FIXTURE_DISCOVERY_TIMES=03:00,15:00
```

The scheduled invocation processes the current UTC day, all configured sports,
commits successful mappings, and keeps queue persistence disabled unless the
job is explicitly invoked with queue persistence enabled. The scheduler also
exposes an immediate trigger through
`JobScheduler.run_job_oddspapi_fixture_discovery_now()`.

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
