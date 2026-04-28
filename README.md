# Sports Data & Prediction Platform (Modular Monolith)
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