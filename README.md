# SofaScore Automation Platform

This project is an automated sports-data and odds intelligence platform built around SofaScore APIs, historical pattern analysis, and Telegram alerts.

It continuously:
- discovers upcoming events from multiple SofaScore sources,
- stores event + odds + market history in PostgreSQL,
- runs key-moment pre-start checks (30 and 0 minutes),
- enriches events with observations/metadata,
- scrapes supplemental OddsPortal market data for mapped leagues,
- analyzes prediction signals (odds patterns, H2H/streak context),
- sends structured alerts and tracks prediction outcomes.

## What The System Actually Does

### 1) Event Discovery (multi-source)
The scheduler ingests upcoming events from several SofaScore feeds:
- Dropping odds (`discovery_source='dropping_odds'`) from `/odds/1/dropping/all` + sport-specific dropping endpoints.
- High-value streak events.
- High-value streak H2H events.
- Team streak events.
- Top H2H events.
- Winning odds events.
- Daily sport extractor (`daily_discovery`) that fetches today's scheduled events with odds.

All discovery paths normalize into the same `events` table with source tagging and dedup/upsert behavior.

### 2) Odds Collection Strategy
The system is intentionally selective to reduce noise and API load:
- Main odds extraction happens only at key moments: **30 min** and **0 min** before start.
- For each selected event, it fetches event-level final odds and writes:
  - `event_odds` (latest 1X2 open/final snapshot values),
  - `odds_snapshot` (time-series snapshots),
  - `markets` + `market_choices` (all available market structures).

### 3) Timestamp Integrity & Reschedule Handling
Before pre-start alerting, it checks recently started events to detect late start-time corrections. Rescheduled events are guarded against duplicate/looped processing in the same cycle.

### 4) OddsPortal Enrichment
For configured season IDs (`oddsportal_config.py` map), a background worker scrapes OddsPortal at the 0-minute phase and persists bookmaker/market-choice data into the same market schema.

### 5) Alerting / Analysis
At key moments, the system evaluates and sends grouped alerts per event:
- Odds market summary alert,
- H2H/streak analysis alert,
- dual/pattern prediction analysis alert.

The alert pipeline uses:
- historical candidate matching from a materialized view (`mv_alert_events`),
- sport-aware filters (including tennis surface handling),
- streak/H2H context and optional standings/ranking enrichments.

### 6) Result Collection + Feedback Loop
Daily jobs pull completed match results, refresh odds/markets for finished events, and update prediction logs with actual outcomes.

## Scope & Boundaries

### In scope
- Data ingestion from SofaScore APIs.
- Odds normalization and persistence.
- OddsPortal supplemental scraping for mapped leagues.
- Real-time-like scheduled alerting to Telegram.
- Historical pattern matching for prediction support.
- Result backfill and prediction log reconciliation.

### Out of scope
- No web dashboard/UI in this repository.
- No ML training pipeline; prediction logic is rule/data-pattern based.
- Not a betting execution engine.

## Main Runtime Flow

1. `main.py start`
2. System init:
- DB connection test
- table creation
- schema auto-migration checks
- SQL views creation
- materialized views creation
3. Scheduler starts jobs:
- Discovery A: `Config.DISCOVERY_TIMES`
- Discovery B: `Config.DISCOVERY2_TIMES`
- Pre-start check: every `Config.POLL_INTERVAL_MINUTES`
- Midnight sync: `04:00`
- Daily discovery: `05:01`
4. Pre-start loop:
- snapshot upcoming events,
- apply timestamp correction pass,
- extract odds for 30/0 minute events,
- refresh MV,
- evaluate and send event-grouped notifications in parallel (per-event thread pool).
5. Midnight flow updates results + prediction outcomes.

## Entry Commands (`main.py`)

- `python main.py start` - run full scheduler.
- `python main.py discovery` - run discovery A now.
- `python main.py discovery2` - run discovery B now.
- `python main.py pre-start` - run pre-start cycle now.
- `python main.py midnight` - run midnight sync now.
- `python main.py results` - collect previous-day results.
- `python main.py results-date --date YYYY-MM-DD` - date-specific results collection.
- `python main.py results-all` - collect all finished event results.
- `python main.py daily-discovery` - run daily sport extraction.
- `python main.py backfill-results --limit N` - backfill missing history.
- `python main.py status` - DB + scheduler status.
- `python main.py events --limit N` - recent event dump.
- `python main.py alerts` - evaluate/send alerts on upcoming events.
- `python main.py refresh-alerts` - refresh materialized alert data.

## Configuration (`config.py`)

Core env-driven controls:
- `DATABASE_URL`, `DB_CONNECT_TIMEOUT`
- `POLL_INTERVAL_MINUTES`, `PRE_START_WINDOW_MINUTES`
- `DISCOVERY_INTERVAL_HOURS`, `DISCOVERY2_INTERVAL_HOURS`
- `TIMEZONE` (default `America/Mexico_City`)
- `ENABLE_TIMESTAMP_CORRECTION`, `ENABLE_ODDS_EXTRACTION`
- `TRACKED_SEASONS_TOGGLE` (`TRACKED_SEASONS_ONLY`)
- Telegram: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `PERSONAL_CHAT_ID`
- Proxy and OddsPortal toggles
- `STREAK_ALERT_MIN_RESULTS`
- `EXCLUDED_SPORTS`

## Database Structure (`models.py`)

### Core tables
- `events`
  - identity/context: `id`, `custom_id`, `slug`, `sport`, `competition`, `country`, `home_team`, `away_team`, `gender`, `season_id`, `round`, `discovery_source`, `alert_sent`
  - timing/audit: `start_time_utc`, `created_at`, `updated_at`
- `seasons`
  - `id`, `name`, `year`, `sport`
- `event_odds` (current 1X2 state per event)
  - `one_open`, `one_final`, `x_open`, `x_final`, `two_open`, `two_final`
  - computed deltas: `var_one`, `var_x`, `var_two`
- `odds_snapshot` (time series)
  - `event_id`, `collected_at`, odds values, `raw_fractional`
- `results`
  - `home_score`, `away_score`, `winner`, `home_sets`, `away_sets`
- `event_observations`
  - typed metadata (example: tennis `ground_type`)
- `bookies`
  - bookmaker master (`name`, `slug`)
- `markets`
  - one market row per `event + bookie + market_name + choice_group`
- `market_choices`
  - market options (`choice_name`, `initial_odds`, `current_odds`, `change`)
- `prediction_logs`
  - stored predictions + later actual outcomes/status
- `oddsportal_league_cache`
  - daily cache of league match URLs by season for faster OP matching

### SQL views/materialized objects
- `event_all_odds` (joined odds + event + result projection)
- `basketball_results` (quarter parsing from stored sets incl. OT extraction)
- `season_events_with_results` (season-level result feed)
- `mv_alert_events` materialized view for fast alert candidate lookup

## File-by-File Responsibilities (Requested Main Scripts)

- `main.py`
  - CLI entrypoint, initialization sequence, command dispatch, log setup.

- `repository.py`
  - DB repositories for events/seasons/odds/results/observations/markets/OP cache.
  - Event upsert, discovery-source priority, cleanup utilities.
  - Market persistence for SofaScore and OddsPortal data.

- `config.py`
  - All runtime configuration from environment.

- `scheduler.py`
  - Full job orchestration and operational pipeline.
  - Discovery A/B, pre-start loop, alert batching, OP worker integration, results jobs.

- `database.py`
  - SQLAlchemy engine/session management.
  - table creation + auto schema migration checks/index add.
  - migration utility logic including `bookie_id` transition support.

- `models.py`
  - SQLAlchemy models + SQL view/materialized view creation helpers.

- `odds_utils.py`
  - Odds conversion/validation helper utilities.

- `sofascore_api.py`
  - HTTP client with retries, endpoint wrappers, event/result/odds extraction and normalization.

- `sofascore_api2.py`
  - Additional SofaScore method extensions attached to base API client:
  - streak/high-value/H2H/winning odds/today events/standings helpers.

- `timezone_utils.py`
  - Timezone-safe local/UTC conversion utilities used across scheduling and formatting.

- `sport_classifier.py`
  - Sport sub-classification (notably tennis singles vs doubles).

- `sport_observations.py`
  - Observation management for sport-specific metadata (especially tennis surface).

- `alert_system.py`
  - Telegram transport, chunk-safe messaging, and rich alert formatting.

- `alert_engine.py`
  - Historical odds-pattern candidate search and tier/rule evaluation for process-1 alerts.

- `streak_alerts.py`
  - H2H and team-form analysis engine with standings/ranking/winning-odds enrichments.

- `oddsportal_scraper.py`
  - Playwright scraping engine (market extraction, opening/current odds, bookie priority, cache use).

- `oddsportal_config.py`
  - OddsPortal league mapping, aliases, and scraping route/market configuration.

- `today_sport_extractor.py`
  - Daily multi-sport discovery job for today's events with odds.

- `odds_alert.py`
  - Odds response -> readable alert formatting + smart low-value suppression behavior.

- `historical_standings.py`
  - Local standings simulator for collected seasons and DB-based historical team-form retrieval.

- `basketball_4q_prediction.py`
  - NBA/Basketball 4th-quarter projection using historical quarter stats + in-game rhythm/momentum.

## Scheduler Jobs In Detail

### Job A: Discovery (`job_discovery`)
- Pull `/dropping/all` first.
- Pull selected sports individually.
- Filter out events starting too soon/already started.
- Deduplicate across feeds.
- Upsert events + odds in parallel DB operations.

### Job B: Discovery2 (`job_discovery2`)
- Pull high-value streaks, team streaks, H2H, winning odds feeds.
- Normalize event payloads.
- Filter upcoming-only.
- Persist using specialized processing paths (event-only or event+odds).

### Job C: Pre-start (`job_pre_start_check`)
- Capture upcoming window snapshot.
- Run late timestamp correction checks.
- Decide key-moment extraction eligibility.
- Extract and persist odds/markets at key moments.
- For tennis key moments, persist court/observation metadata.
- Trigger OP worker for mapped seasons.
- Refresh MVs and evaluate/send grouped alerts.

### Job D: Midnight sync (`job_midnight_sync`)
- Run results collection.
- Update prediction logs with actual outcomes.
- Refresh materialized alert view.

### Job E: Daily discovery (`job_daily_discovery`)
- Cleanup old OddsPortal cache rows.
- Pull today's events+odds by sport and store as `daily_discovery`.

## Libraries / Dependencies

From `requirements.txt`:
- `requests` - HTTP (Telegram + some helper calls).
- `psycopg2-binary` - PostgreSQL driver.
- `python-dotenv` - env loading.
- `schedule` - in-process job scheduling.
- `sqlalchemy` - ORM + SQL execution.
- `alembic` - migration dependency baseline (project uses custom migration logic too).
- `curl-cffi` - SofaScore HTTP client with browser impersonation.
- `pydantic` - typed validation/support utilities.
- `rich` - console output enhancements.
- `pytz` - timezone handling.
- `playwright` - OddsPortal browser scraping.

## External Integrations

- SofaScore API (`https://api.sofascore.com/api/v1`)
- OddsPortal website scraping
- Telegram Bot API for notifications

## Logs

- Main logs rotate by month/week:
  - `logs/{MM_MonthName}/week_{N}/sofascore_odds.log`
- OddsPortal dedicated logs:
  - `logs/oddsportal/{MM_MonthName}/week_{N}/oddsportal.log`

## Onboarding Checklist For New Colleagues

1. Create `.env` with DB + Telegram + scheduler toggles.
2. Install dependencies and Playwright browser binaries.
3. Run `python main.py status` to verify DB init + migrations.
4. Run `python main.py discovery` and inspect `events/event_odds` writes.
5. Run `python main.py pre-start` on test window and confirm alert pipeline logs.
6. Review `scheduler.py` + `repository.py` + `models.py` first; these are core operational files.
7. Use `README.md` + `PLANNING.md` together as canonical architecture references.
