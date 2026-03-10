# Planning & Technical Map

This file is the implementation planning and operating guide for the SofaScore automation platform.
It complements `README.md` by focusing on execution model, ownership boundaries, maintenance priorities, and next engineering steps.

## 1. Project Mission

Build and operate a reliable, always-on pipeline that transforms live SofaScore odds/event data into:
- structured historical datasets,
- context-aware alert signals,
- auditable prediction logs,
- reproducible event intelligence for operations.

## 2. Operational Objectives

1. Keep event ingestion complete and low-latency.
2. Keep odds snapshots high-quality while minimizing unnecessary API calls.
3. Ensure alert signal relevance (avoid spam/low-value alerts).
4. Preserve historical integrity for retrospective analysis.
5. Keep scheduler cycles stable under API volatility and scraping delays.

## 3. System Layers

### Layer A: Ingestion
- `sofascore_api.py`, `sofascore_api2.py`, `today_sport_extractor.py`
- Pull events/odds/results from SofaScore endpoints.

### Layer B: Persistence
- `database.py`, `models.py`, `repository.py`
- Normalize and persist events, odds, snapshots, markets, results, observations, prediction logs.

### Layer C: Orchestration
- `scheduler.py`, `main.py`, `config.py`, `timezone_utils.py`
- Schedule, sequence, gate, and parallelize jobs.

### Layer D: Intelligence
- `alert_engine.py`, `streak_alerts.py`, `historical_standings.py`, `basketball_4q_prediction.py`, `sport_classifier.py`, `sport_observations.py`
- Derive predictions and context from historical + current state.

### Layer E: Delivery
- `alert_system.py`, `odds_alert.py`
- Format and send Telegram alerts.

### Layer F: Supplemental Markets
- `oddsportal_scraper.py`, `oddsportal_config.py`
- Scrape and persist external bookmaker market detail.

## 4. Exact File Scope (Main Scripts)

- `main.py`: startup/CLI, weekly rotating logs, init order, one-off command entrypoints.
- `repository.py`: all data-access write/read contracts.
- `config.py`: runtime toggles, schedules, filters, credentials.
- `scheduler.py`: all job logic and coordination.
- `database.py`: engine/session and migration safety routines.
- `models.py`: schema + views/materialized views.
- `odds_utils.py`: odds conversion/validation.
- `sofascore_api.py`: base API extraction and normalization.
- `sofascore_api2.py`: extended API methods (streak/H2H/winning odds/today/standings).
- `timezone_utils.py`: consistent local/UTC utilities.
- `sport_classifier.py`: tennis singles/doubles classification.
- `sport_observations.py`: observation persistence and formatting.
- `alert_system.py`: notifier + message builders.
- `alert_engine.py`: pattern match/tier rule engine for alerts.
- `streak_alerts.py`: H2H/team-form/ranking/standings analysis.
- `oddsportal_scraper.py`: Playwright scraping and extraction pipeline.
- `oddsportal_config.py`: seasons/routes/aliases/priority bookies.
- `today_sport_extractor.py`: daily discovery extractor.
- `odds_alert.py`: odds-market alert transformation and suppression rules.
- `historical_standings.py`: standings simulation + DB-based form for collected seasons.
- `basketball_4q_prediction.py`: 4Q score projection algorithm.

## 5. End-to-End Flow Plan

1. **Initialize**
- DB connect -> create tables -> schema reconcile -> create views -> create materialized views.

2. **Discover**
- Job A (dropping odds) and Job B (streak/H2H/winning odds feeds) continuously add/update upcoming events.

3. **Pre-start cycle**
- Select upcoming window, pre-calc time-to-start, check timestamp corrections.
- At key moments (30/0), extract odds, snapshot, markets.
- Tennis observation enrichment runs in same cycle.
- OP worker scrapes tracked seasons in background.

4. **Alert evaluation and delivery**
- Refresh MV.
- Evaluate each event for odds alert + H2H/streak + dual/pattern report.
- Send grouped event messages concurrently (via thread pool). Each event has its own thread, preventing slower OP scrapes from blocking faster events.

5. **Post-match reconciliation**
- Daily results pull updates `results`.
- Prediction logs are marked with actual outcomes.
- Materialized alert views refreshed.

## 6. Database Planning Notes

### Critical entities
- `events` is the root identity table.
- `event_odds` is the canonical latest 1X2 state.
- `odds_snapshot` is the temporal history.
- `markets/market_choices` hold full granular market bookie data.
- `results` closes loop for prediction verification.

### Read models
- `mv_alert_events` must stay fresh for fast alert candidate queries.
- `basketball_results` and `season_events_with_results` power historical analysis modules.

### Data quality controls
- Unique constraints in market schema prevent duplicate market-choice rows.
- Event/source upsert logic preserves strongest discovery source semantics.
- Canceled/404 event handling reduces stale data drift.

## 7. Alerting Logic Plan

### Process-1 style pattern matching (`alert_engine.py`)
- Candidate selection via exact odds/variance matching against historical materialized data.
- Tiered result confidence logic with directional agreement constraints.
- Tennis-specific surface filtering using observations.

### Streak/H2H context (`streak_alerts.py`)
- Team form, two-year H2H windows, standings/ranking context.
- Uses DB-simulated standings for collected seasons (`historical_standings.py`) to reduce API dependency.

### Odds-only alerting (`odds_alert.py`)
- Full market digest with movement indicators.
- Suppresses low-value one-market events to reduce noise.

## 8. Library/Dependency Plan

Core dependencies currently required:
- `sqlalchemy`, `psycopg2-binary`
- `curl-cffi`, `requests`
- `playwright`
- `schedule`, `python-dotenv`, `pytz`
- `pydantic`, `rich`, `alembic`

Operational note:
- Playwright installation + browser binaries is mandatory for OddsPortal paths.

## 9. Reliability Risks & Mitigations

1. API schema changes (SofaScore)
- Mitigation: centralized extractors in `sofascore_api.py`, tolerant parsing, detailed logging.

2. Scraper breakage (OddsPortal DOM changes)
- Mitigation: config-driven selectors/routes, cache fallback, retry logic, dedicated OP logs.

3. Scheduler overlap / long cycle latency
- Mitigation: key-moment extraction only, background OP worker, batched thread-pool alert processing.

4. Data duplication
- Mitigation: upsert patterns + unique constraints + repository dedupe logic.

5. Alert spam / low signal
- Mitigation: sport exclusions, key-moment gating, low-value market suppression, streak minimum thresholds.

## 10. Immediate Engineering Priorities

1. Add automated integration tests for:
- discovery extraction normalization,
- pre-start key-moment gating,
- market persistence (SofaScore + OddsPortal),
- alert-engine candidate selection correctness.

2. Add schema drift safety tests around `database.py` migration helpers.

3. Improve observability:
- per-job success/failure counters persisted in DB,
- alert send metrics (sent/skipped/reason),
- OP worker duration and failure cause distribution.

4. Formalize deployment runbook:
- environment matrix (dev/staging/prod),
- restart policy,
- backup + retention strategy for odds snapshots and logs.

## 11. New Teammate Ramp Plan (Suggested)

Day 1:
- Read `README.md`, then `scheduler.py`, `repository.py`, `models.py`.
- Run `python main.py status`, `python main.py discovery`, `python main.py pre-start`.

Day 2:
- Trace one event through DB tables (`events` -> `event_odds` -> `odds_snapshot` -> `markets`).
- Trace one alert cycle through `alert_engine.py` + `alert_system.py`.

Day 3:
- Trace OP path (`oddsportal_config.py` + `oddsportal_scraper.py` + `MarketRepository`).
- Validate midnight result reconciliation and prediction log updates.

## 12. Definition Of “Healthy System”

A healthy runtime means:
- discovery jobs continuously insert/update upcoming events,
- pre-start checks run at configured intervals without overlap stalls,
- key-moment events receive fresh odds snapshots,
- alerts are sent with low noise and no duplicate bursts,
- result collection updates outcomes daily,
- materialized views refresh successfully,
- OddsPortal tracked leagues enrich market coverage when enabled.
