# Pre-start odds ingestion

The pre-start job loads upcoming events, applies the shared timing and competition gates, and builds one candidate plan with source mappings and availability. SofaScore and OddsPapi run separate provider phases over that plan. The optional T−1 job reuses those phases on its own executor. OddsPortal captures openings in parallel on the configured routes, usually at T−120. See the [OddsPortal guide](oddsportal_scraping.md) and [job execution guide](../jobs/results-collection.md).

| Gate | Scope |
|---|---|
| `TRACKED_COMPETITIONS_ONLY` | Limits which events the pre-start job loads. |
| `ENABLE_ODDS_EXTRACTION` and `PRE_START_ODDS_MOMENTS` | Drive the shared odds decision. Minutes are relative to kickoff, with positive values before kickoff and negative values after. |
| `ODDS_EXTRACTION_GENERAL_TRACKED_COMPETITIONS_ONLY` | Clears `should_extract_odds` for untracked candidates. |
| `ODDS_EXTRACTION_SOFASCORE_TRACKED_COMPETITIONS_ONLY` and `ODDS_EXTRACTION_ODDSPAPI_TRACKED_COMPETITIONS_ONLY` | Filter each provider's candidates independently. |
| `ENABLE_PRE_START_T_MINUS_ONE_JOB` | Registers the closing executor's schedule when enabled. The regular pre-start lane excludes the closing minute. |

SofaScore uses the existing external event ID and skips candidates whose eligibility or source availability rules suppress the request. Its adapter takes the catalog market ID where present and normalizes markets from their name, group, period and outcome roles. Choice normalization resolves home/away or over/under and aligns handicap lines to the home-side convention. Canonical types must be enabled for ingestion.

OddsPapi uses the existing `event_source_mappings` row to request odds by `fixtureId`. [Fixture discovery](../jobs/daily-discovery.md) populates these mappings separately. During odds ingestion, enabled source mappings resolve provider market and outcome IDs before persistence. The main-line cache connects `/odds` selections to `/historical-odds`, whose response lacks the same main-line flags.

## OddsPapi request modes

| Mode | Requests |
|---|---|
| Restricted pre-start, `ENABLE_ODDSPAPI_PRE_START_HISTORICAL=false` | At T−5, if 5 is explicitly allowed and passes the shared gates, one logical `/v4/odds` request includes the configured regular and exchange groups. It persists quotes and populates the main-line outcome cache. T−5 is eligible even with closing-only enabled. Other moments are skipped. |
| Historical pre-start, `ENABLE_ODDSPAPI_PRE_START_HISTORICAL=true` | Positive allowed moments use `/v4/odds`. The forced moment T−5 primes the cache with that request, then runs `/v4/historical-odds` when the event kickoff is available. For allowed moments at or after kickoff, the historical lane requires an existing cache and uses `/v4/historical-odds`. |
| Finished-fixture recovery | Once the event has a result and a matching cache, a separate maintenance job reads `/v4/historical-odds` through T−5 inclusive. It requests cached configured regular bookmakers and optionally cached exchange outcomes. See [scheduling, selection and manual execution](../jobs/results-collection.md#oddspapi-prematch-odds-recovery). |

An ordinary positive moment can also enrich opening odds if enabled in `opening_historical_moments`. That policy currently has an empty list. An ordinary current-odds request can be skipped when the stored mapping has `has_odds=false`. Restricted capture and the forced hybrid moment can still request `/odds`.

## OddsPapi configuration

Environment switches are read in [Config](../../infrastructure/settings/config.py). Versioned market filters, opening and forced moments, exchange outcome limits and historical thresholds live in [OddsPapi settings](../../modules/jobs/pre_start_check_job/providers/oddspapi/settings.py).

| Environment setting | Code default | Effect |
|---|---|---|
| `ENABLE_ODDSPAPI_PRE_START_ODDS` | `false` | Enables the OddsPapi provider phase. |
| `ENABLE_ODDSPAPI_PRE_START_HISTORICAL` | `true` | Selects the pre-start request mode. Set to `false` for T−5 current capture and set the recovery start date to enable the daily follow-up. |
| `ODDSPAPI_HISTORY_RECOVERY_FROM_UTC` | unset | Earliest eligible kickoff for recovery, as a timezone-aware ISO 8601 instant. |
| `ODDSPAPI_PRE_START_ALLOWED_MOMENTS` | `[5]` | Additional OddsPapi request gate. Use 5 for restricted capture. With historical access enabled, adding 0 also requires 0 in the shared moments to admit the kickoff lane. |
| `ODDSPAPI_PRE_START_CLOSING_ONLY` | `false` | Limits ordinary positive historical-enabled acquisition to the configured closing minute. Forced hybrid and live historical lanes retain their routes. |
| `ODDSPAPI_PRE_START_BOOKMAKERS` | `ODDSPAPI_DEFAULT_BOOKMAKERS` | Regular bookmaker slugs. An empty list falls back to the default list, whose code default is `pinnacle`. |
| `ODDSPAPI_PRE_START_EXCHANGE_BOOKMAKERS` | unset | Exchange bookmaker slugs included in current `/odds` acquisition. Keep these disjoint from regular slugs. |
| `ENABLE_ODDSPAPI_EXCHANGE_HISTORICAL_REQUESTS` | `false` | Enables eligible historical exchange requests, one request per selected outcome. |
| `ODDSPAPI_PRE_START_PERSIST_MAIN_LINE_ONLY` | `true` | Restricts normal ingestion to main lines. Recovery always uses cached main lines. |
| `ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES` | `true` | Requires eligible active markets/quotes. Set to `false` to admit inactive provider observations as well. |
| `ODDSPAPI_PRE_START_FILTER_POST_KICKOFF_TICKS` | `true` | Caps opening/current selection in the live historical lane at kickoff. The significant-change detector and the recovery cutoff have their own boundaries. |
| `ODDSPAPI_MAINLINE_CACHE_FALLBACK_BOOKMAKERS` | `pinnacle,bet365,betfair-ex` | Donor order for ordinary historical selection when a bookmaker lacks its own cache. Recovery selects only configured bookmakers with cache for that fixture. |
| `ODDSPAPI_MAINLINE_CACHE_RETENTION_DAYS` | `2` | Retention when `mainline_cache_cleanup_enabled` is enabled in provider settings. Cleanup is disabled by default. |
| `ODDSPAPI_PRE_START_WORKERS` / `ODDSPAPI_PRE_START_MAX_EVENTS_PER_RUN` | `4` / `0` | Pre-start concurrency and event cap. Effective concurrency also depends on eligible keys, work items and acquisition mode. Zero events cap means unlimited. |
| `ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS` / `ENABLE_ODDSPAPI_HISTORICAL_AS_OF_PERSIST` | `true` / `true` | Controls normal live detection and historical moment persistence. The forced hybrid route selects the detector at its configured moment. Recovery activates both explicitly. |
| `ENABLE_ODDSPAPI_SAVE_ODDS_RESPONSES` / `ENABLE_ODDSPAPI_SAVE_EXCHANGE_HISTORICAL_RESPONSES` | `true` / `false` | Saves raw current responses or outcome-scoped historical exchange responses. Debug mode can also save regular historical responses. |

The local and production profiles use restricted T−5 with `pinnacle,bet365` and `betfair-ex`. Exchange historical requests are disabled in both. The recovery date is configured per profile. Historical exchange defaults cap selection at 8 outcomes per event and 40 requests per run.

For a restricted deployment, set the following alongside your bookmaker and credential configuration. Replace the example UTC instant with the first kickoff to consider.

```dotenv
ENABLE_ODDSPAPI_PRE_START_ODDS=true
ENABLE_ODDSPAPI_PRE_START_HISTORICAL=false
ODDSPAPI_PRE_START_ALLOWED_MOMENTS=[5]
ODDSPAPI_HISTORY_RECOVERY_FROM_UTC=2026-11-20T12:00:00Z
```

### API keys and quota

`ODDSPAPI_FREE_KEYS` supplies the general key pool. If `ODDSPAPI_PAID_KEY` is configured, it owns `/v4/odds`. Fixtures and historical odds use the free pool when available and fall back to the paid key in a one-key setup. The shared scheduler leases a key for each physical attempt, applies per-key endpoint cooldowns and monitors quotas in PostgreSQL. Workers own sessions, with a hard cap of four workers for OddsPapi.

The client's quota policy treats current odds and fixtures as metered, historical odds as gated by available quota and `/account` as unmetered. The maintenance account refresh runs according to `ODDSPAPI_ACCOUNT_USAGE_REFRESH_HOURS` (24 by default) and backs off after errors using `ODDSPAPI_ACCOUNT_USAGE_REFRESH_RETRY_MINUTES` (60 by default). Historical-enabled pre-start can also run the same due-check. Credential state survives restarts through `oddspapi_api_key_usage` when `ENABLE_ODDSPAPI_ACCOUNT_USAGE_REFRESH` is enabled. Logs and account state use key IDs or fingerprints.

See the [API key scheduler](../../modules/oddspapi/api_key_scheduler.py) for allocation and in-flight accounting, and the [client](../../modules/oddspapi/client.py) for retry and failover behavior. Configure transport with `ODDSPAPI_BASE_URL`, `ODDSPAPI_TIMEOUT_SECONDS`, `ODDSPAPI_MAX_RETRIES` and `ODDSPAPI_ENDPOINT_COOLDOWNS`.

## Historical openings, current odds and snapshots

The historical reader reuses each ordered series for canonical prices, fixed moments and significant changes. A historical opening comes from the earliest eligible tick when **kickoff minus that tick is at least 60 minutes**. The kickoff in memory comes from `Event.starts_at` and is normalized to UTC, as are the provider's `createdAt` timestamps. The latest eligible tick supplies the historical current candidate. The observation cutoff controls which ticks are eligible independently of the opening age. See [significant-change selection](odds-significant-change-snapshots.md).

Recovery reconciles the historical candidate with the latest stored pre-kickoff capture for the same quote. Only observations with a provider timestamp at or before T−5 can win, and the stored capture wins ties. The winner can update the current quote and supply the T−5 observation. Recovery fills a missing opening on the existing quote, preserves a previously stored opening and records the provider's opening timestamp. It writes fixed moments strictly earlier than T−5 and significant ticks through T−5. A dedicated opening snapshot is created by ordinary ingestion; recovery writes an opening tick only if it is otherwise selected as a snapshot. For an exchange, reconciliation affects the side and level carried by the historical observation.

## Canonical writes and reads

Both provider phases use `MarketOddsIngestionService` and `MarketRepository`. Markets and choices are shared across sources, while each quote is identified by `(choice_id, source, exchange_side, exchange_level)`. OddsPortal writes its own opening-only quote. The read resolver selects conventional openings and current prices independently using [provider priority](../../config/odds_read_priority.json) and keeps exchange sides separate.

`initial_captured_at` comes from the provider. `current_updated_at` records the current quote write, `collected_at` records the snapshot write, and `source_collected_at` holds the provider's observation time when supplied. Several fixed moments can share one source tick, so counting snapshots is different from counting configured moments. See [persisted quote and snapshot fields](../architecture/odds_db_ingestion_context.md).

The synchronous provider phases commit before downstream evaluation. The odds alert also waits for a scheduled OddsPortal worker before reading stored quote state. Use the [pre-start simulator](../jobs/pre-start-simulator.md) for a forced T−5 test on an existing event.
