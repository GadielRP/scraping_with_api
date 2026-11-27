# Pre-start odds ingestion

The pre-start job loads upcoming events, applies the shared timing and competition gates, and builds one candidate plan with source mappings and availability. SofaScore and OddsPapi run separate provider phases over that plan. The optional T−1 job reuses those phases on its own executor. OddsPortal captures openings in parallel on the configured routes, usually at T−120. See the [OddsPortal guide](oddsportal_scraping.md) and [job execution guide](../jobs/results-collection.md).

## Candidate plan and execution

1. [`run_pre_start_check_job`](../../modules/jobs/pre_start_check_job/run_pre_start_check_job.py) loads events in the configured lookahead window and computes rounded minutes to kickoff. It starts the OddsPortal worker before recently started event maintenance and in-game checks.
2. [`run_pre_start_odds_moments`](../../modules/jobs/pre_start_check_job/run_pre_start_check_job.py) bulk-loads [source state](../../modules/jobs/pre_start_check_job/odds_source_state.py) for SofaScore and OddsPapi. The state contains the external event ID, availability and, for OddsPapi, the source sport ID.
3. The [candidate builder](../../modules/jobs/pre_start_check_job/event_candidate_builder.py) applies the timing decision, records optional SofaScore metadata and handles rescheduling. It returns `PreStartEventPlan.candidates` and `by_event_id`. Each candidate carries the canonical event, timing, `should_extract_odds`, the resolved SofaScore event ID and any metadata snapshot. The general competition gate can clear `should_extract_odds` while keeping the event available to downstream evaluation.
4. The job attaches stored tennis observations and persists available observations before running SofaScore then OddsPapi. Each provider can filter its own candidate list. An ordinary provider exception is logged and the other phase may proceed. A cooperative deferral propagates back to the job runner.
5. Downstream key-moment evaluation uses the same plan and the persisted data. The dedicated [closing job](../../modules/jobs/pre_start_check_job/run_t_minus_one_odds_job.py) targets the kickoff slot implied by its original scheduled occurrence. It calls the same ingestion path with the closing moment only, an empty OddsPortal context, timestamp correction disabled and downstream evaluation disabled.

| Gate | Scope |
|---|---|
| `TRACKED_COMPETITIONS_ONLY` | Limits which events the pre-start job loads. |
| `ENABLE_ODDS_EXTRACTION` and `PRE_START_ODDS_MOMENTS` | Drive the shared odds decision. Minutes are relative to kickoff, with positive values before kickoff and negative values after. |
| `ODDS_EXTRACTION_GENERAL_TRACKED_COMPETITIONS_ONLY` | Clears `should_extract_odds` for untracked candidates. |
| `ODDS_EXTRACTION_SOFASCORE_TRACKED_COMPETITIONS_ONLY` and `ODDS_EXTRACTION_ODDSPAPI_TRACKED_COMPETITIONS_ONLY` | Filter each provider's candidates independently. |
| `ENABLE_PRE_START_T_MINUS_ONE_JOB` | Registers the closing executor's schedule when enabled. The regular pre-start lane excludes the closing minute. |

## SofaScore acquisition and market resolution

The [SofaScore phase](../../modules/jobs/pre_start_check_job/providers/sofascore/odds_phase.py) calls [`run_provider_odds_phase`](../../modules/odds_ingestion/provider_odds_phase.py) with a resolved external event ID and the provider-local competition filter. `has_odds=NULL` allows a request. `has_odds=false` normally suppresses it, with the bounded missing-odds cooldown allowing a retry in a new configured critical moment. Confirmed missing endpoints are marked unavailable in bulk. The phase distinguishes a missing endpoint from an empty response, an exception and a response that yielded no saved markets. Successful ingestion can feed the tennis observation hook.

1. [`SofaScoreMarketAdapter`](../../modules/odds_ingestion/adapters/sofascore_market_adapter.py) accepts the event's `markets` array, prefers the stable catalog `marketId` over an instance identifier, exposes `choiceGroup` as a line where present and marks choices as main lines.
2. [`resolve_sofascore_key`](../../modules/odds_ingestion/canonical_market_resolver.py) compares normalized name, group and period against the catalog rules. It uses choice roles to distinguish candidates such as three-way 1X2 and two-way Home/Away.
3. [`CanonicalMarketNormalizer`](../../modules/odds_ingestion/canonical_market_normalizer.py) keeps enabled canonical types, normalizes choices and enforces required line values. A prefixed handicap can supply the line when the provider lacks `choiceGroup`. Away handicap signs are aligned to the home-side line so the two choices share one canonical market.
4. The ingestion service writes a single canonical SofaScore bookmaker batch (`bookie_id=1`). The quote carries `source=sofascore`, the catalog market ID as `source_market_id` when available and a null exchange side. Price observations attach to each quote.

| Example provider input | Canonical result |
|---|---|
| Football `marketId=1`, Full time / 1X2 / Full-time, choices `1,X,2` | `1x2_full_time` with choices `1,x,2` |
| Football `marketId=9`, Match goals / Full-time, `choiceGroup=2.5` | `over_under_full_time`, line `2.5`, choices `over,under` |
| Football `marketId=17`, Asian handicap, `(-1.25) Oriente Petrolero` and `(1.25) CD San Antonio` (home and away respectively) | `asian_handicap_full_time`, shared home-side line `-1.25`, choices `1,2` |
| Tennis `marketId=1`, Full time with group `Home/Away` and period `Match`, choices `1,2` | `home_away_full_time_including_overtime` |

OddsPapi uses the existing `event_source_mappings` row to request odds by `fixtureId`. [Fixture discovery](../jobs/daily-discovery.md) populates these mappings separately. During odds ingestion, enabled source mappings resolve provider market and outcome IDs before persistence. The main-line cache connects `/odds` selections to `/historical-odds`, whose response lacks the same main-line flags.

## OddsPapi request modes

| Mode | Requests |
|---|---|
| Restricted pre-start, `ENABLE_ODDSPAPI_PRE_START_HISTORICAL=false` | At T−5, if 5 is explicitly allowed and passes the shared gates, one logical `/v4/odds` request includes the configured regular and exchange groups. It persists quotes and populates the main-line outcome cache. T−5 is eligible even with closing-only enabled. Other moments are skipped. |
| Historical pre-start, `ENABLE_ODDSPAPI_PRE_START_HISTORICAL=true` | Positive allowed moments use `/v4/odds`. The forced moment T−5 primes the cache with that request, then runs `/v4/historical-odds` when the event kickoff is available. For allowed moments at or after kickoff, the historical lane requires an existing cache and uses `/v4/historical-odds`. |
| Finished-fixture recovery | Once the event has a result and a matching cache, a separate maintenance job reads `/v4/historical-odds` through T−5 inclusive. It requests cached configured regular bookmakers and optionally cached exchange outcomes. See [scheduling, selection and manual execution](../jobs/results-collection.md#oddspapi-prematch-odds-recovery). |

An ordinary positive moment can also enrich opening odds if enabled in `opening_historical_moments`. That policy currently has an empty list. An ordinary current-odds request can be skipped when the stored mapping has `has_odds=false`. Restricted capture and the forced hybrid moment can still request `/odds`.

### Acquisition and historical preselection

- The [OddsPapi phase](../../modules/jobs/pre_start_check_job/providers/oddspapi/odds_phase.py) applies the provider and moment gates. The [batch processor](../../modules/jobs/pre_start_check_job/providers/oddspapi/odds_batch_processor.py) owns per-candidate skip reasons, request and persistence counters, and the configured event cap. Missing fixture mappings are skipped. An empty current response or a confirmed missing current endpoint may set availability to false. A missing live historical response does not mark current odds unavailable.
- The [acquisition service](../../modules/jobs/pre_start_check_job/providers/oddspapi/odds_acquisition_service.py) owns the current, hybrid and live routes. Hybrid acquisition keeps the current bookmaker payload as its base, updates the main-line cache from that response and fetches eligible historical data. For ordinary hybrid ingestion, the freshest current observation wins when provider timestamps are comparable. An exact tie or missing timestamp keeps the base. A bounded simulation uses the historical view through its explicit observation limit for matching regular players.
- The [historical payload selector](../../modules/jobs/pre_start_check_job/providers/oddspapi/historical_payload_selector.py) filters the raw historical tree against enabled market and outcome mappings before the reader reduces it. Cache identity also applies at adaptation, including in the forced non-live T−5 route. This is important for markets without lines because the historical payload omits the current `mainLine` flags.
- Regular historical odds are grouped into a request by fixture and eligible bookmaker list. Exchange history uses the [outcome-scoped executor](../../modules/jobs/pre_start_check_job/providers/oddspapi/exchange_historical_fetch_executor.py), with one bookmaker and one outcome per request. The ordinary positive opening route can suppress exchange history for tracked leagues. The live and forced routes use the cached selection and the configured exchange limits. The recovery requires its exchange gate and stops exchange fan-out when its run budget is spent.

### Main-line selection

The [current line selector](../../modules/odds_ingestion/oddspapi_line_selection.py) is shared by the adapter, the main-line cache extractor and main-line exchange selection. Each bookmaker's line is decided independently for the canonical market and period.

1. Require a finite line, an unambiguous complete set of mapped choices and finite decimal odds above 1. Apply the configured activity filter.
2. Prefer a line whose choices are all flagged `mainLine` by the provider. If none qualifies, rank the eligible alternatives.
3. Minimize the price gap `max(prices) - min(prices)` at source precision. Break ties by normalized liquidity. For each choice, compute `limit × min(1, price − 1)`. The line score is the median of those values multiplied by their minimum-to-maximum ratio. Missing or invalid limits make liquidity unavailable. Measured zero ranks above unavailable liquidity.
4. Break any remaining tie by numeric line and source market ID. The selected line reaches persistence with `mainLine=true` on its choices.

When historical adaptation has a cached line selection, its outcome IDs remain authoritative even if the retained current payload flags an alternative as a main line. During ordinary ingestion, saving a family with one selected line demotes superseded quote flags while keeping their stored prices and snapshots. The recovery works on existing quote instruments and does not select a replacement line. See the [offline OddsPapi line audit and repair](../maintenance/oddspapi_line_backfill.md).

## OddsPapi configuration

Environment switches are read in [Config](../../infrastructure/settings/config.py). Versioned market filters, opening and forced moments, exchange outcome limits and historical thresholds live in [OddsPapi settings](../../modules/jobs/pre_start_check_job/providers/oddspapi/settings.py).

| Environment setting | Code default | Effect |
|---|---|---|
| `ENABLE_ODDSPAPI_PRE_START_ODDS` | `false` | Enables the OddsPapi provider phase. |
| `ENABLE_ODDSPAPI_PRE_START_HISTORICAL` | `true` | Selects the pre-start request mode. Set to `false` for T−5 current capture and set the recovery start date to enable the daily follow-up. |
| `ODDSPAPI_HISTORY_RECOVERY_FROM_UTC` | unset | Earliest eligible kickoff for recovery, as a timezone-aware ISO 8601 instant. |
| `ODDSPAPI_PRE_START_ALLOWED_MOMENTS` | `[5]` | Additional OddsPapi request gate. Restricted capture requires 5 explicitly. With historical access enabled, `[]` lifts this extra gate and 0 must be in the shared moments to admit the kickoff lane. |
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

OddsPapi's request allowlist is separate from the scheduler cadence (`POLL_INTERVAL_MINUTES`, default 5) and the shared rounded moments (`PRE_START_ODDS_MOMENTS`). The closing schedule uses `PRE_START_T_MINUS_ONE_INTERVAL_MINUTES` (default 1). `PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES` is a trajectory read setting. It does not widen the capture window.

In provider settings, empty `allowed_market_keys`, `allowed_market_groups` and `allowed_market_periods` leave all enabled, mapped markets eligible for normal ingestion. Exchange historical requests have a separate `exchange_market_keys` list, `exchange_main_line_only=true` and `exchange_include_player_props=false`. Those settings are code policy. The sample environment keeps main-line-only persistence, significant detection and historical moment persistence opt-in for users copying it, even though those three code defaults are true.

The local and production profiles use restricted T−5 with `pinnacle,bet365` and `betfair-ex`. Exchange historical requests are disabled in both. The recovery date is configured per profile. Historical exchange defaults cap selection at 8 outcomes per event and 40 requests per run.

For a restricted deployment, set the following alongside your bookmaker and credential configuration. Replace the example UTC instant with the first kickoff to consider.

```dotenv
ENABLE_ODDSPAPI_PRE_START_ODDS=true
ENABLE_ODDSPAPI_PRE_START_HISTORICAL=false
ODDSPAPI_PRE_START_ALLOWED_MOMENTS=[5]
ODDSPAPI_HISTORY_RECOVERY_FROM_UTC=2026-11-20T12:00:00Z
```

### API keys and quota

The shared [key scheduler](../../modules/oddspapi/api_key_scheduler.py) leases a key per physical attempt. Workers own HTTP sessions and return the lease through the [client](../../modules/oddspapi/client.py) before the payload reaches the historical reader. One serial worker reuses its session across fixtures. Parallel event processing and exchange fan-out are bounded by work items, eligible keys, the configured worker cap and a hard cap of four.

| Endpoint | Credential pool | Accounting |
|---|---|---|
| `/v4/odds` | `ODDSPAPI_PAID_KEY` exclusively when configured, otherwise the configured pool | Metered |
| Fixtures and other ordinary endpoints | `ODDSPAPI_FREE_KEYS` if available, with a paid-key fallback | Metered, including unknown endpoints by default |
| `/v4/historical-odds` | Same as fixtures | Requires available quota without incrementing the estimated count |
| `/v4/account` | Every configured key, refreshed explicitly one at a time | Unmetered |

`ODDSPAPI_KEY` remains a compatibility fallback when both dedicated credential settings are empty. For metered requests the scheduler balances `(estimated_request_count + metered_in_flight) / request_limit`. It infers a limit from the eligible pool when the account is unknown. A key with lower utilization catches up first. Historical requests favor low endpoint load and earlier cooldown readiness. Exhausted, invalid and unsubscribed keys are excluded from ordinary requests. A generic 429 blocks the affected key/endpoint until its retry window elapses.

Metered responses and ambiguous network errors increment the estimate. A quota rejection or invalid key updates its status and triggers failover outside the ordinary transient retry budget. When there is no eligible key, acquisition reports `oddspapi_quota_exhausted`. Transport settings include `ODDSPAPI_BASE_URL`, `ODDSPAPI_TIMEOUT_SECONDS`, `ODDSPAPI_MAX_RETRIES` and `ODDSPAPI_ENDPOINT_COOLDOWNS`. Cooldowns are keyed by fingerprint and endpoint and measure elapsed time with a monotonic clock.

When the OddsPapi pre-start feature is enabled, `oddspapi_api_key_usage` stores fingerprinted account state and estimates. This also lets the scheduler load persisted estimates when HTTP account refresh is disabled. With `ENABLE_ODDSPAPI_ACCOUNT_USAGE_REFRESH=true`, maintenance reconciles usage every `ODDSPAPI_ACCOUNT_USAGE_REFRESH_HOURS` (default 24). Fixture reconciliation and historical-enabled pre-start can run the same due-check. A refresh temporarily holds the selected key out of allocation and leaves the others available. Success replaces the local estimate with the provider count. Failure keeps the existing estimate and applies `ODDSPAPI_ACCOUNT_USAGE_REFRESH_RETRY_MINUTES` (default 60). Persisted timestamps are UTC.

The pre-start summary logs `api_key_assignments` and `api_key_diagnostics` as process-counter deltas. Concurrent work using the same scheduler can contribute to those deltas. Account refresh logs short key IDs and counters. Storage uses SHA-256 fingerprints and excludes raw keys and account response bodies.

## Historical openings, current odds and snapshots

The historical reader reuses each ordered series for canonical prices, fixed moments and significant changes. A historical opening comes from the earliest eligible tick when **kickoff minus that tick is at least 60 minutes**. The kickoff in memory comes from `Event.starts_at` and is normalized to UTC, as are the provider's `createdAt` timestamps. The latest eligible tick supplies the historical current candidate. The observation cutoff controls which ticks are eligible independently of the opening age. See [significant-change selection](odds-significant-change-snapshots.md).

Recovery reconciles the historical candidate with the latest stored pre-kickoff capture for the same quote. Only observations with a provider timestamp at or before T−5 can win, and the stored capture wins ties. The winner can update the current quote and supply the T−5 observation. Recovery fills a missing opening on the existing quote, preserves a previously stored opening and records the provider's opening timestamp. It writes fixed moments strictly earlier than T−5 and significant ticks through T−5. A dedicated opening snapshot is created by ordinary ingestion; recovery writes an opening tick only if it is otherwise selected as a snapshot. For an exchange, reconciliation affects the side and level carried by the historical observation.

## Canonical writes and reads

Both provider phases use `MarketOddsIngestionService` and `MarketRepository`. Markets and choices are shared across sources, while each quote is identified by `(choice_id, source, exchange_side, exchange_level)`. OddsPortal writes its own opening-only quote. The read resolver selects conventional openings and current prices independently using [provider priority](../../config/odds_read_priority.json) and keeps exchange sides separate.

| Normalized OddsPapi field | Persistence |
|---|---|
| `sourceMarketId`, `sourceOutcomeId`, `bookmakerOutcomeId`, `mainLine` | Quote lineage and main-line flag |
| `initialDecimalValue` and `initialChangedAt` | `initial_odds` and `initial_captured_at` |
| `decimalValue` and `changedAt` | Current quote and provider timestamp of its observation |
| `exchangeQuotes` | Separate side/level instruments. An eligible opening belongs to the top back. Historical back prices cannot reconstruct the lay ladder. |
| `momentQuotes` | Additional snapshots attached to the matching quote, with provider timestamp and optional limit/size |

`markets` uses event, bookmaker, canonical market type, nullable line and phase (`is_live`) as its identity. Quotes and snapshots own price state. Ordinary canonical ingestion writes one event's bookmaker batches transactionally. Outcome source IDs and side/level also determine which existing instruments can receive recovered prices.

`initial_captured_at` comes from the provider. `current_updated_at` records the current quote write, `collected_at` records the snapshot write, and `source_collected_at` holds the provider's observation time when supplied. Several fixed moments can share one source tick, so counting snapshots is different from counting configured moments. See [persisted quote and snapshot fields](../architecture/odds_db_ingestion_context.md).

The synchronous provider phases commit before downstream evaluation. The odds alert waits for its scheduled OddsPortal work to finish before reading stored quotes, and skips that evaluation if the wait times out. [`MarketOddsReadRepository.get_market_odds_state`](../../infrastructure/persistence/repositories/market/market_odds_read_repository.py) accepts bookmaker, source, canonical market and live-state filters. Provider filters apply before read priority. The typed price result retains its own quote origin, including side/level, lineage and limit. Signed market lines stay part of read identity.

Trajectory reads are separate. They use the latest eligible provider tick at or before the objective. If provider time is unavailable, the observation must be within the configured tolerance. Real observation time controls which data were available to the evaluation. P4 keeps reconstructed checkpoints at the objective and source changes at their original time, and may add a logical closing point in memory. See the [P4 guide](../pillars/guia-funcional/04-pilar-4-movimiento-temporal.md).

## Development and diagnosis

The [pre-start simulator](../jobs/pre-start-simulator.md) forces a minute and can run the provider phases and downstream evaluation. The OddsPapi-only harness uses the event's **real** minutes to kickoff and still checks the provider allowlist. It is useful for inspecting one mapped event and reporting the quote persistence outcome without involving other providers or alerts.

```bash
python -m scripts.development.oddspapi.simulate_oddspapi_pre_start_odds 12345 --dry-run
```

This dry run fetches and parses the response while suppressing canonical market, quote, snapshot and source availability writes. Acquisition can still populate the main-line cache, and the harness enables raw debug output. An empty `/odds` replay preserves an existing debug file for the same request path. The standalone scripts under `odds_papi/` choose the first configured key by default. Set `ODDSPAPI_SCRIPT_KEY_INDEX=2` to diagnose the second credential without placing it on the command line.

| Tests | Contracts |
|---|---|
| [API key scheduler](../../tests/oddspapi/test_api_key_scheduler.py), [usage repository](../../tests/oddspapi/test_api_key_usage_repository.py), [client](../../tests/oddspapi/test_client.py) | Key eligibility, allocation, cooldowns, status, accounting, persisted estimates and retries. |
| [Closing lane](../../tests/test_t_minus_one_odds_job.py), [missing endpoints](../../tests/test_odds_endpoint_404_handling.py), [memory limits](../../tests/test_pre_start_memory_limits.py) | Existing pre-start execution boundaries and error behavior. |
| [OddsPapi ingestion](../../tests/oddspapi/test_ingestion_service.py), [recovery](../../tests/test_oddspapi_history_recovery.py), [read repository](../../tests/test_market_odds_read_repository.py) | Historical selection, quote persistence, replay and source-aware reads. |

To extend pre-start with another provider, implement a phase that consumes the shared plan, add the provider to source-state loading if it uses stored mappings, and attach the phase and its feature gate in `_ingest_provider_odds`. Use the shared provider runner for a simple fetch/ingest loop. Keep endpoint-specific acquisition inside the provider package and map lineage and quote fields before calling the canonical ingestion service.
