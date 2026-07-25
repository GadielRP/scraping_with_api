# Oddspapi fixture-discovery production readiness

Date: 2026-07-25

## Deployment decision

The change is deployable for a controlled production trial with the existing
Docker Compose deployment. It does not change the 17:45 local schedule and it
does not remove either alert pipeline.

Expected production flags:

```env
ENABLE_LEGACY_ALERT_PIPELINE=true
ENABLE_PILLAR_PIPELINE=false
ALERT_PIPELINE_WORKERS=1
PILLAR_PIPELINE_WORKERS=1
MATCHUP_TEAM_HISTORY_WORKERS=1
MATCHUP_H2H_MAX_EVENTS=200
APP_MEMORY_LIMIT=768m
ODDSPAPI_FIXTURE_DISCOVERY_TIMES=17:45
ODDSPAPI_FIXTURE_DISCOVERY_CATCHUP_LOOKBACK_HOURS=36
ODDSPAPI_FIXTURE_DISCOVERY_MAX_CATCHUP_RUNS=2
```

The first start after deployment can legitimately replay up to two target UTC
dates from the preceding 36 hours because older successful executions predate
the new durable marker table. This is bounded and mappings remain idempotent,
but the startup can take longer than usual.

## What changed

### Missed-run recovery

- `oddspapi_fixture_discovery_runs` stores one durable row per UTC target date.
- A successful or currently running target cannot be claimed twice.
- Failed and interrupted targets remain retryable.
- Startup marks work left `running` by the previous process as interrupted.
- Startup replays eligible missed slots before the immediate pre-start job.
- Partial sport errors are persisted as failure and generate a best-effort
  Telegram operations alert.

### Memory

- Legacy and pillar event fan-out use configurable worker counts and run
  directly without a thread pool when configured with one worker.
- Home and away team histories run sequentially by default.
- Raw H2H input is bounded to the newest 200 events.
- Duplicate historical result dictionaries are no longer retained.
- The Docker build context is allowlisted. The application-code image layer
  fell from 559 MB to 2.91 MB.
- A redundant recursive ownership change was removed. The resulting local
  image fell from approximately 1.14 GB to 665 MB, while Chromium still passed
  a launch smoke test as `appuser`.
- Production Compose gives the app a hard 768 MB containment ceiling. It does
  not reserve that memory. If the app reaches it, the kernel can kill the
  container instead of letting it consume the entire host and take PostgreSQL
  down with it.

### Observability

- `logs/runtime_state.json` persists the active operation, heartbeat, clean
  shutdown marker, process RSS, total cgroup memory, anonymous memory, file
  cache, peak cgroup memory, and cgroup OOM counters.
- `logs/fatal_python.log` captures fatal Python diagnostics.
- The next process start reports an earlier unclean shutdown.
- Scheduler logs list due jobs, lateness, and total batch duration.

An OOM `SIGKILL` cannot execute Python cleanup code. Evidence is therefore the
unclean state file on restart plus `memory.events`/`oom_kill` and Docker's
`OOMKilled` state.

## Validations completed locally

- Production Compose configuration renders successfully.
- Production image builds successfully.
- Imports pass inside the built image.
- Chromium launches as the non-root application user.
- Production-image `python main.py status` initializes successfully against a
  current PostgreSQL copy.
- PostgreSQL created the durable table, unique constraint, and status index.
- Relevant automated suite: 22 passed.
- Representative A/B pre-start workload:
  - baseline: 10 H2H calls, 66 team-history pages, 116.0 MB peak RSS, 43.6 s;
  - optimized: identical calls, 94.2 MB peak RSS, 43.4 s;
  - process peak reduction: 18.8%.

Running the entire historical `tests/` directory is not a valid gate today:
`tests/test_pre_start_check_job.py` performs PostgreSQL-only initialization at
collection time while the test command uses SQLite, causing collection to
terminate before the suite runs. This predates this change. The production
PostgreSQL smoke test above covers the initialization path relevant here.

## Production deployment

Do not delete or recreate the PostgreSQL volume.

Because the host has 1 GB RAM, stop only the application while building so the
old Python process does not compete with Docker BuildKit. Leave PostgreSQL up.

```bash
cd /opt/sofascore

# Confirm the server-owned secret/config file exists.
test -f .env.prod
grep -E '^(ENABLE_(LEGACY_ALERT|PILLAR)_PIPELINE|ALERT_PIPELINE_WORKERS|PILLAR_PIPELINE_WORKERS|MATCHUP_TEAM_HISTORY_WORKERS|MATCHUP_H2H_MAX_EVENTS|APP_MEMORY_LIMIT|ODDSPAPI_FIXTURE_DISCOVERY_(TIMES|CATCHUP_LOOKBACK_HOURS|MAX_CATCHUP_RUNS))' .env.prod

# Render the exact production configuration before changing the running app.
docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml config --quiet

# Keep a rollback image, then stop only the app.
docker tag sofascore-app:latest sofascore-app:pre-fixture-recovery
docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml stop app

# Build and start the new app. PostgreSQL and its volume remain running.
docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml build app
docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml up -d --no-deps app
```

## Immediate verification

```bash
docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml ps

docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml logs --since 15m app

docker inspect sofascore-app \
  --format 'status={{.State.Status}} restarts={{.RestartCount}} oom={{.State.OOMKilled}} exit={{.State.ExitCode}} memory_limit_bytes={{.HostConfig.Memory}}'

docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml exec app \
  cat /app/logs/runtime_state.json

docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml exec postgres \
  psql -U sofascore -d sofascore_odds -c \
  "SELECT target_date,status,trigger,scheduled_local_date,scheduled_time,started_at,finished_at,error
   FROM oddspapi_fixture_discovery_runs
   ORDER BY id DESC LIMIT 10;"
```

Expected startup evidence:

- `Runtime observability started`
- `Catch-up Oddspapi fixture discovery...` only when a due target lacks success
- `Operation finished name=oddspapi_fixture_discovery...`
- immediate `pre_start_check` only after catch-up completes
- `clean_shutdown=false` while the daemon is alive is normal
- `memory_limit_bytes=805306368` confirms the 768 MB app ceiling

Host-level memory and OOM evidence:

```bash
free -m
docker stats --no-stream
journalctl -k --since "30 minutes ago" |
  grep -Ei 'out of memory|oom|killed process'
```

## Trial success criteria

After the next 17:45 local window:

1. The intended UTC target has a row with `status=success`.
2. Logs contain the discovery start and finish summary.
3. `cgroup_oom_kill` has not increased.
4. `docker inspect` reports `OOMKilled=false`.
5. If the process does die, the restarted process reports the previous active
   operation and catch-up produces a successful row without manual recovery.
6. Host evidence (`journalctl -k` or `dmesg`) is checked if the database or
   entire VM becomes unstable despite the app containment boundary.

## Rollback

The schema addition is backward compatible and does not need to be removed.

```bash
cd /opt/sofascore
docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml stop app
docker tag sofascore-app:pre-fixture-recovery sofascore-app:latest
docker compose --env-file .env.prod \
  -f compose.yaml -f compose.prod.yaml up -d --no-deps --force-recreate app
```
