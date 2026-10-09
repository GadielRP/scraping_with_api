
# Database diagnostics queries

This document groups useful Postgres queries for diagnosing issues around event coverage, mapping, market data, odds, and snapshots. Update filters such as `competition`, `event_id`, `competition_id`, or source names before running them in production.

## 1. Event coverage and competition diagnostics

### Round distribution for a competition

```sql
SELECT
    COALESCE(round, 'NULL / Unspecified') AS round,
    COUNT(*) AS total_events,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM events
WHERE competition = 'LaLiga' -- change or remove as needed
GROUP BY round
ORDER BY total_events DESC;
```

Use this to check if a competition is skewed toward a specific round or if rounds are missing or inconsistent.

### Events with and without results by day

```sql
SELECT
    e.starts_at::date AS event_date,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE r.event_id IS NOT NULL) AS with_results,
    COUNT(*) FILTER (WHERE r.event_id IS NULL) AS without_results
FROM events e
LEFT JOIN results r
    ON r.event_id = e.id
GROUP BY e.starts_at::date
ORDER BY event_date DESC;
```

Useful for spotting gaps in result ingestion or delayed processing windows.

### Tracked mappings from oddspapi

```sql
SELECT
    esm.source_event_id AS oddspapi_fixture_id,
    e.id AS event_id,
    e.starts_at,
    e.sport,
    e.competition,
    e.home_team,
    e.away_team,
    e.competition_id,
    e.season_id
FROM public.events e
LEFT JOIN event_source_mappings esm
    ON e.id = esm.event_id
WHERE esm.source = 'oddspapi'
  AND e.competition_id IN (
      176, 318, 129, 167, 88, 168, 50, 171, 172,
      2192, 328, 429, 525, 180, 5153, 310, 2476, 261
  )
ORDER BY e.starts_at DESC;
```

This helps verify that tracked competitions are mapped correctly to the source provider.

### Events missing source mappings

```sql
SELECT
    e.id AS event_id,
    e.starts_at,
    e.competition,
    e.home_team,
    e.away_team,
    e.sport,
    e.competition_id,
    e.season_id
FROM events e
LEFT JOIN event_source_mappings esm
    ON e.id = esm.event_id
WHERE esm.event_id IS NULL
ORDER BY e.starts_at DESC
LIMIT 200;
```

This is a fast way to find untracked or orphaned events.

## 2. Markets and odds diagnostics

### Markets per canonical event

```sql
SELECT
    m.market_type_id,
    cmt.canonical_market_name AS market_type,
    m.line_value AS line,
    b.name AS bookie,
    m.market_id,
    e.id AS event_id,
    e.starts_at,
    e.home_team || ' vs ' || e.away_team AS matchup,
    m.is_live,
    mc.choice_name AS choice,
    mcq.quote_id,
    mcq.source,
    mcq.source_market_id,
    mcq.source_outcome_id,
    mcq.exchange_side,
    mcq.exchange_level,
    mcq.main_line,
    mcq.initial_odds,
    mcq.initial_captured_at,
    mcq.current_odds,
    mcs.source_collected_at AS source_updated_at,
    mcq.current_updated_at,
    mcq.movement
FROM events e
JOIN markets m
    ON m.event_id = e.id
JOIN canonical_market_types cmt
    ON cmt.market_type_id = m.market_type_id
JOIN bookies b
    ON b.bookie_id = m.bookie_id
JOIN market_choices mc
    ON mc.market_id = m.market_id
LEFT JOIN market_choice_quotes mcq
    ON mcq.choice_id = mc.choice_id
LEFT JOIN LATERAL (
    SELECT source_collected_at
    FROM market_choice_snapshots
    WHERE quote_id = mcq.quote_id
    ORDER BY collected_at DESC, snapshot_id DESC
    LIMIT 1
) mcs ON TRUE
WHERE e.id = 380004 -- replace with the event id under investigation
ORDER BY
    bookie,
    m.market_type_id ASC,
    m.line_value ASC NULLS FIRST,
    m.market_id ASC,
    mc.choice_id ASC,
    mcq.exchange_level ASC,
    mcq.source ASC;
```

This query reveals which markets, bookmakers, and outcomes are attached to a canonical event.

### Markets without quotes or choice records

```sql
SELECT
    m.market_id,
    m.event_id,
    e.starts_at,
    m.market_type_id,
    m.line_value,
    b.name AS bookie,
    mc.choice_id,
    mc.choice_name
FROM markets m
LEFT JOIN market_choices mc
    ON mc.market_id = m.market_id
LEFT JOIN market_choice_quotes mcq
    ON mcq.choice_id = mc.choice_id
JOIN events e
    ON e.id = m.event_id
JOIN bookies b
    ON b.bookie_id = m.bookie_id
WHERE mc.choice_id IS NULL
   OR mcq.quote_id IS NULL
ORDER BY e.starts_at DESC, m.market_id;
```

Useful for finding incomplete or broken market ingestion paths.

### Current odds movement by market

```sql
SELECT
    e.id AS event_id,
    e.starts_at,
    e.home_team,
    e.away_team,
    m.market_id,
    cmt.canonical_market_name AS market_type,
    m.line_value AS line,
    b.name AS bookie,
    mc.choice_name AS choice,
    mcq.current_odds,
    mcq.movement,
    mcq.current_updated_at,
    mcq.source
FROM events e
JOIN markets m
    ON m.event_id = e.id
JOIN canonical_market_types cmt
    ON cmt.market_type_id = m.market_type_id
JOIN bookies b
    ON b.bookie_id = m.bookie_id
JOIN market_choices mc
    ON mc.market_id = m.market_id
JOIN market_choice_quotes mcq
    ON mcq.choice_id = mc.choice_id
WHERE e.starts_at > NOW() - INTERVAL '7 days'
ORDER BY mcq.current_updated_at DESC NULLS LAST
LIMIT 200;
```

This is helpful when checking whether odds are updating as expected and whether movement is being recorded.

## 3. Snapshot and time-series checks

### Snapshots per canonical event id

```sql
SELECT
    m.market_type_id,
    cmt.canonical_market_name AS market_type,
    m.line_value AS line,
    b.name AS bookie,
    mc.choice_name AS choice,
    e.starts_at,
    mcs.collected_at AS snapshot_time,
    ROUND((EXTRACT(EPOCH FROM (e.starts_at - mcs.collected_at)) / 60.0)::numeric, 1) AS minutes_before_start,
    (e.starts_at - mcs.collected_at) AS time_before_start,
    mcq.source,
    mcq.exchange_side,
    mcq.source_outcome_id,
    mcs.odds_value,
    mcs.exchange_size,
    mcs.source_limit,
    mcs.snapshot_id,
    m.market_id
FROM events e
JOIN markets m
    ON m.event_id = e.id
JOIN canonical_market_types cmt
    ON cmt.market_type_id = m.market_type_id
JOIN bookies b
    ON b.bookie_id = m.bookie_id
JOIN market_choices mc
    ON mc.market_id = m.market_id
JOIN market_choice_quotes mcq
    ON mcq.choice_id = mc.choice_id
JOIN market_choice_snapshots mcs
    ON mcs.quote_id = mcq.quote_id
WHERE e.id = 115980 -- replace with the event id under investigation
ORDER BY
    m.market_type_id ASC,
    m.line_value ASC NULLS FIRST,
    m.market_id ASC,
    mc.choice_id ASC,
    mcs.collected_at ASC;
```

Ideal for reviewing historical odds points, especially before kickoff or during live updates.

### Stale or delayed snapshots

```sql
SELECT
    e.id AS event_id,
    e.starts_at,
    e.home_team,
    e.away_team,
    m.market_id,
    cmt.canonical_market_name AS market_type,
    b.name AS bookie,
    MAX(mcs.collected_at) AS latest_snapshot_at,
    NOW() - MAX(mcs.collected_at) AS time_since_last_snapshot
FROM events e
JOIN markets m
    ON m.event_id = e.id
JOIN canonical_market_types cmt
    ON cmt.market_type_id = m.market_type_id
JOIN bookies b
    ON b.bookie_id = m.bookie_id
JOIN market_choices mc
    ON mc.market_id = m.market_id
JOIN market_choice_quotes mcq
    ON mcq.choice_id = mc.choice_id
JOIN market_choice_snapshots mcs
    ON mcs.quote_id = mcq.quote_id
WHERE e.starts_at > NOW() - INTERVAL '24 hours'
GROUP BY
    e.id, e.starts_at, e.home_team, e.away_team,
    m.market_id, cmt.canonical_market_name, b.name
HAVING MAX(mcs.collected_at) < NOW() - INTERVAL '15 minutes'
ORDER BY time_since_last_snapshot DESC;
```

Use this to find markets that have stopped receiving updates unexpectedly.

## 4. Data quality and anomaly checks

### Duplicate source mappings

```sql
SELECT
    esm.source,
    esm.source_event_id,
    COUNT(*) AS duplicate_count,
    array_agg(esm.event_id ORDER BY esm.event_id) AS event_ids
FROM event_source_mappings esm
GROUP BY esm.source, esm.source_event_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, esm.source_event_id;
```

This is the quickest way to catch duplicate external-to-internal mappings.

### Potential orphaned quote/snapshot records

```sql
SELECT
    mcq.quote_id,
    mcq.choice_id,
    mcq.source,
    COUNT(mcs.snapshot_id) AS snapshot_count
FROM market_choice_quotes mcq
LEFT JOIN market_choice_snapshots mcs
    ON mcs.quote_id = mcq.quote_id
LEFT JOIN market_choices mc
    ON mc.choice_id = mcq.choice_id
WHERE mc.choice_id IS NULL
GROUP BY mcq.quote_id, mcq.choice_id, mcq.source
ORDER BY snapshot_count DESC, mcq.quote_id;
```

Helps locate quotes that are no longer attached to valid choices or are missing stable lineage.

## Quick notes

- Replace hard-coded ids (`event_id`, `competition_id`) with the actual values you are investigating.
- Use `WHERE` filters early to reduce query costs when scanning large tables.
- For time-series checks, compare `collected_at`, `current_updated_at`, and `source_collected_at` together to identify delayed or partial ingestion.
- If you are debugging a specific source, add `mcq.source = '...'` or a `bookie_id` filter to narrow the result set.



