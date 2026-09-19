# Canonical market identity

This document describes the current market persistence contract used by the
repository. It is intentionally limited to the runtime architecture; the
historical migration plan and local migration audits live under
`docs/refactors/` and `docs/audits/`.

## Source of truth

`canonical_market_types` is the provider-independent catalog of market
semantics. Each row has:

- `canonical_market_key`: stable business key used by provider mappings;
- `market_type_id`: stable numeric identifier used by high-cardinality rows;
- `canonical_market_name`, `canonical_market_group`, and
  `canonical_market_period`: display and grouping metadata;
- `market_family`: normalization family, such as `total` or
  `spread_2way`;
- `requires_line_value`: whether a market requires a numeric line;
- ingestion and trajectory enablement flags.

Catalog IDs are append-only. An existing `market_type_id` must never be
reused for a different market type.

## Persisted identity

The identity of one persisted market is:

```text
(event_id, bookie_id, market_type_id, line_value, is_live)
```

`markets` stores the event/bookmaker shell and the numeric identity:

- `market_type_id` is a mandatory foreign key to
  `canonical_market_types.market_type_id`;
- `line_value` is a signed `NUMERIC` value and is nullable;
- `is_live` distinguishes live and pre-event instances.

`line_value IS NULL` is valid for non-line markets such as 1X2, moneyline,
home/away, and draw-no-bet. Totals and handicaps use values such as `2.5`,
`-1.5`, or `+3.0`.

PostgreSQL partial unique indexes enforce the two NULL-safe identity cases:

```text
event_id + bookie_id + market_type_id + is_live
event_id + bookie_id + market_type_id + line_value + is_live
```

## Price hierarchy

```text
events
  └── markets
        └── market_choices
              └── market_choice_quotes
                    └── market_choice_snapshots
```

- `markets` identifies one canonical market instance.
- `market_choices` identifies outcomes such as `1`, `X`, `2`, `Over`, or
  `Under`.
- `market_choice_quotes` stores the current provider/side/depth instrument.
- `market_choice_snapshots` stores append-only price observations.

Prices do not belong to `markets`; they belong to quotes and snapshots.

## Provider boundary

Provider adapters translate external payloads into the internal canonical
payload before persistence:

```text
provider payload
  → provider adapter
  → canonicalMarketKey + lineValue
  → market repository
  → market_type_id + line_value
```

For SofaScore, the external field is named `choiceGroup`. The adapter maps it
once to the internal `lineValue` field. Persistence and canonical normalizers
must consume `lineValue`, not the provider-specific field name.

Provider market names, groups, periods, source IDs, and source outcome IDs are
lineage data. They do not replace the canonical relational identity.

## Persistence responsibilities

- `canonical_market_types.py` defines the catalog seeds and stable IDs.
- `canonical_market_type_repository.py` loads and validates catalog rows.
- Provider adapters normalize external formats and do not create relational
  identity.
- `market_repository.py` validates canonical keys, resolves numeric IDs, and
  persists markets, choices, quotes, and snapshots atomically.
- Read queries join `markets` to `canonical_market_types` when display names,
  groups, or periods are needed.
- Alembic owns schema changes. Application startup verifies the expected
  revision but does not perform DDL or historical backfills.
