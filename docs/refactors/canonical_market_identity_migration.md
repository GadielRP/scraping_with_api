# Canonical market identity migration

## Decision

`canonical_market_types` owns provider-independent market semantics. Its public
business key remains `canonical_market_key`; its compact storage key is the
stable `SMALLINT market_type_id`. IDs are append-only and must never be reused.

The canonical identity of a persisted market is:

```text
(event_id, bookie_id, market_type_id, line_value, is_live)
```

`line_value` is the signed numeric line/handicap. It is nullable because
moneyline/1X2 markets have no line. PostgreSQL partial unique indexes split
the `NULL` and non-`NULL` cases, because ordinary unique constraints consider
two `NULL` values distinct. During this expand phase, `choice_group` remains a
legacy text mirror for compatibility; it is not the source of truth.

Provider labels and IDs remain lineage, not identity:

```text
provider payload
  -> market_source_mappings
  -> canonical_market_types
  -> markets
  -> market_choices
  -> market_choice_quotes
  -> market_choice_snapshots
```

`market_choice_quotes` is current price state per provider/side/depth.
`market_choice_snapshots` is append-only history. `market_choices` contains
outcome identity only.

## Module boundaries

- `orm_base.py`: declarative registry only; it has no engine/session dependency.
- `odds_models.py`: canonical market catalog, mappings, market shells, choices,
  quotes, and snapshots.
- `database.py`: legacy bootstrap compatibility while Alembic adoption is
  completed. New production schema changes belong in Alembic revisions.
- provider adapters: translate provider payloads and emit
  `canonicalMarketKey`; they do not decide relational identity.
- `MarketRepository`: validates the key against the catalog and persists the
  numeric FK atomically.
- read queries: join directly by `markets.market_type_id`; text/mapping joins
  are migration fallbacks only.

## Safe rollout

1. Back up PostgreSQL and deploy the additive code/schema.
2. For an existing unversioned database, compare it with the pre-change schema
   assumptions and then run `alembic stamp 20260918_00`. Do not blindly stamp
   an unknown schema; the baseline revision intentionally contains no DDL.
3. Run `alembic upgrade head` during deployment, outside application startup.
   Revision `20260918_02` adds/backfills `markets.line_value` and moves the
   canonical uniqueness indexes from the legacy text mirror to that numeric
   column.
4. Audit without writes:

   ```powershell
   python -m scripts.maintenance.backfill_market_type_ids
   ```

5. Review unresolved/ambiguous samples. Then apply deterministic backfills:

   ```powershell
   python -m scripts.maintenance.backfill_market_type_ids --apply
   ```

6. Require `ready_for_contract=true`, monitor unknown-key and conflicting-ID
   logs, and compare market/quote/snapshot counts before and after deployment.
7. In a later revision, make `markets.market_type_id` non-null and remove the
   legacy display identity and duplicated mapping display columns.

No table rewrite, `VACUUM FULL`, destructive deduplication, or automatic
contract step runs at application startup. Ambiguous historical rows require an
explicit mapping decision and are deliberately left untouched.

## Rollback

The expand phase is dual-readable: legacy display columns remain populated.
Rolling back application code therefore does not require removing the numeric
columns. Prefer rolling the application back and retaining additive columns;
run the Alembic downgrade only when no deployed process depends on them.
