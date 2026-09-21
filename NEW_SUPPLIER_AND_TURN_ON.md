# New supplier / campaign + turn-on suppliers

Two scenarios:

1. **Suppliers still parked** (current) — pre-stage a provider; routing stays Retention-only.  
2. **Turn-on day** — full supplier + Unassigned + Upselling routing live.

See also: `PARKED_STATE.md` (parked nightly), `RELEASE_MIGRATION.md` (data migration).

---

## A) New provider added while suppliers are PARKED

Use when a new energy supplier appears in CRM but you are **not** routing leads to supplier pools yet.

### Step by step

| Step | Who | Action |
|------|-----|--------|
| 1 | **CRM / boss** | New row in `public.providers` (normal Prisma flow). |
| 2 | **You** | **Do not** run full turn-on. Optionally pre-create LD objects: |
| 2a | | Run **`09_sync_provider_pools.sql`** once (creates `pools` row, `provider_families`, `{TAG}_NOW` / `{TAG}_IN_WINDOW` rules). |
| 2b | | Run **`10_park_supplier_routing.sql`** again so new rules stay **inactive**. |
| 3 | **You** | **Do not** add `pool_profiles` for agents yet (Pool filter stays Retentions + private bags). |
| 4 | **Databricks** | No change — still parked apply. |

### Verify (optional pre-stage)

```sql
SELECT p.code, p.name FROM public.pools p
WHERE p.code = 'NEW_SUPPLIER_CODE';  -- tag_code from 09

SELECT tag, "isActive" FROM public.pool_rules
WHERE tag LIKE 'NEW_SUPPLIER_CODE%';
-- Expect isActive = false after step 2b
```

**Nightly behaviour while parked:** companies are **not** moved to the new supplier pool until turn-on. Tagging may compute `*_IN_WINDOW` in `ld_working` for QA but apply skips them.

---

## B) Turn-on day — suppliers + campaigns live

Use when boss says go live with per-supplier pools, Unassigned, Upselling.

### Before you start

- [ ] `04_seed_ld_pools.sql` ran (core pools + rules).  
- [ ] `06_pool_rules_uuid.sql` ran if migrating from `ld_rule_*` ids.  
- [ ] `apply_ld_apply_batch.sql` deployed.  
- [ ] Any **new** `providers` rows exist in CRM.  
- [ ] `RELEASE_MIGRATION.md` fallback (**16** + **17**) already done if this is first go-live.

---

### Supabase — run in order

| Step | File / action |
|------|----------------|
| **1** | **`09_sync_provider_pools.sql`** — every `providers` row → pool + `provider_families` + NOW/IN_WINDOW rules (uuid ids, upsert on `tag`). |
| **2** | **Re-activate routing** (SQL below). |
| **3** | **`12_revert_hide_parked_pools.sql`** OR manually add `pool_profiles` so agents see supplier pools they work. |
| **4** | **CRM admin** — link agents to supplier shared pools in `pool_profiles` (not Retention / private). |
| **5** | **Fair-share** — if Jack/Kelly split on: re-activate `pool_links` in CRM; skip **`05`** / **`05_unlink`**. If off: keep **`05`** / links inactive. |

#### SQL — step 2 re-activate (after `09`)

```sql
BEGIN;

UPDATE public.provider_families
SET "isActive" = true,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE COALESCE("isManual", false) = false;

UPDATE public.pool_rules
SET "isActive" = true,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE tag NOT IN ('COMPLAINT', 'CALLBACK', 'RETENTION', 'PAST_RETENTION');

COMMIT;
```

#### Verify

```sql
SELECT tag, COUNT(*) FROM public.pool_rules
WHERE "isActive" = true GROUP BY tag ORDER BY tag;

SELECT COUNT(*) FROM public.provider_families WHERE "isActive" = true;
```

---

### Databricks — `working_table.py` changes

| Step | Change |
|------|--------|
| 1 | **Uncomment** `provider_families` JDBC snapshot (remove empty parked dataframe). |
| 2 | **Propose pool** — change `fill proposed_pool_id` so non-retention tags use `r.poolId` / Unassigned fallback (not `ELSE NULL`). See `13_turn_on_supplier_routing.md`. |
| 3 | **Apply batch** — remove `PARKED_APPLY_TAGS` filter or expand to all supplier tags; or use `proposed_pool_id IS NOT NULL` only. |
| 4 | **Fair-share (optional)** — uncomment `pool_links` snapshot + `spark.sql(_FAIR_SHARE_SQL)`. |
| 5 | Git pull → **Run all**. |

---

### QA after turn-on

```sql
-- Spot-check new supplier pool has companies after first nightly
SELECT p.code, COUNT(c.id) AS companies
FROM public.pools p
LEFT JOIN public.companies c ON c."poolId" = p.id
WHERE p.code IN ('RETENTION', 'PAST_RETENTION', 'UNASSIGNED', 'YOUR_NEW_SUPPLIER_CODE')
GROUP BY p.code;
```

In Databricks:

```sql
SELECT lead_tag, COUNT(*) FROM crm_load.new_crm.ld_working
GROUP BY lead_tag ORDER BY COUNT(*) DESC;
```

---

## C) New supplier AFTER turn-on (suppliers already live)

| Step | Action |
|------|--------|
| 1 | Boss adds row to **`providers`**. |
| 2 | Run **`09_sync_provider_pools.sql`** (creates pool + family + rules). |
| 3 | New rules are inserted **`isActive = true`** by 09 — no **10** needed. |
| 4 | Add **`pool_profiles`** for agents who work that supplier. |
| 5 | Next **Databricks Run all** — no code change if full routing already on. |
| 6 | QA: `SELECT tag, "isActive" FROM pool_rules WHERE tag LIKE 'NEW_TAG%';` |

---

## Park again (rollback)

```
1. 10_park_supplier_routing.sql
2. 12_hide_parked_pools_from_agents.sql
3. Databricks — retention-only apply (PARKED_APPLY_TAGS)
4. Optional: 11_reclaim_from_parked_pools.sql
```

---

## Quick reference

| Doc | Purpose |
|-----|---------|
| `PARKED_STATE.md` | Current parked nightly + callbacks |
| `RELEASE_MIGRATION.md` | Release: fallback **16**, functions, park, reclaim |
| `13_turn_on_supplier_routing.md` | Technical Databricks SQL snippets for propose/apply |
| `NEW_SUPPLIER_AND_TURN_ON.md` | This file — new provider + turn-on steps |
