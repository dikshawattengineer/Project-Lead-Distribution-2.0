# Release / data migration — runbook

Use this when **migrating CRM data** (release, refresh, or re-seed).  
This is **separate** from day-to-day parked nightly (`PARKED_STATE.md`).

**Goal:** fill fallback tables, deploy LD functions, park suppliers, reclaim companies into Retentions — **without** breaking private bags or callbacks.

---

## When to use this

| Situation | Use this doc |
|-----------|----------------|
| First LD go-live on migrated CRM data | Yes |
| Boss re-ran Prisma migrate / data refresh | Yes |
| Build past-sale fallback in Databricks from `external_site_mappings` + contracts | Yes (notebook) |
| Normal nightly after go-live | No — only Databricks **Run all** |
| Adding one new supplier later | `13_turn_on_supplier_routing.md` → **New supplier** section |

---

## Prerequisites

- [ ] Boss **Prisma migrate** applied (`companies`, `pools`, `providers`, `contracts`, … exist).
- [ ] `external_site_mappings` loaded (if you use sourcebridge fallback).
- [ ] Supabase SQL editor access.
- [ ] Databricks job can reach Postgres (pooler credentials in notebook).

---

## Supabase — run in this order

Copy/paste each file in Supabase. Wait for success before the next.

| Step | File | What it does | Changes `poolId`? |
|------|------|----------------|-------------------|
| 1 | **`06_crm_pool_rule_uuid.sql`** | `pool_rules.id` → uuid, unique `tag` (table renamed in Prisma) | No |
| 2 | **`04_parked_minimal.sql`** | Parked prod — pools + `pool_rules` only; `provider_families` stays empty | No |
| 2full | **`04_seed_ld_pools.sql`** | Full seed — use on **supplier turn-on**, not parked go-live | No |
| — | **Databricks `working_table.py`** | Past-sale fallback built in UC (`snap_crm_company_load_sale`) — skip **16** / **17** in prod | No |
| 7 | **`10_park_supplier_routing.sql`** | Park suppliers — only Retention rules active | No |
| 8 | **`11_reclaim_from_parked_pools.sql`** | Move past-sale companies out of supplier/Unassigned → Retention / Past Retention | **Yes** |
| 9 | **`12_hide_parked_pools_from_agents.sql`** | Hide supplier/Unassigned/Upselling from Pool filter; fix `pool_links` nesting | No (`pool_profiles` only) |
| 10 | **`12a_fix_private_one_owner.sql`** | Only if private bags have `agents_linked > 1` | No |
| 11 | **`apply_ld_apply_batch.sql`** | `ld_apply_batch` table + `ld_apply_batch_run()` + `ld_actor_user_id()` | No (until batch run) |
| 12 | **`24_janitor.sql`** | `ld_janitor_run()` — clear `profileId` on disabled profiles | No |

### Skip on release (unless turn-on day)

| File | Why skip |
|------|----------|
| **`14_crm_load_source.sql`** | Old site-level load path — we use **`external_site_mappings`** + Databricks fallback |
| **`16_fallback_from_sourcebridge.sql`** | Dev only — prod builds fallback in Databricks |
| **`17_fill_sale_end_date.sql`** | Dev only — CED included in notebook fallback |
| **`15_company_site_load_and_sale.sql`** | Old `crm_company_site_load` — not needed for current nightly |
| **`09_sync_provider_pools.sql`** | Suppliers parked — run on turn-on (or pre-create pools then re-run **10**) |
| **`03_wipe_old_ld_pool_ids.sql`** | Cleanup only |
| **`12_revert_hide_parked_pools.sql`** | Undo hide — not for migration |
| **`18` / `19` / `21`** | Old split / profile allocate — parked |

### Optional (same release window)

| File | When |
|------|------|
| **`05_unlink_private_pool_links.sql`** | Fair-share off (also done inside **12**) |
| **`22_seed_customer_care_corporate.sql`** | If those pools/rules missing |
| **`27_seed_campaigns.sql`** | If campaign seed needed |

---

## Past-sale fallback (Databricks — not Supabase)

Each nightly run builds **`crm_load.new_crm.snap_crm_company_load_sale`** from:

1. **external_site_mappings** where campaign/source looks like retention  
2. **Any company** with a contract end date on company / site / meter  
3. **Every company** row (retention load assumption)

Picks **best CED** per company: Retention window (1–540) beats Past beats far-out.  
**Does not** change `companies.poolId`. No `crm_company_load_sale` table in prod enrichment.

### Verify fallback (Databricks)

Check notebook output: `snap_crm_company_load_sale (Databricks fallback)` row count, and `with_past_deal` in the working-table summary.

---

## Functions deployed (step 11 + 12)

After **`apply_ld_apply_batch.sql`**:

```sql
SELECT public.ld_actor_user_id();          -- system@watt.co.uk uuid
SELECT public.ld_apply_batch_run();         -- 0 if batch empty
SELECT public.ld_janitor_run();             -- clears disabled-profile assigns
```

Databricks calls **`ld_janitor_run()`** at the start of each run, then **`ld_apply_batch_run()`** at the end.

---

## Databricks — after Supabase steps

1. Pull latest **`working_table.py`** from repo.  
2. **Run all** (top to bottom).  
3. Check notebook output:
   - `janitor` row count (if any)
   - `parked apply only: RETENTION, PAST_RETENTION, COMPLAINT, CALLBACK`
   - `apply companies_moved` count

---

## Post-migration QA

```sql
-- Past-sale not stuck in supplier pools
SELECT COUNT(*) FROM public.companies c
JOIN public.pools p ON p.id = c."poolId"
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN')
  AND COALESCE(p.code, '') NOT IN ('RETENTION', 'PAST_RETENTION', 'COMPLAINT');

-- Active rules while parked
SELECT tag, "isActive"
FROM public.pool_rules
WHERE tag IN ('RETENTION', 'PAST_RETENTION', 'UNASSIGNED', 'UPSELLING')
ORDER BY tag;

-- Private bags one owner
SELECT p.code, COUNT(pp."profileId") AS agents
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.type = 'PRIVATE'
GROUP BY p.id, p.code
HAVING COUNT(pp."profileId") <> 1;
```

---

## Re-run fallback later?

| Script | Safe to re-run? | When |
|--------|-----------------|------|
| **Databricks Run all** | Yes | Rebuilds fallback after mapping / contract refresh |
| **`16_fallback_from_sourcebridge.sql`** | Dev only | Local Supabase testing |
| **`11_reclaim_from_parked_pools.sql`** | Yes | Cleanup only — moves past-sale out of parked pools |

**Do not** re-run **11** on a live parked system unless you know companies drifted back into supplier pools.

---

## One-page checklist (release day)

```
□ Prisma migrate (boss)
□ 06 → 04_parked_minimal (not full 04 while parked; provider_families empty)
□ 10 park suppliers
□ 11 reclaim → Retentions
□ 12 hide supplier pools in UI (+ 12a if needed)
□ apply_ld_apply_batch.sql
□ 24_janitor.sql
□ Databricks Run all
□ QA queries above
```

After release: nightly = **Databricks only** (`PARKED_STATE.md`).
