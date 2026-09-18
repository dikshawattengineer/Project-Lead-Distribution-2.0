# Lead Distribution 2.0 — parked state (current production)

Nightly routing is **parked**: only **Retentions**, **Past Retentions**, **Complaint**, and **Callback** pool moves apply. Supplier pools, **Unassigned**, and **Upselling** are off until turn-on.

**Other guides:**

| Doc | When |
|-----|------|
| **`RELEASE_MIGRATION.md`** | Release / data migration — fallback **16**, **17**, all functions, park, reclaim |
| **`NEW_SUPPLIER_AND_TURN_ON.md`** | New provider in CRM + full supplier turn-on (step by step) |
| **`13_turn_on_supplier_routing.md`** | Databricks SQL snippets for propose-pool / apply when live |

---

## What runs every night

```
Databricks working_table.py (Run all)
  → snapshot CRM tables
  → tag companies (full logic, including supplier tags for QA)
  → propose pool (retention + sticky only while parked)
  → write public.ld_apply_batch
  → SELECT public.ld_apply_batch_run()
```

**Do not** also schedule apply in Supabase (`25_cron.sql` is docs only).

---

## Supabase scripts — run order (one-time / current dev)

| Order | File | Purpose |
|-------|------|---------|
| 0 | Boss Prisma migrate | App tables |
| 0b | `06_crm_pool_rule_uuid.sql` | `crm_pool_rule.id` → uuid, unique `tag` (existing DBs) |
| 1 | `04_seed_ld_pools.sql` | Core pools + rules (upsert on `pools.code`, `crm_pool_rule.tag`) |
| — | `16_fallback_from_sourcebridge.sql` | Past-sale fallback (`legacy_site_mappings` — skip **14** / **15**) |
| — | `09_sync_provider_pools.sql` | **SKIP while parked** (script updated for turn-on day) |
| 2 | `10_park_supplier_routing.sql` | Deactivate supplier rules + `crm_provider_family` |
| 3 | `11_reclaim_from_parked_pools.sql` | Past-sale cos in parked pools → Retention (1–540) or Past Retention (expired only); far-future left alone |
| 3b | `11b_far_future_out_of_past_retention.sql` | **If old 11 ran:** move 541+ CED cos out of Past Retentions → Unassigned |
| 4 | `12_hide_parked_pools_from_agents.sql` | Hide supplier/Unassigned/Upselling from Pool filter |
| 4b | `12a_fix_private_one_owner.sql` | If private bags had extra `pool_profiles` links |
| 5 | `apply_ld_apply_batch.sql` | `ld_apply_batch` + `ld_apply_batch_run()` |
| 6 | `24_janitor.sql` | `ld_janitor_run()` |
| — | `05_unlink_private_pool_links.sql` | Fair-share off (`pool_links` inactive) — also in **12** |

**Undo pool filter hide only:** `12_revert_hide_parked_pools.sql`  
**Turn suppliers on:** `13_turn_on_supplier_routing.md`

---

## What is active vs parked

| Item | Status |
|------|--------|
| `crm_pool_rule` **RETENTION**, **PAST_RETENTION**, **COMPLAINT** | `isActive = true` |
| Supplier / Unassigned / Upselling / Corporate / Customer Care rules | `isActive = false` |
| `crm_provider_family` | `isActive = false` |
| `pool_links` (fair-share) | `isActive = false` |
| Databricks `crm_provider_family` snapshot | Empty (parked) |
| Databricks fair-share SQL | Commented out |
| Databricks apply tags | `RETENTION`, `PAST_RETENTION`, `COMPLAINT`, `CALLBACK` only |

---

## Pool filter (CRM UI)

| Visible | Hidden (while parked) |
|---------|------------------------|
| **Retentions**, **Past Retentions** (shared) | Supplier pools from **09** |
| Each agent’s **own PRIVATE** bag | **Unassigned**, **Upselling** |
| **Complaint** (managers) | |

- `pool_profiles` controls supplier visibility in the **+ Pool** filter.
- **PRIVATE** bags: one owner per pool (`profiles.primaryPoolId`).
- Script **12** turns off `pool_links` nesting so hiding suppliers does not hide private bags.

---

## Tables we own vs Prisma

| LD owns (seed / ETL) | Prisma / CRM owns |
|----------------------|-------------------|
| `crm_pool_rule` | `pools`, `companies`, `profiles` |
| `crm_provider_family` | `pool_profiles`, `pool_links` |
| `ld_apply_batch`, `ld_apply_batch_run()` | `company_pool_placements`, `company_pool_audits` |
| | `callbacks`, `contracts`, `providers` |

**Boss Prisma models needed:** `crm_pool_rule`, `crm_provider_family`  
- `id` = uuid; stable key = **`tag`** (not `ld_rule_*` text).

---

## Apply function (`ld_apply_batch_run`)

- Actor: `system@watt.co.uk`
- Updates `companies.poolId`, closes/opens `company_pool_placements`, writes `company_pool_audits`
- `reason` = NULL; does **not** set `profileId` or `companies.campaignId`
- `sourcePoolId` on placement only when child pool ≠ parent (fair-share — parked)

---

## Callbacks — agent on a shared pool (Retentions)

### My Callbacks tab (CRM)

- **Not touched** by lead distribution.
- Callbacks stay on the agent who created them (`callbacks.createdById` → **My Callbacks**).
- Other agents do not see that callback in their tab.

### Nightly pool moves

While a site has an open callback (`callbacks.status = 'SCHEDULED'`):

1. **Tag** = `CALLBACK` (sticky — beats Retention / supplier tags).
2. **Protected** — will **not** be moved by Retention / Past Retention / supplier routing.
3. **Proposed pool** = agent’s `profiles.primaryPoolId` (their **PRIVATE** bag), or **stay on current pool** if that is missing.
4. **Apply** includes `CALLBACK` — if proposed pool ≠ current pool, nightly run **may move** the company into that agent’s private bag.

So:

| Question | Answer |
|----------|--------|
| Callback still in agent’s **My Callbacks**? | **Yes** |
| Pulled to another agent or supplier/retention churn? | **No** while callback is open |
| Company can move from **Retentions** → agent **private** bag? | **Yes**, if `primaryPoolId` is set and differs |

When the callback is completed/cancelled (no longer `SCHEDULED`), normal **Retention / Past Retention** tagging applies again on the next run.

---

## Tagging vs apply (Upselling / Unassigned)

- **Tagging** still computes `UPSELLING`, `UNASSIGNED`, `PRE_WINDOW`, `*_IN_WINDOW` (for QA counts).
- **Apply** does **not** move those pools while parked.
- When CED enters **1–540 days**, tag → `RETENTION` → apply moves to **Retentions**.
- **Expired** CED → **Past Retentions** (reclaim **11** + apply).
- **Far-future** CED (541+ days) → **Unassigned** when live; **no pool write** while parked (same as Upselling/supplier).
- Full supplier / Unassigned apply resumes on turn-on (`13`).

---

## Fair-share (private agent bags) — parked

- `pool_links` snapshot commented out in Databricks
- `_FAIR_SHARE_SQL` not executed
- `05_unlink_private_pool_links.sql` deactivates parent → child links
- Re-enable: `13_turn_on_supplier_routing.md` + uncomment fair-share in `working_table.py`

---

## Quick QA queries

```sql
-- Active rules
SELECT tag, "isActive" FROM public.crm_pool_rule
WHERE tag IN ('RETENTION', 'PAST_RETENTION', 'UNASSIGNED', 'UPSELLING')
ORDER BY tag;

-- Companies still in parked pools (expect 0 after 11)
SELECT COUNT(*) FROM public.companies c
JOIN public.pools p ON p.id = c."poolId"
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN')
  AND COALESCE(p.code, '') NOT IN ('RETENTION', 'PAST_RETENTION', 'COMPLAINT', 'CALLBACK');

-- Open callbacks
SELECT COUNT(*) FROM public.callbacks WHERE status = 'SCHEDULED';
```

---

## Databricks re-run

Pull latest `working_table.py` → **Run all**. No change needed for parked Supabase work except refreshing placements after **11**.
