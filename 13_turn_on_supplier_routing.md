# Turn supplier routing back on

Use this when boss is ready for **per-supplier pools**, **Unassigned**, and **campaign** routing again.

**Current state (parked):** nightly only moves **Retentions**, **Past Retentions**, **Complaint**, **Callback**. Supplier sync (`09`) and `crm_provider_family` are off. Agent **PRIVATE** pools unchanged.

---

## Before you start

- [ ] New rows in `providers`? Run **`09_sync_provider_pools.sql`** (safe to re-run).
- [ ] Confirm `04_seed_ld_pools.sql` already ran (core pools + rules).
- [ ] Confirm `apply_ld_apply_batch.sql` is deployed (latest `ld_apply_batch_run`).

---

## Supabase (run in order)

| Step | File | What |
|------|------|------|
| 1 | **`09_sync_provider_pools.sql`** | Map every `providers` row → `crm_provider_family` + per-supplier pools + `{TAG}_NOW` / `{TAG}_IN_WINDOW` rules |
| 2 | **Re-activate rules** (see SQL below) | Turn `crm_pool_rule` + `crm_provider_family` back on |
| 3 | **`pool_profiles`** (CRM admin) | Link agents to supplier shared pools they should work (BG, E.ON, …) |
| 4 | Optional: **`05_unlink_private_pool_links.sql`** | Only if fair-share to Jack/Kelly is **off** (current). Skip if turning fair-share on |

### SQL — re-activate supplier routing (after `09`)

```sql
BEGIN;

UPDATE public.crm_provider_family
SET "isActive" = true,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE COALESCE("isManual", false) = false;

UPDATE public.crm_pool_rule
SET "isActive" = true,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE id IN (
  'ld_rule_ub_now', 'ld_rule_ub_in',
  'ld_rule_eon_dfv', 'ld_rule_eon_now', 'ld_rule_eon_in',
  'ld_rule_bg_now', 'ld_rule_bg_in',
  'ld_rule_other_now', 'ld_rule_other_in',
  'ld_rule_pre', 'ld_rule_fallback',
  'ld_rule_upsell', 'ld_rule_customer_care', 'ld_rule_corporate'
)
OR id LIKE 'ld_rule_%_now'
OR id LIKE 'ld_rule_%_in';

COMMIT;
```

Check:

```sql
SELECT tag, COUNT(*) FROM public.crm_pool_rule
WHERE "isActive" = true GROUP BY tag ORDER BY tag;

SELECT COUNT(*) FROM public.crm_provider_family WHERE "isActive" = true;
```

---

## Databricks (`working_table.py`)

| Step | Change |
|------|--------|
| 1 | **Uncomment** `crm_provider_family` snapshot (replace empty parked snapshot) |
| 2 | **Propose pool** — restore supplier + Unassigned fallback (remove “retention only” `ELSE NULL`) |
| 3 | **Apply batch** — include supplier tags again (`BG_NOW`, `EON_IN_WINDOW`, …, `PRE_WINDOW` → Unassigned) |
| 4 | Optional: **fair-share** — uncomment `pool_links` snapshot + `_FAIR_SHARE_SQL` (private agent bags) |
| 5 | Pull latest repo → **Run all** |

### Apply batch tags to include again (example)

```python
AND lead_tag IN (
  'RETENTION', 'PAST_RETENTION', 'COMPLAINT', 'CALLBACK',
  'BG_NOW', 'BG_IN_WINDOW', 'EON_NOW', 'EON_IN_WINDOW', 'EON_DFV',
  'UB_NOW', 'UB_IN_WINDOW', 'OTHER_NOW', 'OTHER_IN_WINDOW',
  # + per-supplier tags from 09, e.g. 'YU_ENERGY_NOW', 'YU_ENERGY_IN_WINDOW'
)
```

Or remove the `lead_tag IN (...)` filter and rely on `proposed_pool_id IS NOT NULL` once propose-pool is restored.

### Propose pool (restore supplier path)

Change `fill proposed_pool_id` back to:

```sql
ELSE COALESCE(r.`poolId`, upool.pool_id)  -- UNASSIGNED fallback
```

instead of `ELSE NULL` for non-retention tags.

---

## Agent visibility (`pool_profiles`)

After parking, agents only see **Retentions**, **Past Retentions**, **PRIVATE** bags; managers see **Complaint**.

When suppliers go live:

- [ ] Add `pool_profiles` rows so agents can open each **supplier shared pool** they work.
- [ ] Keep **Unassigned** off agent profiles unless you want them to see waiters.
- [ ] Managers keep **Complaint** only.

---

## Campaign column (private links)

When **fair-share** is on (Kelly linked to Retentions / Past Retentions):

- `proposed_campaign_id` = parent pool in batch
- `apply_ld_apply_batch.sql` sets `sourcePoolId` only when **child pool ≠ parent** (PRIVATE vs shared parent)
- No `companies.campaignId` — campaign lives on **placement**

---

## Do NOT re-run unless needed

| File | When |
|------|------|
| `10_park_supplier_routing.sql` | Only if parking again |
| `11_reclaim_from_parked_pools.sql` | One-time cleanup; not for turn-on |
| `12_hide_parked_pools_from_agents.sql` | Only when hiding supplier pools from UI |
| `16_fallback_from_sourcebridge.sql` | Migration only — do not repeat |

---

## Quick checklist (turn-on day)

```
1. 09_sync_provider_pools.sql
2. Re-activate SQL (crm_pool_rule + crm_provider_family)
3. pool_profiles — agents on supplier pools
4. Git pull working_table.py — uncomment provider snapshot + full apply
5. apply_ld_apply_batch.sql — already deployed
6. Databricks Run all
7. QA: counts by lead_tag, spot-check BG / E.ON / Unassigned
```

---

## Park again (rollback)

1. `10_park_supplier_routing.sql`
2. `12_hide_parked_pools_from_agents.sql`
3. Databricks retention-only apply (current `main` branch)
4. Optional: `11_reclaim_from_parked_pools.sql`
