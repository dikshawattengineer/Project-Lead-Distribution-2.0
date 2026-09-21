# Lead Distribution 2.0 — learn it fast

One place to understand the system, debug it, and know what to run when.

---

## 1. Mental model (30 seconds)

```
Supabase = rules + apply moves + CRM data
Databricks = snapshot → tag → propose pool → write batch → call apply
CRM UI    = who SEES which pool (pool_profiles) — mostly boss/admin
```

| Question | Where answered |
|----------|----------------|
| Which pool should this company be in? | Databricks `working_table.py` |
| What moves actually happen tonight? | `PARKED_APPLY_TAGS` + `ld_apply_batch_run()` |
| Can agent see that pool? | `pool_profiles` (+ manager role bypass) |
| Retention vs supplier load? | `external_site_mappings` (+ fallback while deals thin) |

**Golden rule:** LD moves `companies.poolId`. It does **not** assign agents (`profileId`).

---

## 2. Nightly flow (every run)

```
1. ld_janitor_run()           — clear disabled agents off companies
2. JDBC snapshot              — companies, contracts, pool_rules, external_site_mappings, …
3. Build snap_crm_company_load_sale  — past-sale fallback (Databricks, not Supabase 16)
4. ld_working                 — one row per company, all flags
5. Tag lead_tag               — sticky first, then retention clock, then supplier (QA)
6. Propose pool               — pool_rules → proposed_pool_id
7. (fair-share OFF parked)    — parent_pool_id = proposed_pool_id
8. Write ld_apply_batch       — only PARKED_APPLY_TAGS while parked
9. ld_apply_batch_run()       — UPDATE companies.poolId + placements + audits
```

**Do not** also cron apply in Supabase (`25_cron.sql`) — double apply.

---

## 3. Tag priority (who wins)

| Order | Tag | Sticky? |
|-------|-----|---------|
| 1 | COMPLAINT | Yes |
| 2 | CALLBACK | Yes — uses agent `primaryPoolId` private bag |
| 3 | LOCKED / GDPR | Yes |
| 4 | CUSTOMER_CARE | Off while parked |
| 5 | RETENTION / PAST_RETENTION / UPSELLING | From past sale + CED clock |
| 6 | CORPORATE / UNASSIGNED | Site count rules |
| 7 | EON_DFV, *_NOW, *_IN_WINDOW, PRE_WINDOW | Supplier (off apply while parked) |

### Retention clock (same company — retention ALWAYS wins)

| CED | Tag | Pool (when apply on) |
|-----|-----|----------------------|
| 1–540 days left | RETENTION | Retentions |
| Expired / no CED | PAST_RETENTION | Past Retentions |
| 541+ days | UPSELLING (tag only) | Unassigned holding — **not** Upselling pool |

If one meter past + one in window → **RETENTION**.

---

## 4. Parked NOW vs supplier turn-on LATER

### Parked (production today)

| Item | State |
|------|--------|
| Seed | `04_parked_minimal.sql` |
| Apply tags | RETENTION, PAST_RETENTION, COMPLAINT, CALLBACK |
| provider_families | Empty |
| Fair-share / pool_links | Off in notebook |
| Fallback | Built in Databricks from mappings + contracts |

**Supabase go-live (fresh):**
```
04_parked_minimal → apply_ld_apply_batch → 24_janitor → Databricks Run all
23_link_agents_to_retention_pools.sql  (once — agent visibility)
```

Skip: 09, 11, 16, 17, full 04.

### Turn-on day (suppliers + fair-share)

```
04_seed_ld_pools.sql
09_sync_provider_pools.sql
10_park reversed / 13_turn_on_supplier_routing.md
Uncomment pool_links + fair-share in working_table.py
Expand PARKED_APPLY_TAGS
Tighten fallback (retention mappings only — not all companies)
```

See: `13_turn_on_supplier_routing.md`, `NEW_SUPPLIER_AND_TURN_ON.md`

---

## 5. Key tables (who owns what)

| Table | Owner | Purpose |
|-------|-------|---------|
| `pool_rules` | LD seed | tag → poolId, priority, isActive |
| `provider_families` | LD 09 | provider → tagCode, pool, windowDays |
| `pools` | Prisma | Retentions, Past, private bags, … |
| `pool_profiles` | CRM/admin | **who sees which pool** |
| `pool_links` | CRM | private child under shared parent |
| `companies.poolId` | LD apply | where company lives now |
| `company_pool_placements` | LD apply | history; `sourcePoolId` = Campaign parent |
| `external_site_mappings` | Boss | load origin Retention vs Supplier |
| `ld_apply_batch` | Databricks | tonight’s proposed moves |

---

## 6. Past-sale fallback (Databricks)

Built in **Snapshot** cell → `snap_crm_company_load_sale`:

1. Retention rows from `external_site_mappings` (`%retention%`)
2. Any company with contract CED
3. Every company (migration shortcut — tighten at supplier turn-on)

Drives `hasPastSale`, `lastDealEndDate`, retention tagging.

**Empty mappings** → part 3 treats all as retention load (what you saw on dev).

---

## 7. Apply & placements

| Move type | poolId | sourcePoolId (Campaign) |
|-----------|--------|---------------------------|
| Shared → shared | Retentions | NULL |
| Shared → private (callback/fair-share) | Agent PRIVATE | Parent e.g. Retentions |

Linking `pool_links` in CRM **does not** auto-fill Campaign — **apply** does on move.

---

## 8. Debug SQL — copy/paste in Supabase

### A. System health

```sql
-- Rules active while parked
SELECT tag, "isActive", description
FROM pool_rules
WHERE tag IN ('RETENTION','PAST_RETENTION','COMPLAINT','UPSELLING','UNASSIGNED')
ORDER BY tag;

-- Provider families (should be 0 while parked)
SELECT COUNT(*) FROM provider_families;

-- Functions exist
SELECT public.ld_actor_user_id();
```

### B. Where are companies?

```sql
SELECT p.code, p.name, COUNT(*) AS companies
FROM companies c
JOIN pools p ON p.id = c."poolId"
GROUP BY p.code, p.name
ORDER BY companies DESC;
```

### C. Agent can’t see leads?

```sql
-- Shared pools linked to anyone?
SELECT p.code, COUNT(pp."profileId") AS profiles_linked
FROM pools p
LEFT JOIN pool_profiles pp ON pp."poolId" = p.id
WHERE p.code IN ('RETENTION','PAST_RETENTION')
GROUP BY p.code;

-- One agent’s links
SELECT pr.email, p.code
FROM pool_profiles pp
JOIN profiles pr ON pr.id = pp."profileId"
JOIN pools p ON p.id = pp."poolId"
WHERE lower(pr.email) = 'test2@watt.co.uk';
```

Fix: `23_link_agents_to_retention_pools.sql`

### D. Wrong pool (Bath Street in Past when far-future)?

```sql
-- Company pool + best CED
SELECT c.id, co.name, p.code AS pool,
       MAX(ct."endDate") AS max_end,
       (MAX(ct."endDate"::date) - CURRENT_DATE) AS days_left
FROM companies co
JOIN pools p ON p.id = co."poolId"
LEFT JOIN contracts ct ON ct."companyId" = co.id
WHERE co.name ILIKE '%bath street%'
GROUP BY c.id, co.name, p.code;
```

Far-future (541+) should not be in Past Retentions — tag UPSELLING, pool Unassigned if 11b ran.

### E. Last apply moves

```sql
SELECT a."createdAt", co.name, p.name AS new_pool, a.action
FROM company_pool_audits a
JOIN companies co ON co.id = a."companyId"
JOIN pools p ON p.id = a."poolId"
WHERE a.action = 'AUTO_ASSIGN'
ORDER BY a."createdAt" DESC
LIMIT 50;
```

### F. Campaign on private bag

```sql
SELECT co.name, child.name AS pool, parent.name AS campaign
FROM company_pool_placements pl
JOIN companies co ON co.id = pl."companyId"
JOIN pools child ON child.id = pl."poolId"
LEFT JOIN pools parent ON parent.id = pl."sourcePoolId"
WHERE pl."endedAt" IS NULL
  AND child.type = 'PRIVATE'
LIMIT 30;
```

### G. Mappings / fallback source

```sql
SELECT COUNT(*) FROM external_site_mappings;
SELECT COUNT(*) FROM external_site_mappings
WHERE LOWER(COALESCE(campaign, source, '')) LIKE '%retention%';
```

---

## 9. Debug in Databricks

After **Run all**, check notebook output:

| Line | Meaning |
|------|---------|
| `JDBC host =` | Must be **prod** Supabase |
| `companies N` | Snapshot count — match Supabase `SELECT COUNT(*) FROM companies` |
| `snap_crm_company_load_sale (Databricks fallback) N` | Fallback rows |
| `company clock bags` | RETENTION / PAST_RETENTION / UPSELLING counts |
| `parked apply only: …` | How many in tonight’s batch |
| `apply companies_moved N` | What apply actually changed |

SQL in Databricks:

```sql
SELECT lead_tag, COUNT(*) FROM crm_load.new_crm.ld_working GROUP BY lead_tag ORDER BY 2 DESC;

SELECT COUNT(*) FROM crm_load.new_crm.snap_crm_company_load_sale;

SELECT * FROM crm_load.new_crm.ld_working
WHERE company_id = '<uuid>';  -- one company deep-dive
```

---

## 10. When to run which script

| Situation | Run |
|-----------|-----|
| Fresh prod go-live | 04_parked_minimal, apply, janitor, Databricks |
| Agent sees no companies | 23 (or CRM link to Retentions/Past) |
| Test one agent | 23 block A |
| All sales agents | 23 block C |
| Wrong supplier pools visible | 12 (optional if CRM role already hides) |
| Stuck in supplier/Unassigned after migration | 11 (one-time) |
| Far-future in Past Retentions | 11b (one-time) |
| New supplier in CRM | 09 + 13 |
| Full supplier turn-on | 04_seed, 09, 13, notebook fair-share on |

---

## 11. Common problems

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Manager sees leads, agent doesn’t | `pool_profiles` 0 on RETENTION/PAST | 23 or CRM admin |
| Databricks 15k companies, CRM empty | Wrong JDBC credentials | Fix Password cell |
| Everyone tagged retention | Empty `external_site_mappings` + fallback part 3 | OK parked; seed mappings at turn-on |
| Nothing moves nightly | Empty batch or wrong tags | Check `PARKED_APPLY_TAGS`, lead_tag counts |
| apply error system user | No `system@watt.co.uk` | Create auth user |
| Double moves | Supabase cron + Databricks | Remove duplicate cron |
| Callback date changed | LD doesn’t touch callbacks | CRM issue |
| Campaign blank on shared pool | Normal — sourcePoolId only on private child | |

---

## 12. File map (repo)

| File | Remember as |
|------|-------------|
| `working_table.py` | The brain — nightly |
| `04_parked_minimal.sql` | Pools + rules (parked) |
| `04_seed_ld_pools.sql` | Full pools (turn-on) |
| `09_sync_provider_pools.sql` | Providers → families + rules |
| `apply_ld_apply_batch.sql` | Apply function |
| `24_janitor.sql` | Leaver cleanup |
| `23_link_agents_to_retention_pools.sql` | Agent sees shared pools |
| `12_hide_parked_pools_from_agents.sql` | Hide Unassigned from agents (optional) |
| `11` / `11b` | One-time pool cleanup |
| `PARKED_STATE.md` | Parked reference |
| `RELEASE_MIGRATION.md` | Migration day |
| `13_turn_on_supplier_routing.md` | Turn suppliers on |
| `04_POOL_SEEDS.md` | Minimal vs full seed |

---

## 13. Learn in order (1 hour path)

1. Read **section 1–3** above (mental model + tags).
2. Skim `04_parked_minimal.sql` — what pools/rules exist.
3. Open `working_table.py` → **Snapshot** (fallback) → **ld_working** → **fill lead_tag** → **Step 4 apply**.
4. Run debug SQL **B + C** in Supabase on dev.
5. Run Databricks once; match **companies** count to Supabase.
6. Pick one company UUID; trace tag in `ld_working`.
7. When ready for suppliers, read `13_turn_on_supplier_routing.md`.

---

## 14. One-page cheat sheet

```
PARKED APPLY:  RETENTION | PAST_RETENTION | COMPLAINT | CALLBACK

CED:  1-540 → RETENTION → Retentions
      expired → PAST_RETENTION → Past Retentions
      541+ → UPSELLING tag → Unassigned pool (managers)

STICKY: Complaint > Callback > Locked > retention clock

VISIBILITY: pool_profiles (23) — NOT ld seed
MOVES: ld_apply_batch_run() — NOT manual pool edits

FALLBACK: Databricks snap_crm_company_load_sale — NOT Supabase 16
```
