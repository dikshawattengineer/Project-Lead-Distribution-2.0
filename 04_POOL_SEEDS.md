# Pool seeds — minimal (now) vs full (turn-on)

Managers see every pool linked in CRM. Empty supplier bags confuse people — **do not** create E.ON / BG / UB pools until supplier routing is live.

## Which file when

| File | When | Pools created |
|------|------|----------------|
| **`04_parked_minimal.sql`** | **Prod go-live (parked)** | Retentions, Past Retentions, Complaint, Unassigned, Upselling, Customer Care |
| **`04_seed_ld_pools.sql`** | **Supplier turn-on day** | Above + E.ON, E.ON DFV, BG, UB, Other, Corporate + all `*_NOW` / `*_IN_WINDOW` rules |

## Parked minimal — active rules

| Tag | Pool | `isActive` | Nightly apply (Databricks) |
|-----|------|------------|----------------------------|
| `RETENTION` | Retentions | yes | **Yes** |
| `PAST_RETENTION` | Past Retentions | yes | **Yes** |
| `COMPLAINT` | Complaint | yes | **Yes** |
| `CALLBACK` | (agent private) | — | **Yes** |
| `UPSELLING` | Upselling | no | No (tag only) |
| `UNASSIGNED` / `PRE_WINDOW` | Unassigned | no | No (holding / tag only) |
| `CUSTOMER_CARE` | Customer Care | no | No until enabled |

No **`10_park_supplier_routing.sql`** needed after minimal — rules already inactive.

## Dev — reset after full 04 + 09 sync

If dev already has supplier pools from **`09_sync`**, run in order:

```
1. 04_dev_reset_to_parked_minimal.sql   ← drops supplier pools, moves companies out
2. 04_parked_minimal.sql              ← refresh minimal pools + rules
3. apply, janitor, Databricks (fallback built in notebook)
```

**Dev only** — review before any prod use.

## Prod parked checklist

```
□ Prisma migrate
□ 06 (if needed)
□ 04_parked_minimal.sql     ← not full 04 (seeds pool_rules only; provider_families empty)
□ apply_ld_apply_batch.sql
□ 24_janitor.sql
□ Databricks Run all (parked apply)
```

Skip: **09**, full **04**, **12** (until you want to hide pools from agents).

## Turn-on day (build on top)

```
□ 04_seed_ld_pools.sql      ← full pools + supplier rules (or 09 if pools only from providers)
□ 09_sync_provider_pools.sql
□ 13_turn_on_supplier_routing.md
□ 12_hide_parked_pools_from_agents.sql (optional — agents only)
```

## Databricks — two modes (same notebook)

| Mode | Config | What moves |
|------|--------|------------|
| **Parked (now)** | `PARKED_APPLY_TAGS` in `working_table.py` | Retention, Past Retention, Complaint, Callback only |
| **Full (turn-on)** | Expand tags per `13_turn_on_supplier_routing.md` | Supplier, Unassigned, Upselling, … |

Tagging still computes supplier / Unassigned / Upselling for **QA counts** while parked — apply does not write those pools.
