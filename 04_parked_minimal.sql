-- Parked prod go-live ONLY — minimal shared pools + rules.
-- Use this so managers do not see empty E.ON / BG / UB supplier bags in CRM.
-- Full seed (all suppliers + corporate): 04_seed_ld_pools.sql on turn-on day.
-- Safe to re-run (upsert on pools.code, pool_rules.tag).
-- After this: apply_ld_apply_batch, 24_janitor, Databricks parked run (fallback in notebook).
-- Skip 09, 11 (unless migrated cos in wrong pools), 12 (optional).

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS pools_code_uidx
  ON public.pools (code)
  WHERE code IS NOT NULL;

DROP INDEX IF EXISTS public.pool_rules_priority_uidx;
CREATE INDEX IF NOT EXISTS pool_rules_priority_idx
  ON public.pool_rules (priority);

CREATE UNIQUE INDEX IF NOT EXISTS pool_rules_tag_uidx
  ON public.pool_rules (tag);

INSERT INTO public.pools (id, code, name, type, "isLocked", "createdAt", "updatedAt")
SELECT
  gen_random_uuid(),
  v.code,
  v.name,
  v.type::pool_type,
  v.locked,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM (
  VALUES
    ('COMPLAINT',      'Complaint',       'STANDARD', true),
    ('RETENTION',      'Retentions',      'STANDARD', false),
    ('PAST_RETENTION', 'Past Retentions', 'STANDARD', false),
    ('UNASSIGNED',     'Unassigned',      'STANDARD', false),
    ('UPSELLING',      'Upselling',       'STANDARD', false),
    ('CUSTOMER_CARE',  'Customer Care',   'STANDARD', false)
) AS v(code, name, type, locked)
ON CONFLICT (code) DO UPDATE SET
  name = EXCLUDED.name,
  type = EXCLUDED.type,
  "isLocked" = EXCLUDED."isLocked",
  "updatedAt" = CURRENT_TIMESTAMP;

INSERT INTO public.pool_rules
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  gen_random_uuid(),
  v.priority,
  v.tag,
  p.id,
  v.active,
  v.description,
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM (
  VALUES
    (10, 'COMPLAINT',      'COMPLAINT',      true,  'Ongoing complaint — locked Complaint pool'),
    (11, 'CUSTOMER_CARE',  'CUSTOMER_CARE',  false, 'Past sale, 7–60 days since last sale'),
    (12, 'PAST_RETENTION', 'PAST_RETENTION', true,  'Any past deal, OOC'),
    (13, 'RETENTION',      'RETENTION',      true,  'Any past deal, 1–540 days'),
    (14, 'UPSELLING',      'UPSELLING',      false, 'Any past deal, >540 days — no apply while parked'),
    (90, 'PRE_WINDOW',     'UNASSIGNED',     false, 'Wait — too far out'),
    (99, 'UNASSIGNED',     'UNASSIGNED',     false, 'Fallback — holding pool, no apply while parked')
) AS v(priority, tag, pool_code, active, description)
JOIN public.pools p ON p.code = v.pool_code
ON CONFLICT (tag) DO UPDATE SET
  priority = EXCLUDED.priority,
  "poolId" = EXCLUDED."poolId",
  "isActive" = EXCLUDED."isActive",
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

COMMIT;

SELECT code, name, "isLocked"
FROM public.pools
WHERE code IN (
  'COMPLAINT', 'RETENTION', 'PAST_RETENTION',
  'UNASSIGNED', 'UPSELLING', 'CUSTOMER_CARE'
)
ORDER BY code;

SELECT tag, "isActive", description
FROM public.pool_rules
WHERE tag IN (
  'COMPLAINT', 'CUSTOMER_CARE', 'PAST_RETENTION', 'RETENTION',
  'UPSELLING', 'PRE_WINDOW', 'UNASSIGNED'
)
ORDER BY tag;
