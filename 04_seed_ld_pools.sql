-- Lead distribution shared pools: id = UUID v4, code = stable key.
-- Run once on a fresh DB (or after removing old ld_pool_* rows).
-- Safe to re-run (upsert on pools.code, crm_pool_rule.tag).

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS pools_code_uidx
  ON public.pools (code)
  WHERE code IS NOT NULL;

DROP INDEX IF EXISTS public.crm_pool_rule_priority_uidx;
CREATE INDEX IF NOT EXISTS crm_pool_rule_priority_idx
  ON public.crm_pool_rule (priority);

CREATE UNIQUE INDEX IF NOT EXISTS crm_pool_rule_tag_uidx
  ON public.crm_pool_rule (tag);

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
    ('EON',            'E.ON',            'STANDARD', false),
    ('EON_DFV',        'E.ON DFV',        'STANDARD', false),
    ('BG',             'British Gas',     'STANDARD', false),
    ('UB',             'Utility Bidder',  'STANDARD', false),
    ('OTHER',          'Other',           'STANDARD', false),
    ('UNASSIGNED',     'Unassigned',      'STANDARD', false),
    ('COMPLAINT',      'Complaint',       'STANDARD', true),
    ('RETENTION',      'Retentions',      'STANDARD', false),
    ('PAST_RETENTION', 'Past Retentions', 'STANDARD', false),
    ('UPSELLING',      'Upselling',       'STANDARD', false),
    ('CUSTOMER_CARE',  'Customer Care',   'STANDARD', false),
    ('CORPORATE',      'Corporate',       'STANDARD', false)
) AS v(code, name, type, locked)
ON CONFLICT (code) DO UPDATE SET
  name = EXCLUDED.name,
  type = EXCLUDED.type,
  "isLocked" = EXCLUDED."isLocked",
  "updatedAt" = CURRENT_TIMESTAMP;

INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  gen_random_uuid(),
  v.priority,
  v.tag,
  p.id,
  true,
  v.description,
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM (
  VALUES
    (10, 'COMPLAINT',        'COMPLAINT',      'Ongoing complaint — locked Complaint pool'),
    (11, 'CUSTOMER_CARE',    'CUSTOMER_CARE',  'Past sale, 7–60 days since last sale'),
    (12, 'PAST_RETENTION',   'PAST_RETENTION', 'Any past deal, OOC'),
    (13, 'RETENTION',        'RETENTION',      'Any past deal, 1–540 days'),
    (14, 'UPSELLING',        'UPSELLING',      'Any past deal, >540 days'),
    (15, 'CORPORATE',        'CORPORATE',      '21–200 sites, DFV or CED ≤365'),
    (20, 'UB_NOW',           'UB',             'UB expired / no CED'),
    (21, 'UB_IN_WINDOW',     'UB',             'UB in window 1–365'),
    (28, 'EON_DFV',          'EON_DFV',        'E.ON deemed / flexible / variable only'),
    (29, 'EON_NOW',          'EON',            'E.ON expired / no CED → main E.ON'),
    (30, 'EON_IN_WINDOW',    'EON',            'E.ON in window 1–365'),
    (39, 'BG_NOW',           'BG',             'British Gas expired / no CED'),
    (40, 'BG_IN_WINDOW',     'BG',             'British Gas in window 1–548'),
    (49, 'OTHER_NOW',        'OTHER',          'Unknown supplier expired / no CED'),
    (50, 'OTHER_IN_WINDOW',  'OTHER',          'Unknown supplier in window'),
    (90, 'PRE_WINDOW',       'UNASSIGNED',     'Wait — too far out'),
    (99, 'UNASSIGNED',       'UNASSIGNED',     'Fallback')
) AS v(priority, tag, pool_code, description)
JOIN public.pools p ON p.code = v.pool_code
ON CONFLICT (tag) DO UPDATE SET
  priority = EXCLUDED.priority,
  "poolId" = EXCLUDED."poolId",
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

COMMIT;

SELECT code, id, name, "isLocked"
FROM public.pools
WHERE code IN (
  'EON', 'EON_DFV', 'BG', 'UB', 'OTHER', 'UNASSIGNED',
  'COMPLAINT', 'RETENTION', 'PAST_RETENTION', 'UPSELLING', 'CUSTOMER_CARE', 'CORPORATE'
)
ORDER BY code;
