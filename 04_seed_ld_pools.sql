-- Lead distribution shared pools: id = UUID v4, code = stable key.
-- Run once on a fresh DB (or after removing old ld_pool_* rows).
-- Safe to re-run (upsert on pools.code, crm_pool_rule.id).

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS pools_code_uidx
  ON public.pools (code)
  WHERE code IS NOT NULL;

DROP INDEX IF EXISTS public.crm_pool_rule_priority_uidx;
CREATE INDEX IF NOT EXISTS crm_pool_rule_priority_idx
  ON public.crm_pool_rule (priority);

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
  v.rule_id,
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
    ('ld_rule_complaint',     10, 'COMPLAINT',        'COMPLAINT',      'Ongoing complaint — locked Complaint pool'),
    ('ld_rule_customer_care', 11, 'CUSTOMER_CARE',    'CUSTOMER_CARE',  'Past sale, 7–60 days since last sale'),
    ('ld_rule_past_ret',      12, 'PAST_RETENTION',   'PAST_RETENTION', 'Any past deal, OOC'),
    ('ld_rule_ret',           13, 'RETENTION',        'RETENTION',      'Any past deal, 1–540 days'),
    ('ld_rule_upsell',        14, 'UPSELLING',        'UPSELLING',      'Any past deal, >540 days'),
    ('ld_rule_corporate',     15, 'CORPORATE',        'CORPORATE',      '21–200 sites, DFV or CED ≤365'),
    ('ld_rule_ub_now',        20, 'UB_NOW',           'UB',             'UB expired / no CED'),
    ('ld_rule_ub_in',         21, 'UB_IN_WINDOW',     'UB',             'UB in window 1–365'),
    ('ld_rule_eon_dfv',       28, 'EON_DFV',          'EON_DFV',        'E.ON deemed / flexible / variable only'),
    ('ld_rule_eon_now',       29, 'EON_NOW',          'EON',            'E.ON expired / no CED → main E.ON'),
    ('ld_rule_eon_in',        30, 'EON_IN_WINDOW',    'EON',            'E.ON in window 1–365'),
    ('ld_rule_bg_now',        39, 'BG_NOW',           'BG',             'British Gas expired / no CED'),
    ('ld_rule_bg_in',         40, 'BG_IN_WINDOW',     'BG',             'British Gas in window 1–548'),
    ('ld_rule_other_now',     49, 'OTHER_NOW',        'OTHER',          'Unknown supplier expired / no CED'),
    ('ld_rule_other_in',      50, 'OTHER_IN_WINDOW',  'OTHER',          'Unknown supplier in window'),
    ('ld_rule_pre',           90, 'PRE_WINDOW',       'UNASSIGNED',     'Wait — too far out'),
    ('ld_rule_fallback',      99, 'UNASSIGNED',       'UNASSIGNED',     'Fallback')
) AS v(rule_id, priority, tag, pool_code, description)
JOIN public.pools p ON p.code = v.pool_code
ON CONFLICT (id) DO UPDATE SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = EXCLUDED."isActive",
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
