-- Customer Care + Corporate shared pools and tag rules.
-- Run in Supabase. Safe to re-run. Does not change companies.poolId.
-- Quality gates / custom split / sourcebridge stay parked.

BEGIN;

INSERT INTO public.crm_pool (id, code, name, "isLocked", "createdAt", "updatedAt")
VALUES
  ('ld_pool_customer_care', 'CUSTOMER_CARE', 'Customer Care', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_corporate',     'CORPORATE',     'Corporate',     false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO UPDATE SET
  code = EXCLUDED.code,
  name = EXCLUDED.name,
  "isLocked" = EXCLUDED."isLocked",
  "updatedAt" = CURRENT_TIMESTAMP;

-- Free priority 11–12 for Customer Care, then Retention clock, then Corporate.
UPDATE public.crm_pool_rule
SET priority = 14, "updatedAt" = CURRENT_TIMESTAMP
WHERE id = 'ld_rule_upsell' AND priority < 14;

UPDATE public.crm_pool_rule
SET priority = 13, "updatedAt" = CURRENT_TIMESTAMP
WHERE id = 'ld_rule_ret' AND priority < 13;

UPDATE public.crm_pool_rule
SET priority = 12, "updatedAt" = CURRENT_TIMESTAMP
WHERE id = 'ld_rule_past_ret' AND priority < 12;

INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
VALUES
  (
    'ld_rule_customer_care', 11, 'CUSTOMER_CARE', 'ld_pool_customer_care', true,
    'Past sale, 7–60 days since last sale', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_corporate', 15, 'CORPORATE', 'ld_pool_corporate', true,
    '21–200 sites, DFV or CED ≤365; no Retention/sticky', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  )
ON CONFLICT (id) DO UPDATE SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = EXCLUDED."isActive",
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

COMMIT;

SELECT id, code, name
FROM public.crm_pool
WHERE id IN ('ld_pool_customer_care', 'ld_pool_corporate');

SELECT priority, tag, "poolId"
FROM public.crm_pool_rule
WHERE tag IN (
  'COMPLAINT', 'CUSTOMER_CARE', 'PAST_RETENTION', 'RETENTION', 'UPSELLING', 'CORPORATE'
)
ORDER BY priority;
