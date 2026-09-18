-- Park per-supplier routing until campaign / supplier pools are turned back on.
-- Nightly applies RETENTION + PAST_RETENTION (+ complaint/callback) only.
-- Does not delete pools or provider rows — flips isActive off. Safe to re-run.
-- Re-enable: run 09_sync_provider_pools.sql and set rules active again.

BEGIN;

UPDATE public.crm_pool_rule
SET "isActive" = false,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE tag NOT IN ('COMPLAINT', 'CALLBACK', 'RETENTION', 'PAST_RETENTION')
   OR id LIKE 'ld_rule_%_now'
   OR id LIKE 'ld_rule_%_in';

UPDATE public.crm_pool_rule
SET "isActive" = true,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE tag IN ('COMPLAINT', 'RETENTION', 'PAST_RETENTION')
  AND id IN ('ld_rule_complaint', 'ld_rule_ret', 'ld_rule_past_ret');

UPDATE public.crm_provider_family
SET "isActive" = false,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE COALESCE("isManual", false) = false;

COMMIT;

SELECT tag, COUNT(*) AS active_rules
FROM public.crm_pool_rule
WHERE COALESCE("isActive", true) = true
GROUP BY tag
ORDER BY tag;
