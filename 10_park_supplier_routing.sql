-- Park per-supplier routing until campaign / supplier pools are turned back on.
-- Nightly applies RETENTION + PAST_RETENTION (+ complaint/callback) only.
-- Does not delete pools or provider rows — flips isActive off. Safe to re-run.
-- Re-enable: run 09_sync_provider_pools.sql and set rules active again.

BEGIN;

UPDATE public.pool_rules
SET "isActive" = false,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE tag NOT IN ('COMPLAINT', 'CALLBACK', 'RETENTION', 'PAST_RETENTION');

UPDATE public.pool_rules
SET "isActive" = true,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE tag IN ('COMPLAINT', 'RETENTION', 'PAST_RETENTION');

UPDATE public.provider_families
SET "isActive" = false,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE COALESCE("isManual", false) = false;

COMMIT;

SELECT tag, COUNT(*) AS active_rules
FROM public.pool_rules
WHERE COALESCE("isActive", true) = true
GROUP BY tag
ORDER BY tag;
