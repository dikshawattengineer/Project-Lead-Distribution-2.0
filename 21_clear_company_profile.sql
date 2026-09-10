-- Pool-only for now: clear Agent on companies. Does not change poolId.
-- Shared bags stay. Do not run ld_apply_profile_run().
-- Safe to re-run.

UPDATE public.companies
SET "profileId" = NULL,
    "updatedAt" = NOW()
WHERE "profileId" IS NOT NULL;

-- Optional: turn off Retention fair-share policies (notebook no longer reads them)
UPDATE public.crm_pool_split_policy
SET "isActive" = false,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE id IN (
  'ld_policy_retention_nightly',
  'ld_policy_past_retention_nightly'
);

SELECT
  COUNT(*) FILTER (WHERE "profileId" IS NOT NULL) AS still_have_agent,
  COUNT(*) FILTER (WHERE "poolId" LIKE 'ld_pool_%') AS on_shared_pool
FROM public.companies;
