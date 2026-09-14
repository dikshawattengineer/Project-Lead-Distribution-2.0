-- Cron-safe apply. Same SQL every night after Databricks writes public.ld_apply_batch.
-- Empty batch = 0 updates (not an error). Re-runs only move companies whose pool actually changed.
-- Writes STANDARD shared pools only. Does not set profileId or PRIVATE agent pools.

CREATE TABLE IF NOT EXISTS public.ld_apply_batch (
  company_id text NOT NULL,
  proposed_pool_id text NOT NULL
);

CREATE OR REPLACE FUNCTION public.ld_apply_batch_run()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  n integer := 0;
  actor uuid := '3fc12748-605f-4d13-ae70-eec60b55d726';
BEGIN
  IF to_regclass('public.ld_apply_batch') IS NULL THEN
    RETURN 0;
  END IF;

  WITH batch AS (
    SELECT b.company_id, b.proposed_pool_id, c."poolId" AS old_pool_id
    FROM public.ld_apply_batch b
    JOIN public.companies c ON c.id = b.company_id
    JOIN public.pools p ON p.id = b.proposed_pool_id
    WHERE b.proposed_pool_id IS NOT NULL
      AND c."poolId" IS DISTINCT FROM b.proposed_pool_id
      AND p.type = 'STANDARD'
  ),
  closed AS (
    UPDATE public.company_pool_placements pl
    SET "endedAt" = NOW()
    FROM batch
    WHERE pl."companyId" = batch.company_id
      AND pl."endedAt" IS NULL
    RETURNING 1
  ),
  moved AS (
    UPDATE public.companies c
    SET "poolId" = batch.proposed_pool_id,
        "updatedAt" = NOW()
    FROM batch
    WHERE c.id = batch.company_id
    RETURNING c.id, batch.proposed_pool_id, batch.old_pool_id
  ),
  placed AS (
    INSERT INTO public.company_pool_placements
      (id, "companyId", "poolId", "sourcePoolId", "distributedAt", "endedAt", "actorId")
    SELECT
      gen_random_uuid()::text,
      moved.id,
      moved.proposed_pool_id,
      moved.old_pool_id,
      NOW(),
      NULL,
      actor
    FROM moved
    RETURNING 1
  ),
  audited AS (
    INSERT INTO public.company_pool_audits
      (id, "companyId", "poolId", action, "actorId", reason, "createdAt")
    SELECT
      gen_random_uuid()::text,
      moved.id,
      moved.proposed_pool_id,
      'AUTO_ASSIGN',
      actor,
      'lead_distribution_shared',
      NOW()
    FROM moved
    RETURNING 1
  )
  SELECT COUNT(*) INTO n FROM audited;

  RETURN n;
END;
$$;

-- Databricks Step 4 calls this after it writes ld_apply_batch.
-- Manual check in SQL editor:
-- SELECT public.ld_apply_batch_run() AS companies_moved;
