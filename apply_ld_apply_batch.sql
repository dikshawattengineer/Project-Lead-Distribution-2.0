-- Cron-safe apply. Same SQL every night after Databricks writes public.ld_apply_batch.
-- Empty batch = 0 updates (not an error). Re-runs only move companies whose pool actually changed.

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
BEGIN
  IF to_regclass('public.ld_apply_batch') IS NULL THEN
    RETURN 0;
  END IF;

  WITH changed AS (
    UPDATE public.companies c
    SET "poolId" = b.proposed_pool_id,
        "updatedAt" = NOW()
    FROM public.ld_apply_batch b
    WHERE c.id = b.company_id
      AND b.proposed_pool_id IS NOT NULL
      AND c."poolId" IS DISTINCT FROM b.proposed_pool_id
    RETURNING c.id, b.proposed_pool_id
  ),
  audited AS (
    INSERT INTO public.crm_company_pool_audit
      (id, "companyId", "poolId", action, "actorId", "createdAt")
    SELECT
      gen_random_uuid()::text,
      changed.id,
      changed.proposed_pool_id,
      'AUTO_ASSIGN',
      '3fc12748-605f-4d13-ae70-eec60b55d726',
      NOW()
    FROM changed
    RETURNING 1
  )
  SELECT COUNT(*) INTO n FROM audited;

  RETURN n;
END;
$$;

-- Nightly / SQL editor / pg_cron: one line, never edit by hand.
SELECT public.ld_apply_batch_run() AS companies_moved;
