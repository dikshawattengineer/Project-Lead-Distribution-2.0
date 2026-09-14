-- Cron-safe apply. Same SQL every night after Databricks writes public.ld_apply_batch.
-- Empty batch = 0 updates (not an error).
-- Writes STANDARD parents, or PRIVATE children that are active in pool_links.
-- Campaign stamp uses existing company_pool_placements.sourcePoolId = parent
-- (Retention / Past Retention / E.ON). Does not ALTER companies.
-- Does not set profileId.

CREATE TABLE IF NOT EXISTS public.ld_apply_batch (
  company_id text NOT NULL,
  proposed_pool_id text NOT NULL,
  proposed_campaign_id text
);

-- Our scratch table only (not Prisma). Adds the column if an older 2-col batch exists.
ALTER TABLE public.ld_apply_batch
  ADD COLUMN IF NOT EXISTS proposed_campaign_id text;

-- Upsert campaigns from STANDARD parent pools. Databricks calls this every night.
-- No manual seed. New ld_pool_* parent → new campaign on the next job.
CREATE OR REPLACE FUNCTION public.ld_seed_campaigns()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  n integer := 0;
  actor uuid;
BEGIN
  SELECT COALESCE(
    (
      SELECT pr."userId"
      FROM public.profiles pr
      WHERE pr."userId" = '3fc12748-605f-4d13-ae70-eec60b55d726'
      LIMIT 1
    ),
    (SELECT pr."userId" FROM public.profiles pr ORDER BY pr."createdAt" LIMIT 1)
  )
  INTO actor;

  IF actor IS NULL THEN
    RETURN 0;
  END IF;

  WITH upserted AS (
    INSERT INTO public.campaigns (id, name, "createdById", "createdAt", "updatedAt")
    SELECT
      p.id,
      p.name,
      actor,
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP
    FROM public.pools p
    WHERE p.type = 'STANDARD'
      AND p.id LIKE 'ld_pool_%'
    ON CONFLICT (id) DO UPDATE
    SET
      name = EXCLUDED.name,
      "updatedAt" = CURRENT_TIMESTAMP
    RETURNING 1
  )
  SELECT COUNT(*) INTO n FROM upserted;

  RETURN n;
END;
$$;

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

  PERFORM public.ld_seed_campaigns();

  WITH batch AS (
    SELECT
      b.company_id,
      b.proposed_pool_id,
      COALESCE(
        NULLIF(b.proposed_campaign_id, ''),
        CASE WHEN p.type = 'STANDARD' THEN b.proposed_pool_id END
      ) AS proposed_campaign_id,
      c."poolId" AS old_pool_id
    FROM public.ld_apply_batch b
    JOIN public.companies c ON c.id = b.company_id
    JOIN public.pools p ON p.id = b.proposed_pool_id
    WHERE b.proposed_pool_id IS NOT NULL
      AND c."poolId" IS DISTINCT FROM b.proposed_pool_id
      AND (
        p.type = 'STANDARD'
        OR (
          p.type = 'PRIVATE'
          AND EXISTS (
            SELECT 1
            FROM public.pool_links l
            WHERE l."childPoolId" = b.proposed_pool_id
              AND l."isActive" = true
              AND l."childType" = 'PRIVATE'
          )
        )
      )
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
    RETURNING c.id, batch.proposed_pool_id, batch.old_pool_id, batch.proposed_campaign_id
  ),
  placed AS (
    INSERT INTO public.company_pool_placements
      (id, "companyId", "poolId", "sourcePoolId", "distributedAt", "endedAt", "actorId")
    SELECT
      gen_random_uuid()::text,
      moved.id,
      moved.proposed_pool_id,
      COALESCE(moved.proposed_campaign_id, moved.old_pool_id),
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
      CASE WHEN p.type = 'PRIVATE' THEN 'lead_distribution_private'
           ELSE 'lead_distribution_shared' END,
      NOW()
    FROM moved
    JOIN public.pools p ON p.id = moved.proposed_pool_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO n FROM audited;

  RETURN n;
END;
$$;

-- Databricks Step 4 calls this after it writes ld_apply_batch.
-- Manual check in SQL editor:
-- SELECT public.ld_apply_batch_run() AS companies_moved;
