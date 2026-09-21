-- Cron-safe apply. Same SQL every night after Databricks writes public.ld_apply_batch.
-- Empty batch = 0 pool moves (not an error).
-- Writes STANDARD parents, or PRIVATE children that are active in pool_links.
-- Campaign = placements.sourcePoolId from proposed_campaign_id (shared + private).
-- Shared: campaign often equals pool; private: campaign = parent shared pool.
-- Does not set profileId.

CREATE TABLE IF NOT EXISTS public.ld_apply_batch (
  company_id text NOT NULL,
  proposed_pool_id text NOT NULL,
  proposed_campaign_id text
);

ALTER TABLE public.ld_apply_batch
  ADD COLUMN IF NOT EXISTS proposed_campaign_id text;

-- UI Campaign = pools.type Campaign. Flip our parent bags if the enum exists.
DO $$
BEGIN
  UPDATE public.pools
  SET type = 'CAMPAIGN'::pool_type, "updatedAt" = CURRENT_TIMESTAMP
  WHERE EXISTS (
      SELECT 1 FROM public.pool_rules r
      WHERE r."poolId" = pools.id AND COALESCE(r."isActive", true) = true
    )
    AND type::text <> 'PRIVATE';

  UPDATE public.pool_links
  SET "parentType" = 'CAMPAIGN'::pool_type, "updatedAt" = CURRENT_TIMESTAMP
  WHERE "parentType"::text = 'STANDARD';
EXCEPTION
  WHEN invalid_text_representation THEN
    RAISE NOTICE 'pool_type has no CAMPAIGN; parents stay STANDARD';
END $$;

CREATE OR REPLACE FUNCTION public.ld_actor_user_id()
RETURNS uuid
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, auth
AS $$
  SELECT u.id
  FROM auth.users u
  WHERE lower(u.email) = 'system@watt.co.uk'
  LIMIT 1;
$$;

CREATE OR REPLACE FUNCTION public.ld_apply_batch_run()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  n integer := 0;
  actor uuid;
BEGIN
  IF to_regclass('public.ld_apply_batch') IS NULL THEN
    RETURN 0;
  END IF;

  SELECT public.ld_actor_user_id() INTO actor;
  IF actor IS NULL THEN
    RAISE EXCEPTION 'ld_actor_user_id: no user for system@watt.co.uk';
  END IF;

  WITH batch AS (
    SELECT
      b.company_id,
      b.proposed_pool_id,
      COALESCE(
        NULLIF(b.proposed_campaign_id, ''),
        CASE WHEN p.type::text IN ('STANDARD', 'CAMPAIGN') THEN b.proposed_pool_id END
      ) AS proposed_campaign_id,
      c."poolId" AS old_pool_id
    FROM public.ld_apply_batch b
    JOIN public.companies c ON c.id = b.company_id
    JOIN public.pools p ON p.id = b.proposed_pool_id
    WHERE b.proposed_pool_id IS NOT NULL
      AND (
        p.type::text IN ('STANDARD', 'CAMPAIGN')
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
  pool_moves AS (
    SELECT *
    FROM batch
    WHERE old_pool_id IS DISTINCT FROM proposed_pool_id
  ),
  closed AS (
    UPDATE public.company_pool_placements pl
    SET "endedAt" = NOW()
    FROM pool_moves m
    WHERE pl."companyId" = m.company_id
      AND pl."endedAt" IS NULL
    RETURNING 1
  ),
  moved AS (
    UPDATE public.companies c
    SET "poolId" = m.proposed_pool_id,
        "updatedAt" = NOW()
    FROM pool_moves m
    WHERE c.id = m.company_id
    RETURNING c.id, m.proposed_pool_id, m.old_pool_id, m.proposed_campaign_id
  ),
  placed AS (
    INSERT INTO public.company_pool_placements
      (id, "companyId", "poolId", "sourcePoolId", "distributedAt", "endedAt", "actorId")
    SELECT
      gen_random_uuid()::text,
      moved.id,
      moved.proposed_pool_id,
      moved.proposed_campaign_id,
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
      NULL,
      NOW()
    FROM moved
    RETURNING 1
  )
  SELECT COUNT(*) INTO n FROM audited;

  -- Shared + private: refresh campaign on open placement (pool may stay same).
  UPDATE public.company_pool_placements pl
  SET "sourcePoolId" = x.proposed_campaign_id
  FROM (
    SELECT
      b.company_id,
      COALESCE(
        NULLIF(b.proposed_campaign_id, ''),
        CASE WHEN p.type::text IN ('STANDARD', 'CAMPAIGN') THEN b.proposed_pool_id END
      ) AS proposed_campaign_id
    FROM public.ld_apply_batch b
    JOIN public.pools p ON p.id = b.proposed_pool_id
  ) x
  WHERE pl."companyId" = x.company_id
    AND pl."endedAt" IS NULL
    AND x.proposed_campaign_id IS NOT NULL
    AND pl."sourcePoolId" IS DISTINCT FROM x.proposed_campaign_id;

  RETURN n;
END;
$$;

-- Databricks Step 4 calls this after it writes ld_apply_batch.
-- Manual check:
-- SELECT public.ld_apply_batch_run() AS companies_moved;
