-- Cron-safe apply. Same SQL every night after Databricks writes public.ld_apply_batch.
-- Empty batch = 0 pool moves (not an error).
-- Writes STANDARD parents, or PRIVATE children that are active in pool_links.
-- Stamps companies.campaignId = parent pool (Retentions / E.ON / …) on every
-- batch row so the Campaign column shows the parent after fair-share.
-- Does not set profileId.

CREATE TABLE IF NOT EXISTS public.ld_apply_batch (
  company_id text NOT NULL,
  proposed_pool_id text NOT NULL,
  proposed_campaign_id text
);

ALTER TABLE public.ld_apply_batch
  ADD COLUMN IF NOT EXISTS proposed_campaign_id text;

ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS "campaignId" text;

-- UI Campaign = pools.type Campaign. Flip our parent bags if the enum exists.
DO $$
BEGIN
  UPDATE public.pools
  SET type = 'CAMPAIGN'::pool_type, "updatedAt" = CURRENT_TIMESTAMP
  WHERE EXISTS (
      SELECT 1 FROM public.crm_pool_rule r
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

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.companies'::regclass
      AND conname ILIKE 'companies_campaignid_fkey'
  ) THEN
    ALTER TABLE public.companies
      ADD CONSTRAINT companies_campaignId_fkey
      FOREIGN KEY ("campaignId") REFERENCES public.campaigns(id);
  END IF;
EXCEPTION
  WHEN duplicate_object THEN
    NULL;
END $$;

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
    WHERE EXISTS (
        SELECT 1 FROM public.crm_pool_rule r
        WHERE r."poolId" = p.id AND COALESCE(r."isActive", true) = true
      )
      AND p.type::text IN ('STANDARD', 'CAMPAIGN')
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
  n_camp integer := 0;
  actor uuid := '3fc12748-605f-4d13-ae70-eec60b55d726';
  has_campaign boolean := false;
BEGIN
  IF to_regclass('public.ld_apply_batch') IS NULL THEN
    RETURN 0;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'companies'
      AND column_name = 'campaignId'
  ) INTO has_campaign;

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

  -- Same agent bag (Kelly on both Retention + Past Retention) does not move
  -- poolId, so Campaign stayed on the old parent. Restamp sourcePoolId always.
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

  PERFORM public.ld_seed_campaigns();

  IF has_campaign THEN
    UPDATE public.companies c
    SET
      "campaignId" = x.proposed_campaign_id,
      "updatedAt" = NOW()
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
    WHERE c.id = x.company_id
      AND x.proposed_campaign_id IS NOT NULL
      AND EXISTS (SELECT 1 FROM public.campaigns cam WHERE cam.id = x.proposed_campaign_id)
      AND c."campaignId" IS DISTINCT FROM x.proposed_campaign_id;

    GET DIAGNOSTICS n_camp = ROW_COUNT;
    RAISE NOTICE 'campaigns stamped %', n_camp;
  END IF;

  RETURN n;
END;
$$;

-- Databricks Step 4 calls this after it writes ld_apply_batch.
-- Manual check:
-- SELECT public.ld_apply_batch_run() AS companies_moved;
