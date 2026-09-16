-- Sync every providers row → routing map + pool (UUID) + NOW/IN_WINDOW rules.
-- pools.id = gen_random_uuid(); pools.code = stable key (BG, YU_ENERGY, …).
-- Run after 04_seed_ld_pools.sql. Safe to re-run. Does not change companies.poolId.

BEGIN;

CREATE TABLE IF NOT EXISTS public.crm_provider_family (
  "providerId"   text PRIMARY KEY
    REFERENCES public.providers(id),
  family         text NOT NULL,
  "tagCode"      text NOT NULL,
  "poolId"       text NOT NULL
    REFERENCES public.pools(id),
  "windowDays"   integer NOT NULL DEFAULT 365,
  "displayName"  text,
  "isActive"     boolean NOT NULL DEFAULT true,
  "isManual"     boolean NOT NULL DEFAULT false,
  "matchedPattern" text,
  "createdAt"    timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt"    timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE public.crm_provider_family
  ADD COLUMN IF NOT EXISTS "tagCode" text;
ALTER TABLE public.crm_provider_family
  ADD COLUMN IF NOT EXISTS "poolId" text;
ALTER TABLE public.crm_provider_family
  ADD COLUMN IF NOT EXISTS "windowDays" integer NOT NULL DEFAULT 365;
ALTER TABLE public.crm_provider_family
  ADD COLUMN IF NOT EXISTS "isManual" boolean NOT NULL DEFAULT false;
ALTER TABLE public.crm_provider_family
  ADD COLUMN IF NOT EXISTS "matchedPattern" text;

DO $$
DECLARE
  cname text;
BEGIN
  SELECT con.conname INTO cname
  FROM pg_constraint con
  JOIN pg_class rel ON rel.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
  WHERE nsp.nspname = 'public'
    AND rel.relname = 'crm_provider_family'
    AND con.contype = 'c'
    AND pg_get_constraintdef(con.oid) ILIKE '%family%';
  IF cname IS NOT NULL THEN
    EXECUTE format('ALTER TABLE public.crm_provider_family DROP CONSTRAINT %I', cname);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS pools_code_uidx
  ON public.pools (code)
  WHERE code IS NOT NULL;

COMMIT;

-- ---------------------------------------------------------------------------
-- Classify every provider → tagCode / pool_code / window (one session, no BEGIN)
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE ld_provider_route ON COMMIT DROP AS
WITH base AS (
  SELECT
    pr.id AS provider_id,
    NULLIF(BTRIM(pr."displayName"), '') AS display_name,
    LOWER(COALESCE(pr."displayName", '')) AS name_l
  FROM public.providers pr
),
bucketed AS (
  SELECT
    provider_id,
    display_name,
    CASE
      WHEN name_l LIKE '%british gas lite%'
        OR name_l LIKE '%bg lite%'
        THEN 'BG_LITE'
      WHEN name_l LIKE '%british gas%'
        OR name_l LIKE 'bg %'
        OR name_l = 'bg'
        THEN 'BG'
      WHEN name_l LIKE '%e.on%'
        OR name_l LIKE '%e-on%'
        OR name_l LIKE 'eon%'
        THEN 'EON'
      WHEN name_l LIKE '%utility bidder%'
        OR name_l LIKE '%utilitybidder%'
        THEN 'UB'
      ELSE 'SUPPLIER'
    END AS bucket
  FROM base
  WHERE display_name IS NOT NULL
),
slugged AS (
  SELECT
    provider_id,
    display_name,
    bucket,
    UPPER(
      TRIM(BOTH '_' FROM regexp_replace(
        regexp_replace(display_name, '[^a-zA-Z0-9]+', '_', 'g'),
        '_+', '_', 'g'
      ))
    ) AS raw_slug
  FROM bucketed
),
coded AS (
  SELECT
    provider_id,
    display_name,
    bucket,
    CASE bucket
      WHEN 'BG' THEN 'BG'
      WHEN 'EON' THEN 'EON'
      WHEN 'UB' THEN 'UB'
      WHEN 'BG_LITE' THEN
        CASE
          WHEN raw_slug = '' OR raw_slug IS NULL THEN 'BG_LITE'
          ELSE LEFT(raw_slug, 48)
        END
      ELSE
        CASE
          WHEN raw_slug = '' OR raw_slug IS NULL
            THEN 'P_' || UPPER(REPLACE(LEFT(provider_id, 8), '-', ''))
          ELSE LEFT(raw_slug, 48)
        END
    END AS tag_code,
    CASE bucket
      WHEN 'BG' THEN 548
      ELSE 365
    END AS window_days,
    CASE bucket
      WHEN 'BG' THEN 'British Gas'
      WHEN 'EON' THEN 'E.ON'
      WHEN 'UB' THEN 'Utility Bidder'
      ELSE display_name
    END AS pool_name
  FROM slugged
),
dedup AS (
  SELECT
    c.*,
    COUNT(*) OVER (PARTITION BY c.tag_code) AS slug_n
  FROM coded c
),
final AS (
  SELECT
    provider_id,
    display_name,
    bucket,
    CASE
      WHEN bucket IN ('BG', 'EON', 'UB') THEN tag_code
      WHEN slug_n > 1
        THEN LEFT(tag_code, 40) || '_' || UPPER(REPLACE(LEFT(provider_id, 6), '-', ''))
      ELSE tag_code
    END AS tag_code,
    window_days,
    pool_name,
    CASE bucket
      WHEN 'BG' THEN 'BG'
      WHEN 'EON' THEN 'EON'
      WHEN 'UB' THEN 'UB'
      WHEN slug_n > 1
        THEN LEFT(tag_code, 40) || '_' || UPPER(REPLACE(LEFT(provider_id, 6), '-', ''))
      ELSE tag_code
    END AS pool_code
  FROM dedup
)
SELECT
  provider_id,
  display_name,
  bucket,
  tag_code,
  pool_code,
  window_days,
  pool_name
FROM final;

-- Per-supplier pools only (shared BG / EON / UB already in 04)
INSERT INTO public.pools (id, code, name, type, "isLocked", "createdAt", "updatedAt")
SELECT
  gen_random_uuid(),
  r.pool_code,
  r.pool_name,
  'STANDARD'::pool_type,
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM (
  SELECT DISTINCT pool_code, pool_name
  FROM ld_provider_route
  WHERE bucket IN ('BG_LITE', 'SUPPLIER')
) r
ON CONFLICT (code) DO UPDATE SET
  name = EXCLUDED.name,
  "updatedAt" = CURRENT_TIMESTAMP;

INSERT INTO public.crm_provider_family
  ("providerId", family, "tagCode", "poolId", "windowDays", "displayName",
   "isActive", "isManual", "matchedPattern", "createdAt", "updatedAt")
SELECT
  r.provider_id,
  r.bucket,
  r.tag_code,
  p.id,
  r.window_days,
  r.display_name,
  true,
  false,
  r.bucket,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM ld_provider_route r
JOIN public.pools p ON p.code = r.pool_code
ON CONFLICT ("providerId") DO UPDATE SET
  family = EXCLUDED.family,
  "tagCode" = EXCLUDED."tagCode",
  "poolId" = EXCLUDED."poolId",
  "windowDays" = EXCLUDED."windowDays",
  "displayName" = EXCLUDED."displayName",
  "matchedPattern" = EXCLUDED."matchedPattern",
  "updatedAt" = CURRENT_TIMESTAMP
WHERE crm_provider_family."isManual" = false;

WITH tags AS (
  SELECT DISTINCT tag_code, pool_code, window_days
  FROM ld_provider_route
  WHERE tag_code NOT IN ('EON', 'BG', 'UB', 'OTHER')
),
numbered AS (
  SELECT t.*, 200 + ROW_NUMBER() OVER (ORDER BY t.tag_code) * 2 AS pri_now
  FROM tags t
)
INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  'ld_rule_' || LOWER(n.tag_code) || '_now',
  n.pri_now,
  n.tag_code || '_NOW',
  p.id,
  true,
  n.tag_code || ' expired / no CED',
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM numbered n
JOIN public.pools p ON p.code = n.pool_code
ON CONFLICT (id) DO UPDATE SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = true,
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

WITH tags AS (
  SELECT DISTINCT tag_code, pool_code, window_days
  FROM ld_provider_route
  WHERE tag_code NOT IN ('EON', 'BG', 'UB', 'OTHER')
),
numbered AS (
  SELECT t.*, 201 + ROW_NUMBER() OVER (ORDER BY t.tag_code) * 2 AS pri_in
  FROM tags t
)
INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  'ld_rule_' || LOWER(n.tag_code) || '_in',
  n.pri_in,
  n.tag_code || '_IN_WINDOW',
  p.id,
  true,
  n.tag_code || ' in window 1–' || n.window_days::text,
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM numbered n
JOIN public.pools p ON p.code = n.pool_code
ON CONFLICT (id) DO UPDATE SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = true,
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

SELECT family AS bucket, COUNT(*) AS providers
FROM public.crm_provider_family
WHERE "isActive" = true
GROUP BY family
ORDER BY providers DESC;

SELECT COUNT(*) AS supplier_now_rules
FROM public.crm_pool_rule
WHERE tag LIKE '%_NOW'
  AND "isActive" = true
  AND id LIKE 'ld_rule_%';
