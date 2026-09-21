-- Sync every providers row → routing map + pool (UUID) + NOW/IN_WINDOW rules.
-- pools.id = gen_random_uuid(); pools.code = stable key (BG, YU_ENERGY, …).
-- pool_rules.id = uuid; upsert key = tag (e.g. YU_ENERGY_NOW). Run 06 first on existing DBs.
-- Run after 04_seed_ld_pools.sql. Safe to re-run. Does not change companies.poolId.
-- Skip while parked — update now so turn-on day is ready.

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS pool_rules_tag_uidx
  ON public.pool_rules (tag);

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
    CASE
      WHEN bucket = 'BG' THEN 'BG'
      WHEN bucket = 'EON' THEN 'EON'
      WHEN bucket = 'UB' THEN 'UB'
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

DELETE FROM public.provider_families pf
USING ld_provider_route r
WHERE pf."providerId" = r.provider_id
  AND COALESCE(pf."isManual", false) = false;

INSERT INTO public.provider_families
  (id, family, "tagCode", "poolId", "windowDays", "displayName",
   "isActive", "isManual", "matchedPattern", "providerId", "createdAt", "updatedAt")
SELECT
  gen_random_uuid(),
  r.bucket,
  r.tag_code,
  p.id,
  r.window_days,
  r.display_name,
  true,
  false,
  r.bucket,
  r.provider_id,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM ld_provider_route r
JOIN public.pools p ON p.code = r.pool_code
WHERE NOT EXISTS (
  SELECT 1
  FROM public.provider_families pf
  WHERE pf."providerId" = r.provider_id
);

UPDATE public.provider_families pf
SET
  family = r.bucket,
  "tagCode" = r.tag_code,
  "poolId" = p.id,
  "windowDays" = r.window_days,
  "displayName" = r.display_name,
  "matchedPattern" = r.bucket,
  "updatedAt" = CURRENT_TIMESTAMP
FROM ld_provider_route r
JOIN public.pools p ON p.code = r.pool_code
WHERE pf."providerId" = r.provider_id
  AND COALESCE(pf."isManual", false) = false;

WITH tags AS (
  SELECT DISTINCT tag_code, pool_code, window_days
  FROM ld_provider_route
  WHERE tag_code NOT IN ('EON', 'BG', 'UB', 'OTHER')
),
numbered AS (
  SELECT t.*, 200 + ROW_NUMBER() OVER (ORDER BY t.tag_code) * 2 AS pri_now
  FROM tags t
)
INSERT INTO public.pool_rules
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  gen_random_uuid(),
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
ON CONFLICT (tag) DO UPDATE SET
  priority = EXCLUDED.priority,
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
INSERT INTO public.pool_rules
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  gen_random_uuid(),
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
ON CONFLICT (tag) DO UPDATE SET
  priority = EXCLUDED.priority,
  "poolId" = EXCLUDED."poolId",
  "isActive" = true,
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

SELECT family AS bucket, COUNT(*) AS providers
FROM public.provider_families
WHERE "isActive" = true
GROUP BY family
ORDER BY providers DESC;

SELECT COUNT(*) AS supplier_now_rules
FROM public.pool_rules
WHERE tag LIKE '%_NOW'
  AND tag NOT IN (
    'EON_NOW', 'BG_NOW', 'UB_NOW', 'OTHER_NOW',
    'EON_IN_WINDOW', 'BG_IN_WINDOW', 'UB_IN_WINDOW', 'OTHER_IN_WINDOW'
  )
  AND "isActive" = true;
