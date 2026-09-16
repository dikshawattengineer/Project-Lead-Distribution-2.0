-- Sync every providers row → routing map + pool + NOW/IN_WINDOW rules.
--
-- Buckets (not one pool per BG/EON name):
--   British Gas Lite     → own pool (displayName), window 365
--   Other British Gas*   → ld_pool_bg "British Gas", window 548
--   E.ON / E-ON / EON*   → ld_pool_eon "E.ON", window 365
--   Utility Bidder       → ld_pool_ub, window 365
--   Every other known    → own pool, name = providers.displayName, window 365
--   Unknown / no provider → OTHER (ld_pool_other) — notebook only
--
-- E.ON DFV pool/rule is contract-type only (seed_dfv_pools / this file).
-- Past due EON_NOW → main E.ON (not DFV).
--
-- Safe to re-run. Does not change companies.poolId.
-- Run after base shared pools exist (04 / seed). Requires public.providers.

BEGIN;

-- ---------------------------------------------------------------------------
-- Schema: routing map (extends old BG/EON/UB/OTHER family)
-- ---------------------------------------------------------------------------
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

-- Old CHECK only allowed BG/EON/UB/OTHER — drop so per-supplier codes work
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

-- Priority need not be unique once every supplier has its own tags
DROP INDEX IF EXISTS public.crm_pool_rule_priority_uidx;
CREATE INDEX IF NOT EXISTS crm_pool_rule_priority_idx
  ON public.crm_pool_rule (priority);

-- ---------------------------------------------------------------------------
-- Ensure core bags exist (idempotent)
-- ---------------------------------------------------------------------------
INSERT INTO public.pools (id, code, name, type, "isLocked", "createdAt", "updatedAt")
VALUES
  ('ld_pool_eon',       'EON',     'E.ON',            'STANDARD', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_eon_dfv',   'EON_DFV', 'E.ON DFV',        'STANDARD', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_bg',        'BG',      'British Gas',     'STANDARD', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_ub',        'UB',      'Utility Bidder',  'STANDARD', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_other',     'OTHER',   'Other',           'STANDARD', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_unassigned','UNASSIGNED','Unassigned',    'STANDARD', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO UPDATE SET
  code = EXCLUDED.code,
  name = EXCLUDED.name,
  "updatedAt" = CURRENT_TIMESTAMP;

-- E.ON DFV = real DFV only. Past due → main E.ON.
INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
VALUES
  (
    'ld_rule_eon_dfv', 28, 'EON_DFV', 'ld_pool_eon_dfv', true,
    'E.ON deemed / flexible / variable only', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_eon_now', 29, 'EON_NOW', 'ld_pool_eon', true,
    'E.ON expired / no CED → main E.ON', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_eon_in', 30, 'EON_IN_WINDOW', 'ld_pool_eon', true,
    'E.ON in window 1–365', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_bg_now', 39, 'BG_NOW', 'ld_pool_bg', true,
    'British Gas expired / no CED', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_bg_in', 40, 'BG_IN_WINDOW', 'ld_pool_bg', true,
    'British Gas in window 1–548', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_ub_now', 20, 'UB_NOW', 'ld_pool_ub', true,
    'Utility Bidder expired / no CED', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_ub_in', 21, 'UB_IN_WINDOW', 'ld_pool_ub', true,
    'Utility Bidder in window 1–365', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_other_now', 49, 'OTHER_NOW', 'ld_pool_other', true,
    'Unknown supplier expired / no CED', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_other_in', 50, 'OTHER_IN_WINDOW', 'ld_pool_other', true,
    'Unknown supplier in window', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_pre', 90, 'PRE_WINDOW', 'ld_pool_unassigned', true,
    'Wait — too far out', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  ),
  (
    'ld_rule_fallback', 99, 'UNASSIGNED', 'ld_pool_unassigned', true,
    'Fallback', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
  )
ON CONFLICT (id) DO UPDATE SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = EXCLUDED."isActive",
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

COMMIT;

-- ---------------------------------------------------------------------------
-- Classify every provider → tagCode / pool / window
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
-- Same slug from two suppliers → disambiguate (not for shared BG/EON/UB)
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
      WHEN 'BG' THEN 'ld_pool_bg'
      WHEN 'EON' THEN 'ld_pool_eon'
      WHEN 'UB' THEN 'ld_pool_ub'
      ELSE NULL
    END AS shared_pool_id
  FROM dedup
)
SELECT
  provider_id,
  display_name,
  bucket,
  tag_code,
  window_days,
  pool_name,
  COALESCE(shared_pool_id, 'ld_pool_' || LOWER(tag_code)) AS pool_id
FROM final;

-- Per-supplier pools (skip shared BG / EON / UB)
INSERT INTO public.pools (id, code, name, type, "isLocked", "createdAt", "updatedAt")
SELECT DISTINCT
  r.pool_id,
  r.tag_code,
  r.pool_name,
  'STANDARD',
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM ld_provider_route r
WHERE r.bucket IN ('BG_LITE', 'SUPPLIER')
ON CONFLICT (id) DO UPDATE SET
  code = EXCLUDED.code,
  name = EXCLUDED.name,
  "updatedAt" = CURRENT_TIMESTAMP;

-- Routing map
INSERT INTO public.crm_provider_family
  ("providerId", family, "tagCode", "poolId", "windowDays", "displayName",
   "isActive", "isManual", "matchedPattern", "createdAt", "updatedAt")
SELECT
  r.provider_id,
  r.bucket,
  r.tag_code,
  r.pool_id,
  r.window_days,
  r.display_name,
  true,
  false,
  r.bucket,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM ld_provider_route r
ON CONFLICT ("providerId") DO UPDATE SET
  family = EXCLUDED.family,
  "tagCode" = EXCLUDED."tagCode",
  "poolId" = EXCLUDED."poolId",
  "windowDays" = EXCLUDED."windowDays",
  "displayName" = EXCLUDED."displayName",
  "matchedPattern" = EXCLUDED."matchedPattern",
  "updatedAt" = CURRENT_TIMESTAMP
WHERE crm_provider_family."isManual" = false;

-- One NOW + one IN_WINDOW rule per distinct tagCode (shared BG/EON/UB included)
WITH tags AS (
  SELECT DISTINCT
    tag_code,
    pool_id,
    window_days,
    bucket
  FROM ld_provider_route
),
numbered AS (
  SELECT
    t.*,
    200 + ROW_NUMBER() OVER (ORDER BY t.tag_code) * 2 AS pri_now
  FROM tags t
)
INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  'ld_rule_' || LOWER(n.tag_code) || '_now',
  n.pri_now,
  n.tag_code || '_NOW',
  n.pool_id,
  true,
  n.tag_code || ' expired / no CED',
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM numbered n
WHERE n.tag_code NOT IN ('EON', 'BG', 'UB', 'OTHER')
ON CONFLICT (id) DO UPDATE SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = true,
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

WITH tags AS (
  SELECT DISTINCT
    tag_code,
    pool_id,
    window_days
  FROM ld_provider_route
),
numbered AS (
  SELECT
    t.*,
    201 + ROW_NUMBER() OVER (ORDER BY t.tag_code) * 2 AS pri_in
  FROM tags t
)
INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
SELECT
  'ld_rule_' || LOWER(n.tag_code) || '_in',
  n.pri_in,
  n.tag_code || '_IN_WINDOW',
  n.pool_id,
  true,
  n.tag_code || ' in window 1–' || n.window_days::text,
  false,
  CURRENT_TIMESTAMP,
  CURRENT_TIMESTAMP
FROM numbered n
WHERE n.tag_code NOT IN ('EON', 'BG', 'UB', 'OTHER')
ON CONFLICT (id) DO UPDATE SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = true,
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

-- QA
SELECT family AS bucket, COUNT(*) AS providers
FROM public.crm_provider_family
WHERE "isActive" = true
GROUP BY family
ORDER BY providers DESC;

SELECT id, code, name
FROM public.pools
WHERE id LIKE 'ld_pool_%'
  AND type::text IN ('STANDARD', 'CAMPAIGN')
ORDER BY name
LIMIT 40;

SELECT COUNT(*) AS supplier_rules
FROM public.crm_pool_rule
WHERE id LIKE 'ld_rule_%'
  AND tag LIKE '%_NOW'
  AND "isActive" = true;
