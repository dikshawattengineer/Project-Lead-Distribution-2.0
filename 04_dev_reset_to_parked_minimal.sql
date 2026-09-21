-- DEV ONLY — tear down supplier / full-04 pools after 09 sync, then re-seed minimal.
-- Keeps: RETENTION, PAST_RETENTION, COMPLAINT, UNASSIGNED, UPSELLING, CUSTOMER_CARE + PRIVATE bags.
-- Moves companies out of deleted pools first (Retention / Past / Unassigned clock).
-- Do NOT run on production without review.
--
-- After this script succeeds, run: 04_parked_minimal.sql

BEGIN;

CREATE TEMP TABLE ld_keep_pool_code ON COMMIT DROP AS
SELECT unnest(ARRAY[
  'RETENTION', 'PAST_RETENTION', 'COMPLAINT',
  'UNASSIGNED', 'UPSELLING', 'CUSTOMER_CARE'
]) AS code;

CREATE TEMP TABLE ld_drop_pool ON COMMIT DROP AS
SELECT p.id AS pool_id, p.code, p.name
FROM public.pools p
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN')
  AND COALESCE(p.code, '') NOT IN (SELECT code FROM ld_keep_pool_code);

SELECT COUNT(*) AS shared_pools_to_drop FROM ld_drop_pool;
SELECT code, name FROM ld_drop_pool ORDER BY code;

CREATE TEMP TABLE ld_contract_ced ON COMMIT DROP AS
SELECT DISTINCT ON (company_id)
  company_id,
  end_date,
  (end_date - CURRENT_DATE) AS days_left
FROM (
  SELECT NULLIF(BTRIM(c."companyId"), '') AS company_id, c."endDate"::date AS end_date
  FROM public.contracts c
  WHERE c."endDate" IS NOT NULL AND NULLIF(BTRIM(c."companyId"), '') IS NOT NULL
  UNION ALL
  SELECT s."companyId", c."endDate"::date
  FROM public.contracts c
  JOIN public.company_sites s ON s.id = c."siteId"
  WHERE c."endDate" IS NOT NULL AND s."companyId" IS NOT NULL
  UNION ALL
  SELECT s."companyId", c."endDate"::date
  FROM public.contracts c
  JOIN public.site_meters sm ON sm.id = c."siteMeterId"
  JOIN public.company_sites s ON s.id = sm."companySiteId"
  WHERE c."endDate" IS NOT NULL AND s."companyId" IS NOT NULL
) x
WHERE company_id IS NOT NULL
ORDER BY
  company_id,
  CASE
    WHEN end_date > CURRENT_DATE AND (end_date - CURRENT_DATE) <= 540 THEN 0
    WHEN end_date <= CURRENT_DATE THEN 1
    ELSE 2
  END,
  end_date DESC NULLS LAST;

CREATE TEMP TABLE ld_company_move ON COMMIT DROP AS
SELECT
  c.id AS company_id,
  CASE
    WHEN ced.days_left BETWEEN 1 AND 540 THEN pr.id
    WHEN ced.days_left IS NULL OR ced.days_left < 1 THEN po.id
    ELSE pu.id
  END AS target_pool_id
FROM public.companies c
JOIN ld_drop_pool dp ON dp.pool_id = c."poolId"
LEFT JOIN ld_contract_ced ced ON ced.company_id = c.id
JOIN public.pools pr ON pr.code = 'RETENTION'
JOIN public.pools po ON po.code = 'PAST_RETENTION'
JOIN public.pools pu ON pu.code = 'UNASSIGNED';

SELECT COUNT(*) AS companies_to_move FROM ld_company_move;

UPDATE public.companies c
SET
  "poolId" = t.target_pool_id,
  "updatedAt" = CURRENT_TIMESTAMP
FROM ld_company_move t
WHERE c.id = t.company_id;

UPDATE public.company_pool_placements pl
SET "endedAt" = CURRENT_TIMESTAMP
FROM ld_drop_pool dp
WHERE pl."poolId" = dp.pool_id
  AND pl."endedAt" IS NULL;

UPDATE public.company_pool_placements pl
SET "sourcePoolId" = NULL
FROM ld_drop_pool dp
WHERE pl."sourcePoolId" = dp.pool_id;

DELETE FROM public.pool_profiles pp
USING ld_drop_pool dp
WHERE pp."poolId" = dp.pool_id;

DELETE FROM public.pool_links pl
USING ld_drop_pool dp
WHERE pl."parentPoolId" = dp.pool_id
   OR pl."childPoolId" = dp.pool_id;

DELETE FROM public.provider_families
WHERE COALESCE("isManual", false) = false;

DELETE FROM public.pool_rules r
USING ld_drop_pool dp
WHERE r."poolId" = dp.pool_id;

DELETE FROM public.pool_rules r
WHERE r.tag NOT IN (
  'COMPLAINT', 'CUSTOMER_CARE', 'PAST_RETENTION', 'RETENTION',
  'UPSELLING', 'PRE_WINDOW', 'UNASSIGNED', 'CALLBACK'
);

DELETE FROM public.pools p
USING ld_drop_pool dp
WHERE p.id = dp.pool_id;

COMMIT;

-- Then run 04_parked_minimal.sql to refresh names / isActive on kept pools + rules.

SELECT code, name, type::text
FROM public.pools
WHERE type::text IN ('STANDARD', 'CAMPAIGN')
ORDER BY code;

SELECT COUNT(*) AS provider_family_rows FROM public.provider_families;
