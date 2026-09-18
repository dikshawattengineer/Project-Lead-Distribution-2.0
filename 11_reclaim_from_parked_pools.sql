-- One-time (safe to re-run): pull companies OUT of parked shared pools
-- (supplier, Unassigned, Upselling, …) into Retentions or Past Retentions only.
-- Far-future CED (541+ days) stays put — Unassigned apply is off while parked.
-- Only companies with a past sale (deals or crm_company_load_sale).
-- PRIVATE agent pools are not touched. Run after 10_park_supplier_routing.sql.
-- Then run Databricks to refresh placements / audits.

BEGIN;

CREATE TEMP TABLE ld_parked_pool ON COMMIT DROP AS
SELECT p.id AS pool_id, p.code
FROM public.pools p
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN')
  AND COALESCE(p.code, '') NOT IN ('RETENTION', 'PAST_RETENTION', 'COMPLAINT');

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

CREATE TEMP TABLE ld_past_sale_co ON COMMIT DROP AS
SELECT DISTINCT company_id
FROM (
  SELECT d."companyId" AS company_id FROM public.deals d WHERE d."companyId" IS NOT NULL
  UNION
  SELECT ls."companyId" FROM public.crm_company_load_sale ls
) s
WHERE company_id IS NOT NULL;

CREATE TEMP TABLE ld_reclaim_target ON COMMIT DROP AS
SELECT
  c.id AS company_id,
  CASE
    WHEN ced.days_left BETWEEN 1 AND 540 THEN pr.id
    WHEN ced.days_left IS NULL OR ced.days_left < 1 THEN po.id
  END AS target_pool_id
FROM public.companies c
JOIN ld_parked_pool pk ON pk.pool_id = c."poolId"
JOIN ld_past_sale_co ps ON ps.company_id = c.id
LEFT JOIN ld_contract_ced ced ON ced.company_id = c.id
JOIN public.pools pr ON pr.code = 'RETENTION'
JOIN public.pools po ON po.code = 'PAST_RETENTION'
WHERE CASE
    WHEN ced.days_left BETWEEN 1 AND 540 THEN pr.id
    WHEN ced.days_left IS NULL OR ced.days_left < 1 THEN po.id
  END IS NOT NULL
  AND c."poolId" IS DISTINCT FROM CASE
    WHEN ced.days_left BETWEEN 1 AND 540 THEN pr.id
    WHEN ced.days_left IS NULL OR ced.days_left < 1 THEN po.id
  END;

UPDATE public.companies c
SET "poolId" = t.target_pool_id,
    "updatedAt" = CURRENT_TIMESTAMP
FROM ld_reclaim_target t
WHERE c.id = t.company_id;

-- Summary must run before COMMIT (temp tables use ON COMMIT DROP).
SELECT
  (SELECT COUNT(*) FROM ld_parked_pool) AS parked_shared_pools,
  (SELECT COUNT(*) FROM ld_reclaim_target) AS companies_reclaimed;

COMMIT;
