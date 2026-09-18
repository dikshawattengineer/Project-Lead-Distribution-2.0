-- One-time fix: old 11_reclaim put far-future CED (541+ days) into Past Retentions.
-- Those belong in Unassigned when live; while parked nightly does not apply Unassigned.
-- Run in Supabase if companies still show Past Retentions with only far-future CEDs.
-- Safe to re-run. Does not touch Retention window (1–540) or truly expired CED.

BEGIN;

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

CREATE TEMP TABLE ld_far_future_misfiled ON COMMIT DROP AS
SELECT
  c.id AS company_id,
  c.name,
  ced.end_date,
  ced.days_left
FROM public.companies c
JOIN public.pools p ON p.id = c."poolId"
JOIN ld_contract_ced ced ON ced.company_id = c.id
WHERE p.code = 'PAST_RETENTION'
  AND ced.days_left > 540;

-- Preview before update
SELECT COUNT(*) AS companies_to_move FROM ld_far_future_misfiled;

SELECT company_id, name, end_date, days_left
FROM ld_far_future_misfiled
ORDER BY name
LIMIT 50;

UPDATE public.companies c
SET
  "poolId" = (SELECT id FROM public.pools WHERE code = 'UNASSIGNED' LIMIT 1),
  "updatedAt" = CURRENT_TIMESTAMP
FROM ld_far_future_misfiled t
WHERE c.id = t.company_id;

SELECT COUNT(*) AS moved_to_unassigned FROM ld_far_future_misfiled;

COMMIT;
