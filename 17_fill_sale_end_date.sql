-- Fill lastDealEndDate from the SOONEST meter that has startDate AND endDate.
-- Empty sister meter is ignored (not treated as day 0).
-- Run after 16 if you need a second pass. Safe to re-run. Does not change poolId.

UPDATE public.crm_company_load_sale sl
SET
  "lastDealEndDate" = x.end_date,
  "companySiteId" = COALESCE(x.site_id, sl."companySiteId"),
  "updatedAt" = CURRENT_TIMESTAMP
FROM (
  SELECT DISTINCT ON (company_id)
    company_id,
    site_id,
    end_date
  FROM (
    SELECT
      COALESCE(NULLIF(BTRIM(c."companyId"), ''), s."companyId") AS company_id,
      COALESCE(c."siteId", s.id) AS site_id,
      c."endDate"::date AS end_date
    FROM public.contracts c
    LEFT JOIN public.company_sites s
      ON s.id = c."siteId"
    WHERE c."startDate" IS NOT NULL
      AND c."endDate" IS NOT NULL
  ) d
  WHERE company_id IS NOT NULL
  ORDER BY company_id, end_date ASC NULLS LAST
) x
WHERE sl."companyId" = x.company_id;
