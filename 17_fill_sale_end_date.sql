-- Fill lastDealEndDate from all sites/meters: Retention (1–540) first,
-- then Past, then Upselling; latest date inside that bag.
-- Joins companyId, siteId, and siteMeterId. Empty meters ignored.
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
      NULLIF(BTRIM(c."companyId"), '') AS company_id,
      c."siteId" AS site_id,
      c."endDate"::date AS end_date
    FROM public.contracts c
    WHERE c."endDate" IS NOT NULL
      AND NULLIF(BTRIM(c."companyId"), '') IS NOT NULL

    UNION ALL

    SELECT
      s."companyId",
      s.id,
      c."endDate"::date
    FROM public.contracts c
    JOIN public.company_sites s ON s.id = c."siteId"
    WHERE c."endDate" IS NOT NULL
      AND s."companyId" IS NOT NULL

    UNION ALL

    SELECT
      s."companyId",
      s.id,
      c."endDate"::date
    FROM public.contracts c
    JOIN public.site_meters sm ON sm.id = c."siteMeterId"
    JOIN public.company_sites s ON s.id = sm."companySiteId"
    WHERE c."endDate" IS NOT NULL
      AND s."companyId" IS NOT NULL
  ) d
  WHERE company_id IS NOT NULL
  ORDER BY
    company_id,
    CASE
      WHEN end_date > CURRENT_DATE AND (end_date - CURRENT_DATE) <= 540 THEN 0
      WHEN end_date <= CURRENT_DATE THEN 1
      ELSE 2
    END,
    end_date DESC NULLS LAST
) x
WHERE sl."companyId" = x.company_id;
