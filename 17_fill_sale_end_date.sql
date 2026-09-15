-- Fill lastDealEndDate from all sites/meters: Retention (1–540) first,
-- then Past, then Upselling; latest date inside that bag.
-- Empty sister meter is ignored (not day 0).
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
      COALESCE(
        NULLIF(BTRIM(c."companyId"), ''),
        s."companyId",
        s2."companyId"
      ) AS company_id,
      COALESCE(c."siteId", s.id, s2.id) AS site_id,
      c."endDate"::date AS end_date
    FROM public.contracts c
    LEFT JOIN public.company_sites s
      ON s.id = c."siteId"
    LEFT JOIN public.site_meters sm
      ON sm.id = c."siteMeterId"
    LEFT JOIN public.company_sites s2
      ON s2.id = sm."companySiteId"
    WHERE c."endDate" IS NOT NULL
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
