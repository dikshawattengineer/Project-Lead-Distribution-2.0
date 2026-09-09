-- Fill lastDealEndDate from CRM contracts on fallback companies only.
-- Run after 16. Safe to re-run. Does not change poolId.

UPDATE public.crm_company_load_sale sl
SET
  "lastDealEndDate" = x.end_date,
  "updatedAt" = CURRENT_TIMESTAMP
FROM (
  SELECT
    c."companyId" AS company_id,
    MAX(c."endDate"::date) AS end_date
  FROM public.contracts c
  GROUP BY c."companyId"
) x
WHERE sl."companyId" = x.company_id;
