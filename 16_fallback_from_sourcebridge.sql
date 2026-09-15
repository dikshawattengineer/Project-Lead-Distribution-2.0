-- Fallback: every retention-source lead → crm_company_load_sale
--
-- Looks at every site / meter on the company (empty meters ignored).
-- Retention always wins over Past Retention and Upselling:
--   1) any CED 1–540 days  → Retention  (latest of those dates)
--   2) else any CED past/today → Past Retention
--   3) else CED 541+       → Upselling
--   4) no CED on any meter → Past Retention
--
-- Who is inserted:
--   1) legacy_site_mappings — load origin is column "campaign"
--      (Retention / Supplier). That is NOT the lead-tag Campaign pool.
--      Still LIKE '%retention%' on campaign and source just in case.
--   2) any meter with an endDate
--   3) every public.companies row (this load is all retention)
--
-- Do not use crm_load_source.
-- Safe to re-run. Does not change poolId.

BEGIN;

CREATE TABLE IF NOT EXISTS public.crm_company_load_sale (
  id                  text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "companyId"         text NOT NULL
    REFERENCES public.companies(id),
  "companySiteId"     text
    REFERENCES public.company_sites(id),
  source              text NOT NULL DEFAULT 'Retention',
  "hasPastSale"       boolean NOT NULL DEFAULT true,
  "lastDealEndDate"   date,
  "createdAt"         timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt"         timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE ("companyId")
);

CREATE INDEX IF NOT EXISTS crm_company_load_sale_site_idx
  ON public.crm_company_load_sale ("companySiteId");

CREATE TEMP TABLE ld_contract_ced ON COMMIT DROP AS
SELECT DISTINCT ON (company_id)
  company_id,
  site_id,
  end_date
FROM (
  -- companyId on the contract
  SELECT
    NULLIF(BTRIM(c."companyId"), '') AS company_id,
    c."siteId" AS site_id,
    c."endDate"::date AS end_date
  FROM public.contracts c
  WHERE c."endDate" IS NOT NULL
    AND NULLIF(BTRIM(c."companyId"), '') IS NOT NULL

  UNION ALL

  -- siteId → company_sites
  SELECT
    s."companyId" AS company_id,
    s.id AS site_id,
    c."endDate"::date AS end_date
  FROM public.contracts c
  JOIN public.company_sites s
    ON s.id = c."siteId"
  WHERE c."endDate" IS NOT NULL
    AND s."companyId" IS NOT NULL

  UNION ALL

  -- siteMeterId → site_meters → company_sites (current meter contracts)
  SELECT
    s."companyId" AS company_id,
    s.id AS site_id,
    c."endDate"::date AS end_date
  FROM public.contracts c
  JOIN public.site_meters sm
    ON sm.id = c."siteMeterId"
  JOIN public.company_sites s
    ON s.id = sm."companySiteId"
  WHERE c."endDate" IS NOT NULL
    AND s."companyId" IS NOT NULL
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

INSERT INTO public.crm_company_load_sale
  ("companyId", "companySiteId", source, "hasPastSale", "lastDealEndDate", "updatedAt")
SELECT DISTINCT ON (u.company_id)
  u.company_id,
  u.site_id,
  u.source,
  true,
  u.end_date,
  CURRENT_TIMESTAMP
FROM (
  -- 1) Retention rows from legacy_site_mappings
  --    campaign = load origin (Retention / Supplier), not pool Campaign
  SELECT
    m."companyId" AS company_id,
    COALESCE(ced.site_id, m."companySiteId") AS site_id,
    COALESCE(
      NULLIF(BTRIM(m.campaign), ''),
      NULLIF(BTRIM(m.source), ''),
      'Retention'
    ) AS source,
    ced.end_date,
    m."migratedAt" AS migrated_at
  FROM public.legacy_site_mappings m
  LEFT JOIN ld_contract_ced ced
    ON ced.company_id = m."companyId"
  WHERE LOWER(COALESCE(m.campaign, m.source, '')) LIKE '%retention%'

  UNION ALL

  -- 2) Keep until migration — any company with an endDate on at least one meter
  SELECT
    ced.company_id,
    ced.site_id,
    'Retention' AS source,
    ced.end_date,
    NULL::timestamp AS migrated_at
  FROM ld_contract_ced ced

  UNION ALL

  -- 3) This load is all retention — every company, even if mapping/CED did not join
  SELECT
    co.id AS company_id,
    ced.site_id,
    'Retention' AS source,
    ced.end_date,
    NULL::timestamp AS migrated_at
  FROM public.companies co
  LEFT JOIN ld_contract_ced ced
    ON ced.company_id = co.id
) u
WHERE u.company_id IS NOT NULL
ORDER BY
  u.company_id,
  CASE WHEN u.end_date IS NOT NULL THEN 0 ELSE 1 END,
  CASE
    WHEN u.end_date > CURRENT_DATE AND (u.end_date - CURRENT_DATE) <= 540 THEN 0
    WHEN u.end_date <= CURRENT_DATE THEN 1
    ELSE 2
  END,
  u.end_date DESC NULLS LAST,
  u.migrated_at DESC NULLS LAST
ON CONFLICT ("companyId") DO UPDATE SET
  "companySiteId" = COALESCE(EXCLUDED."companySiteId", crm_company_load_sale."companySiteId"),
  source = COALESCE(EXCLUDED.source, crm_company_load_sale.source),
  "hasPastSale" = true,
  "lastDealEndDate" = COALESCE(EXCLUDED."lastDealEndDate", crm_company_load_sale."lastDealEndDate"),
  "updatedAt" = CURRENT_TIMESTAMP;

UPDATE public.crm_company_load_sale sl
SET
  "lastDealEndDate" = x.end_date,
  "companySiteId" = COALESCE(x.site_id, sl."companySiteId"),
  "updatedAt" = CURRENT_TIMESTAMP
FROM ld_contract_ced x
WHERE sl."companyId" = x.company_id
  AND sl."lastDealEndDate" IS DISTINCT FROM x.end_date;

COMMIT;

SELECT
  COUNT(*) AS rows,
  COUNT("lastDealEndDate") AS with_contract_end_date,
  COUNT(*) - COUNT("lastDealEndDate") AS missing_end_date
FROM public.crm_company_load_sale;
