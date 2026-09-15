-- Fallback: every retention-source lead → crm_company_load_sale
--
-- Tag buckets (same as CRM Nightly):
--   Past Retention: CED today or past, or no CED (< 1 day)
--   Retention:      1–540 days left
--   Upselling:      541+ days left
--
-- Which CED: the LATEST contract endDate on the company
-- (all sites / meters, via siteId or siteMeterId). Empty meters ignored.
-- That is why a live 197/333-day meter beats an expired sister site.
--
-- Who is inserted:
--   1) legacy_site_mappings — THIS is the source table (LIKE '%retention%')
--   2) any meter with an endDate
--   3) every public.companies row (this load is all retention)
--
-- Do not use crm_load_source. Source is legacy_site_mappings.source.
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
) x
WHERE company_id IS NOT NULL
ORDER BY company_id, end_date DESC NULLS LAST;

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
  -- 1) Retention rows from legacy_site_mappings (the source table)
  SELECT
    m."companyId" AS company_id,
    COALESCE(ced.site_id, m."companySiteId") AS site_id,
    COALESCE(m.source, 'Retention') AS source,
    ced.end_date,
    m."migratedAt" AS migrated_at
  FROM public.legacy_site_mappings m
  LEFT JOIN ld_contract_ced ced
    ON ced.company_id = m."companyId"
  WHERE LOWER(COALESCE(m.source, '')) LIKE '%retention%'

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
