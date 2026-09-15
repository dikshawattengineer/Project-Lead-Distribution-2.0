-- Fallback: every retention-source lead → crm_company_load_sale
--
-- Tag reacts to this table (source), not to the supplier on the contract:
--   on fallback + CED     → Past Retention / Retention / Upselling
--   on fallback + no CED  → Unassigned
--   not on fallback       → supplier rules (E.ON / BG / …)
--
-- Who is inserted:
--   1) legacy_site_mappings — this is the source table (LIKE '%retention%')
--   2) company_sites.loadSourceId → crm_load_source.kind = RETENTION
--   3) any meter with startDate AND endDate (keep until migration is done)
--
-- Supplier files later: stamp company_sites.loadSourceId = eon_supplier / bg_supplier / …
-- (crm_load_source.kind = SUPPLIER). Do NOT insert those into this table.
-- ld_working.source_kind will read that stamp so tag uses supplier rules.
--
-- CED from the meter that has dates (gas dated + elec empty still counts).
-- contracts use siteId, not companySiteId.
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
    COALESCE(NULLIF(BTRIM(c."companyId"), ''), s."companyId") AS company_id,
    COALESCE(c."siteId", s.id) AS site_id,
    c."endDate"::date AS end_date
  FROM public.contracts c
  LEFT JOIN public.company_sites s
    ON s.id = c."siteId"
  WHERE c."startDate" IS NOT NULL
    AND c."endDate" IS NOT NULL
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
  -- 1) Every mapped company (this file is a retention load)
  SELECT
    m."companyId" AS company_id,
    COALESCE(ced.site_id, m."companySiteId") AS site_id,
    COALESCE(m.source, 'Retention') AS source,
    ced.end_date,
    m."migratedAt" AS migrated_at
  FROM public.legacy_site_mappings m
  LEFT JOIN ld_contract_ced ced
    ON ced.company_id = m."companyId"

  UNION ALL

  -- 2) Site stamped RETENTION on loadSourceId
  SELECT
    s."companyId" AS company_id,
    COALESCE(ced.site_id, s.id) AS site_id,
    COALESCE(src.name, 'Retention') AS source,
    ced.end_date,
    s."loadSourceAt" AS migrated_at
  FROM public.company_sites s
  JOIN public.crm_load_source src
    ON src.id = s."loadSourceId"
  LEFT JOIN ld_contract_ced ced
    ON ced.company_id = s."companyId"
  WHERE src.kind = 'RETENTION'

  UNION ALL

  -- 3) Keep until migration — any company with start+end on at least one meter
  SELECT
    ced.company_id,
    ced.site_id,
    'Retention' AS source,
    ced.end_date,
    NULL::timestamp AS migrated_at
  FROM ld_contract_ced ced
) u
WHERE u.company_id IS NOT NULL
ORDER BY
  u.company_id,
  CASE WHEN u.end_date IS NOT NULL THEN 0 ELSE 1 END,
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
