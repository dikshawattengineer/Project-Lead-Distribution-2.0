-- Site → load-source link + Retention sale fallback.
-- Run after 14_crm_load_source.sql (needs public.crm_load_source).
-- Safe to re-run. Does not change companies.poolId.
--
-- Colleague fills crm_company_site_load (old site id → company_sites.id + code).
-- Nightly: link → company_sites → companyId
--   if any site is RETENTION and no CRM deal → use crm_company_load_sale.

BEGIN;

CREATE TABLE IF NOT EXISTS public.crm_company_site_load (
  id                text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "companySiteId"   text NOT NULL
    REFERENCES public.company_sites(id),
  "loadSourceId"    text NOT NULL
    REFERENCES public.crm_load_source(id),
  "oldSiteId"       text,
  "loadedAt"        timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "createdAt"       timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE ("companySiteId", "loadSourceId")
);

CREATE INDEX IF NOT EXISTS crm_company_site_load_site_idx
  ON public.crm_company_site_load ("companySiteId");

CREATE INDEX IF NOT EXISTS crm_company_site_load_source_idx
  ON public.crm_company_site_load ("loadSourceId");

CREATE TABLE IF NOT EXISTS public.crm_company_load_sale (
  id                      text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "companyId"             text NOT NULL
    REFERENCES public.companies(id),
  "companySiteId"         text
    REFERENCES public.company_sites(id),
  "gasSignedAt"           timestamp without time zone,
  "gasLiveAt"             timestamp without time zone,
  "gasTermMonths"         integer,
  "gasRenewalDate"        date,
  "elecSignedAt"          timestamp without time zone,
  "elecLiveAt"            timestamp without time zone,
  "elecTermMonths"        integer,
  "elecRenewalDate"       date,
  "lastDealEndDate"       date,
  "hasPastSale"           boolean NOT NULL DEFAULT true,
  "createdAt"             timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt"             timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE ("companyId")
);

CREATE INDEX IF NOT EXISTS crm_company_load_sale_site_idx
  ON public.crm_company_load_sale ("companySiteId");

COMMIT;
