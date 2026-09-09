-- Fallback from colleague table public.sourcebridge.
-- newcrmid = company_sites.id  →  join for companyId.
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

INSERT INTO public.crm_company_load_sale
  ("companyId", "companySiteId", source, "hasPastSale", "updatedAt")
SELECT DISTINCT ON (s."companyId")
  s."companyId",
  s.id,
  COALESCE(b.source, 'Retention'),
  true,
  CURRENT_TIMESTAMP
FROM public.sourcebridge b
JOIN public.company_sites s
  ON s.id = b.newcrmid
WHERE LOWER(TRIM(COALESCE(b.source, ''))) = 'retention'
ORDER BY s."companyId", s.id
ON CONFLICT ("companyId") DO UPDATE SET
  "companySiteId" = EXCLUDED."companySiteId",
  source = EXCLUDED.source,
  "hasPastSale" = true,
  "updatedAt" = CURRENT_TIMESTAMP;

COMMIT;
