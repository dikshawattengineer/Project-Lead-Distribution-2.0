-- Load source lookup + stamp on company_sites (not companies).
-- Safe to re-run. Does not change companies.poolId.
-- Nightly still assigns the pool on companies; it rolls site sources up.
--
-- If you already ran the old companies.loadSourceId version, this moves it.

BEGIN;

CREATE TABLE IF NOT EXISTS public.crm_load_source (
  id              text PRIMARY KEY,
  name            text NOT NULL,
  kind            text NOT NULL
    CHECK (kind IN ('SUPPLIER', 'RETENTION')),
  family          text
    CHECK (family IS NULL OR family IN ('EON', 'BG', 'UB', 'OTHER')),
  "isActive"      boolean NOT NULL DEFAULT true,
  "createdAt"     timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt"     timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO public.crm_load_source (id, name, kind, family)
VALUES
  ('eon_supplier',   'E.ON supplier file',           'SUPPLIER',  'EON'),
  ('bg_supplier',    'British Gas supplier file',    'SUPPLIER',  'BG'),
  ('ub_supplier',    'Utility Bidder supplier file', 'SUPPLIER',  'UB'),
  ('other_supplier', 'Other supplier file',          'SUPPLIER',  'OTHER'),
  ('retention',      'Retention / Watt-sold file',   'RETENTION', NULL)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  kind = EXCLUDED.kind,
  family = EXCLUDED.family,
  "updatedAt" = CURRENT_TIMESTAMP;

ALTER TABLE public.company_sites
  ADD COLUMN IF NOT EXISTS "loadSourceId" text
    REFERENCES public.crm_load_source(id);

ALTER TABLE public.company_sites
  ADD COLUMN IF NOT EXISTS "loadSourceAt" timestamp without time zone;

CREATE INDEX IF NOT EXISTS company_sites_load_source_idx
  ON public.company_sites ("loadSourceId");

-- Drop the company-level stamp if the earlier script was run.
ALTER TABLE public.companies
  DROP COLUMN IF EXISTS "loadSourceAt";

ALTER TABLE public.companies
  DROP COLUMN IF EXISTS "loadSourceId";

COMMIT;

-- SELECT * FROM public.crm_load_source ORDER BY kind, id;
-- SELECT id, "companyId", "loadSourceId", "loadSourceAt" FROM public.company_sites LIMIT 20;

-- INSERT INTO public.crm_load_source (id, name, kind, family)
-- VALUES ('yu_supplier', 'Yu Energy supplier file', 'SUPPLIER', 'OTHER');
