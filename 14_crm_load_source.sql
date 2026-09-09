-- Lookup of load codes + stamp on companies.
-- Safe to re-run. Does not change poolId or tagging.
-- Add more codes anytime with INSERT (see bottom). CRM Load uses id as the widget.

BEGIN;

CREATE TABLE IF NOT EXISTS public.crm_load_source (
  id              text PRIMARY KEY,          -- widget value: eon_supplier, retention, …
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
  ('eon_supplier',   'E.ON supplier file',     'SUPPLIER',  'EON'),
  ('bg_supplier',    'British Gas supplier file', 'SUPPLIER', 'BG'),
  ('ub_supplier',    'Utility Bidder supplier file', 'SUPPLIER', 'UB'),
  ('other_supplier', 'Other supplier file',    'SUPPLIER',  'OTHER'),
  ('retention',      'Retention / Watt-sold file', 'RETENTION', NULL)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  kind = EXCLUDED.kind,
  family = EXCLUDED.family,
  "updatedAt" = CURRENT_TIMESTAMP;

ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS "loadSourceId" text
    REFERENCES public.crm_load_source(id);

ALTER TABLE public.companies
  ADD COLUMN IF NOT EXISTS "loadSourceAt" timestamp without time zone;

CREATE INDEX IF NOT EXISTS companies_load_source_idx
  ON public.companies ("loadSourceId");

COMMIT;

-- Checks
-- SELECT * FROM public.crm_load_source ORDER BY kind, id;
-- SELECT id, "loadSourceId", "loadSourceAt" FROM public.companies LIMIT 20;

-- Add a new code later (then use that id on the CRM Load job):
-- INSERT INTO public.crm_load_source (id, name, kind, family)
-- VALUES ('yu_supplier', 'Yu Energy supplier file', 'SUPPLIER', 'OTHER');
