-- While parked: empty Unassigned — no nightly apply writes there.
-- Move every company currently in UNASSIGNED → UPSELLING (also hidden via script 12).
-- Nightly will tag UPSELLING / UNASSIGNED / PRE_WINDOW for QA but apply skips them.
-- Safe to re-run. Run after 11b if you previously moved cos to Unassigned.

BEGIN;

CREATE TEMP TABLE ld_unassigned_co ON COMMIT DROP AS
SELECT c.id AS company_id, c.name
FROM public.companies c
JOIN public.pools p ON p.id = c."poolId"
WHERE p.code = 'UNASSIGNED';

SELECT COUNT(*) AS companies_in_unassigned FROM ld_unassigned_co;

SELECT company_id, name
FROM ld_unassigned_co
ORDER BY name
LIMIT 50;

UPDATE public.companies c
SET
  "poolId" = (SELECT id FROM public.pools WHERE code = 'UPSELLING' LIMIT 1),
  "updatedAt" = CURRENT_TIMESTAMP
FROM ld_unassigned_co t
WHERE c.id = t.company_id;

SELECT COUNT(*) AS moved_to_upselling FROM ld_unassigned_co;

COMMIT;
