-- After 11b: Unassigned holds far-future cos while parked. Clear that pool from
-- the Pool filter — removes pool_profiles rows for UNASSIGNED only.
-- Does not touch managers' Complaint links or any other pool_profiles.
-- Safe to re-run. No Databricks needed.

BEGIN;

DELETE FROM public.pool_profiles pp
USING public.pools p
WHERE pp."poolId" = p.id
  AND p.code = 'UNASSIGNED';

SELECT
  p.code,
  COUNT(pp."profileId") AS profiles_still_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.code = 'UNASSIGNED'
GROUP BY p.code;

COMMIT;
