-- Remove sales-agent access to parked shared pools (Unassigned, supplier, Upselling, …).
-- Agents keep: PRIVATE pools (their bags) + Retentions + Past Retentions only.
-- Does not delete pools or move companies — run 11_reclaim + Databricks for that.
-- Safe to re-run.

BEGIN;

DELETE FROM public.pool_profiles pp
USING public.pools p
WHERE pp."poolId" = p.id
  AND p.type::text <> 'PRIVATE'
  AND COALESCE(p.code, '') NOT IN ('RETENTION', 'PAST_RETENTION');

COMMIT;

SELECT p.code, p.name, p.type::text AS pool_type, COUNT(pp."profileId") AS agents_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN', 'PRIVATE')
GROUP BY p.id, p.code, p.name, p.type
ORDER BY p.type::text, p.code;
