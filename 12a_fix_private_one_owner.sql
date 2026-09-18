-- Undo mistaken cross-links from an older 12 run (77 profiles × each PRIVATE bag).
-- Each PRIVATE pool = one owner (profiles.primaryPoolId). Safe to re-run.

BEGIN;

DELETE FROM public.pool_profiles pp
USING public.pools priv
WHERE pp."poolId" = priv.id
  AND priv.type = 'PRIVATE'
  AND NOT EXISTS (
    SELECT 1
    FROM public.profiles pr
    WHERE pr.id = pp."profileId"
      AND pr."primaryPoolId" = priv.id
  );

INSERT INTO public.pool_profiles ("poolId", "profileId")
SELECT pr."primaryPoolId", pr.id
FROM public.profiles pr
JOIN public.pools priv ON priv.id = pr."primaryPoolId"
WHERE priv.type = 'PRIVATE'
ON CONFLICT ("poolId", "profileId") DO NOTHING;

COMMIT;

SELECT p.code, p.name, COUNT(pp."profileId") AS agents_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.type = 'PRIVATE'
GROUP BY p.id, p.code, p.name
HAVING COUNT(pp."profileId") <> 1
ORDER BY agents_linked DESC, p.name;
