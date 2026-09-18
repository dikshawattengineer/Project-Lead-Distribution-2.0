-- Undo 12_hide_parked_pools_from_agents.sql (pool_profiles only).
-- Run if you ran 12 by mistake. Watt CRM does not need 12 while parked.
-- Re-links supplier / Unassigned shared pools in pool_profiles for agents with a PRIVATE bag.
-- Does NOT move companies (11 unchanged). Does NOT turn supplier routing back on (10 unchanged).
-- Safe to re-run.

BEGIN;

WITH agent_profile AS (
  SELECT DISTINCT pp."profileId" AS profile_id
  FROM public.pool_profiles pp
  JOIN public.pools p ON p.id = pp."poolId"
  WHERE p.type = 'PRIVATE'
  UNION
  SELECT pr.id
  FROM public.profiles pr
  JOIN public.pools p ON p.id = pr."primaryPoolId"
  WHERE p.type = 'PRIVATE'
)
INSERT INTO public.pool_profiles ("poolId", "profileId")
SELECT p.id, a.profile_id
FROM agent_profile a
CROSS JOIN public.pools p
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN')
  AND COALESCE(p.code, '') <> 'COMPLAINT'
ON CONFLICT ("poolId", "profileId") DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'profiles'
      AND column_name = 'role'
  ) THEN
    INSERT INTO public.pool_profiles ("poolId", "profileId")
    SELECT p.id, pr.id
    FROM public.profiles pr
    CROSS JOIN public.pools p
    WHERE p.code = 'COMPLAINT'
      AND UPPER(COALESCE(pr.role::text, '')) IN ('MANAGER', 'ADMIN')
    ON CONFLICT ("poolId", "profileId") DO NOTHING;
  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'profiles'
      AND column_name = 'isManager'
  ) THEN
    INSERT INTO public.pool_profiles ("poolId", "profileId")
    SELECT p.id, pr.id
    FROM public.profiles pr
    CROSS JOIN public.pools p
    WHERE p.code = 'COMPLAINT'
      AND COALESCE(pr."isManager", false) = true
    ON CONFLICT ("poolId", "profileId") DO NOTHING;
  END IF;
END $$;

COMMIT;

SELECT p.code, p.name, p.type::text AS pool_type, COUNT(pp."profileId") AS profiles_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN', 'PRIVATE')
   OR p.code = 'COMPLAINT'
GROUP BY p.id, p.code, p.name, p.type
ORDER BY p.type::text, p.code;
