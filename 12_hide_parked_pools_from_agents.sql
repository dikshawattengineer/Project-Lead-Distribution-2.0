-- Trim the Pool filter while supplier routing is parked (run after 10 + 11).
-- Safe to re-run. Undo supplier links only: 12_revert_hide_parked_pools.sql
--
-- Why private bags vanished when hiding suppliers: CRM nests PRIVATE bags under STANDARD
-- parents in pool_links. Removing supplier pool_profiles hid the parent → child vanished too.
-- Fix: (1) turn off pool_links, (2) direct owner → own PRIVATE bag, (3) unlink STANDARD suppliers.
--
-- Unlinks from pool_profiles (pools stay in DB):
--   • Every pool from 09_sync_provider_pools.sql + 04 supplier bags + UNASSIGNED + UPSELLING
-- Keeps: RETENTION, PAST_RETENTION, each agent's own PRIVATE bag, COMPLAINT (managers).

BEGIN;

-- 1) Stop nesting PRIVATE under supplier STANDARD parents (same as 05).
UPDATE public.pool_links
SET "isActive" = false,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE CAST("childType" AS text) = 'PRIVATE'
  AND COALESCE("isActive", true) = true;

-- 2) Direct link: each profile → their own PRIVATE bag (primaryPoolId).
INSERT INTO public.pool_profiles ("poolId", "profileId")
SELECT pr."primaryPoolId", pr.id
FROM public.profiles pr
JOIN public.pools priv ON priv.id = pr."primaryPoolId"
WHERE priv.type = 'PRIVATE'
ON CONFLICT ("poolId", "profileId") DO NOTHING;

-- 3) Unlink all parked STANDARD / CAMPAIGN shared pools — never PRIVATE.
DELETE FROM public.pool_profiles pp
USING public.pools p
WHERE pp."poolId" = p.id
  AND p.type::text IN ('STANDARD', 'CAMPAIGN')
  AND COALESCE(p.code, '') NOT IN ('RETENTION', 'PAST_RETENTION', 'COMPLAINT');

-- Complaint: remove from non-managers only.
-- Adjust role column / values to match your profiles table if this fails.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'profiles'
      AND column_name = 'role'
  ) THEN
    DELETE FROM public.pool_profiles pp
    USING public.pools p,
          public.profiles pr
    WHERE pp."poolId" = p.id
      AND pr.id = pp."profileId"
      AND p.code = 'COMPLAINT'
      AND UPPER(COALESCE(pr.role::text, '')) NOT IN ('MANAGER', 'ADMIN');
  ELSIF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'profiles'
      AND column_name = 'isManager'
  ) THEN
    DELETE FROM public.pool_profiles pp
    USING public.pools p,
          public.profiles pr
    WHERE pp."poolId" = p.id
      AND pr.id = pp."profileId"
      AND p.code = 'COMPLAINT'
      AND COALESCE(pr."isManager", false) = false;
  ELSE
    RAISE NOTICE 'No profiles.role or profiles.isManager — add managers to COMPLAINT pool manually in CRM';
  END IF;
END $$;

COMMIT;

-- Private bags must still be linked (unchanged by this script):
SELECT p.code, p.name, COUNT(pp."profileId") AS agents_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.type = 'PRIVATE'
GROUP BY p.id, p.code, p.name
ORDER BY p.name;

-- Shared pools left in pool_profiles after trim:
SELECT p.code, p.name, p.type::text AS pool_type, COUNT(pp."profileId") AS profiles_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN')
   OR p.code = 'COMPLAINT'
GROUP BY p.id, p.code, p.name, p.type
ORDER BY p.type::text, p.code;
