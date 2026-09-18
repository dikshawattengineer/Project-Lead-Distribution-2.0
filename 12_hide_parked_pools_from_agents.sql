-- Remove sales-agent access to parked shared pools (Unassigned, supplier, Upselling, …).
-- Agents keep: PRIVATE pools + Retentions + Past Retentions.
-- Complaint: managers only (not stripped here — see second DELETE below).
-- Safe to re-run.

BEGIN;

-- Drop parked shared pools from all profiles (not Retention / Past Retention / Complaint).
DELETE FROM public.pool_profiles pp
USING public.pools p
WHERE pp."poolId" = p.id
  AND p.type::text <> 'PRIVATE'
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

SELECT p.code, p.name, p.type::text AS pool_type, COUNT(pp."profileId") AS profiles_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.type::text IN ('STANDARD', 'CAMPAIGN', 'PRIVATE')
   OR p.code = 'COMPLAINT'
GROUP BY p.id, p.code, p.name, p.type
ORDER BY p.type::text, p.code;
