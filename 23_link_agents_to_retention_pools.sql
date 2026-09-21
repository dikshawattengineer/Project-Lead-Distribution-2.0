-- Link agent profiles → shared Retentions + Past Retentions (pool_profiles).
-- LD seed (04) creates pools but does NOT wire CRM visibility — run this after
-- agents exist in public.profiles. Safe to re-run (ON CONFLICT DO NOTHING).
--
-- Pick ONE block below. Private bags are usually already linked by CRM (primaryPoolId).

-- ---------------------------------------------------------------------------
-- A) ONE agent — by email (testing)
-- ---------------------------------------------------------------------------
INSERT INTO public.pool_profiles ("poolId", "profileId")
SELECT p.id, pr.id
FROM public.profiles pr
JOIN public.pools p
  ON p.code IN ('RETENTION', 'PAST_RETENTION')
WHERE lower(pr.email) = lower('test2@watt.co.uk')
  AND COALESCE(pr.disabled, false) = false
ON CONFLICT ("poolId", "profileId") DO NOTHING;

-- ---------------------------------------------------------------------------
-- B) SOME agents — list emails (uncomment and edit; comment out A first)
-- ---------------------------------------------------------------------------
-- INSERT INTO public.pool_profiles ("poolId", "profileId")
-- SELECT p.id, pr.id
-- FROM public.profiles pr
-- JOIN public.pools p
--   ON p.code IN ('RETENTION', 'PAST_RETENTION')
-- WHERE lower(pr.email) IN (
--     lower('test2@watt.co.uk'),
--     lower('agent.two@watt.co.uk')
--   )
--   AND COALESCE(pr.disabled, false) = false
-- ON CONFLICT ("poolId", "profileId") DO NOTHING;

-- ---------------------------------------------------------------------------
-- C) ALL sales agents — non-manager, non-disabled (comment out A first)
--    profiles has roleId (not role) — join public.roles
-- ---------------------------------------------------------------------------
-- INSERT INTO public.pool_profiles ("poolId", "profileId")
-- SELECT p.id, pr.id
-- FROM public.profiles pr
-- JOIN public.pools p
--   ON p.code IN ('RETENTION', 'PAST_RETENTION')
-- LEFT JOIN public.roles r
--   ON r.id = pr."roleId"
-- WHERE COALESCE(pr.disabled, false) = false
--   AND UPPER(COALESCE(r.name, '')) NOT IN ('MANAGER', 'ADMIN')
-- ON CONFLICT ("poolId", "profileId") DO NOTHING;

-- Alternative C — only Energy Consultant (edit role name to match public.roles):
-- INSERT INTO public.pool_profiles ("poolId", "profileId")
-- SELECT p.id, pr.id
-- FROM public.profiles pr
-- JOIN public.pools p
--   ON p.code IN ('RETENTION', 'PAST_RETENTION')
-- LEFT JOIN public.roles r
--   ON r.id = pr."roleId"
-- WHERE COALESCE(pr.disabled, false) = false
--   AND UPPER(COALESCE(r.name, '')) = 'ENERGY CONSULTANT'
-- ON CONFLICT ("poolId", "profileId") DO NOTHING;

-- Discover role names if the join fails:
-- SELECT r.id, r.name, COUNT(pr.id) AS profiles
-- FROM public.roles r
-- LEFT JOIN public.profiles pr ON pr."roleId" = r.id
-- GROUP BY r.id, r.name
-- ORDER BY profiles DESC;

-- ---------------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------------
SELECT p.code, COUNT(pp."profileId") AS profiles_linked
FROM public.pools p
LEFT JOIN public.pool_profiles pp ON pp."poolId" = p.id
WHERE p.code IN ('RETENTION', 'PAST_RETENTION')
GROUP BY p.code
ORDER BY p.code;

SELECT pr.email, p.code, p.name
FROM public.pool_profiles pp
JOIN public.profiles pr ON pr.id = pp."profileId"
JOIN public.pools p ON p.id = pp."poolId"
WHERE p.code IN ('RETENTION', 'PAST_RETENTION')
ORDER BY pr.email, p.code;
