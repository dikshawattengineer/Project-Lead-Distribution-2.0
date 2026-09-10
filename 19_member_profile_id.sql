-- Retention: members are profiles. Company stays on the shared bag.
-- Run once in Supabase after 02 + 18. Safe to re-run.
-- Supplier policies come later — this file only Retention / Past Retention.

BEGIN;

ALTER TABLE public.crm_pool_member
  ADD COLUMN IF NOT EXISTS "profileId" text
    REFERENCES public.profiles(id);

UPDATE public.crm_pool_member m
SET "profileId" = p.id,
    "updatedAt" = CURRENT_TIMESTAMP
FROM public.profiles p
WHERE m."profileId" IS NULL
  AND p."primaryPoolId" = m."childPoolId";

CREATE UNIQUE INDEX IF NOT EXISTS crm_pool_member_policy_profile_uidx
  ON public.crm_pool_member ("policyId", "profileId")
  WHERE "profileId" IS NOT NULL;

-- So the shared bags list these people (UI roster), not only ld_agent_*
INSERT INTO public.crm_pool_profile ("poolId", "profileId")
SELECT pol."parentPoolId", m."profileId"
FROM public.crm_pool_member m
JOIN public.crm_pool_split_policy pol ON pol.id = m."policyId"
WHERE m."isActive" = true
  AND m."profileId" IS NOT NULL
  AND pol.id IN (
    'ld_policy_retention_nightly',
    'ld_policy_past_retention_nightly'
  )
ON CONFLICT ("poolId", "profileId") DO NOTHING;

CREATE TABLE IF NOT EXISTS public.ld_apply_profile_batch (
  company_id text NOT NULL,
  proposed_profile_id text
);

CREATE OR REPLACE FUNCTION public.ld_apply_profile_run()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  n integer := 0;
BEGIN
  IF to_regclass('public.ld_apply_profile_batch') IS NULL THEN
    RETURN 0;
  END IF;

  WITH changed AS (
    UPDATE public.companies c
    SET "profileId" = b.proposed_profile_id,
        "updatedAt" = NOW()
    FROM public.ld_apply_profile_batch b
    WHERE c.id = b.company_id
      AND c."profileId" IS DISTINCT FROM b.proposed_profile_id
    RETURNING c.id
  )
  SELECT COUNT(*) INTO n FROM changed;

  RETURN n;
END;
$$;

COMMIT;

-- Check
SELECT m."policyId", m."profileId", m."childPoolId", p.forename, p.surname
FROM public.crm_pool_member m
LEFT JOIN public.profiles p ON p.id = m."profileId"
WHERE m."policyId" LIKE 'ld_policy_%retention%'
ORDER BY m."policyId", p.forename, p.surname;

SELECT COUNT(*) AS members_missing_profile
FROM public.crm_pool_member
WHERE "isActive" = true
  AND "policyId" LIKE 'ld_policy_%retention%'
  AND "profileId" IS NULL;
