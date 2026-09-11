-- Nightly-style janitor for new CRM. Run BEFORE the Databricks snapshot.
-- Does NOT zero every company pool (that would break sticky).
-- Leavers = profiles.disabled. TPS / blacklist later (no those tables here).

CREATE OR REPLACE FUNCTION public.ld_janitor_run()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  n integer := 0;
BEGIN
  UPDATE public.companies c
  SET "profileId" = NULL,
      "updatedAt" = NOW()
  FROM public.profiles p
  WHERE c."profileId" = p.id
    AND COALESCE(p.disabled, false) = true;

  GET DIAGNOSTICS n = ROW_COUNT;

  RETURN n;
END;
$$;

-- SELECT public.ld_janitor_run() AS companies_cleared;
