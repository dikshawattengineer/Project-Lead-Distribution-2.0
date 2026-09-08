-- Apply Databricks staging public.ld_apply_batch to live companies.
-- Run in Supabase SQL editor AFTER the notebook writes the batch.
-- Does not build tags. Databricks already decided proposed_pool_id.

BEGIN;

UPDATE public.companies c
SET "poolId" = b.proposed_pool_id,
    "updatedAt" = NOW()
FROM public.ld_apply_batch b
WHERE c.id = b.company_id;

INSERT INTO public.crm_company_pool_audit
  (id, "companyId", "poolId", action, "actorId", "createdAt")
SELECT
  gen_random_uuid()::text,
  b.company_id,
  b.proposed_pool_id,
  'AUTO_ASSIGN',
  '3fc12748-605f-4d13-ae70-eec60b55d726',
  NOW()
FROM public.ld_apply_batch b;

COMMIT;

SELECT "poolId", COUNT(*)
FROM public.companies
GROUP BY "poolId"
ORDER BY 2 DESC;
