-- Run ONCE before 04 when replacing old ld_pool_* string ids with UUID pools.
-- Wipes LD rules pointing at old pools. Does not touch companies.poolId
-- (remap those separately if companies still point at ld_pool_*).

BEGIN;

DELETE FROM public.crm_pool_rule
WHERE "poolId" IN (SELECT id FROM public.pools WHERE id LIKE 'ld_pool_%');

-- poolId on crm_provider_family only exists after 09 schema — clear whole table if missing
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'crm_provider_family'
      AND column_name = 'poolId'
  ) THEN
    DELETE FROM public.crm_provider_family
    WHERE "poolId" IN (SELECT id FROM public.pools WHERE id LIKE 'ld_pool_%');
  ELSE
    DELETE FROM public.crm_provider_family;
  END IF;
END $$;

DELETE FROM public.pools
WHERE id LIKE 'ld_pool_%';

COMMIT;

SELECT COUNT(*) AS old_ld_pools_left
FROM public.pools
WHERE id LIKE 'ld_pool_%';
