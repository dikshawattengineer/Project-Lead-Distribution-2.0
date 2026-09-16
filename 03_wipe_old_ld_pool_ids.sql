-- Run ONCE before 04 when replacing old ld_pool_* string ids with UUID pools.
-- Wipes LD rules pointing at old pools. Does not touch companies.poolId
-- (remap those separately if companies still point at ld_pool_*).

BEGIN;

DELETE FROM public.crm_pool_rule
WHERE "poolId" IN (SELECT id FROM public.pools WHERE id LIKE 'ld_pool_%');

DELETE FROM public.crm_provider_family
WHERE "poolId" IN (SELECT id FROM public.pools WHERE id LIKE 'ld_pool_%');

DELETE FROM public.pools
WHERE id LIKE 'ld_pool_%';

COMMIT;

SELECT COUNT(*) AS old_ld_pools_left
FROM public.pools
WHERE id LIKE 'ld_pool_%';
