-- Only E.ON has a DFV pool. BG / Other / UB expired → normal supplier pool.
-- Safe to re-run. Remaps companies already left on the old * _dfv bags.

BEGIN;

INSERT INTO public.crm_pool (id, code, name, "isLocked", "createdAt", "updatedAt")
VALUES
  ('ld_pool_eon_dfv', 'EON_DFV', 'E.ON DFV', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO UPDATE
SET
  code = EXCLUDED.code,
  name = EXCLUDED.name,
  "isLocked" = EXCLUDED."isLocked",
  "updatedAt" = CURRENT_TIMESTAMP;

INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
VALUES
  ('ld_rule_eon_now',   29, 'EON_NOW',   'ld_pool_eon_dfv', true, 'E.ON expired / no CED / DFV', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_rule_bg_now',    39, 'BG_NOW',    'ld_pool_bg',      true, 'BG expired / no CED → British Gas', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_rule_ub_now',    20, 'UB_NOW',    'ld_pool_ub',      true, 'UB expired / no CED → Utility Bidder', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_rule_other_now', 49, 'OTHER_NOW', 'ld_pool_other',   true, 'Other expired / no CED → Other', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO UPDATE
SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = EXCLUDED."isActive",
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

UPDATE public.companies
SET "poolId" = 'ld_pool_bg', "updatedAt" = NOW()
WHERE "poolId" = 'ld_pool_bg_dfv';

UPDATE public.companies
SET "poolId" = 'ld_pool_ub', "updatedAt" = NOW()
WHERE "poolId" = 'ld_pool_ub_dfv';

UPDATE public.companies
SET "poolId" = 'ld_pool_other', "updatedAt" = NOW()
WHERE "poolId" = 'ld_pool_other_dfv';

COMMIT;
