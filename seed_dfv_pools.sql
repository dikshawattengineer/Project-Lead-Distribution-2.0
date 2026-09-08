-- Per-supplier DFV / zero-day pools (Nightly: DFV OR expired OR no CED, still filtered by supplier).
-- E.ON DFV is only for E.ON. Safe to re-run. Does not touch companies.poolId.

BEGIN;

INSERT INTO public.crm_pool (id, code, name, "isLocked", "createdAt", "updatedAt")
VALUES
  ('ld_pool_eon_dfv',   'EON_DFV',   'E.ON DFV',          false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_bg_dfv',    'BG_DFV',    'British Gas DFV',   false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_ub_dfv',    'UB_DFV',    'Utility Bidder DFV',false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_pool_other_dfv', 'OTHER_DFV', 'Other DFV',         false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO UPDATE
SET
  code = EXCLUDED.code,
  name = EXCLUDED.name,
  "isLocked" = EXCLUDED."isLocked",
  "updatedAt" = CURRENT_TIMESTAMP;

INSERT INTO public.crm_pool_rule
  (id, priority, tag, "poolId", "isActive", description, "splitEnabled", "createdAt", "updatedAt")
VALUES
  ('ld_rule_eon_now',   29, 'EON_NOW',   'ld_pool_eon_dfv',   true, 'E.ON expired / no CED / DFV', false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_rule_bg_now',    39, 'BG_NOW',    'ld_pool_bg_dfv',    true, 'BG expired / no CED / DFV',   false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_rule_ub_now',    20, 'UB_NOW',    'ld_pool_ub_dfv',    true, 'UB expired / no CED / DFV',   false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
  ('ld_rule_other_now', 49, 'OTHER_NOW', 'ld_pool_other_dfv', true, 'Other expired / no CED / DFV',false, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO UPDATE
SET
  priority = EXCLUDED.priority,
  tag = EXCLUDED.tag,
  "poolId" = EXCLUDED."poolId",
  "isActive" = EXCLUDED."isActive",
  description = EXCLUDED.description,
  "updatedAt" = CURRENT_TIMESTAMP;

COMMIT;
