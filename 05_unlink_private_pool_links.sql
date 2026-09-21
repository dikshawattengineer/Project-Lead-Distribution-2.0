-- Turn off fair-share: unlink STANDARD/CAMPAIGN parents from PRIVATE agent bags.
-- Does not delete PRIVATE pools or move companies — next Databricks apply moves
-- non-sticky companies onto the shared parent from pool_rules.
-- Safe to re-run. Re-create links in CRM when fair-share is turned back on.

BEGIN;

-- Soft off (keeps history)
UPDATE public.pool_links
SET "isActive" = false,
    "updatedAt" = CURRENT_TIMESTAMP
WHERE CAST("childType" AS text) = 'PRIVATE';

-- Hard delete instead (uncomment if you prefer remove rows entirely):
-- DELETE FROM public.pool_links
-- WHERE CAST("childType" AS text) = 'PRIVATE';

COMMIT;

SELECT
  COUNT(*) FILTER (WHERE COALESCE("isActive", true) = true) AS active_private_links,
  COUNT(*) AS total_private_child_links
FROM public.pool_links
WHERE CAST("childType" AS text) = 'PRIVATE';
