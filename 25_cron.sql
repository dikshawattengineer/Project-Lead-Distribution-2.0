-- Shared-pool apply on a timer. Janitor + Databricks must run first.
-- Needs pg_cron (Supabase: enable in Dashboard → Extensions if this errors).

CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
  'ld_apply_shared_pools',
  '30 2 * * *',
  $cmd$SELECT public.ld_apply_batch_run();$cmd$
);

-- Manual tonight:
--   SELECT public.ld_janitor_run();
--   (Databricks: Password → Janitor → Snapshot → tag → write shared)
--   SELECT public.ld_apply_batch_run();
