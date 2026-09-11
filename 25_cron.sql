-- Nightly cron is a Databricks Job on working_table.py (top to bottom):
--   Password → Janitor → Snapshot → tag → write ld_apply_batch → ld_apply_batch_run()
-- Do NOT schedule this pg_cron if the Job already applies — same batch would run twice.
--
-- Optional fallback only (no Databricks Job): enable pg_cron, then uncomment below.
-- Needs the apply function from apply_ld_apply_batch.sql first.

-- CREATE EXTENSION IF NOT EXISTS pg_cron;
-- SELECT cron.schedule(
--   'ld_apply_shared_pools',
--   '30 2 * * *',
--   $cmd$SELECT public.ld_apply_batch_run();$cmd$
-- );
--
-- To drop an old apply-only job:
--   SELECT cron.unschedule('ld_apply_shared_pools');

SELECT 'cron = Databricks Job on this notebook (janitor + apply inside)' AS note;
