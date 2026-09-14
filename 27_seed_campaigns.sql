-- Optional one-off check. Nightly Databricks already calls ld_seed_campaigns()
-- (created in apply_ld_apply_batch.sql) from parent pools. Do not run this
-- every day.

SELECT public.ld_seed_campaigns() AS campaigns_upserted;

SELECT id, name
FROM public.campaigns
ORDER BY name;
