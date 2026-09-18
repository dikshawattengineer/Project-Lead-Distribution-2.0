-- One-time: align crm_pool_rule with Prisma (id = uuid, tag = stable upsert key).
-- Run AFTER boss Prisma migrate. Safe to re-run (tag index + id default only).
-- Databricks unchanged — joins on tag, not id.

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS crm_pool_rule_tag_uidx
  ON public.crm_pool_rule (tag);

-- Convert legacy text PK (ld_rule_*) → uuid if Prisma has not already done it.
DO $$
DECLARE
  id_type text;
  pkey_name text;
BEGIN
  SELECT c.data_type INTO id_type
  FROM information_schema.columns c
  WHERE c.table_schema = 'public'
    AND c.table_name = 'crm_pool_rule'
    AND c.column_name = 'id';

  IF id_type IN ('text', 'character varying') THEN
    ALTER TABLE public.crm_pool_rule
      ADD COLUMN IF NOT EXISTS id_new uuid DEFAULT gen_random_uuid();

    UPDATE public.crm_pool_rule
    SET id_new = gen_random_uuid()
    WHERE id_new IS NULL;

    SELECT con.conname INTO pkey_name
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE nsp.nspname = 'public'
      AND rel.relname = 'crm_pool_rule'
      AND con.contype = 'p';

    IF pkey_name IS NOT NULL THEN
      EXECUTE format('ALTER TABLE public.crm_pool_rule DROP CONSTRAINT %I', pkey_name);
    END IF;

    ALTER TABLE public.crm_pool_rule DROP COLUMN id;
    ALTER TABLE public.crm_pool_rule RENAME COLUMN id_new TO id;
    ALTER TABLE public.crm_pool_rule ALTER COLUMN id SET NOT NULL;
    ALTER TABLE public.crm_pool_rule ADD PRIMARY KEY (id);
    ALTER TABLE public.crm_pool_rule ALTER COLUMN id SET DEFAULT gen_random_uuid();
  END IF;
END $$;

COMMIT;

SELECT id, tag, priority, "isActive"
FROM public.crm_pool_rule
WHERE tag IN ('RETENTION', 'PAST_RETENTION', 'COMPLAINT')
ORDER BY priority;
