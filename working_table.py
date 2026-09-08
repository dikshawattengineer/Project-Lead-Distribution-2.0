# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Working table
# MAGIC
# MAGIC Snapshot CRM tables, then one SQL builds `ld_working`.
# MAGIC Tag order: **sticky first** (Nightly), then Retention, then supplier.
# MAGIC
# MAGIC **Does not write `companies.poolId`.**

# COMMAND ----------

# DBTITLE 1,Password
PG_HOST = "db.qkulzujhtjbjjmlohmcd.supabase.co"
PG_PORT = 5432
PG_DATABASE = "postgres"
PG_USER = "postgres"
PG_PASSWORD = ""  # paste here

USE_POOLER = True
PG_POOLER_HOST = "aws-1-eu-west-2.pooler.supabase.com"
PG_POOLER_PORT = 5432
PG_POOLER_USER = "postgres.qkulzujhtjbjjmlohmcd"

JDBC_HOST = PG_POOLER_HOST if USE_POOLER else PG_HOST
JDBC_USER = PG_POOLER_USER if USE_POOLER else PG_USER
JDBC_URL = f"jdbc:postgresql://{JDBC_HOST}:{PG_PORT}/{PG_DATABASE}"

def jdbc_table(pg_table: str):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", pg_table)
        .option("user", JDBC_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("sslmode", "require")
        .load()
    )

print("JDBC host =", JDBC_HOST)

# COMMAND ----------

# DBTITLE 1,Snapshot
from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS crm_load.new_crm")

for table in [
    "companies",
    "contracts",
    "providers",
    "company_sites",
    "callbacks",
    "crm_pool",
    "notes",
    "profiles",
    "crm_company_pool_audit",
]:
    df = jdbc_table(f"public.{table}")
    dest = f"crm_load.new_crm.snap_{table}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dest)
    print(table, df.count())

deal_companies_dest = "crm_load.new_crm.snap_deal_companies"
try:
    deals = jdbc_table("public.deals")
    deals.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("crm_load.new_crm.snap_deals")
    print("deals", deals.count(), "columns", deals.columns)
    cols = {c.lower(): c for c in deals.columns}
    if "companyid" in cols:
        deal_cos = (
            deals.select(F.col(cols["companyid"]).alias("company_id"))
            .where("company_id IS NOT NULL")
            .distinct()
        )
    elif "company_id" in cols:
        deal_cos = (
            deals.select(F.col(cols["company_id"]).alias("company_id"))
            .where("company_id IS NOT NULL")
            .distinct()
        )
    else:
        print("deals has no companyId yet — treating as empty")
        deal_cos = spark.createDataFrame([], "company_id string")
except Exception as e:
    print("public.deals not ready:", str(e)[:240])
    deal_cos = spark.createDataFrame([], "company_id string")

deal_cos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(deal_companies_dest)
print("deal companies", deal_cos.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working table (SQL)

# COMMAND ----------

# DBTITLE 1,ld_working
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE crm_load.new_crm.ld_working AS
# MAGIC WITH contracts_f AS (
# MAGIC   SELECT
# MAGIC     c.`companyId`                         AS company_id,
# MAGIC     c.`providerId`                        AS provider_id,
# MAGIC     p.`displayName`                       AS provider_name,
# MAGIC     c.`endDate`                           AS end_date,
# MAGIC     DATEDIFF(c.`endDate`, CURRENT_DATE)   AS raw_days_left,
# MAGIC     COALESCE(DATEDIFF(c.`endDate`, CURRENT_DATE), 0) AS days_left,
# MAGIC     UPPER(c.`utilityType`)                AS utility_type,
# MAGIC     CASE
# MAGIC       WHEN LOWER(p.`displayName`) LIKE '%british gas lite%' THEN 'OTHER'
# MAGIC       WHEN LOWER(p.`displayName`) LIKE '%british gas%'      THEN 'BG'
# MAGIC       WHEN LOWER(p.`displayName`) LIKE '%e.on%'             THEN 'EON'
# MAGIC       WHEN LOWER(p.`displayName`) LIKE '%e-on%'             THEN 'EON'
# MAGIC       WHEN LOWER(p.`displayName`) LIKE 'eon%'               THEN 'EON'
# MAGIC       WHEN LOWER(p.`displayName`) LIKE '%utility bidder%'   THEN 'UB'
# MAGIC       ELSE 'OTHER'
# MAGIC     END AS family
# MAGIC   FROM crm_load.new_crm.snap_contracts c
# MAGIC   LEFT JOIN crm_load.new_crm.snap_providers p
# MAGIC     ON c.`providerId` = p.id
# MAGIC ),
# MAGIC winning AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY company_id
# MAGIC         ORDER BY
# MAGIC           days_left ASC,
# MAGIC           CASE family WHEN 'EON' THEN 1 WHEN 'BG' THEN 2 WHEN 'UB' THEN 3 ELSE 4 END
# MAGIC       ) AS rn
# MAGIC     FROM contracts_f
# MAGIC   ) x
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC sites AS (
# MAGIC   SELECT `companyId` AS company_id, COUNT(*) AS site_count
# MAGIC   FROM crm_load.new_crm.snap_company_sites
# MAGIC   GROUP BY `companyId`
# MAGIC ),
# MAGIC callbacks AS (
# MAGIC   SELECT
# MAGIC     s.`companyId` AS company_id,
# MAGIC     FIRST(pr.`primaryPoolId`) AS callback_owner_pool_id
# MAGIC   FROM crm_load.new_crm.snap_callbacks cb
# MAGIC   JOIN crm_load.new_crm.snap_company_sites s
# MAGIC     ON cb.`companySiteId` = s.id
# MAGIC   LEFT JOIN crm_load.new_crm.snap_profiles pr
# MAGIC     ON cb.`createdById` = pr.`userId`
# MAGIC   WHERE cb.status = 'SCHEDULED'
# MAGIC   GROUP BY s.`companyId`
# MAGIC ),
# MAGIC complaint_notes AS (
# MAGIC   SELECT DISTINCT `companyId` AS company_id
# MAGIC   FROM crm_load.new_crm.snap_notes
# MAGIC   WHERE COALESCE(`isHidden`, false) = false
# MAGIC     AND (
# MAGIC       LOWER(CONCAT(COALESCE(title, ''), ' ', COALESCE(description, '')))
# MAGIC         LIKE '%ongoing complaint%'
# MAGIC       OR title LIKE '(Ongoing Complaint)%'
# MAGIC       OR description LIKE '(Ongoing Complaint)%'
# MAGIC     )
# MAGIC ),
# MAGIC complaint_xfer AS (
# MAGIC   SELECT DISTINCT `companyId` AS company_id
# MAGIC   FROM crm_load.new_crm.snap_crm_company_pool_audit
# MAGIC   WHERE `poolId` = 'ld_pool_complaint'
# MAGIC )
# MAGIC SELECT
# MAGIC   co.id                                            AS company_id,
# MAGIC   co.`poolId`                                      AS current_pool_id,
# MAGIC   CAST(NULL AS STRING)                             AS lead_tag,
# MAGIC   COALESCE(s.site_count, 0)                        AS site_count,
# MAGIC   w.provider_id                                    AS win_provider_id,
# MAGIC   w.provider_name                                  AS win_provider_name,
# MAGIC   w.family                                         AS win_family,
# MAGIC   w.end_date                                       AS win_end_date,
# MAGIC   w.raw_days_left,
# MAGIC   w.days_left,
# MAGIC   CASE WHEN d.company_id IS NOT NULL THEN true ELSE false END AS has_any_past_deal,
# MAGIC   CASE WHEN cb.company_id IS NOT NULL THEN true ELSE false END AS has_open_callback,
# MAGIC   cb.callback_owner_pool_id,
# MAGIC   COALESCE(pl.`isLocked`, false)                   AS is_current_pool_locked,
# MAGIC   CASE
# MAGIC     WHEN LOWER(COALESCE(pl.name, '')) LIKE '%gdpr%'
# MAGIC       OR LOWER(COALESCE(pl.code, '')) LIKE '%gdpr%'
# MAGIC     THEN true ELSE false
# MAGIC   END AS is_gdpr_pool,
# MAGIC   CASE WHEN cn.company_id IS NOT NULL THEN true ELSE false END AS has_complaint_note,
# MAGIC   CASE WHEN xf.company_id IS NOT NULL THEN true ELSE false END AS has_complaint_transfer,
# MAGIC   CAST(NULL AS STRING)                             AS proposed_pool_id,
# MAGIC   CURRENT_TIMESTAMP()                              AS snapshot_at
# MAGIC FROM crm_load.new_crm.snap_companies co
# MAGIC LEFT JOIN winning w   ON co.id = w.company_id
# MAGIC LEFT JOIN sites s     ON co.id = s.company_id
# MAGIC LEFT JOIN callbacks cb ON co.id = cb.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_deal_companies d ON co.id = d.company_id
# MAGIC LEFT JOIN complaint_notes cn ON co.id = cn.company_id
# MAGIC LEFT JOIN complaint_xfer xf ON co.id = xf.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_crm_pool pl ON co.`poolId` = pl.id
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS companies,
# MAGIC   SUM(CASE WHEN has_open_callback THEN 1 ELSE 0 END) AS open_callbacks,
# MAGIC   SUM(CASE WHEN has_complaint_note THEN 1 ELSE 0 END) AS complaint_notes,
# MAGIC   SUM(CASE WHEN has_complaint_transfer THEN 1 ELSE 0 END) AS complaint_transfers,
# MAGIC   SUM(CASE WHEN is_current_pool_locked OR is_gdpr_pool THEN 1 ELSE 0 END) AS locked_or_gdpr,
# MAGIC   SUM(CASE WHEN has_any_past_deal THEN 1 ELSE 0 END) AS with_past_deal
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Tag (sticky first)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ld_before_tag AS
# MAGIC SELECT * FROM crm_load.new_crm.ld_working
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,fill lead_tag
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE crm_load.new_crm.ld_working AS
# MAGIC SELECT
# MAGIC   company_id,
# MAGIC   current_pool_id,
# MAGIC   CASE
# MAGIC     WHEN has_complaint_note
# MAGIC       OR has_complaint_transfer
# MAGIC       OR current_pool_id = 'ld_pool_complaint'
# MAGIC       THEN 'COMPLAINT'
# MAGIC     WHEN has_open_callback THEN 'CALLBACK'
# MAGIC     WHEN is_current_pool_locked OR is_gdpr_pool THEN 'LOCKED'
# MAGIC     WHEN has_any_past_deal THEN
# MAGIC       CASE
# MAGIC         WHEN raw_days_left IS NULL OR days_left < 1 THEN 'PAST_RETENTION'
# MAGIC         WHEN days_left <= 365 THEN 'RETENTION'
# MAGIC         ELSE 'UPSELLING'
# MAGIC       END
# MAGIC     WHEN win_family IS NULL THEN 'UNASSIGNED'
# MAGIC     WHEN raw_days_left IS NULL OR days_left <= 0 THEN
# MAGIC       CASE win_family
# MAGIC         WHEN 'EON' THEN 'EON_NOW'
# MAGIC         WHEN 'BG'  THEN 'BG_NOW'
# MAGIC         WHEN 'UB'  THEN 'UB_NOW'
# MAGIC         ELSE 'OTHER_NOW'
# MAGIC       END
# MAGIC     WHEN days_left <= CASE WHEN win_family = 'BG' THEN 548 ELSE 365 END THEN
# MAGIC       CASE win_family
# MAGIC         WHEN 'EON' THEN 'EON_IN_WINDOW'
# MAGIC         WHEN 'BG'  THEN 'BG_IN_WINDOW'
# MAGIC         WHEN 'UB'  THEN 'UB_IN_WINDOW'
# MAGIC         ELSE 'OTHER_IN_WINDOW'
# MAGIC       END
# MAGIC     ELSE 'PRE_WINDOW'
# MAGIC   END AS lead_tag,
# MAGIC   site_count,
# MAGIC   win_provider_id,
# MAGIC   win_provider_name,
# MAGIC   win_family,
# MAGIC   win_end_date,
# MAGIC   raw_days_left,
# MAGIC   days_left,
# MAGIC   has_any_past_deal,
# MAGIC   has_open_callback,
# MAGIC   callback_owner_pool_id,
# MAGIC   is_current_pool_locked,
# MAGIC   is_gdpr_pool,
# MAGIC   has_complaint_note,
# MAGIC   has_complaint_transfer,
# MAGIC   (
# MAGIC     has_complaint_note
# MAGIC     OR has_complaint_transfer
# MAGIC     OR current_pool_id = 'ld_pool_complaint'
# MAGIC     OR has_open_callback
# MAGIC     OR is_current_pool_locked
# MAGIC     OR is_gdpr_pool
# MAGIC   ) AS is_protected,
# MAGIC   CAST(NULL AS STRING) AS proposed_pool_id,
# MAGIC   snapshot_at
# MAGIC FROM ld_before_tag
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lead_tag, is_protected, COUNT(*) AS companies
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC GROUP BY lead_tag, is_protected
# MAGIC ORDER BY companies DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Propose pool

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ld_before_pool AS
# MAGIC SELECT * FROM crm_load.new_crm.ld_working
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,fill proposed_pool_id
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE crm_load.new_crm.ld_working AS
# MAGIC SELECT
# MAGIC   company_id,
# MAGIC   current_pool_id,
# MAGIC   lead_tag,
# MAGIC   CASE
# MAGIC     WHEN lead_tag = 'COMPLAINT' THEN 'ld_pool_complaint'
# MAGIC     WHEN lead_tag = 'CALLBACK' THEN COALESCE(callback_owner_pool_id, current_pool_id)
# MAGIC     WHEN lead_tag = 'LOCKED' THEN current_pool_id
# MAGIC     WHEN is_protected THEN current_pool_id
# MAGIC     WHEN lead_tag = 'PAST_RETENTION' THEN 'ld_pool_retention_ooc'
# MAGIC     WHEN lead_tag = 'RETENTION' THEN 'ld_pool_retention'
# MAGIC     WHEN lead_tag = 'UPSELLING' THEN 'ld_pool_upselling'
# MAGIC     WHEN lead_tag IN ('EON_NOW', 'EON_IN_WINDOW') THEN 'ld_pool_eon'
# MAGIC     WHEN lead_tag IN ('BG_NOW', 'BG_IN_WINDOW') THEN 'ld_pool_bg'
# MAGIC     WHEN lead_tag IN ('UB_NOW', 'UB_IN_WINDOW') THEN 'ld_pool_ub'
# MAGIC     WHEN lead_tag IN ('OTHER_NOW', 'OTHER_IN_WINDOW') THEN 'ld_pool_other'
# MAGIC     WHEN lead_tag IN ('PRE_WINDOW', 'UNASSIGNED') THEN 'ld_pool_unassigned'
# MAGIC     ELSE 'ld_pool_unassigned'
# MAGIC   END AS proposed_pool_id,
# MAGIC   site_count,
# MAGIC   win_provider_id,
# MAGIC   win_provider_name,
# MAGIC   win_family,
# MAGIC   win_end_date,
# MAGIC   raw_days_left,
# MAGIC   days_left,
# MAGIC   has_any_past_deal,
# MAGIC   has_open_callback,
# MAGIC   callback_owner_pool_id,
# MAGIC   is_current_pool_locked,
# MAGIC   is_gdpr_pool,
# MAGIC   has_complaint_note,
# MAGIC   has_complaint_transfer,
# MAGIC   is_protected,
# MAGIC   snapshot_at
# MAGIC FROM ld_before_pool
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lead_tag, proposed_pool_id, COUNT(*) AS companies
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC GROUP BY lead_tag, proposed_pool_id
# MAGIC ORDER BY companies DESC
# MAGIC ;
