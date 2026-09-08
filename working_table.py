# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Working table
# MAGIC
# MAGIC Snapshot CRM tables, then one SQL builds `ld_working`.
# MAGIC Tag order: **sticky first** (Nightly), then Retention, then supplier.
# MAGIC
# MAGIC `*_NOW` = that supplier's expired / no CED / DFV → that supplier's DFV pool only.
# MAGIC E.ON DFV is **not** a dump bag for every missing date.
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

# Stamp DFV on the winning-contract type column (Nightly 1/2/4 or deemed/flexible/variable).
contracts = spark.table("crm_load.new_crm.snap_contracts")
col_by_lower = {c.lower(): c for c in contracts.columns}
type_col = None
for name in ("type", "contracttype", "contract_type", "tarifftype", "tariff", "producttype"):
    if name in col_by_lower:
        type_col = col_by_lower[name]
        break

if type_col:
    raw = F.lower(F.trim(F.coalesce(F.col(type_col).cast("string"), F.lit(""))))
    is_dfv = (
        raw.isin("1", "2", "4", "deemed", "flexible", "variable", "d", "f", "v", "dfv", "fvd")
        | raw.contains("deemed")
        | raw.contains("flexible")
        | raw.contains("variable")
    )
    contracts = (
        contracts
        .withColumn("ld_contract_type", F.col(type_col).cast("string"))
        .withColumn("ld_is_dfv", is_dfv)
    )
    print("DFV type column:", type_col)
    contracts.groupBy("ld_contract_type", "ld_is_dfv").count().show(50, False)
else:
    contracts = (
        contracts
        .withColumn("ld_contract_type", F.lit(None).cast("string"))
        .withColumn("ld_is_dfv", F.lit(False))
    )
    print("No contract type column — DFV only from expired / no CED on that supplier")

contracts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "crm_load.new_crm.snap_contracts"
)

# Deals may be empty until migration.
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
# MAGIC
# MAGIC `is_win_dfv` is the **winning** contract only (Nightly supplier filter).
# MAGIC No supplier → Unassigned, never E.ON DFV.

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
# MAGIC     c.`ld_contract_type`                  AS contract_type,
# MAGIC     COALESCE(c.`ld_is_dfv`, false)        AS is_dfv,
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
# MAGIC ),
# MAGIC last_deals AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       d.`companyId` AS company_id,
# MAGIC       DATEDIFF(ct.`endDate`, CURRENT_DATE) AS last_deal_raw_days_left,
# MAGIC       COALESCE(DATEDIFF(ct.`endDate`, CURRENT_DATE), 0) AS last_deal_days_left,
# MAGIC       DATEDIFF(CURRENT_DATE, COALESCE(d.`signedAt`, d.`createdAt`)) AS last_deal_days_since,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY d.`companyId`
# MAGIC         ORDER BY COALESCE(d.`signedAt`, d.`createdAt`) DESC
# MAGIC       ) AS rn
# MAGIC     FROM crm_load.new_crm.snap_deals d
# MAGIC     LEFT JOIN crm_load.new_crm.snap_contracts ct
# MAGIC       ON d.`contractId` = ct.id
# MAGIC   ) x
# MAGIC   WHERE rn = 1
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
# MAGIC   w.contract_type                                  AS win_contract_type,
# MAGIC   COALESCE(w.is_dfv, false)                        AS is_win_dfv,
# MAGIC   w.raw_days_left,
# MAGIC   w.days_left,
# MAGIC   ld.last_deal_raw_days_left,
# MAGIC   ld.last_deal_days_left,
# MAGIC   ld.last_deal_days_since,
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
# MAGIC LEFT JOIN last_deals ld ON co.id = ld.company_id
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
# MAGIC   SUM(CASE WHEN has_any_past_deal THEN 1 ELSE 0 END) AS with_past_deal,
# MAGIC   SUM(CASE WHEN is_win_dfv THEN 1 ELSE 0 END) AS win_dfv
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Tag (sticky first)
# MAGIC
# MAGIC `*_NOW` = winning supplier is DFV **or** expired **or** no CED.
# MAGIC `win_family IS NULL` stays `UNASSIGNED` — not E.ON DFV.

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
# MAGIC         WHEN last_deal_raw_days_left IS NULL OR last_deal_days_left < 1 THEN 'PAST_RETENTION'
# MAGIC         WHEN last_deal_days_left <= 540 THEN 'RETENTION'
# MAGIC         ELSE 'UPSELLING'
# MAGIC       END
# MAGIC     WHEN win_family IS NULL THEN 'UNASSIGNED'
# MAGIC     WHEN is_win_dfv OR raw_days_left IS NULL OR days_left <= 0 THEN
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
# MAGIC   win_contract_type,
# MAGIC   is_win_dfv,
# MAGIC   raw_days_left,
# MAGIC   days_left,
# MAGIC   last_deal_raw_days_left,
# MAGIC   last_deal_days_left,
# MAGIC   last_deal_days_since,
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
# MAGIC SELECT lead_tag, is_win_dfv, is_protected, COUNT(*) AS companies
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC GROUP BY lead_tag, is_win_dfv, is_protected
# MAGIC ORDER BY companies DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Propose pool
# MAGIC
# MAGIC `EON_NOW` → `ld_pool_eon_dfv` (E.ON DFV). In-window stays `ld_pool_eon`.
# MAGIC Same split for BG / UB / Other. No supplier → Unassigned.

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
# MAGIC     WHEN lead_tag = 'EON_NOW' THEN 'ld_pool_eon_dfv'
# MAGIC     WHEN lead_tag = 'BG_NOW' THEN 'ld_pool_bg_dfv'
# MAGIC     WHEN lead_tag = 'UB_NOW' THEN 'ld_pool_ub_dfv'
# MAGIC     WHEN lead_tag = 'OTHER_NOW' THEN 'ld_pool_other_dfv'
# MAGIC     WHEN lead_tag = 'EON_IN_WINDOW' THEN 'ld_pool_eon'
# MAGIC     WHEN lead_tag = 'BG_IN_WINDOW' THEN 'ld_pool_bg'
# MAGIC     WHEN lead_tag = 'UB_IN_WINDOW' THEN 'ld_pool_ub'
# MAGIC     WHEN lead_tag = 'OTHER_IN_WINDOW' THEN 'ld_pool_other'
# MAGIC     WHEN lead_tag IN ('PRE_WINDOW', 'UNASSIGNED') THEN 'ld_pool_unassigned'
# MAGIC     ELSE 'ld_pool_unassigned'
# MAGIC   END AS proposed_pool_id,
# MAGIC   site_count,
# MAGIC   win_provider_id,
# MAGIC   win_provider_name,
# MAGIC   win_family,
# MAGIC   win_end_date,
# MAGIC   win_contract_type,
# MAGIC   is_win_dfv,
# MAGIC   raw_days_left,
# MAGIC   days_left,
# MAGIC   last_deal_raw_days_left,
# MAGIC   last_deal_days_left,
# MAGIC   last_deal_days_since,
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
# MAGIC SELECT lead_tag, proposed_pool_id, win_family, is_win_dfv, COUNT(*) AS companies
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC GROUP BY lead_tag, proposed_pool_id, win_family, is_win_dfv
# MAGIC ORDER BY companies DESC
# MAGIC ;
