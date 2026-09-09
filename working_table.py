# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Working table
# MAGIC
# MAGIC Snapshot CRM tables, then one SQL builds `ld_working`.
# MAGIC Tag order: **sticky first** (Nightly), then Retention, then supplier.
# MAGIC
# MAGIC Only **E.ON** has a DFV pool (`ld_pool_eon_dfv`): E.ON deemed/flexible/variable
# MAGIC **or** expired **or** no CED. BG / Other / UB expired stay on the normal supplier pool.
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
    "site_meters",
    "callbacks",
    "crm_pool",
    "crm_pool_rule",
    "notes",
    "profiles",
    "crm_company_pool_audit",
]:
    df = jdbc_table(f"public.{table}")
    dest = f"crm_load.new_crm.snap_{table}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dest)
    print(table, df.count())

try:
    sale = jdbc_table("public.crm_company_load_sale")
    sale.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.snap_crm_company_load_sale"
    )
    print("crm_company_load_sale", sale.count())
except Exception as e:
    spark.createDataFrame(
        [], "companyId string, lastDealEndDate date, hasPastSale boolean"
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.snap_crm_company_load_sale"
    )
    print("crm_company_load_sale skip", str(e)[:160])

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

# Last deal (Prisma): deals.siteMeterId → site_meters → company_sites → company.
# Clock: that deal's contracts.endDate − today. Dead meters / cancelled still off.

def _col(df, *names):
    mapping = {c.lower(): c for c in df.columns}
    for name in names:
        if name.lower() in mapping:
            return mapping[name.lower()]
    return None

last_deals_schema = (
    "company_id string, last_deal_raw_days_left int, "
    "last_deal_days_left int, last_deal_days_since int"
)
empty_last = spark.createDataFrame([], last_deals_schema)
empty_cos = spark.createDataFrame([], "company_id string")

deal_companies_dest = "crm_load.new_crm.snap_deal_companies"
last_deals_dest = "crm_load.new_crm.ld_last_deals"
try:
    deals = jdbc_table("public.deals")
    deals.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.snap_deals"
    )
    print("deals", deals.count(), "columns", deals.columns)

    sites = spark.table("crm_load.new_crm.snap_company_sites")
    site_meters = spark.table("crm_load.new_crm.snap_site_meters")
    contracts_s = spark.table("crm_load.new_crm.snap_contracts")
    d_company = _col(deals, "companyId")
    d_site = _col(deals, "siteId")
    d_meter = _col(deals, "siteMeterId")
    d_contract = _col(deals, "contractId")
    d_signed = _col(deals, "signedAt")
    d_created = _col(deals, "createdAt")
    s_id = _col(sites, "id")
    s_co = _col(sites, "companyId")
    m_id = _col(site_meters, "id")
    m_site = _col(site_meters, "companySiteId")
    c_id = _col(contracts_s, "id")
    c_end = _col(contracts_s, "endDate")

    def _deal_col(name):
        return F.col(f"d.{name}") if name else F.lit(None)

    ts_cols = [_deal_col(c) for c in (d_signed, d_created) if c]
    deal_ts = F.coalesce(*ts_cols) if ts_cols else F.lit(None).cast("timestamp")
    contract_expr = _deal_col(d_contract)

    linked = None
    path = "none"
    if d_meter and m_id and m_site and s_id and s_co:
        linked = (
            deals.alias("d")
            .join(site_meters.alias("m"), F.col(f"d.{d_meter}") == F.col(f"m.{m_id}"), "inner")
            .join(sites.alias("s"), F.col(f"m.{m_site}") == F.col(f"s.{s_id}"), "inner")
            .select(
                F.col(f"s.{s_co}").alias("company_id"),
                contract_expr.alias("contract_id"),
                deal_ts.alias("deal_ts"),
            )
        )
        path = "company-site-meter-deal"
    if linked is None and d_site and s_id and s_co:
        linked = (
            deals.alias("d")
            .join(sites.alias("s"), F.col(f"d.{d_site}") == F.col(f"s.{s_id}"), "inner")
            .select(
                F.col(f"s.{s_co}").alias("company_id"),
                contract_expr.alias("contract_id"),
                deal_ts.alias("deal_ts"),
            )
        )
        path = "company-site-deal"
    if linked is None and d_company:
        linked = deals.alias("d").select(
            _deal_col(d_company).alias("company_id"),
            contract_expr.alias("contract_id"),
            deal_ts.alias("deal_ts"),
        )
        path = "company-deal"

    print("last-deal path:", path)

    if linked is None:
        deal_cos = empty_cos
        last_deal_df = empty_last
    else:
        if c_id and c_end:
            linked = linked.join(
                contracts_s.select(
                    F.col(c_id).alias("_cid"),
                    F.col(c_end).alias("end_date"),
                ),
                F.col("contract_id") == F.col("_cid"),
                "left",
            )
        else:
            linked = linked.withColumn("end_date", F.lit(None).cast("date"))

        from pyspark.sql.window import Window

        deal_cos = (
            linked.where("company_id IS NOT NULL")
            .select("company_id")
            .distinct()
        )
        w = Window.partitionBy("company_id").orderBy(F.col("deal_ts").desc_nulls_last())
        last_deal_df = (
            linked.where("company_id IS NOT NULL")
            .withColumn("rn", F.row_number().over(w))
            .where("rn = 1")
            .select(
                "company_id",
                F.datediff(F.col("end_date"), F.current_date()).alias("last_deal_raw_days_left"),
                F.coalesce(F.datediff(F.col("end_date"), F.current_date()), F.lit(0)).alias(
                    "last_deal_days_left"
                ),
                F.datediff(F.current_date(), F.col("deal_ts")).alias("last_deal_days_since"),
            )
        )
except Exception as e:
    print("public.deals not ready:", str(e)[:240])
    deal_cos = empty_cos
    last_deal_df = empty_last

deal_cos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(deal_companies_dest)
last_deal_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(last_deals_dest)
print("deal companies", deal_cos.count())
print("last deals", last_deal_df.count())

# Nightly: hasRejectedDeal blocks Retention only (supplier bags still allowed).
# isExclusivelyDeEnergised blocks supplier bags only (segment 0 / Unassigned).
rejected_dest = "crm_load.new_crm.snap_rejected_deal_companies"
dead_dest = "crm_load.new_crm.snap_exclusively_deenergised"
rejected = empty_cos

try:
    deals_s = spark.table("crm_load.new_crm.snap_deals")
    d_co = _col(deals_s, "companyId")
    d_id = _col(deals_s, "id")
    d_st = _col(deals_s, "status")
    parts = []
    reject_re = r"reject|declin|cancel"
    if d_co and d_st:
        parts.append(
            deals_s.where(F.lower(F.col(d_st).cast("string")).rlike(reject_re)).select(
                F.col(d_co).alias("company_id")
            )
        )
    try:
        cre = jdbc_table("public.compliance_review_entries")
        cre.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            "crm_load.new_crm.snap_compliance_review_entries"
        )
        print("compliance_review_entries", cre.count())
        c_deal = _col(cre, "dealId")
        c_st = _col(cre, "status")
        if c_deal and c_st and d_id and d_co:
            parts.append(
                cre.alias("cre")
                .where(F.lower(F.col(f"cre.{c_st}").cast("string")).rlike(reject_re))
                .join(
                    deals_s.alias("dd"),
                    F.col(f"cre.{c_deal}") == F.col(f"dd.{d_id}"),
                    "inner",
                )
                .select(F.col(f"dd.{d_co}").alias("company_id"))
            )
    except Exception as e:
        print("compliance_review_entries not ready:", str(e)[:200])
    if parts:
        rejected = parts[0]
        for extra in parts[1:]:
            rejected = rejected.unionByName(extra)
        rejected = rejected.where("company_id IS NOT NULL").distinct()
except Exception as e:
    print("rejected deals skip:", str(e)[:200])

rejected.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(rejected_dest)
print("rejected deal companies", rejected.count())

meters = spark.table("crm_load.new_crm.snap_site_meters")
sites_s = spark.table("crm_load.new_crm.snap_company_sites")
m_site = _col(meters, "companySiteId")
m_dead = _col(meters, "isDeEnergised")
s_id = _col(sites_s, "id")
s_co = _col(sites_s, "companyId")
if m_site and m_dead and s_id and s_co:
    dead = (
        meters.alias("m")
        .join(sites_s.alias("s"), F.col(f"m.{m_site}") == F.col(f"s.{s_id}"), "inner")
        .groupBy(F.col(f"s.{s_co}").alias("company_id"))
        .agg(
            F.count("*").alias("meters"),
            F.sum(F.when(F.col(f"m.{m_dead}") == True, 0).otherwise(1)).alias("live_meters"),
        )
        .where("meters > 0 AND live_meters = 0")
        .select("company_id")
    )
else:
    dead = empty_cos
    print("isDeEnergised not on site_meters — skip")
dead.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dead_dest)
print("exclusively de-energised companies", dead.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Working table (SQL)
# MAGIC
# MAGIC `is_win_dfv` is the **winning** contract only (Nightly supplier filter).
# MAGIC No supplier → Unassigned, never E.ON DFV.
# MAGIC Last deal: Prisma `deals.siteMeterId` → `site_meters` → `company_sites`.
# MAGIC Days left is still that deal's `contracts.endDate` − today.

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
# MAGIC     CAST(c.`type` AS STRING)              AS contract_type,
# MAGIC     CASE
# MAGIC       WHEN LOWER(TRIM(COALESCE(CAST(c.`type` AS STRING), ''))) IN (
# MAGIC         '1', '2', '4', 'deemed', 'flexible', 'variable', 'd', 'f', 'v', 'dfv', 'fvd'
# MAGIC       )
# MAGIC         OR LOWER(CAST(c.`type` AS STRING)) LIKE '%deemed%'
# MAGIC         OR LOWER(CAST(c.`type` AS STRING)) LIKE '%flexible%'
# MAGIC         OR LOWER(CAST(c.`type` AS STRING)) LIKE '%variable%'
# MAGIC       THEN true ELSE false
# MAGIC     END AS is_dfv,
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
# MAGIC   SELECT
# MAGIC     company_id,
# MAGIC     last_deal_raw_days_left,
# MAGIC     last_deal_days_left,
# MAGIC     last_deal_days_since
# MAGIC   FROM crm_load.new_crm.ld_last_deals
# MAGIC ),
# MAGIC load_sale AS (
# MAGIC   SELECT
# MAGIC     `companyId` AS company_id,
# MAGIC     `lastDealEndDate` AS last_deal_end_date,
# MAGIC     COALESCE(`hasPastSale`, true) AS has_past_sale
# MAGIC   FROM crm_load.new_crm.snap_crm_company_load_sale
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
# MAGIC   COALESCE(
# MAGIC     ld.last_deal_raw_days_left,
# MAGIC     DATEDIFF(ls.last_deal_end_date, CURRENT_DATE)
# MAGIC   ) AS last_deal_raw_days_left,
# MAGIC   COALESCE(
# MAGIC     ld.last_deal_days_left,
# MAGIC     COALESCE(DATEDIFF(ls.last_deal_end_date, CURRENT_DATE), 0)
# MAGIC   ) AS last_deal_days_left,
# MAGIC   ld.last_deal_days_since,
# MAGIC   CASE
# MAGIC     WHEN d.company_id IS NOT NULL THEN true
# MAGIC     WHEN COALESCE(ls.has_past_sale, false) THEN true
# MAGIC     ELSE false
# MAGIC   END AS has_any_past_deal,
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
# MAGIC   CASE WHEN rd.company_id IS NOT NULL THEN true ELSE false END AS has_rejected_deal,
# MAGIC   CASE WHEN de.company_id IS NOT NULL THEN true ELSE false END AS is_exclusively_deenergised,
# MAGIC   CAST(NULL AS STRING)                             AS proposed_pool_id,
# MAGIC   CURRENT_TIMESTAMP()                              AS snapshot_at
# MAGIC FROM crm_load.new_crm.snap_companies co
# MAGIC LEFT JOIN winning w   ON co.id = w.company_id
# MAGIC LEFT JOIN sites s     ON co.id = s.company_id
# MAGIC LEFT JOIN callbacks cb ON co.id = cb.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_deal_companies d ON co.id = d.company_id
# MAGIC LEFT JOIN last_deals ld ON co.id = ld.company_id
# MAGIC LEFT JOIN load_sale ls ON co.id = ls.company_id
# MAGIC LEFT JOIN complaint_notes cn ON co.id = cn.company_id
# MAGIC LEFT JOIN complaint_xfer xf ON co.id = xf.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_rejected_deal_companies rd ON co.id = rd.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_exclusively_deenergised de ON co.id = de.company_id
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
# MAGIC   SUM(CASE WHEN is_win_dfv THEN 1 ELSE 0 END) AS win_dfv,
# MAGIC   SUM(CASE WHEN has_rejected_deal THEN 1 ELSE 0 END) AS rejected_deals,
# MAGIC   SUM(CASE WHEN is_exclusively_deenergised THEN 1 ELSE 0 END) AS all_meters_dead
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
# MAGIC     WHEN COALESCE(has_complaint_note, false)
# MAGIC       OR COALESCE(has_complaint_transfer, false)
# MAGIC       OR current_pool_id = 'ld_pool_complaint'
# MAGIC       THEN 'COMPLAINT'
# MAGIC     WHEN COALESCE(has_open_callback, false) THEN 'CALLBACK'
# MAGIC     WHEN COALESCE(is_current_pool_locked, false) OR COALESCE(is_gdpr_pool, false) THEN 'LOCKED'
# MAGIC     WHEN has_any_past_deal AND NOT COALESCE(has_rejected_deal, false) THEN
# MAGIC       CASE
# MAGIC         WHEN last_deal_raw_days_left IS NULL OR last_deal_days_left < 1 THEN 'PAST_RETENTION'
# MAGIC         WHEN last_deal_days_left <= 540 THEN 'RETENTION'
# MAGIC         ELSE 'UPSELLING'
# MAGIC       END
# MAGIC     WHEN COALESCE(is_exclusively_deenergised, false) THEN 'UNASSIGNED'
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
# MAGIC   has_rejected_deal,
# MAGIC   is_exclusively_deenergised,
# MAGIC   (
# MAGIC     COALESCE(has_complaint_note, false)
# MAGIC     OR COALESCE(has_complaint_transfer, false)
# MAGIC     OR current_pool_id = 'ld_pool_complaint'
# MAGIC     OR COALESCE(has_open_callback, false)
# MAGIC     OR COALESCE(is_current_pool_locked, false)
# MAGIC     OR COALESCE(is_gdpr_pool, false)
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
# MAGIC Tag → **parent** shared pool via `crm_pool_rule` (snapshot).
# MAGIC New pool = seed the pool + one rule row. No CASE edit.
# MAGIC Custom split comes later. Sticky still wins: callback / locked stay; complaint uses the rule.
# MAGIC Next: **write the shared parent**, apply, then fair-share to agents.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ld_before_pool AS
# MAGIC SELECT * FROM crm_load.new_crm.ld_working
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,snapshot crm_pool_rule only
# Needed if this session already snapshotted without crm_pool_rule.
_rules = jdbc_table("public.crm_pool_rule")
_rules.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "crm_load.new_crm.snap_crm_pool_rule"
)
print("crm_pool_rule", _rules.count())

# COMMAND ----------

# DBTITLE 1,fill proposed_pool_id
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE crm_load.new_crm.ld_working AS
# MAGIC SELECT
# MAGIC   w.company_id,
# MAGIC   w.current_pool_id,
# MAGIC   w.lead_tag,
# MAGIC   CASE
# MAGIC     WHEN w.lead_tag = 'CALLBACK' THEN COALESCE(w.callback_owner_pool_id, w.current_pool_id)
# MAGIC     WHEN w.lead_tag = 'LOCKED' THEN w.current_pool_id
# MAGIC     WHEN COALESCE(w.is_protected, false) AND w.lead_tag <> 'COMPLAINT' THEN w.current_pool_id
# MAGIC     ELSE COALESCE(r.`poolId`, 'ld_pool_unassigned')
# MAGIC   END AS proposed_pool_id,
# MAGIC   w.site_count,
# MAGIC   w.win_provider_id,
# MAGIC   w.win_provider_name,
# MAGIC   w.win_family,
# MAGIC   w.win_end_date,
# MAGIC   w.win_contract_type,
# MAGIC   w.is_win_dfv,
# MAGIC   w.raw_days_left,
# MAGIC   w.days_left,
# MAGIC   w.last_deal_raw_days_left,
# MAGIC   w.last_deal_days_left,
# MAGIC   w.last_deal_days_since,
# MAGIC   w.has_any_past_deal,
# MAGIC   w.has_open_callback,
# MAGIC   w.callback_owner_pool_id,
# MAGIC   w.is_current_pool_locked,
# MAGIC   w.is_gdpr_pool,
# MAGIC   w.has_complaint_note,
# MAGIC   w.has_complaint_transfer,
# MAGIC   w.has_rejected_deal,
# MAGIC   w.is_exclusively_deenergised,
# MAGIC   w.is_protected,
# MAGIC   w.snapshot_at
# MAGIC FROM ld_before_pool w
# MAGIC LEFT JOIN crm_load.new_crm.snap_crm_pool_rule r
# MAGIC   ON r.tag = w.lead_tag
# MAGIC  AND COALESCE(r.`isActive`, true) = true
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lead_tag, proposed_pool_id, win_family, is_win_dfv, COUNT(*) AS companies
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC GROUP BY lead_tag, proposed_pool_id, win_family, is_win_dfv
# MAGIC ORDER BY companies DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Write shared pools first
# MAGIC
# MAGIC Same write as today. Companies go to the **shared parent** (E.ON, BG, …).
# MAGIC Then in Supabase: `SELECT public.ld_apply_batch_run();`
# MAGIC
# MAGIC Agent fair-share is **Step 5**, after this apply. Do not skip the apply
# MAGIC if you want CRM to show the shared bag before agents get a share.

# COMMAND ----------

# DBTITLE 1,write shared pools to ld_apply_batch
def _write_apply_batch(moves_df, label):
    n = moves_df.count()
    print(label, n)
    display(moves_df)
    (
        moves_df.write.format("postgresql")
        .option("host", PG_POOLER_HOST)
        .option("port", "5432")
        .option("database", "postgres")
        .option("dbtable", "public.ld_apply_batch")
        .option("user", PG_POOLER_USER)
        .option("password", PG_PASSWORD)
        .mode("overwrite")
        .save()
    )
    print("batch table written — run SELECT public.ld_apply_batch_run();")


shared_moves = spark.sql(
    """
    SELECT company_id, proposed_pool_id
    FROM crm_load.new_crm.ld_working
    WHERE COALESCE(is_protected, false) = false
      AND proposed_pool_id IS NOT NULL
      AND COALESCE(current_pool_id, '') <> COALESCE(proposed_pool_id, '')
    """
)
_write_apply_batch(shared_moves, "shared-pool rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Agent fair-share (Nightly `AllocateCompanies`)
# MAGIC
# MAGIC Run **after** Step 4 apply. Reads who is on each shared parent, then
# MAGIC shares into linked agent pools (`crm_pool_member` + cap).
# MAGIC
# MAGIC Sticky is never moved. No members → stop; they stay on the shared bag.
# MAGIC Custom split is not in this step.
# MAGIC
# MAGIC Then Step 6 writes those agent moves and you apply again.

# COMMAND ----------

# DBTITLE 1,snapshot fair-share config
def _snap_optional(table):
    try:
        df = jdbc_table(f"public.{table}")
        dest = f"crm_load.new_crm.snap_{table}"
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dest)
        print(table, df.count())
        return True
    except Exception as exc:
        print(f"{table} not loaded — fair-share will skip ({exc})")
        return False


has_policy = _snap_optional("crm_pool_split_policy")
has_member = _snap_optional("crm_pool_member")

# COMMAND ----------

# DBTITLE 1,fair-share proposed_pool_id
from collections import defaultdict

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def _lc(df, *names):
    mapping = {c.lower(): c for c in df.columns}
    for name in names:
        if name.lower() in mapping:
            return mapping[name.lower()]
    return None


def _nightly_avg(total, member_caps):
    """Nightly AvgLeadsForSegment: even split, then shrink for tight caps."""
    members = len(member_caps)
    if members == 0 or total <= 0:
        return 0
    avg = total // members
    remain_total = total
    remain_members = members
    for cap in member_caps:
        if cap is not None and cap >= 0 and cap < avg:
            remain_total -= cap
            remain_members -= 1
    if remain_members <= 0:
        return 0
    return (remain_total // remain_members) + 1


def _room(count, cap):
    if cap is None or cap < 0:
        return True
    return count < cap


fair_schema = StructType(
    [
        StructField("company_id", StringType(), False),
        StructField("fair_share_pool_id", StringType(), True),
        StructField("fair_share_reason", StringType(), True),
    ]
)
empty_fair = spark.createDataFrame([], fair_schema)

if not (has_policy and has_member):
    empty_fair.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.ld_fair_share"
    )
    print("Fair-share skipped — policy/member tables missing. Proposed stays on shared parent.")
else:
    pol = spark.table("crm_load.new_crm.snap_crm_pool_split_policy")
    mem = spark.table("crm_load.new_crm.snap_crm_pool_member")
    p_id = _lc(pol, "id")
    p_parent = _lc(pol, "parentPoolId")
    p_strategy = _lc(pol, "strategy")
    p_active = _lc(pol, "isActive")
    m_policy = _lc(mem, "policyId")
    m_child = _lc(mem, "childPoolId")
    m_max = _lc(mem, "maxRecords")
    m_sort = _lc(mem, "sortOrder")
    m_active = _lc(mem, "isActive")

    policies = [
        r.asDict()
        for r in pol.select(
            F.col(p_id).alias("policy_id"),
            F.col(p_parent).alias("parent_pool_id"),
            F.col(p_strategy).alias("strategy"),
            F.col(p_active).alias("is_active"),
        ).collect()
    ]
    members = [
        r.asDict()
        for r in mem.select(
            F.col(m_policy).alias("policy_id"),
            F.col(m_child).alias("child_pool_id"),
            F.col(m_max).alias("max_records") if m_max else F.lit(None).cast("int").alias("max_records"),
            (F.col(m_sort) if m_sort else F.lit(100)).alias("sort_order"),
            F.col(m_active).alias("is_active"),
        ).collect()
    ]

    policy_by_id = {p["policy_id"]: p for p in policies}
    members_by_parent = defaultdict(list)
    for m in members:
        if not m.get("is_active"):
            continue
        pol = policy_by_id.get(m["policy_id"])
        if not pol:
            continue
        if not pol.get("is_active"):
            continue
        if str(pol.get("strategy") or "").upper() != "FAIR_SHARE":
            continue
        members_by_parent[pol["parent_pool_id"]].append(m)

    members_by_parent = {
        parent: sorted(
            rows,
            key=lambda r: (r.get("sort_order") or 100, r["child_pool_id"] or ""),
        )
        for parent, rows in members_by_parent.items()
        if rows
    }

    print("active FAIR_SHARE parents:", len(members_by_parent))
    for parent, rows in members_by_parent.items():
        print(" ", parent, "→", len(rows), "agents", [r["child_pool_id"] for r in rows])

    if not members_by_parent:
        empty_fair.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            "crm_load.new_crm.ld_fair_share"
        )
        print("No members linked — companies stay on shared parents.")
    else:
        working = spark.table("crm_load.new_crm.ld_working").collect()
        by_parent = defaultdict(list)
        passthrough = []
        for row in working:
            d = row.asDict()
            parent = d.get("proposed_pool_id")
            if d.get("is_protected") or parent not in members_by_parent:
                passthrough.append(
                    (
                        d["company_id"],
                        d.get("proposed_pool_id"),
                        "sticky" if d.get("is_protected") else "no_members",
                    )
                )
            else:
                by_parent[parent].append(d)

        updates = list(passthrough)
        for parent, companies in by_parent.items():
            roster = members_by_parent[parent]
            cap_of = {
                r["child_pool_id"]: (
                    None
                    if r.get("max_records") is None
                    else int(r["max_records"])
                )
                for r in roster
            }
            member_ids = [r["child_pool_id"] for r in roster]
            member_set = set(member_ids)

            assigned = {}
            reason = {}
            for c in companies:
                cid = c["company_id"]
                current = c.get("current_pool_id")
                if current in member_set:
                    assigned[cid] = current
                    reason[cid] = "keep_child"
                else:
                    assigned[cid] = parent
                    reason[cid] = "on_parent" if current == parent else "reclaim_wrong"

            def held(child):
                return [
                    c["company_id"]
                    for c in companies
                    if assigned[c["company_id"]] == child
                ]

            # Cap reclaim (Nightly DeAllocateExcess vs max_records)
            for child in member_ids:
                cap = cap_of[child]
                ids = sorted(held(child))
                if cap is not None and cap >= 0 and len(ids) > cap:
                    for cid in ids[cap:]:
                        assigned[cid] = parent
                        reason[cid] = "reclaim_over_max"

            # Average reclaim (Nightly DeAllocateExcess vs avg)
            total = len(companies)
            avg = _nightly_avg(total, [cap_of[ch] for ch in member_ids])
            if avg > 0:
                for child in member_ids:
                    cap = cap_of[child]
                    limit = avg
                    if cap is not None and cap >= 0:
                        limit = min(cap, avg)
                    ids = sorted(held(child))
                    if len(ids) > limit:
                        for cid in ids[limit:]:
                            assigned[cid] = parent
                            reason[cid] = "reclaim_over_avg"

            # Fill lowest (Nightly AllocateSegmentCompanies)
            free = sorted(
                c["company_id"]
                for c in companies
                if assigned[c["company_id"]] == parent
            )
            counts = {child: len(held(child)) for child in member_ids}
            current_min = min(counts.values()) if counts else 0
            while free:
                moved = False
                for child in member_ids:
                    if not free:
                        break
                    if _room(counts[child], cap_of[child]) and counts[child] <= current_min:
                        cid = free.pop(0)
                        assigned[cid] = child
                        reason[cid] = "fair_share"
                        counts[child] += 1
                        moved = True
                if not moved:
                    current_min += 1
                    if all(not _room(counts[ch], cap_of[ch]) for ch in member_ids):
                        break

            for c in companies:
                cid = c["company_id"]
                updates.append((cid, assigned[cid], reason[cid]))

        spark.createDataFrame(updates, fair_schema).write.mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable("crm_load.new_crm.ld_fair_share")
        print("fair-share rows", len(updates))

# proposed_pool_id on ld_working stays the shared parent (Step 3 / Step 4).
spark.sql(
    """
    SELECT
      COALESCE(f.fair_share_reason, 'no_fair_share_row') AS reason,
      COUNT(*) AS companies
    FROM crm_load.new_crm.ld_working w
    LEFT JOIN crm_load.new_crm.ld_fair_share f ON w.company_id = f.company_id
    GROUP BY 1
    ORDER BY companies DESC
    """
).show(50, False)

spark.sql(
    """
    SELECT
      w.proposed_pool_id AS shared_parent,
      f.fair_share_pool_id AS agent_pool,
      f.fair_share_reason,
      COUNT(*) AS companies
    FROM crm_load.new_crm.ld_working w
    LEFT JOIN crm_load.new_crm.ld_fair_share f ON w.company_id = f.company_id
    GROUP BY 1, 2, 3
    ORDER BY companies DESC
    """
).show(50, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Write agent fair-share
# MAGIC
# MAGIC Second batch: parent → agent pool. Run only after Step 4 apply and Step 5.
# MAGIC Then `SELECT public.ld_apply_batch_run();` again.
# MAGIC Empty members → 0 rows.

# COMMAND ----------

# DBTITLE 1,write agent pools to ld_apply_batch
agent_moves = spark.sql(
    """
    SELECT w.company_id, f.fair_share_pool_id AS proposed_pool_id
    FROM crm_load.new_crm.ld_working w
    JOIN crm_load.new_crm.ld_fair_share f
      ON w.company_id = f.company_id
    WHERE COALESCE(w.is_protected, false) = false
      AND f.fair_share_pool_id IS NOT NULL
      AND f.fair_share_reason IN (
        'fair_share', 'reclaim_wrong', 'reclaim_over_max', 'reclaim_over_avg'
      )
      AND COALESCE(w.current_pool_id, '') <> COALESCE(f.fair_share_pool_id, '')
    """
)
_write_apply_batch(agent_moves, "agent-pool rows")

