# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1 — Working table
# MAGIC
# MAGIC Snapshot CRM tables, then one SQL builds `ld_working`.
# MAGIC Tag order: **sticky first**, then Customer Care (7–60 days since sale),
# MAGIC then Retention clock, then Corporate (21–200), then supplier.
# MAGIC
# MAGIC Only **E.ON** has a DFV pool (`ld_pool_eon_dfv`): **contract type**
# MAGIC deemed/flexible/variable only (Nightly FVD). Past due / no CED → main E.ON.
# MAGIC Supplier tags come from `crm_provider_family` (sync `09_sync_provider_pools.sql`):
# MAGIC each known provider → own pool (name = displayName), except shared BG / E.ON / UB.
# MAGIC BG (not Lite) window 548; everyone else 365. Unknown provider → Other.
# MAGIC
# MAGIC `source_kind` on `ld_working` is read from `legacy_site_mappings`.
# MAGIC Load origin is the **`campaign` column** (Retention / Supplier) — that is
# MAGIC NOT the lead-tag Campaign pool (Retentions / Past Retentions / E.ON).
# MAGIC Still match `LIKE '%retention%'` on `campaign` and `source` just in case.
# MAGIC Deal or fallback with no CED = Nightly day 0 → Past Retention, not Unassigned.
# MAGIC
# MAGIC **Does not write `companies.poolId` until Step 4.**
# MAGIC Tag → CAMPAIGN / STANDARD parent. If that parent has active `pool_links`,
# MAGIC fair-share non-sticky companies onto those PRIVATE children (Nightly
# MAGIC average: new leads fill the shorter bag first).

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


def pg_query(sql: str):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("query", sql)
        .option("user", JDBC_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("sslmode", "require")
        .load()
    )

print("JDBC host =", JDBC_HOST)

# COMMAND ----------

# DBTITLE 1,Janitor (leavers) — run before snapshot
# Nightly Janitor first: pull leavers off books. Does not wipe every pool.
# Requires 24_janitor.sql once in Supabase (creates the function). TPS / blacklist later.
try:
    _janitor = pg_query("SELECT public.ld_janitor_run() AS companies_cleared")
    print("janitor companies_cleared", _janitor.collect()[0]["companies_cleared"])
except Exception as exc:
    print("janitor skip — run 24_janitor.sql first:", str(exc)[:200])

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
    "pools",
    "pool_links",
    "crm_pool_rule",
    "notes",
    "profiles",
    "company_pool_audits",
]:
    df = jdbc_table(f"public.{table}")
    dest = f"crm_load.new_crm.snap_{table}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(dest)
    print(table, df.count())

try:
    _fam = jdbc_table("public.crm_provider_family")
    _fam.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.snap_crm_provider_family"
    )
    print("crm_provider_family", _fam.count())
except Exception as exc:
    print("crm_provider_family skip — run 09_sync_provider_pools.sql first:", str(exc)[:200])
    spark.createDataFrame(
        [],
        "providerId string, family string, tagCode string, poolId string, "
        "windowDays int, displayName string, isActive boolean",
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.snap_crm_provider_family"
    )

try:
    sale = jdbc_table("public.crm_company_load_sale")
    sale.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.snap_crm_company_load_sale"
    )
    print("crm_company_load_sale", sale.count())
except Exception as e:
    spark.createDataFrame(
        [],
        "companyId string, companySiteId string, source string, hasPastSale boolean, lastDealEndDate date",
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "crm_load.new_crm.snap_crm_company_load_sale"
    )
    print("crm_company_load_sale skip", str(e)[:160])

# pools.id = UUID; pools.code = stable key (BG, COMPLAINT, …).
spark.sql(
    """
    CREATE OR REPLACE TABLE crm_load.new_crm.ld_pool_by_code AS
    SELECT code, FIRST(id) AS pool_id
    FROM crm_load.new_crm.snap_pools
    WHERE code IS NOT NULL AND TRIM(code) <> ''
    GROUP BY code
    """
)
print("ld_pool_by_code", spark.table("crm_load.new_crm.ld_pool_by_code").count())

# Load origin = legacy_site_mappings.campaign (Retention / Supplier).
# Not the lead-tag Campaign pool. Column `source` still matched just in case.
maps = jdbc_table("public.legacy_site_mappings")
_map_cols = {c.lower(): c for c in maps.columns}
if "campaign" not in _map_cols:
    maps = maps.withColumn("campaign", F.lit(None).cast("string"))
maps.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "crm_load.new_crm.snap_legacy_site_mappings"
)
print("legacy_site_mappings", maps.count(), maps.columns)
if "campaign" in {c.lower() for c in maps.columns}:
    print("mapping campaign (load origin, not pool Campaign)")
    maps.groupBy([c for c in maps.columns if c.lower() == "campaign"][0]).count().show(30, False)

spark.createDataFrame(
    [],
    "id string, name string, kind string, family string",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "crm_load.new_crm.snap_crm_load_source"
)

sites = spark.table("crm_load.new_crm.snap_company_sites")
if "loadSourceId" not in sites.columns:
    sites.withColumn("loadSourceId", F.lit(None).cast("string")).write.mode(
        "overwrite"
    ).option("overwriteSchema", "true").saveAsTable("crm_load.new_crm.snap_company_sites")

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

# Nightly: hasRejectedDeal blocks Retention / Customer Care only.
# The deal stays on the Compliance Rejected tab — we do not pin the company pool.
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

# Company clock: every contract on the company (companyId / siteId / meter).
# Retention 1–540 beats Past beats Upselling. Fallback lastDealEndDate included.
from pyspark.sql.window import Window

contracts_s = spark.table("crm_load.new_crm.snap_contracts")
print("contract columns", contracts_s.columns)
print("site_meter columns", meters.columns)

c_id = _col(contracts_s, "id")
c_co = _col(contracts_s, "companyId")
c_site = _col(contracts_s, "siteId")
c_meter = _col(contracts_s, "siteMeterId")
c_end = _col(contracts_s, "endDate")
m_id = _col(meters, "id")
m_site = _col(meters, "companySiteId", "siteId")

ced_parts = []
if c_co and c_end:
    ced_parts.append(
        contracts_s.where(F.col(c_end).isNotNull())
        .select(
            F.when(
                F.trim(F.col(c_co).cast("string")) == "",
                F.lit(None).cast("string"),
            ).otherwise(F.trim(F.col(c_co).cast("string"))).alias("company_id"),
            F.col(c_end).cast("date").alias("end_date"),
            F.lit("companyId").alias("path"),
        )
        .where("company_id IS NOT NULL")
    )
if c_site and c_end and s_id and s_co:
    ced_parts.append(
        contracts_s.alias("c")
        .join(sites_s.alias("s"), F.col(f"c.{c_site}") == F.col(f"s.{s_id}"), "inner")
        .where(F.col(f"c.{c_end}").isNotNull())
        .select(
            F.col(f"s.{s_co}").alias("company_id"),
            F.col(f"c.{c_end}").cast("date").alias("end_date"),
            F.lit("siteId").alias("path"),
        )
        .where("company_id IS NOT NULL")
    )
if c_meter and c_end and m_id and m_site and s_id and s_co:
    ced_parts.append(
        contracts_s.alias("c")
        .join(meters.alias("m"), F.col(f"c.{c_meter}") == F.col(f"m.{m_id}"), "inner")
        .join(sites_s.alias("s"), F.col(f"m.{m_site}") == F.col(f"s.{s_id}"), "inner")
        .where(F.col(f"c.{c_end}").isNotNull())
        .select(
            F.col(f"s.{s_co}").alias("company_id"),
            F.col(f"c.{c_end}").cast("date").alias("end_date"),
            F.lit("siteMeterId").alias("path"),
        )
        .where("company_id IS NOT NULL")
    )
else:
    print("meter-contract join skipped — siteMeterId/companySiteId missing")

try:
    sale_s = spark.table("crm_load.new_crm.snap_crm_company_load_sale")
    sale_co = _col(sale_s, "companyId")
    sale_end = _col(sale_s, "lastDealEndDate")
    if sale_co and sale_end:
        ced_parts.append(
            sale_s.where(F.col(sale_end).isNotNull())
            .select(
                F.col(sale_co).alias("company_id"),
                F.col(sale_end).cast("date").alias("end_date"),
                F.lit("fallback").alias("path"),
            )
            .where("company_id IS NOT NULL")
        )
except Exception as e:
    print("fallback CED skip", str(e)[:160])

if not ced_parts:
    company_ced = spark.createDataFrame([], "company_id string, end_date date")
else:
    all_ced = ced_parts[0]
    for extra in ced_parts[1:]:
        all_ced = all_ced.unionByName(extra)
    print("CED rows by path")
    all_ced.groupBy("path").count().show(10, False)
    ranked = (
        all_ced
        .withColumn("days", F.datediff(F.col("end_date"), F.current_date()))
        .withColumn(
            "bag",
            F.when((F.col("days") >= 1) & (F.col("days") <= 540), F.lit(0))
            .when((F.col("days").isNull()) | (F.col("days") < 1), F.lit(1))
            .otherwise(F.lit(2)),
        )
    )
    print(
        "companies with both a 1-540 CED and a past CED (these move Past → Retention):",
        ranked.groupBy("company_id")
        .agg(
            F.max(F.when(F.col("bag") == 0, 1).otherwise(0)).alias("has_ret"),
            F.max(F.when(F.col("bag") == 1, 1).otherwise(0)).alias("has_past"),
        )
        .where("has_ret = 1 AND has_past = 1")
        .count(),
    )
    company_ced = ranked.groupBy("company_id").agg(
        F.coalesce(
            F.max(F.when((F.col("days") >= 1) & (F.col("days") <= 540), F.col("end_date"))),
            F.max(F.when((F.col("days").isNull()) | (F.col("days") < 1), F.col("end_date"))),
            F.max("end_date"),
        ).alias("end_date")
    )

company_ced.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "crm_load.new_crm.ld_company_ced"
)
ced_days = F.datediff(F.col("end_date"), F.current_date())
print("company clock bags (0=Retention 1=Past 2=Upselling)")
(
    company_ced.withColumn(
        "bag",
        F.when((ced_days >= 1) & (ced_days <= 540), F.lit("RETENTION"))
        .when((ced_days.isNull()) | (ced_days < 1), F.lit("PAST_RETENTION"))
        .otherwise(F.lit("UPSELLING")),
    )
    .groupBy("bag")
    .count()
    .show(10, False)
)

# COMMAND ----------


# MAGIC %md
# MAGIC ## Working table (SQL)
# MAGIC
# MAGIC `is_win_dfv` is the **winning** contract only (Nightly FVD = contract type).
# MAGIC No supplier → Other / Unassigned, never E.ON DFV.
# MAGIC Family / tagCode / windowDays from `crm_provider_family` (09 sync).
# MAGIC Last deal: Prisma `deals.siteMeterId` → `site_meters` → `company_sites`.
# MAGIC Days left is still that deal's `contracts.endDate` − today.

# COMMAND ----------

# DBTITLE 1,ld_working
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE crm_load.new_crm.ld_working AS
# MAGIC WITH contract_cos AS (
# MAGIC   SELECT c.id AS contract_id, NULLIF(TRIM(c.`companyId`), '') AS company_id
# MAGIC   FROM crm_load.new_crm.snap_contracts c
# MAGIC   WHERE NULLIF(TRIM(c.`companyId`), '') IS NOT NULL
# MAGIC   UNION
# MAGIC   SELECT c.id, s.`companyId`
# MAGIC   FROM crm_load.new_crm.snap_contracts c
# MAGIC   JOIN crm_load.new_crm.snap_company_sites s
# MAGIC     ON s.id = c.`siteId`
# MAGIC   WHERE s.`companyId` IS NOT NULL
# MAGIC   UNION
# MAGIC   SELECT c.id, s.`companyId`
# MAGIC   FROM crm_load.new_crm.snap_contracts c
# MAGIC   JOIN crm_load.new_crm.snap_site_meters sm
# MAGIC     ON sm.id = c.`siteMeterId`
# MAGIC   JOIN crm_load.new_crm.snap_company_sites s
# MAGIC     ON s.id = sm.`companySiteId`
# MAGIC   WHERE s.`companyId` IS NOT NULL
# MAGIC ),
# MAGIC contracts_f AS (
# MAGIC   SELECT
# MAGIC     cc.company_id,
# MAGIC     c.`providerId`                        AS provider_id,
# MAGIC     p.`displayName`                       AS provider_name,
# MAGIC     CAST(c.`endDate` AS DATE)             AS end_date,
# MAGIC     DATEDIFF(CAST(c.`endDate` AS DATE), CURRENT_DATE) AS raw_days_left,
# MAGIC     COALESCE(DATEDIFF(CAST(c.`endDate` AS DATE), CURRENT_DATE), 0) AS days_left,
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
# MAGIC     COALESCE(
# MAGIC       NULLIF(TRIM(f.`tagCode`), ''),
# MAGIC       CASE
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%british gas lite%'
# MAGIC           OR LOWER(p.`displayName`) LIKE '%bg lite%' THEN 'BG_LITE'
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%british gas%' THEN 'BG'
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%e.on%'
# MAGIC           OR LOWER(p.`displayName`) LIKE '%e-on%'
# MAGIC           OR LOWER(p.`displayName`) LIKE 'eon%' THEN 'EON'
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%utility bidder%' THEN 'UB'
# MAGIC         WHEN p.id IS NULL THEN 'OTHER'
# MAGIC         ELSE 'OTHER'
# MAGIC       END
# MAGIC     ) AS tag_code,
# MAGIC     COALESCE(
# MAGIC       f.`windowDays`,
# MAGIC       CASE
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%british gas lite%' THEN 365
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%british gas%' THEN 548
# MAGIC         ELSE 365
# MAGIC       END
# MAGIC     ) AS window_days,
# MAGIC     COALESCE(
# MAGIC       NULLIF(TRIM(f.family), ''),
# MAGIC       CASE
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%british gas lite%' THEN 'BG_LITE'
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%british gas%' THEN 'BG'
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%e.on%'
# MAGIC           OR LOWER(p.`displayName`) LIKE '%e-on%'
# MAGIC           OR LOWER(p.`displayName`) LIKE 'eon%' THEN 'EON'
# MAGIC         WHEN LOWER(p.`displayName`) LIKE '%utility bidder%' THEN 'UB'
# MAGIC         ELSE 'OTHER'
# MAGIC       END
# MAGIC     ) AS family
# MAGIC   FROM contract_cos cc
# MAGIC   JOIN crm_load.new_crm.snap_contracts c
# MAGIC     ON c.id = cc.contract_id
# MAGIC   LEFT JOIN crm_load.new_crm.snap_providers p
# MAGIC     ON c.`providerId` = p.id
# MAGIC   LEFT JOIN crm_load.new_crm.snap_crm_provider_family f
# MAGIC     ON f.`providerId` = c.`providerId`
# MAGIC    AND COALESCE(f.`isActive`, true) = true
# MAGIC   WHERE cc.company_id IS NOT NULL
# MAGIC ),
# MAGIC winning AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY company_id
# MAGIC         ORDER BY
# MAGIC           CASE WHEN raw_days_left IS NULL THEN 1 ELSE 0 END,
# MAGIC           days_left ASC,
# MAGIC           CASE tag_code
# MAGIC             WHEN 'EON' THEN 1
# MAGIC             WHEN 'BG' THEN 2
# MAGIC             WHEN 'UB' THEN 3
# MAGIC             ELSE 4
# MAGIC           END
# MAGIC       ) AS rn
# MAGIC     FROM contracts_f
# MAGIC   ) x
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC latest_ced AS (
# MAGIC   SELECT
# MAGIC     company_id,
# MAGIC     COALESCE(
# MAGIC       MAX(CASE WHEN raw_days_left BETWEEN 1 AND 540 THEN end_date END),
# MAGIC       MAX(CASE WHEN raw_days_left IS NULL OR raw_days_left < 1 THEN end_date END),
# MAGIC       MAX(end_date)
# MAGIC     ) AS end_date
# MAGIC   FROM contracts_f
# MAGIC   WHERE end_date IS NOT NULL
# MAGIC   GROUP BY company_id
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
# MAGIC   SELECT DISTINCT a.`companyId` AS company_id
# MAGIC   FROM crm_load.new_crm.snap_company_pool_audits a
# MAGIC   INNER JOIN crm_load.new_crm.ld_pool_by_code cp ON cp.code = 'COMPLAINT'
# MAGIC   WHERE a.`poolId` = cp.pool_id
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
# MAGIC     COALESCE(`hasPastSale`, true) AS has_past_sale,
# MAGIC     source AS fallback_source
# MAGIC   FROM crm_load.new_crm.snap_crm_company_load_sale
# MAGIC ),
# MAGIC load_src AS (
# MAGIC   SELECT
# MAGIC     x.company_id,
# MAGIC     CASE
# MAGIC       WHEN MAX(CASE WHEN x.is_retention THEN 1 ELSE 0 END) > 0 THEN 'RETENTION'
# MAGIC       WHEN MAX(CASE WHEN x.has_mapping THEN 1 ELSE 0 END) > 0 THEN 'SUPPLIER'
# MAGIC       WHEN MAX(CASE WHEN x.site_kind = 'RETENTION' THEN 1 ELSE 0 END) > 0 THEN 'RETENTION'
# MAGIC       WHEN MAX(CASE WHEN x.site_kind = 'SUPPLIER' THEN 1 ELSE 0 END) > 0 THEN 'SUPPLIER'
# MAGIC     END AS source_kind,
# MAGIC     MAX(x.mapping_source) AS mapping_source,
# MAGIC     MAX(x.site_family) AS source_family,
# MAGIC     MAX(x.site_source_id) AS load_source_id
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       m.`companyId` AS company_id,
# MAGIC       (
# MAGIC         LOWER(COALESCE(m.`campaign`, '')) LIKE '%retention%'
# MAGIC         OR LOWER(COALESCE(m.`source`, '')) LIKE '%retention%'
# MAGIC       ) AS is_retention,
# MAGIC       true AS has_mapping,
# MAGIC       COALESCE(m.`campaign`, m.`source`) AS mapping_source,
# MAGIC       CAST(NULL AS STRING) AS site_kind,
# MAGIC       CAST(NULL AS STRING) AS site_family,
# MAGIC       CAST(NULL AS STRING) AS site_source_id
# MAGIC     FROM crm_load.new_crm.snap_legacy_site_mappings m
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       s.`companyId` AS company_id,
# MAGIC       false AS is_retention,
# MAGIC       false AS has_mapping,
# MAGIC       CAST(NULL AS STRING) AS mapping_source,
# MAGIC       src.kind AS site_kind,
# MAGIC       src.family AS site_family,
# MAGIC       src.id AS site_source_id
# MAGIC     FROM crm_load.new_crm.snap_company_sites s
# MAGIC     LEFT JOIN crm_load.new_crm.snap_crm_load_source src
# MAGIC       ON src.id = s.`loadSourceId`
# MAGIC   ) x
# MAGIC   GROUP BY x.company_id
# MAGIC )
# MAGIC SELECT
# MAGIC   co.id                                            AS company_id,
# MAGIC   co.`poolId`                                      AS current_pool_id,
# MAGIC   CAST(NULL AS STRING)                             AS lead_tag,
# MAGIC   COALESCE(s.site_count, 0)                        AS site_count,
# MAGIC   w.provider_id                                    AS win_provider_id,
# MAGIC   w.provider_name                                  AS win_provider_name,
# MAGIC   w.family                                         AS win_family,
# MAGIC   w.tag_code                                       AS win_tag_code,
# MAGIC   COALESCE(w.window_days, 365)                     AS win_window_days,
# MAGIC   w.end_date                                       AS win_end_date,
# MAGIC   w.contract_type                                  AS win_contract_type,
# MAGIC   COALESCE(w.is_dfv, false)                        AS is_win_dfv,
# MAGIC   w.raw_days_left,
# MAGIC   w.days_left,
# MAGIC   DATEDIFF(lc.end_date, CURRENT_DATE) AS last_deal_raw_days_left,
# MAGIC   CASE WHEN lc.end_date IS NOT NULL
# MAGIC        THEN COALESCE(DATEDIFF(lc.end_date, CURRENT_DATE), 0)
# MAGIC   END AS last_deal_days_left,
# MAGIC   ld.last_deal_days_since,
# MAGIC   CASE
# MAGIC     WHEN d.company_id IS NOT NULL THEN true
# MAGIC     WHEN COALESCE(ls.has_past_sale, false) THEN true
# MAGIC     ELSE false
# MAGIC   END AS has_any_past_deal,
# MAGIC   ls.has_past_sale                             AS hasPastSale,
# MAGIC   lc.end_date AS lastDealEndDate,
# MAGIC   ls.fallback_source,
# MAGIC   CASE
# MAGIC     WHEN src.source_kind IS NOT NULL THEN src.source_kind
# MAGIC     WHEN ls.company_id IS NOT NULL THEN 'RETENTION'
# MAGIC   END                                          AS source_kind,
# MAGIC   src.mapping_source,
# MAGIC   src.source_family,
# MAGIC   src.load_source_id,
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
# MAGIC LEFT JOIN latest_ced lc ON co.id = lc.company_id
# MAGIC LEFT JOIN load_src src ON co.id = src.company_id
# MAGIC LEFT JOIN complaint_notes cn ON co.id = cn.company_id
# MAGIC LEFT JOIN complaint_xfer xf ON co.id = xf.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_rejected_deal_companies rd ON co.id = rd.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_exclusively_deenergised de ON co.id = de.company_id
# MAGIC LEFT JOIN crm_load.new_crm.snap_pools pl ON co.`poolId` = pl.id
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
# MAGIC E.ON DFV = real DFV contract type only. `*_NOW` = expired / no CED on that
# MAGIC supplier tag (main pool). `*_IN_WINDOW` uses that supplier's windowDays.
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
# MAGIC       OR current_pool_id = (
# MAGIC         SELECT pool_id FROM crm_load.new_crm.ld_pool_by_code WHERE code = 'COMPLAINT' LIMIT 1
# MAGIC       )
# MAGIC       THEN 'COMPLAINT'
# MAGIC     WHEN COALESCE(has_open_callback, false) THEN 'CALLBACK'
# MAGIC     WHEN COALESCE(is_current_pool_locked, false) OR COALESCE(is_gdpr_pool, false) THEN 'LOCKED'
# MAGIC     WHEN has_any_past_deal
# MAGIC       AND NOT COALESCE(has_rejected_deal, false)
# MAGIC       AND last_deal_days_since IS NOT NULL
# MAGIC       AND last_deal_days_since >= 7
# MAGIC       AND last_deal_days_since <= 60
# MAGIC       THEN 'CUSTOMER_CARE'
# MAGIC     WHEN has_any_past_deal AND NOT COALESCE(has_rejected_deal, false) THEN
# MAGIC       CASE
# MAGIC         WHEN last_deal_raw_days_left IS NULL OR last_deal_days_left < 1 THEN 'PAST_RETENTION'
# MAGIC         WHEN last_deal_days_left <= 540 THEN 'RETENTION'
# MAGIC         ELSE 'UPSELLING'
# MAGIC       END
# MAGIC     WHEN site_count > 200 THEN 'UNASSIGNED'
# MAGIC     WHEN site_count >= 21 AND site_count <= 200
# MAGIC       AND (
# MAGIC         COALESCE(is_win_dfv, false)
# MAGIC         OR raw_days_left IS NULL
# MAGIC         OR days_left <= 365
# MAGIC       )
# MAGIC       THEN 'CORPORATE'
# MAGIC     WHEN site_count >= 21 THEN 'UNASSIGNED'
# MAGIC     WHEN COALESCE(is_exclusively_deenergised, false) THEN 'UNASSIGNED'
# MAGIC     WHEN COALESCE(win_tag_code, win_family) IS NULL THEN 'UNASSIGNED'
# MAGIC     WHEN COALESCE(is_win_dfv, false)
# MAGIC       AND COALESCE(win_tag_code, win_family) = 'EON'
# MAGIC       THEN 'EON_DFV'
# MAGIC     WHEN raw_days_left IS NULL OR days_left <= 0 THEN
# MAGIC       CONCAT(COALESCE(win_tag_code, win_family, 'OTHER'), '_NOW')
# MAGIC     WHEN days_left <= COALESCE(win_window_days, 365) THEN
# MAGIC       CONCAT(COALESCE(win_tag_code, win_family, 'OTHER'), '_IN_WINDOW')
# MAGIC     ELSE 'PRE_WINDOW'
# MAGIC   END AS lead_tag,
# MAGIC   site_count,
# MAGIC   win_provider_id,
# MAGIC   win_provider_name,
# MAGIC   win_family,
# MAGIC   win_tag_code,
# MAGIC   win_window_days,
# MAGIC   win_end_date,
# MAGIC   win_contract_type,
# MAGIC   is_win_dfv,
# MAGIC   raw_days_left,
# MAGIC   days_left,
# MAGIC   last_deal_raw_days_left,
# MAGIC   last_deal_days_left,
# MAGIC   last_deal_days_since,
# MAGIC   has_any_past_deal,
# MAGIC   hasPastSale,
# MAGIC   lastDealEndDate,
# MAGIC   fallback_source,
# MAGIC   source_kind,
# MAGIC   mapping_source,
# MAGIC   source_family,
# MAGIC   load_source_id,
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
# MAGIC     OR current_pool_id = (
# MAGIC       SELECT pool_id FROM crm_load.new_crm.ld_pool_by_code WHERE code = 'COMPLAINT' LIMIT 1
# MAGIC     )
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
# MAGIC Tag → **parent** CAMPAIGN (or STANDARD) pool via `crm_pool_rule` (snapshot).
# MAGIC Next cell: if that parent has `pool_links`, fair-share to PRIVATE children.
# MAGIC Sticky still wins: callback / locked stay; complaint uses the rule.

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
# MAGIC     WHEN w.lead_tag = 'COMPLAINT' THEN COALESCE(r.`poolId`, cpool.pool_id)
# MAGIC     WHEN w.lead_tag = 'CALLBACK' THEN COALESCE(w.callback_owner_pool_id, w.current_pool_id)
# MAGIC     WHEN w.lead_tag = 'LOCKED' THEN w.current_pool_id
# MAGIC     WHEN COALESCE(w.is_protected, false) AND w.lead_tag <> 'COMPLAINT' THEN w.current_pool_id
# MAGIC     ELSE COALESCE(r.`poolId`, upool.pool_id)
# MAGIC   END AS proposed_pool_id,
# MAGIC   w.site_count,
# MAGIC   w.win_provider_id,
# MAGIC   w.win_provider_name,
# MAGIC   w.win_family,
# MAGIC   w.win_tag_code,
# MAGIC   w.win_window_days,
# MAGIC   w.win_end_date,
# MAGIC   w.win_contract_type,
# MAGIC   w.is_win_dfv,
# MAGIC   w.raw_days_left,
# MAGIC   w.days_left,
# MAGIC   w.last_deal_raw_days_left,
# MAGIC   w.last_deal_days_left,
# MAGIC   w.last_deal_days_since,
# MAGIC   w.has_any_past_deal,
# MAGIC   w.hasPastSale,
# MAGIC   w.lastDealEndDate,
# MAGIC   w.fallback_source,
# MAGIC   w.source_kind,
# MAGIC   w.mapping_source,
# MAGIC   w.source_family,
# MAGIC   w.load_source_id,
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
# MAGIC LEFT JOIN crm_load.new_crm.ld_pool_by_code cpool ON cpool.code = 'COMPLAINT'
# MAGIC LEFT JOIN crm_load.new_crm.ld_pool_by_code upool ON upool.code = 'UNASSIGNED'
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
# MAGIC ## Step 3b — Fair share to PRIVATE children
# MAGIC
# MAGIC Reads `pool_links`. Parent with no children stays shared.
# MAGIC Already on a valid child → keep. Sticky → not moved.
# MAGIC New / on-parent companies → Nightly average: each new lead goes to the
# MAGIC linked child with the fewest kept companies for that parent (Kelly gets
# MAGIC the next few if she is short). Does not reclaim from the larger bag.
# MAGIC `parent_pool_id` is the campaign (Retention / Past Retention / E.ON / …).
# MAGIC Apply must restamp `sourcePoolId` even when the agent bag stays the same
# MAGIC (Kelly is a child of both Retentions and Past Retentions).

# COMMAND ----------

# DBTITLE 1,snapshot pool_links
_links = jdbc_table("public.pool_links")
_links.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "crm_load.new_crm.snap_pool_links"
)
print("pool_links", _links.count())
display(_links)

# COMMAND ----------

# DBTITLE 1,fair share proposed_pool_id
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ld_before_split AS
# MAGIC SELECT * FROM crm_load.new_crm.ld_working
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE crm_load.new_crm.ld_working AS
# MAGIC WITH links AS (
# MAGIC   SELECT
# MAGIC     `parentPoolId` AS parent_pool_id,
# MAGIC     `childPoolId` AS child_pool_id
# MAGIC   FROM crm_load.new_crm.snap_pool_links
# MAGIC   WHERE COALESCE(`isActive`, true) = true
# MAGIC     AND CAST(`parentType` AS STRING) IN ('STANDARD', 'CAMPAIGN')
# MAGIC     AND CAST(`childType` AS STRING) = 'PRIVATE'
# MAGIC ),
# MAGIC members AS (
# MAGIC   SELECT parent_pool_id, child_pool_id
# MAGIC   FROM links
# MAGIC ),
# MAGIC keep AS (
# MAGIC   SELECT
# MAGIC     w.company_id,
# MAGIC     w.current_pool_id AS child_id,
# MAGIC     w.proposed_pool_id AS parent_pool_id
# MAGIC   FROM ld_before_split w
# MAGIC   INNER JOIN links l
# MAGIC     ON l.parent_pool_id = w.proposed_pool_id
# MAGIC    AND l.child_pool_id = w.current_pool_id
# MAGIC   WHERE COALESCE(w.is_protected, false) = false
# MAGIC ),
# MAGIC need AS (
# MAGIC   SELECT
# MAGIC     w.company_id,
# MAGIC     w.proposed_pool_id AS parent_pool_id,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY w.proposed_pool_id ORDER BY w.company_id) AS cand_rn
# MAGIC   FROM ld_before_split w
# MAGIC   WHERE COALESCE(w.is_protected, false) = false
# MAGIC     AND EXISTS (
# MAGIC       SELECT 1 FROM members m WHERE m.parent_pool_id = w.proposed_pool_id
# MAGIC     )
# MAGIC     AND NOT EXISTS (SELECT 1 FROM keep k WHERE k.company_id = w.company_id)
# MAGIC ),
# MAGIC bag AS (
# MAGIC   SELECT
# MAGIC     m.parent_pool_id,
# MAGIC     m.child_pool_id,
# MAGIC     COUNT(k.company_id) AS have_n
# MAGIC   FROM members m
# MAGIC   LEFT JOIN keep k
# MAGIC     ON k.parent_pool_id = m.parent_pool_id
# MAGIC    AND k.child_id = m.child_pool_id
# MAGIC   GROUP BY m.parent_pool_id, m.child_pool_id
# MAGIC ),
# MAGIC parent_need AS (
# MAGIC   SELECT parent_pool_id, COUNT(*) AS need_n
# MAGIC   FROM need
# MAGIC   GROUP BY parent_pool_id
# MAGIC ),
# MAGIC slots AS (
# MAGIC   SELECT
# MAGIC     b.parent_pool_id,
# MAGIC     b.child_pool_id,
# MAGIC     b.have_n,
# MAGIC     pe.slot_i
# MAGIC   FROM bag b
# MAGIC   INNER JOIN parent_need p
# MAGIC     ON p.parent_pool_id = b.parent_pool_id
# MAGIC    AND p.need_n > 0
# MAGIC   LATERAL VIEW EXPLODE(sequence(1, p.need_n)) pe AS slot_i
# MAGIC ),
# MAGIC ranked_slots AS (
# MAGIC   SELECT
# MAGIC     parent_pool_id,
# MAGIC     child_pool_id,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY parent_pool_id
# MAGIC       ORDER BY have_n + slot_i, child_pool_id, slot_i
# MAGIC     ) AS fill_rn
# MAGIC   FROM slots
# MAGIC ),
# MAGIC assigned AS (
# MAGIC   SELECT n.company_id, r.child_pool_id
# MAGIC   FROM need n
# MAGIC   INNER JOIN ranked_slots r
# MAGIC     ON r.parent_pool_id = n.parent_pool_id
# MAGIC    AND r.fill_rn = n.cand_rn
# MAGIC )
# MAGIC SELECT
# MAGIC   w.company_id,
# MAGIC   w.current_pool_id,
# MAGIC   w.lead_tag,
# MAGIC   w.proposed_pool_id AS parent_pool_id,
# MAGIC   CASE
# MAGIC     WHEN COALESCE(w.is_protected, false) THEN w.proposed_pool_id
# MAGIC     ELSE COALESCE(k.child_id, a.child_pool_id, w.proposed_pool_id)
# MAGIC   END AS proposed_pool_id,
# MAGIC   w.site_count,
# MAGIC   w.win_provider_id,
# MAGIC   w.win_provider_name,
# MAGIC   w.win_family,
# MAGIC   w.win_tag_code,
# MAGIC   w.win_window_days,
# MAGIC   w.win_end_date,
# MAGIC   w.win_contract_type,
# MAGIC   w.is_win_dfv,
# MAGIC   w.raw_days_left,
# MAGIC   w.days_left,
# MAGIC   w.last_deal_raw_days_left,
# MAGIC   w.last_deal_days_left,
# MAGIC   w.last_deal_days_since,
# MAGIC   w.has_any_past_deal,
# MAGIC   w.hasPastSale,
# MAGIC   w.lastDealEndDate,
# MAGIC   w.fallback_source,
# MAGIC   w.source_kind,
# MAGIC   w.mapping_source,
# MAGIC   w.source_family,
# MAGIC   w.load_source_id,
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
# MAGIC FROM ld_before_split w
# MAGIC LEFT JOIN keep k ON k.company_id = w.company_id
# MAGIC LEFT JOIN assigned a ON a.company_id = w.company_id
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT parent_pool_id, proposed_pool_id, is_protected, COUNT(*) AS companies
# MAGIC FROM crm_load.new_crm.ld_working
# MAGIC GROUP BY parent_pool_id, proposed_pool_id, is_protected
# MAGIC ORDER BY companies DESC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Write pools, then apply
# MAGIC
# MAGIC Writes `ld_apply_batch` (STANDARD parent, or PRIVATE child when linked)
# MAGIC plus `proposed_campaign_id` = parent (Retention / E.ON / …).
# MAGIC Apply moves the pool and writes that parent on
# MAGIC `company_pool_placements.sourcePoolId` (no ALTER on companies).
# MAGIC Campaign on the company is `sourcePoolId` (parent before split). No campaigns upsert.

# COMMAND ----------

# DBTITLE 1,write shared pools then apply in CRM
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
    print("batch table written")


shared_moves = spark.sql(
    """
    SELECT
      company_id,
      proposed_pool_id,
      parent_pool_id AS proposed_campaign_id
    FROM crm_load.new_crm.ld_working
    WHERE proposed_pool_id IS NOT NULL
      AND parent_pool_id IS NOT NULL
      AND (
        COALESCE(is_protected, false) = false
        OR lead_tag IN ('COMPLAINT', 'CALLBACK')
      )
    """
)
_write_apply_batch(shared_moves, "pool + campaign rows (parent or linked child)")

_applied = pg_query("SELECT public.ld_apply_batch_run() AS companies_moved")
print("apply companies_moved", _applied.collect()[0]["companies_moved"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cron = this notebook
# MAGIC
# MAGIC Databricks Job on this notebook (top to bottom):
# MAGIC **Password → Janitor → Snapshot → tag → parent → fair-share → apply.**
# MAGIC That is the nightly cron. Do **not** also schedule `25_cron.sql` in Supabase
# MAGIC or apply runs twice.
