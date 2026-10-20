# PySpark 3.1+ job - Banking: Daily customer transaction risk & AML flags (nested types)
# Uses native Python higher-order functions (F.filter / F.transform) available from Spark 3.1.
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DB = "aahhd01q_pnc_poc"
# Works in IPython/Jupyter and spark-submit: set RUN_DATE env var or edit the default
run_date = os.environ.get("RUN_DATE", "2026-10-18")

spark = (
    SparkSession.builder
    .appName("daily_txn_risk")
    .config("spark.sql.catalogImplementation", "hive")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://172.16.0.4:9000")
    .config("spark.sql.warehouse.dir", "hdfs://172.16.0.4:9000/user/hive/warehouse")
    .config("hive.metastore.uris", "thrift://172.16.0.4:9083")
    .config("spark.sql.hive.metastore.version", "4.1.0")
    .config("spark.sql.hive.metastore.jars", "maven")
    .enableHiveSupport()
    .getOrCreate()
)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# ---------- SOURCES ----------
txn       = spark.table(f"{DB}.transactions").filter(F.col("txn_date") == run_date)
accounts  = spark.table(f"{DB}.accounts")
customers = spark.table(f"{DB}.customers")
fx  = (spark.table(f"{DB}.fx_rates").filter(F.col("rate_date") == run_date)
       .select(F.col("currency").alias("fx_currency"), "usd_rate"))
hrc = (spark.table(f"{DB}.high_risk_countries")
       .select(F.col("country").alias("hr_country"), "risk_level"))

# ---------- T1: customers - ARRAY<STRUCT> ----------
cust = (customers
        .withColumn("primary_address",
                    F.element_at(F.filter("addresses", lambda a: a["is_primary"]), 1))
        .withColumn("residence_country", F.col("primary_address.country"))
        .withColumn("address_countries",
                    F.array_distinct(F.transform("addresses", lambda a: a["country"])))
        .withColumn("multi_country_flag", F.size("address_countries") > 1)
        .select("customer_id", "segment", "residence_country",
                "address_countries", "multi_country_flag"))

# ---------- T2: accounts - MAP ----------
acct = (accounts
        .withColumn("product_code", F.col("attributes")["product_code"])
        .withColumn("overdraft_limit", F.col("attributes")["overdraft_limit"].cast("double"))
        .withColumn("is_joint", F.col("attributes")["joint_holder"] == "Y")
        .select("account_id", "customer_id", "product_code", "overdraft_limit", "is_joint"))

# ---------- T3: transactions - flatten STRUCT ----------
posted = (txn.filter(F.col("status") == "POSTED").dropDuplicates(["txn_id"])
          .withColumn("cp_name",    F.col("counterparty.name"))
          .withColumn("cp_bic",     F.col("counterparty.bank_bic"))
          .withColumn("cp_country", F.col("counterparty.country")))

# ---------- T4: joins ----------
enriched = (posted
            .join(acct, "account_id", "inner")
            .join(cust, "customer_id", "inner")
            .join(F.broadcast(fx),  F.col("currency") == F.col("fx_currency"), "left")
            .join(F.broadcast(hrc), F.col("cp_country") == F.col("hr_country"), "left"))

# ---------- T5: derived columns ----------
enriched = (enriched
            .withColumn("amount_usd",
                        F.round(F.col("amount") * F.coalesce(F.col("usd_rate"), F.lit(1.0)), 2)
                         .cast("double"))
            .withColumn("is_large_txn", F.col("amount_usd") >= 10000)
            .withColumn("is_cash", F.col("txn_type").isin("CASH_DEP", "CASH_WDL"))
            .withColumn("is_high_risk_cp", F.col("hr_country").isNotNull())
            .withColumn("is_cross_border",
                        F.col("cp_country").isNotNull() &
                        (F.col("cp_country") != F.col("residence_country"))))

# ---------- T6: structuring window (3-day rolling per customer) ----------
w3d = (Window.partitionBy("customer_id")
       .orderBy(F.col("txn_ts").cast("long"))
       .rangeBetween(-3 * 86400, 0))
enriched = (enriched
            .withColumn("near_threshold",
                        F.when(F.col("is_cash") & F.col("amount_usd").between(8000, 9999.99), 1)
                         .otherwise(0))
            .withColumn("near_thr_cnt_3d", F.sum("near_threshold").over(w3d))
            .withColumn("structuring_flag", F.col("near_thr_cnt_3d") >= 3))

# ---------- T7: ARRAY<STRUCT> of fired rules per transaction ----------
def rule(cond, rid, desc, score):
    return F.when(cond, F.struct(F.lit(rid).alias("rule_id"),
                                 F.lit(desc).alias("description"),
                                 F.lit(score).alias("score")))

scored = (enriched
          .withColumn("fired_raw", F.array(
              rule(F.col("structuring_flag"), "R01", "Cash structuring", 90),
              rule(F.col("is_high_risk_cp"),  "R02", "High-risk counterparty country", 70),
              rule(F.col("is_large_txn"),     "R03", "Large transaction >= 10k USD", 40),
              rule(F.col("is_cross_border") & F.col("multi_country_flag"),
                   "R04", "Cross-border with multi-country profile", 30)))
          .withColumn("fired_rules", F.filter("fired_raw", lambda r: r.isNotNull())))

# ---------- T8: explode -> aggregate -> collect back to ARRAY<STRUCT> ----------
rule_rows = (scored
             .select("customer_id", "txn_id", "amount_usd", F.explode("fired_rules").alias("r"))
             .groupBy("customer_id", F.col("r.rule_id").alias("rule_id"))
             .agg(F.count("txn_id").alias("hit_count"),
                  F.sum("amount_usd").alias("total_amount_usd"),
                  F.max("r.score").alias("max_score")))

reasons = (rule_rows.groupBy("customer_id")
           .agg(F.sort_array(F.collect_list(
                    F.struct("rule_id", "hit_count", "total_amount_usd", "max_score")))
                .alias("risk_reasons")))

# ---------- T9: MAP<STRING,DOUBLE> exposure by currency ----------
exposure = (scored.groupBy("customer_id", "currency")
            .agg(F.sum("amount_usd").alias("usd"))
            .groupBy("customer_id")
            .agg(F.map_from_entries(F.collect_list(F.struct("currency", "usd")))
                 .alias("exposure_by_currency")))

# ---------- T10: customer-level aggregate ----------
base = (scored.groupBy("customer_id", "segment", "residence_country")
        .agg(F.first("address_countries").alias("address_countries"),
             F.first("multi_country_flag").alias("multi_country_flag"),
             F.count("txn_id").alias("txn_count"),
             F.sum("amount_usd").alias("total_amount_usd"),
             F.sum(F.when(F.col("is_large_txn"), 1).otherwise(0)).alias("large_txn_count"),
             F.max("structuring_flag").alias("structuring_flag"),
             F.collect_set("cp_country").alias("counterparty_countries")))

REASONS_T = "array<struct<rule_id:string,hit_count:bigint,total_amount_usd:double,max_score:int>>"

summary = (base
           .join(reasons, "customer_id", "left")
           .join(exposure, "customer_id", "left")
           .withColumn("risk_reasons",
                       F.coalesce("risk_reasons", F.array().cast(REASONS_T)))
           .withColumn("risk_score",
                       F.when(F.size("risk_reasons") == 0, F.lit(10))
                        .otherwise(F.array_max(F.transform("risk_reasons", lambda r: r["max_score"]))))
           .withColumn("run_date", F.lit(run_date)))

# ---------- TARGET ----------
(summary.select("customer_id", "segment", "residence_country", "address_countries",
                "multi_country_flag", "txn_count", "total_amount_usd", "large_txn_count",
                "structuring_flag", "counterparty_countries", "exposure_by_currency",
                "risk_reasons", "risk_score", "run_date")
 .write.mode("overwrite").insertInto(f"{DB}.customer_daily_risk"))
