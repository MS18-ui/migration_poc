# PySpark 2.4 job - Banking: Daily customer transaction risk & AML flags (with nested types)
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("daily_txn_risk")
         .enableHiveSupport().getOrCreate())
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

run_date = "2026-10-18"

# ---------- SOURCES ----------
# txn.counterparty : STRUCT<name, bank_bic, account_no, country>
txn = spark.table("curated.transactions").filter(F.col("txn_date") == run_date)
# accounts.attributes : MAP<STRING,STRING>
accounts = spark.table("curated.accounts")
# customers.addresses : ARRAY<STRUCT<type, line1, city, country, is_primary>>
customers = spark.table("curated.customers")
fx = (spark.read.parquet("hdfs:///data/ref/fx_rates/")
      .filter(F.col("rate_date") == run_date)
      .select(F.col("currency").alias("fx_currency"), "usd_rate"))
hrc = (spark.table("ref.high_risk_countries")
       .select(F.col("country").alias("hr_country"), "risk_level"))

# ---------- TRANSFORMATIONS ----------
# T1: customers - ARRAY<STRUCT> via higher-order functions
cust = (customers
        .withColumn("primary_address",
                    F.expr("element_at(filter(addresses, a -> a.is_primary), 1)"))
        .withColumn("residence_country", F.col("primary_address.country"))
        .withColumn("address_countries",
                    F.expr("array_distinct(transform(addresses, a -> a.country))"))
        .withColumn("multi_country_flag", F.size("address_countries") > 1)
        .select("customer_id", "segment", "residence_country",
                "address_countries", "multi_country_flag"))

# T2: accounts - MAP lookups
acct = (accounts
        .withColumn("product_code", F.col("attributes").getItem("product_code"))
        .withColumn("overdraft_limit",
                    F.col("attributes").getItem("overdraft_limit").cast("double"))
        .withColumn("is_joint", F.col("attributes").getItem("joint_holder") == "Y")
        .select("account_id", "customer_id", "product_code", "overdraft_limit", "is_joint"))

# T3: transactions - flatten STRUCT
posted = (txn.filter(F.col("status") == "POSTED").dropDuplicates(["txn_id"])
          .withColumn("cp_name",    F.col("counterparty.name"))
          .withColumn("cp_bic",     F.col("counterparty.bank_bic"))
          .withColumn("cp_country", F.col("counterparty.country")))

# T4: joins
enriched = (posted
            .join(acct, "account_id", "inner")
            .join(cust, "customer_id", "inner")
            .join(F.broadcast(fx),  F.col("currency") == F.col("fx_currency"), "left")
            .join(F.broadcast(hrc), F.col("cp_country") == F.col("hr_country"), "left"))

# T5: derived columns
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

# T6: structuring window (3-day rolling, per customer)
w3d = (Window.partitionBy("customer_id")
       .orderBy(F.col("txn_ts").cast("long"))
       .rangeBetween(-3 * 86400, 0))
enriched = (enriched
            .withColumn("near_threshold",
                        F.when(F.col("is_cash") & F.col("amount_usd").between(8000, 9999.99), 1)
                         .otherwise(0))
            .withColumn("near_thr_cnt_3d", F.sum("near_threshold").over(w3d))
            .withColumn("structuring_flag", F.col("near_thr_cnt_3d") >= 3))

# T7: build ARRAY<STRUCT> of fired rules per transaction
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
          .withColumn("fired_rules", F.expr("filter(fired_raw, r -> r is not null)")))

# T8: EXPLODE rules -> aggregate per (customer, rule) -> COLLECT back into ARRAY<STRUCT>
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

# T9: MAP<STRING,DOUBLE> exposure by currency
exposure = (scored.groupBy("customer_id", "currency")
            .agg(F.sum("amount_usd").alias("usd"))
            .groupBy("customer_id")
            .agg(F.map_from_entries(F.collect_list(F.struct("currency", "usd")))
                 .alias("exposure_by_currency")))

# T10: customer-level aggregate
base = (scored.groupBy("customer_id", "segment", "residence_country")
        .agg(F.first("address_countries").alias("address_countries"),
             F.first("multi_country_flag").alias("multi_country_flag"),
             F.count("txn_id").alias("txn_count"),
             F.sum("amount_usd").alias("total_amount_usd"),
             F.sum(F.when(F.col("is_large_txn"), 1).otherwise(0)).alias("large_txn_count"),
             F.max("structuring_flag").alias("structuring_flag"),
             F.collect_set("cp_country").alias("counterparty_countries")))

EMPTY_REASONS = ("cast(array() as array<struct<rule_id:string,hit_count:bigint,"
                 "total_amount_usd:double,max_score:int>>)")

summary = (base
           .join(reasons, "customer_id", "left")
           .join(exposure, "customer_id", "left")
           .withColumn("risk_reasons", F.expr("coalesce(risk_reasons, %s)" % EMPTY_REASONS))
           .withColumn("risk_score",
                       F.expr("CASE WHEN size(risk_reasons) = 0 THEN 10 "
                              "ELSE array_max(transform(risk_reasons, r -> r.max_score)) END"))
           .withColumn("run_date", F.lit(run_date)))

# ---------- TARGET ----------
(summary.select("customer_id", "segment", "residence_country", "address_countries",
                "multi_country_flag", "txn_count", "total_amount_usd", "large_txn_count",
                "structuring_flag", "counterparty_countries", "exposure_by_currency",
                "risk_reasons", "risk_score", "run_date")
 .write.mode("overwrite").insertInto("risk.customer_daily_risk"))
