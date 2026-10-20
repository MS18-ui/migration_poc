"""
generated_pipeline.py
PySpark 3.x translation of blueprint: daily_txn_risk
Daily customer transaction risk and AML flags with nested-type rules.

Run example:
    spark-submit generated_pipeline.py --run-date 2026-10-18
"""
import argparse
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Global configuration (from blueprint: job.parameters / job.spark_conf)
# ---------------------------------------------------------------------------
JOB_NAME = "daily_txn_risk"

JOB_PARAMETERS: Dict[str, str] = {
    "run_date": "2026-10-18",
}

SPARK_CONF: Dict[str, str] = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}

# ---------------------------------------------------------------------------
# Source schemas (declared in blueprint)
# ---------------------------------------------------------------------------
SRC_TRANSACTIONS_SCHEMA: List[Tuple[str, str]] = [
    ("txn_id", "STRING"),
    ("account_id", "STRING"),
    ("txn_date", "STRING"),
    ("txn_ts", "TIMESTAMP"),
    ("txn_type", "STRING"),
    ("status", "STRING"),
    ("amount", "DECIMAL(18,2)"),
    ("currency", "STRING"),
    ("counterparty", "STRUCT<name:STRING,bank_bic:STRING,account_no:STRING,country:STRING>"),
]

SRC_ACCOUNTS_SCHEMA: List[Tuple[str, str]] = [
    ("account_id", "STRING"),
    ("customer_id", "STRING"),
    ("attributes", "MAP<STRING,STRING>"),
]

SRC_CUSTOMERS_SCHEMA: List[Tuple[str, str]] = [
    ("customer_id", "STRING"),
    ("segment", "STRING"),
    ("addresses", "ARRAY<STRUCT<type:STRING,line1:STRING,city:STRING,country:STRING,is_primary:BOOLEAN>>"),
]

SRC_FX_RATES_SCHEMA: List[Tuple[str, str]] = [
    ("currency", "STRING"),
    ("usd_rate", "DOUBLE"),
    ("rate_date", "STRING"),
]

SRC_HIGH_RISK_COUNTRIES_SCHEMA: List[Tuple[str, str]] = [
    ("country", "STRING"),
    ("risk_level", "STRING"),
]

# ---------------------------------------------------------------------------
# Target schema (declared in blueprint; applied before insert)
# ---------------------------------------------------------------------------
TGT_CUSTOMER_DAILY_RISK_SCHEMA: List[Tuple[str, str]] = [
    ("customer_id", "STRING"),
    ("segment", "STRING"),
    ("residence_country", "STRING"),
    ("address_countries", "ARRAY<STRING>"),
    ("multi_country_flag", "BOOLEAN"),
    ("txn_count", "BIGINT"),
    ("total_amount_usd", "DOUBLE"),
    ("large_txn_count", "BIGINT"),
    ("structuring_flag", "BOOLEAN"),
    ("counterparty_countries", "ARRAY<STRING>"),
    ("exposure_by_currency", "MAP<STRING,DOUBLE>"),
    ("risk_reasons", "ARRAY<STRUCT<rule_id:STRING,hit_count:BIGINT,total_amount_usd:DOUBLE,max_score:INT>>"),
    ("risk_score", "INT"),
    ("run_date", "STRING"),
]

# AML rules for t07_scored_txn (blueprint build_array_of_struct)
# (rule_id, description, score, condition)
AML_RULES: List[Tuple[str, str, int, str]] = [
    ("R01", "Cash structuring", 90, "structuring_flag"),
    ("R02", "High-risk counterparty country", 70, "is_high_risk_cp"),
    ("R03", "Large transaction >= 10k USD", 40, "is_large_txn"),
    ("R04", "Cross-border with multi-country profile", 30, "is_cross_border AND multi_country_flag"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Every job parameter is exposed as --<param with _ replaced by ->."""
    parser = argparse.ArgumentParser(description=f"{JOB_NAME} pipeline")
    for key, default in JOB_PARAMETERS.items():
        parser.add_argument(
            f"--{key.replace('_', '-')}",
            dest=key,
            default=default,
            help=f"job parameter {key} (default: {default})",
        )
    return parser.parse_args(argv)


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .enableHiveSupport()
    )
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def project_schema(df: DataFrame, columns: List[Tuple[str, str]]) -> DataFrame:
    """Select and cast to the declared blueprint schema."""
    return df.select([F.col(name).cast(dtype).alias(name) for name, dtype in columns])


def read_hive(spark: SparkSession, obj: str, columns: List[Tuple[str, str]],
              pre_filter=None) -> DataFrame:
    df = spark.table(obj)
    if pre_filter is not None:
        df = df.filter(pre_filter)
    return project_schema(df, columns)


def read_parquet(spark: SparkSession, path: str, columns: List[Tuple[str, str]],
                 pre_filter=None) -> DataFrame:
    ddl = ", ".join(f"{name} {dtype}" for name, dtype in columns)
    df = spark.read.schema(ddl).parquet(path)
    if pre_filter is not None:
        df = df.filter(pre_filter)
    return df


def write_hive_insert_overwrite(df: DataFrame, table: str) -> None:
    """
    method: insert_into, mode: overwrite_dynamic_partition, partition_by: run_date.
    insertInto is positional, so the frame must already be in table column order
    with the partition column (run_date) last. spark.sql.sources.partitionOverwriteMode
    = dynamic ensures only the partitions present in df are replaced.
    """
    (df.write
       .mode("overwrite")
       .insertInto(table))


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run_pipeline(params: Optional[Dict[str, str]] = None) -> None:
    params = {**JOB_PARAMETERS, **(params or {})}
    run_date: str = params["run_date"]

    spark = build_spark(JOB_NAME)

    # ---- Sources --------------------------------------------------------
    # src_transactions: hive_table, pre_filter txn_date = :run_date
    df_src_transactions = read_hive(
        spark, "curated.transactions", SRC_TRANSACTIONS_SCHEMA,
        pre_filter=F.col("txn_date") == F.lit(run_date),
    )
    # src_accounts: hive_table
    df_src_accounts = read_hive(spark, "curated.accounts", SRC_ACCOUNTS_SCHEMA)
    # src_customers: hive_table
    df_src_customers = read_hive(spark, "curated.customers", SRC_CUSTOMERS_SCHEMA)
    # src_fx_rates: parquet, pre_filter rate_date = :run_date
    df_src_fx_rates = read_parquet(
        spark, "hdfs:///data/ref/fx_rates/", SRC_FX_RATES_SCHEMA,
        pre_filter=F.col("rate_date") == F.lit(run_date),
    )
    # Blueprint join condition is "currency = fx_currency" and
    # "cp_country = hr_country"; those right-side names do not exist in the
    # sources, so they are produced here by renaming the source columns.
    df_src_fx_rates = df_src_fx_rates.withColumnRenamed("currency", "fx_currency")
    # src_high_risk_countries: hive_table
    df_src_high_risk_countries = read_hive(
        spark, "ref.high_risk_countries", SRC_HIGH_RISK_COUNTRIES_SCHEMA,
    ).withColumnRenamed("country", "hr_country")

    # ---- t01_customer_profile: nested_derive ----------------------------
    df_t01_customer_profile = (
        df_src_customers
        .withColumn("primary_address",
                    F.expr("element_at(filter(addresses, a -> a.is_primary), 1)"))
        .withColumn("residence_country", F.expr("primary_address.country"))
        .withColumn("address_countries",
                    F.expr("array_distinct(transform(addresses, a -> a.country))"))
        .withColumn("multi_country_flag", F.expr("size(address_countries) > 1"))
        .select("customer_id", "segment", "residence_country",
                "address_countries", "multi_country_flag")
    )

    # ---- t02_account_profile: derive_columns ----------------------------
    df_t02_account_profile = (
        df_src_accounts
        .withColumn("product_code", F.expr("attributes['product_code']"))
        .withColumn("overdraft_limit",
                    F.expr("CAST(attributes['overdraft_limit'] AS DOUBLE)"))
        .withColumn("is_joint", F.expr("attributes['joint_holder'] = 'Y'"))
        .select("account_id", "customer_id", "product_code",
                "overdraft_limit", "is_joint")
    )

    # ---- t03_posted_txn: filter_dedupe_flatten --------------------------
    df_t03_posted_txn = (
        df_src_transactions
        .withColumn("cp_name", F.expr("counterparty.name"))
        .withColumn("cp_bic", F.expr("counterparty.bank_bic"))
        .withColumn("cp_country", F.expr("counterparty.country"))
        .filter(F.expr("status = 'POSTED'"))
        .dropDuplicates(["txn_id"])
    )

    # ---- t04_enriched_join: join ----------------------------------------
    df_t04_enriched_join = (
        df_t03_posted_txn
        .join(df_t02_account_profile, on=["account_id"], how="inner")
        .join(df_t01_customer_profile, on=["customer_id"], how="inner")
        .join(F.broadcast(df_src_fx_rates),
              F.col("currency") == F.col("fx_currency"), "left")
        .join(F.broadcast(df_src_high_risk_countries),
              F.col("cp_country") == F.col("hr_country"), "left")
    )

    # ---- t05_txn_enriched: derive_columns -------------------------------
    df_t05_txn_enriched = (
        df_t04_enriched_join
        .withColumn("amount_usd",
                    F.expr("CAST(ROUND(amount * COALESCE(usd_rate, 1.0), 2) AS DOUBLE)"))
        .withColumn("is_large_txn", F.expr("amount_usd >= 10000"))
        .withColumn("is_cash", F.expr("txn_type IN ('CASH_DEP', 'CASH_WDL')"))
        .withColumn("is_high_risk_cp", F.expr("hr_country IS NOT NULL"))
        .withColumn("is_cross_border",
                    F.expr("cp_country IS NOT NULL AND cp_country <> residence_country"))
    )

    # ---- t06_structuring_window: window ---------------------------------
    # RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW (259200 s = 3 days)
    w_structuring = (
        Window.partitionBy("customer_id")
        .orderBy(F.expr("CAST(txn_ts AS BIGINT)"))
        .rangeBetween(-259200, 0)
    )
    df_t06_structuring_window = (
        df_t05_txn_enriched
        .withColumn(
            "near_thr_cnt_3d",
            F.sum(F.expr(
                "CASE WHEN is_cash AND amount_usd BETWEEN 8000 AND 9999.99 "
                "THEN 1 ELSE 0 END"
            )).over(w_structuring),
        )
        .withColumn("structuring_flag", F.expr("near_thr_cnt_3d >= 3"))
    )

    # ---- t07_scored_txn: build_array_of_struct --------------------------
    # Each rule yields a struct when its condition holds, otherwise NULL.
    # post_filter (r IS NOT NULL) removes the NULL entries from the array.
    rule_exprs = [
        f"CASE WHEN ({cond}) THEN named_struct("
        f"'rule_id', '{rid}', 'description', '{desc}', 'score', CAST({score} AS INT)) END"
        for rid, desc, score, cond in AML_RULES
    ]
    fired_rules_expr = (
        f"filter(array({', '.join(rule_exprs)}), r -> r IS NOT NULL)"
    )
    df_t07_scored_txn = (
        df_t06_structuring_window
        .withColumn("fired_rules", F.expr(fired_rules_expr))
    )

    # ---- t08_rule_hits: explode_aggregate -------------------------------
    df_t08_rule_hits = (
        df_t07_scored_txn
        .select("customer_id", "txn_id", "amount_usd",
                F.explode("fired_rules").alias("r"))
        .filter(F.expr("r IS NOT NULL"))
        .groupBy("customer_id", F.expr("r.rule_id AS rule_id"))
        .agg(
            F.expr("COUNT(txn_id) AS hit_count"),
            F.expr("SUM(amount_usd) AS total_amount_usd"),
            F.expr("MAX(r.score) AS max_score"),
        )
    )

    # ---- t09_customer_reasons: collect_to_array_of_struct ---------------
    df_t09_customer_reasons = (
        df_t08_rule_hits
        .groupBy("customer_id")
        .agg(F.expr(
            "SORT_ARRAY(COLLECT_LIST(STRUCT(rule_id, hit_count, "
            "total_amount_usd, max_score))) AS risk_reasons"
        ))
    )

    # ---- t10_currency_exposure: collect_to_map --------------------------
    df_t10_currency_usd = (
        df_t07_scored_txn
        .groupBy("customer_id", "currency")
        .agg(F.expr("SUM(amount_usd) AS usd"))
    )
    df_t10_currency_exposure = (
        df_t10_currency_usd
        .groupBy("customer_id")
        .agg(F.expr(
            "MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(currency, usd))) "
            "AS exposure_by_currency"
        ))
    )

    # ---- t11_customer_base: aggregate -----------------------------------
    df_t11_customer_base = (
        df_t07_scored_txn
        .groupBy("customer_id", "segment", "residence_country")
        .agg(
            F.expr("FIRST(address_countries) AS address_countries"),
            F.expr("FIRST(multi_country_flag) AS multi_country_flag"),
            F.expr("COUNT(txn_id) AS txn_count"),
            F.expr("SUM(amount_usd) AS total_amount_usd"),
            F.expr("SUM(CASE WHEN is_large_txn THEN 1 ELSE 0 END) AS large_txn_count"),
            F.expr("MAX(structuring_flag) AS structuring_flag"),
            F.expr("COLLECT_SET(cp_country) AS counterparty_countries"),
        )
    )

    # ---- t12_customer_risk_summary: join_derive -------------------------
    df_t12_customer_risk_summary = (
        df_t11_customer_base
        .join(df_t09_customer_reasons, on=["customer_id"], how="left")
        .join(df_t10_currency_exposure, on=["customer_id"], how="left")
        .withColumn(
            "risk_reasons",
            F.expr("COALESCE(risk_reasons, CAST(ARRAY() AS ARRAY<STRUCT<"
                   "rule_id:STRING,hit_count:BIGINT,total_amount_usd:DOUBLE,"
                   "max_score:INT>>))"),
        )
        .withColumn(
            "risk_score",
            F.expr("CASE WHEN SIZE(risk_reasons) = 0 THEN 10 "
                   "ELSE ARRAY_MAX(TRANSFORM(risk_reasons, r -> r.max_score)) END"),
        )
        .withColumn("run_date", F.lit(run_date).cast("string"))
    )

    # ---- tgt_customer_daily_risk: hive_table target ---------------------
    # Project to the declared target schema; run_date is the partition column
    # and must be last for insertInto.
    df_tgt_customer_daily_risk = project_schema(
        df_t12_customer_risk_summary, TGT_CUSTOMER_DAILY_RISK_SCHEMA
    )
    write_hive_insert_overwrite(df_tgt_customer_daily_risk, "risk.customer_daily_risk")

    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    args = parse_args()
    run_pipeline({key: getattr(args, key) for key in JOB_PARAMETERS})
