"""
daily_txn_risk - Daily customer transaction risk and AML flags (PySpark 3.x).

Generated from the daily_txn_risk YAML blueprint.
Run example:
    spark-submit daily_txn_risk.py --run-date 2026-10-18
"""

import argparse

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Job parameters and global configuration (from blueprint job / spark_conf)
# ---------------------------------------------------------------------------
JOB_NAME = "daily_txn_risk"
JOB_PARAMETERS = {"run_date": "2026-10-18"}

GLOBAL_CONFIGS = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    # Ensures dynamic partition overwrite also works when inserting into Hive tables.
    "spark.hadoop.hive.exec.dynamic.partition": "true",
    "spark.hadoop.hive.exec.dynamic.partition.mode": "nonstrict",
}

TARGET_TABLE = "risk.customer_daily_risk"


def build_spark(app_name: str = JOB_NAME) -> SparkSession:
    """Create the SparkSession and apply global configs on the builder."""
    builder = SparkSession.builder.appName(app_name).enableHiveSupport()
    for key, value in GLOBAL_CONFIGS.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def bind_sql(expr: str, params: dict) -> str:
    """Substitute :name placeholders with quoted literal values."""
    for key, value in params.items():
        expr = expr.replace(f":{key}", f"'{value}'")
    return expr


def conform(df: DataFrame, spec: list) -> DataFrame:
    """
    Select, cast and rename columns according to the blueprint schema.
    spec: list of (column, spark_type_or_None, rename_to_or_None).
    Complex types (STRUCT / ARRAY / MAP) are passed through with dtype None.
    """
    cols = []
    for name, dtype, rename in spec:
        c = F.col(name)
        if dtype is not None:
            c = c.cast(dtype)
        cols.append(c.alias(rename or name))
    return df.select(cols)


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------
def read_txn(spark: SparkSession, params: dict) -> DataFrame:
    df = spark.table("curated.transactions").where(bind_sql("txn_date = :run_date", params))
    return conform(df, [
        ("txn_id", "string", None),
        ("account_id", "string", None),
        ("txn_date", "date", None),
        ("txn_ts", "timestamp", None),
        ("txn_type", "string", None),
        ("status", "string", None),
        ("currency", "string", None),
        ("amount", "decimal(18,2)", None),
        ("counterparty", None, None),  # STRUCT(name, bank_bic, account_no, country)
    ])


def read_accounts(spark: SparkSession) -> DataFrame:
    df = spark.table("curated.accounts")
    return conform(df, [
        ("account_id", "string", None),
        ("customer_id", "string", None),
        ("attributes", None, None),  # MAP<STRING,STRING>
    ])


def read_customers(spark: SparkSession) -> DataFrame:
    df = spark.table("curated.customers")
    return conform(df, [
        ("customer_id", "string", None),
        ("segment", "string", None),
        ("addresses", None, None),  # ARRAY<STRUCT<type,line1,city,country,is_primary>>
    ])


def read_fx(spark: SparkSession, params: dict) -> DataFrame:
    df = spark.read.parquet("hdfs:///data/ref/fx_rates/").where(
        bind_sql("rate_date = :run_date", params)
    )
    return conform(df, [
        ("currency", "string", "fx_currency"),
        ("usd_rate", "double", None),
        ("rate_date", "date", None),
    ])


def read_high_risk_countries(spark: SparkSession) -> DataFrame:
    df = spark.table("ref.high_risk_countries")
    return conform(df, [
        ("country", "string", "hr_country"),
        ("risk_level", "string", None),
    ])


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------
def build_cust_t1(df_customers: DataFrame) -> DataFrame:
    """nested_derive: residence country, distinct address countries, multi-country flag."""
    df = (
        df_customers
        .withColumn(
            "residence_country",
            F.expr("element_at(filter(addresses, a -> a.is_primary), 1).country"),
        )
        .withColumn(
            "address_countries",
            F.expr("array_distinct(transform(addresses, a -> a.country))"),
        )
        .withColumn(
            "multi_country_flag",
            F.expr("size(array_distinct(transform(addresses, a -> a.country))) > 1"),
        )
    )
    return df.select(
        "customer_id", "segment", "residence_country",
        "address_countries", "multi_country_flag",
    )


def build_acct_t2(df_accounts: DataFrame) -> DataFrame:
    """derive_columns: product code, overdraft limit, joint-holder flag from MAP attributes."""
    df = (
        df_accounts
        .withColumn("product_code", F.expr("attributes['product_code']"))
        .withColumn("overdraft_limit", F.expr("CAST(attributes['overdraft_limit'] AS DOUBLE)"))
        .withColumn("is_joint", F.expr("attributes['joint_holder'] = 'Y'"))
    )
    return df.select(
        "account_id", "customer_id", "product_code", "overdraft_limit", "is_joint",
    )


def build_posted_t3(df_txn: DataFrame) -> DataFrame:
    """filter_dedupe_flatten: keep POSTED, dedupe on txn_id, flatten counterparty."""
    df = df_txn.where(F.expr("status = 'POSTED'"))
    df = df.dropDuplicates(["txn_id"])
    return (
        df
        .withColumn("cp_name", F.col("counterparty.name"))
        .withColumn("cp_bic", F.col("counterparty.bank_bic"))
        .withColumn("cp_country", F.col("counterparty.country"))
    )


def build_enriched_t4(
    df_posted: DataFrame,
    df_acct: DataFrame,
    df_cust: DataFrame,
    df_fx: DataFrame,
    df_hrc: DataFrame,
) -> DataFrame:
    """join: posted -> accounts (inner) -> customers (inner) -> fx (left, broadcast) -> hrc (left, broadcast)."""
    df = df_posted.join(df_acct, on="account_id", how="inner")
    df = df.join(df_cust, on="customer_id", how="inner")
    df = df.join(F.broadcast(df_fx), F.col("currency") == F.col("fx_currency"), "left")
    df = df.join(F.broadcast(df_hrc), F.col("cp_country") == F.col("hr_country"), "left")
    return df


def build_amounts_t5(df: DataFrame) -> DataFrame:
    """derive_columns: USD amount and AML indicator flags."""
    return (
        df
        .withColumn(
            "amount_usd",
            F.expr("CAST(ROUND(amount * COALESCE(usd_rate, 1.0), 2) AS DOUBLE)"),
        )
        .withColumn("is_large_txn", F.expr("amount_usd >= 10000"))
        .withColumn("is_cash", F.expr("txn_type IN ('CASH_DEP', 'CASH_WDL')"))
        .withColumn("is_high_risk_cp", F.expr("hr_country IS NOT NULL"))
        .withColumn(
            "is_cross_border",
            F.expr("cp_country IS NOT NULL AND cp_country <> residence_country"),
        )
    )


def build_near_t6(df: DataFrame) -> DataFrame:
    """derive_columns: near-threshold cash transactions (8,000 - 9,999.99 USD)."""
    return df.withColumn(
        "near_threshold",
        F.expr(
            "CASE WHEN is_cash AND amount_usd BETWEEN 8000 AND 9999.99 THEN 1 ELSE 0 END"
        ),
    )


def build_window_t7(df: DataFrame) -> DataFrame:
    """window: rolling 3-day (259200 s) count of near-threshold cash txns per customer."""
    w = (
        Window.partitionBy("customer_id")
        .orderBy(F.expr("CAST(txn_ts AS BIGINT)"))
        .rangeBetween(-259200, Window.currentRow)
    )
    return df.withColumn("near_thr_cnt_3d", F.sum("near_threshold").over(w))


def build_flags_t8(df: DataFrame) -> DataFrame:
    """derive_columns: structuring flag."""
    return df.withColumn("structuring_flag", F.expr("near_thr_cnt_3d >= 3"))


RULES = [
    # (rule_id, condition, description, score)
    ("R01", "structuring_flag", "Cash structuring", 90),
    ("R02", "is_high_risk_cp", "High-risk counterparty country", 70),
    ("R03", "is_large_txn", "Large transaction >= 10k USD", 40),
    ("R04", "is_cross_border AND multi_country_flag",
     "Cross-border with multi-country profile", 30),
]


def build_rules_t9(df: DataFrame) -> DataFrame:
    """
    build_array_of_struct: fired_rules = array of (rule_id, description, score) for
    every rule whose condition is TRUE; NULL elements are removed.
    """
    elements = [
        f"CASE WHEN coalesce(({cond}), false) THEN "
        f"named_struct('rule_id', '{rid}', 'description', '{desc}', 'score', {score}) "
        f"END"
        for rid, cond, desc, score in RULES
    ]
    array_expr = "filter(array(" + ", ".join(elements) + "), x -> x IS NOT NULL)"
    return df.withColumn("fired_rules", F.expr(array_expr))


def build_rule_rows_t10(df: DataFrame) -> DataFrame:
    """explode_aggregate: one row per (customer, rule) with hit count, amount and max score."""
    exploded = (
        df.select("customer_id", "txn_id", "amount_usd",
                  F.explode("fired_rules").alias("r"))
        .where(F.expr("r IS NOT NULL"))  # post_filter: r IS NOT NULL
    )
    return (
        exploded
        .groupBy("customer_id", F.col("r.rule_id").alias("rule_id"))
        .agg(
            F.count("txn_id").alias("hit_count"),
            F.sum("amount_usd").alias("total_amount_usd"),
            F.max("r.score").alias("max_score"),
        )
    )


def build_reasons_t11(df: DataFrame) -> DataFrame:
    """collect_to_array_of_struct: sorted array of risk reasons per customer."""
    return df.groupBy("customer_id").agg(
        F.expr(
            "sort_array(collect_list(struct(rule_id, hit_count, total_amount_usd, max_score)))"
        ).alias("risk_reasons")
    )


def build_exposure_t12(df: DataFrame) -> DataFrame:
    """collect_to_map: USD exposure per currency, as MAP<STRING,DOUBLE> per customer."""
    usd_by_currency = (
        df.groupBy("customer_id", "currency")
        .agg(F.sum("amount_usd").alias("usd"))
    )
    # Null currency cannot be a map key; such rows are excluded from the exposure map.
    return (
        usd_by_currency
        .where(F.col("currency").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.expr(
                "map_from_entries(collect_list(struct(currency, usd)))"
            ).alias("exposure_by_currency")
        )
    )


def build_base_t13(df: DataFrame) -> DataFrame:
    """aggregate: customer x segment x residence-country base metrics."""
    return df.groupBy("customer_id", "segment", "residence_country").agg(
        F.expr("first(address_countries)").alias("address_countries"),
        F.expr("first(multi_country_flag)").alias("multi_country_flag"),
        F.expr("count(txn_id)").alias("txn_count"),
        F.expr("sum(amount_usd)").alias("total_amount_usd"),
        F.expr("sum(CASE WHEN is_large_txn THEN 1 ELSE 0 END)").alias("large_txn_count"),
        F.expr("max(structuring_flag)").alias("structuring_flag"),
        F.expr("collect_set(cp_country)").alias("counterparty_countries"),
    )


def build_summary_t14(
    df_base: DataFrame, df_reasons: DataFrame, df_exposure: DataFrame
) -> DataFrame:
    """join: base LEFT JOIN reasons LEFT JOIN exposure on customer_id."""
    df = df_base.join(df_reasons, on="customer_id", how="left")
    df = df.join(df_exposure, on="customer_id", how="left")
    return df


def build_summary_t15(df: DataFrame, run_date: str) -> DataFrame:
    """derive_columns: default empty reasons, risk score, run_date."""
    empty_reasons_type = (
        "ARRAY<STRUCT<rule_id:STRING,hit_count:BIGINT,"
        "total_amount_usd:DOUBLE,max_score:INT>>"
    )
    return (
        df
        .withColumn(
            "risk_reasons",
            F.expr(f"coalesce(risk_reasons, CAST(array() AS {empty_reasons_type}))"),
        )
        .withColumn(
            "risk_score",
            F.expr(
                "CASE WHEN size(risk_reasons) = 0 THEN 10 "
                "ELSE array_max(transform(risk_reasons, r -> r.max_score)) END"
            ),
        )
        .withColumn("run_date", F.lit(run_date))
    )


# ---------------------------------------------------------------------------
# Target
# ---------------------------------------------------------------------------
TARGET_SCHEMA = [
    ("customer_id", "string"),
    ("segment", "string"),
    ("residence_country", "string"),
    ("address_countries", "array<string>"),
    ("multi_country_flag", "boolean"),
    ("txn_count", "bigint"),
    ("total_amount_usd", "double"),
    ("large_txn_count", "bigint"),
    ("structuring_flag", "boolean"),
    ("counterparty_countries", "array<string>"),
    ("exposure_by_currency", "map<string,double>"),
    ("risk_reasons",
     "array<struct<rule_id:string,hit_count:bigint,total_amount_usd:double,max_score:int>>"),
    ("risk_score", "int"),
    ("run_date", "string"),  # partition column, must be last for insertInto
]


def write_customer_daily_risk(df: DataFrame) -> DataFrame:
    """
    hive_table target: overwrite_dynamic_partition via insert_into, partitioned by run_date.
    insertInto matches columns by position, so the projection follows the table schema order.
    """
    projected = df.select([F.col(name).cast(dtype).alias(name) for name, dtype in TARGET_SCHEMA])
    (
        projected.write
        .mode("overwrite")          # dynamic partition overwrite (see partitionOverwriteMode)
        .insertInto(TARGET_TABLE)
    )
    return projected


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run_pipeline(run_date: str = JOB_PARAMETERS["run_date"]) -> None:
    params = {"run_date": run_date}
    spark = build_spark(JOB_NAME)
    try:
        # Sources
        df_txn = read_txn(spark, params)
        df_accounts = read_accounts(spark)
        df_customers = read_customers(spark)
        df_fx = read_fx(spark, params)
        df_hrc = read_high_risk_countries(spark)

        # Transformations
        df_cust_t1 = build_cust_t1(df_customers)
        df_acct_t2 = build_acct_t2(df_accounts)
        df_posted_t3 = build_posted_t3(df_txn)
        df_enriched_t4 = build_enriched_t4(df_posted_t3, df_acct_t2, df_cust_t1, df_fx, df_hrc)
        df_amounts_t5 = build_amounts_t5(df_enriched_t4)
        df_near_t6 = build_near_t6(df_amounts_t5)
        df_window_t7 = build_window_t7(df_near_t6)
        df_flags_t8 = build_flags_t8(df_window_t7)
        df_rules_t9 = build_rules_t9(df_flags_t8)
        df_rule_rows_t10 = build_rule_rows_t10(df_rules_t9)
        df_reasons_t11 = build_reasons_t11(df_rule_rows_t10)
        df_exposure_t12 = build_exposure_t12(df_rules_t9)
        df_base_t13 = build_base_t13(df_rules_t9)
        df_summary_t14 = build_summary_t14(df_base_t13, df_reasons_t11, df_exposure_t12)
        df_summary_t15 = build_summary_t15(df_summary_t14, run_date)

        # Target
        df_tgt_customer_daily_risk = write_customer_daily_risk(df_summary_t15)
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=JOB_NAME)
    parser.add_argument(
        "--run-date",
        default=JOB_PARAMETERS["run_date"],
        help="Business run date (YYYY-MM-DD). Overrides job.parameters.run_date.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(run_date=args.run_date)
