#!/usr/bin/env python3
"""
generated_pipeline.py

PySpark 3.x translation of blueprint pyspark2_daily_txn_risk (banking_aml).

GATES (fail-fast, see blueprint findings):
  F-002  deterministic dedupe ordering      -> --dedupe-order-by (required)
  F-008  target partition spec              -> --target-partition-by (required)
  F-008  overwrite scope approval           -> --overwrite-scope-approved (required)
  F-015  target format / Iceberg mapping    -> --target-format iceberg (required)
The job refuses to read or write data until these are supplied by the operator.
Not verified by this script: F-003 metadata/schema, F-014 Spark 3 construct support.
Runtime checks: F-004 key uniqueness, F-010 NULL currency, F-008 empty output.
"""
import argparse
import logging
import re
import sys

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

JOB_NAME = "pyspark2_daily_txn_risk"
DEFAULT_RUN_DATE = "2026-10-18"                  # F-016: scheduler should supply this
DEFAULT_FX_PATH = "hdfs:///data/ref/fx_rates/"   # F-016: confirm per environment
TARGET_TABLE = "risk.customer_daily_risk"

GLOBAL_CONFIGS = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}

TARGET_COLUMNS = [
    "customer_id", "segment", "residence_country", "address_countries",
    "multi_country_flag", "txn_count", "total_amount_usd", "large_txn_count",
    "structuring_flag", "counterparty_countries", "exposure_by_currency",
    "risk_reasons", "risk_score", "run_date",
]

log = logging.getLogger(JOB_NAME)


class BlockedFindingError(RuntimeError):
    """A blocking blueprint finding has not been resolved by the operator."""


class DataQualityError(RuntimeError):
    """A runtime data check failed."""


# ---------------------------------------------------------------- arguments
def parse_list(text):
    if not text:
        return []
    return [x.strip() for x in text.split(",") if x.strip()]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=JOB_NAME)
    # job.parameters
    p.add_argument("--run-date", default=DEFAULT_RUN_DATE,
                   help="Business date YYYY-MM-DD (job parameter run_date).")
    # source configuration
    p.add_argument("--fx-path", default=DEFAULT_FX_PATH,
                   help="FX rates parquet path (F-016).")
    # gates
    p.add_argument("--dedupe-order-by", default=None,
                   help="F-002: comma list of col[:asc|desc] defining the winner per txn_id. "
                        "Must end with a unique tie-breaker column.")
    p.add_argument("--target-format", default=None,
                   help="F-015: target format; must be 'iceberg' once mapping is confirmed.")
    p.add_argument("--target-partition-by", default=None,
                   help="F-008: comma list of target partition columns.")
    p.add_argument("--overwrite-scope-approved", action="store_true",
                   help="F-008: set only after the overwrite scope is approved.")
    return p.parse_args(argv)


def validate_run_date(run_date):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date or ""):
        raise ValueError(f"run_date must be YYYY-MM-DD, got {run_date!r}")


def require_gates(args):
    problems = []
    if not args.dedupe_order_by:
        problems.append("F-002: --dedupe-order-by not supplied (no arbitrary dedupe).")
    if args.target_format != "iceberg":
        problems.append("F-015: --target-format must be 'iceberg' (mapping unconfirmed).")
    if not parse_list(args.target_partition_by):
        problems.append("F-008: --target-partition-by not supplied (overwrite scope unknown).")
    if not args.overwrite_scope_approved:
        problems.append("F-008: --overwrite-scope-approved not set.")
    if problems:
        raise BlockedFindingError("Pipeline blocked by open findings:\n  - " + "\n  - ".join(problems))


def parse_order_spec(spec):
    """Return list of (column_name, Column) ordering pairs."""
    pairs = []
    for item in parse_list(spec):
        name, _, direction = item.partition(":")
        direction = (direction or "asc").strip().lower()
        if direction not in ("asc", "desc"):
            raise ValueError(f"bad sort direction in {item!r}")
        col = F.col(name.strip())
        pairs.append((name.strip(), col.desc() if direction == "desc" else col.asc()))
    if not pairs:
        raise ValueError("dedupe order spec is empty")
    return pairs


# ---------------------------------------------------------------- helpers
def assert_unique(df, cols, label):
    """F-004: duplicate-key check; non-unique keys would multiply rows."""
    dups = df.groupBy(*cols).count().filter(F.col("count") > 1).limit(1).count()
    if dups:
        raise DataQualityError(f"{label}: duplicate key values on {cols}")


def spark_session():
    builder = SparkSession.builder.appName(JOB_NAME).enableHiveSupport()
    for k, v in GLOBAL_CONFIGS.items():
        builder = builder.config(k, v)
    return builder.getOrCreate()


# ---------------------------------------------------------------- sources
def read_sources(spark, run_date, fx_path):
    # src_transactions: pre_filter txn_date = :run_date (schema unverified, F-003)
    df_src_transactions = (spark.table("curated.transactions")
                           .filter(F.col("txn_date") == F.lit(run_date)))
    df_src_accounts = spark.table("curated.accounts")
    df_src_customers = spark.table("curated.customers")
    # src_fx: pre_filter rate_date = :run_date
    df_src_fx = (spark.read.parquet(fx_path)
                 .filter(F.col("rate_date") == F.lit(run_date)))
    df_src_hrc = spark.table("ref.high_risk_countries")
    return df_src_transactions, df_src_accounts, df_src_customers, df_src_fx, df_src_hrc


# ---------------------------------------------------------------- transformations
def t01_cust_profile(customers):
    # F-006: zero primaries -> NULL residence_country; multiple primaries -> first in array order
    # F-013: size() on NULL depends on spark.sql.legacy.sizeOfNull
    df = (customers
          .withColumn("primary_address", F.expr("element_at(filter(addresses, a -> a.is_primary), 1)"))
          .withColumn("residence_country", F.expr("primary_address.country"))
          .withColumn("address_countries", F.expr("array_distinct(transform(addresses, a -> a.country))"))
          .withColumn("multi_country_flag", F.expr("size(address_countries) > 1")))
    return df.select("customer_id", "segment", "residence_country",
                     "address_countries", "multi_country_flag")


def t02_acct_attrs(accounts):
    df = (accounts
          .withColumn("product_code", F.expr("attributes['product_code']"))
          .withColumn("overdraft_limit", F.expr("CAST(attributes['overdraft_limit'] AS DOUBLE)"))
          .withColumn("is_joint", F.expr("attributes['joint_holder'] = 'Y'")))
    return df.select("account_id", "customer_id", "product_code", "overdraft_limit", "is_joint")


def t03_posted_txn(txn, order_spec):
    # F-017: counterparty account_no is not carried forward
    df = (txn
          .filter(F.col("status") == F.lit("POSTED"))
          .withColumn("cp_name", F.expr("counterparty.name"))
          .withColumn("cp_bic", F.expr("counterparty.bank_bic"))
          .withColumn("cp_country", F.expr("counterparty.country")))
    # F-002: deterministic winner per txn_id; fail if declared ordering still leaves ties
    names = [n for n, _ in order_spec]
    ties = (df.groupBy("txn_id", *names).count()
              .filter(F.col("count") > 1).limit(1).count())
    if ties:
        raise DataQualityError("F-002: ties remain under the declared dedupe ordering; "
                               "add a unique final tie-breaker")
    w = Window.partitionBy("txn_id").orderBy(*[c for _, c in order_spec])
    df = (df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn"))
    return df


def t04_enriched_join(t03, t02, t01, fx, hrc):
    # F-004 key-uniqueness checks
    assert_unique(t02, ["account_id"], "t02_acct_attrs")
    assert_unique(t01, ["customer_id"], "t01_cust_profile")
    assert_unique(fx, ["fx_currency"], "src_fx (rate_date filtered)")
    assert_unique(hrc, ["country"], "src_hrc")
    hrc_ref = hrc.withColumnRenamed("country", "hr_country")
    df = t03.join(t02, on="account_id", how="inner")            # F-005: inner drops
    df = df.join(t01, on="customer_id", how="inner")            # F-005: inner drops
    df = df.join(F.broadcast(fx), F.col("currency") == F.col("fx_currency"), "left")   # F-019
    df = df.join(F.broadcast(hrc_ref), F.col("cp_country") == F.col("hr_country"), "left")
    return df


def t05_amounts(df):
    # F-001: missing FX preserved as rate 1.0 (business decision pending); logged, not changed.
    # F-011: ROUND then CAST to DOUBLE preserved as written.
    missing = df.filter(F.col("usd_rate").isNull()).count()
    log.warning("F-001: %d rows with NULL usd_rate are treated as rate 1.0", missing)
    return (df
            .withColumn("amount_usd", F.expr("CAST(ROUND(amount * COALESCE(usd_rate, 1.0), 2) AS DOUBLE)"))
            .withColumn("is_large_txn", F.expr("amount_usd >= 10000"))
            .withColumn("is_cash", F.expr("txn_type IN ('CASH_DEP', 'CASH_WDL')"))
            .withColumn("is_high_risk_cp", F.expr("hr_country IS NOT NULL"))
            .withColumn("is_cross_border", F.expr("cp_country IS NOT NULL AND cp_country <> residence_country")))


def t06_near_threshold(df):
    return df.withColumn("near_threshold", F.expr(
        "CASE WHEN is_cash AND amount_usd BETWEEN 8000 AND 9999.99 THEN 1 ELSE 0 END"))


def t07_structuring_window(df):
    # F-009: RANGE 3*86400 seconds, inclusive of current row; ordering truncated to seconds
    w = (Window.partitionBy("customer_id")
               .orderBy(F.expr("CAST(txn_ts AS BIGINT)"))
               .rangeBetween(-3 * 86400, 0))
    return df.withColumn("near_thr_cnt_3d", F.sum("near_threshold").over(w))


def t08_structuring_flag(df):
    return df.withColumn("structuring_flag", F.expr("near_thr_cnt_3d >= 3"))


def t09_rules(df):
    fired_raw = F.expr(
        "array("
        "CASE WHEN structuring_flag THEN named_struct('rule_id', 'R01', 'description', "
        "'Cash structuring', 'score', 90) END, "
        "CASE WHEN is_high_risk_cp THEN named_struct('rule_id', 'R02', 'description', "
        "'High-risk counterparty country', 'score', 70) END, "
        "CASE WHEN is_large_txn THEN named_struct('rule_id', 'R03', 'description', "
        "'Large transaction >= 10k USD', 'score', 40) END, "
        "CASE WHEN is_cross_border AND multi_country_flag THEN named_struct('rule_id', 'R04', "
        "'description', 'Cross-border with multi-country profile', 'score', 30) END)")
    df = df.withColumn("fired_raw", fired_raw)
    return df.withColumn("fired_rules", F.expr("filter(fired_raw, r -> r IS NOT NULL)"))


def t10_rule_hits(df):
    # Non-outer explode: transactions with no fired rules drop out (as in source)
    exploded = (df.select("customer_id", "txn_id", "amount_usd",
                          F.explode("fired_rules").alias("r"))
                  .select("customer_id", F.col("r.rule_id").alias("rule_id"), "txn_id",
                          "amount_usd", F.col("r.score").alias("score")))
    return (exploded.groupBy("customer_id", "rule_id")
                    .agg(F.count("txn_id").alias("hit_count"),
                         F.sum("amount_usd").alias("total_amount_usd"),
                         F.max("score").alias("max_score")))


def t11_reasons(df):
    # F-018: unbounded collect_list per customer
    return df.groupBy("customer_id").agg(F.expr(
        "sort_array(collect_list(named_struct('rule_id', rule_id, 'hit_count', hit_count, "
        "'total_amount_usd', total_amount_usd, 'max_score', max_score)))").alias("risk_reasons"))


def t12_exposure_pairs(df):
    # F-010: NULL currency forms a NULL group; policy undefined, so fail at runtime
    null_ccy = df.filter(F.col("currency").isNull()).limit(1).count()
    if null_ccy:
        raise DataQualityError("F-010: NULL currency present; NULL-currency policy not declared")
    return df.groupBy("customer_id", "currency").agg(F.sum("amount_usd").alias("usd"))


def t13_exposure(df):
    return df.groupBy("customer_id").agg(F.expr(
        "map_from_entries(collect_list(named_struct('currency', currency, 'usd', usd)))"
    ).alias("exposure_by_currency"))


def t14_customer_base(df):
    return (df.groupBy("customer_id", "segment", "residence_country")
              .agg(F.expr("FIRST(address_countries)").alias("address_countries"),
                   F.expr("FIRST(multi_country_flag)").alias("multi_country_flag"),
                   F.expr("COUNT(txn_id)").alias("txn_count"),
                   F.expr("SUM(amount_usd)").alias("total_amount_usd"),
                   F.expr("SUM(CASE WHEN is_large_txn THEN 1 ELSE 0 END)").alias("large_txn_count"),
                   F.expr("MAX(structuring_flag)").alias("structuring_flag"),
                   F.expr("COLLECT_SET(cp_country)").alias("counterparty_countries")))


def t15_summary(t14, t11, t13, run_date):
    df = t14.join(t11, on="customer_id", how="left")
    df = df.join(t13, on="customer_id", how="left")
    # Columns evaluated in order: risk_reasons coalesced before risk_score reads it
    df = df.withColumn("risk_reasons", F.expr(
        "COALESCE(risk_reasons, CAST(array() AS ARRAY<STRUCT<rule_id:STRING,hit_count:BIGINT,"
        "total_amount_usd:DOUBLE,max_score:INT>>))"))
    # F-012: default score 10 for customers with no fired rules
    df = df.withColumn("risk_score", F.expr(
        "CASE WHEN size(risk_reasons) = 0 THEN 10 "
        "ELSE array_max(transform(risk_reasons, r -> r.max_score)) END"))
    df = df.withColumn("run_date", F.lit(run_date))
    return df.select(*TARGET_COLUMNS)


# ---------------------------------------------------------------- target
def write_target(df, args):
    partition_cols = parse_list(args.target_partition_by)
    missing = [c for c in partition_cols if c not in df.columns]
    if missing:
        raise DataQualityError(f"F-008: partition columns missing from output: {missing}")
    # F-008: empty input is not defined by the source; refuse to overwrite on empty output
    if df.limit(1).count() == 0:
        raise DataQualityError("F-008: empty output for run_date; empty-input behaviour undefined")
    # Iceberg dynamic partition overwrite (only partitions present in output are replaced)
    df.writeTo(TARGET_TABLE).overwritePartitions()
    log.info("wrote %s with dynamic partition overwrite", TARGET_TABLE)


# ---------------------------------------------------------------- pipeline
def run_pipeline(argv=None):
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)
    validate_run_date(args.run_date)
    require_gates(args)                       # fail before any data access
    order_spec = parse_order_spec(args.dedupe_order_by)

    spark = spark_session()
    try:
        (df_src_transactions, df_src_accounts, df_src_customers,
         df_src_fx, df_src_hrc) = read_sources(spark, args.run_date, args.fx_path)

        df_t01_cust_profile = t01_cust_profile(df_src_customers)
        df_t02_acct_attrs = t02_acct_attrs(df_src_accounts)
        df_t03_posted_txn = t03_posted_txn(df_src_transactions, order_spec)
        df_t04_enriched_join = t04_enriched_join(
            df_t03_posted_txn, df_t02_acct_attrs, df_t01_cust_profile, df_src_fx, df_src_hrc)
        df_t05_amounts = t05_amounts(df_t04_enriched_join)
        df_t06_near_threshold = t06_near_threshold(df_t05_amounts)
        df_t07_structuring_window = t07_structuring_window(df_t06_near_threshold)
        df_t08_structuring_flag = t08_structuring_flag(df_t07_structuring_window)
        df_t09_rules = t09_rules(df_t08_structuring_flag)

        df_t10_rule_hits = t10_rule_hits(df_t09_rules)
        df_t11_reasons = t11_reasons(df_t10_rule_hits)
        df_t12_exposure_pairs = t12_exposure_pairs(df_t09_rules)
        df_t13_exposure = t13_exposure(df_t12_exposure_pairs)
        df_t14_customer_base = t14_customer_base(df_t09_rules)
        df_t15_summary = t15_summary(df_t14_customer_base, df_t11_reasons,
                                     df_t13_exposure, args.run_date)

        df_tgt_customer_daily_risk = df_t15_summary
        write_target(df_tgt_customer_daily_risk, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        run_pipeline(sys.argv[1:])
    except BlockedFindingError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
