#!/usr/bin/env python3
"""Generated PySpark pipeline for blueprint 'daily_sales' (source: oozie workflow.xml).

STATUS: PARTIAL DRAFT. Generation is blocked for part of the pipeline by open findings:
  - F-001 (blocks generation): clean_orders is an external script (clean_orders.py)
    that was not supplied. Its logic is NOT translated here.
  - F-002 (blocks generation): no metadata snapshot for mysql sales.orders; schema,
    keys and partitions are not validated.
  - F-003: no authentication method defined; the password is read from an
    environment variable whose NAME is passed on the command line. No literals.
  - F-004: write semantics for /raw/orders are not specified; --write-mode is required.
  - F-007: no schedule supplied; this script is not scheduled.
"""
import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession

# global_configs from the blueprint, recorded as Spark conf tags.
GLOBAL_CONFIGS = {
    "source_workflow": "workflow.xml",
    "source_hash": "UNKNOWN_NOT_SUPPLIED",
    "metadata_snapshot": "UNKNOWN_NOT_SUPPLIED",
}

JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"


def build_spark(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in GLOBAL_CONFIGS.items():
        builder = builder.config(f"blueprint.global.{key}", value)
    return builder.getOrCreate()


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="daily_sales pipeline (generated draft)")
    # import_orders (sqoop_import) parameters
    p.add_argument("--connect", default="jdbc:mysql://db/sales",
                   help="JDBC URL (blueprint param: connect)")
    p.add_argument("--table", default="orders", help="Source table (param: table)")
    p.add_argument("--target-dir", default="/raw/orders",
                   help="HDFS target directory (param: target_dir)")
    p.add_argument("--db-user", default=None,
                   help="Database user name (no literal credentials in the script)")
    p.add_argument("--password-env", default=None,
                   help="NAME of the environment variable holding the DB password (param: auth)")
    p.add_argument("--write-mode", default=None, choices=["overwrite", "append"],
                   help="Required. Write semantics for target_dir (param: write_mode, F-004)")
    # transformation (spark_job_reference) parameters, recorded only
    p.add_argument("--spark-app-name", default="daily_sales",
                   help="Spark application name (param: spark_app_name)")
    p.add_argument("--jar", default="clean_orders.py",
                   help="External job reference for clean_orders (param: jar)")
    return p.parse_args(argv)


def import_orders(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    """Step 1 (sqoop_import): read orders over JDBC and land it in target_dir.

    No schema, pre_filter or partition column is defined in the blueprint (F-002),
    so the full table is read in a single JDBC partition.
    """
    if args.write_mode is None:
        raise SystemExit("--write-mode is required (F-004: write semantics unspecified)")
    if not args.db_user or not args.password_env:
        raise SystemExit("--db-user and --password-env are required (F-003)")
    password = os.environ.get(args.password_env)
    if password is None:
        raise SystemExit(f"environment variable {args.password_env} is not set")

    properties = {
        "user": args.db_user,
        "password": password,
        "driver": JDBC_DRIVER,
    }
    df_import_orders = spark.read.jdbc(url=args.connect, table=args.table, properties=properties)
    # Format is not specified in the blueprint; parquet is an assumption to be confirmed.
    df_import_orders.write.mode(args.write_mode).parquet(args.target_dir)
    return df_import_orders


def clean_orders(df_import_orders: DataFrame, args: argparse.Namespace) -> DataFrame:
    """Step 2 (spark_job_reference): NOT TRANSLATED.

    The logic lives in an external script (clean_orders.py) that was not supplied
    (F-001). It must be analysed as a separate Spark job or provided as a reviewed
    mapping before this step can be generated.
    """
    raise NotImplementedError(
        f"clean_orders ({args.jar}) is an external job; generation blocked by F-001"
    )


def run_pipeline(argv=None) -> None:
    args = parse_args(argv)
    spark = build_spark(args.spark_app_name)
    try:
        df_import_orders = import_orders(spark, args)
        df_clean_orders = clean_orders(df_import_orders, args)  # noqa: F841
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        run_pipeline(sys.argv[1:])
    except NotImplementedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
