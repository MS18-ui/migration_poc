import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession

# global_configs from the blueprint
GLOBAL_CONFIGS = {
    "oozie_workflow_name": "daily_sales",
    "on_error": "fail",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(GLOBAL_CONFIGS["oozie_workflow_name"])


def build_spark(app_name: str) -> SparkSession:
    """Create the SparkSession, applying global_configs."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )


def step_import_orders(spark: SparkSession) -> DataFrame:
    """Step 1: import_orders (sqoop_import -> spark.read.jdbc).

    Original sqoop command:
      import --connect jdbc:mysql://db/sales --table orders --target-dir /raw/orders
    """
    jdbc_url = "jdbc:mysql://db/sales"
    table = "orders"
    target_path = "/raw/orders"

    user = os.environ.get("SALES_DB_USER")
    password = os.environ.get("SALES_DB_PASSWORD")
    if not user or not password:
        raise RuntimeError("SALES_DB_USER and SALES_DB_PASSWORD must be set")

    properties = {
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    df_import_orders = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
    df_import_orders.write.mode("overwrite").parquet(target_path)
    log.info("import_orders: wrote %s", target_path)
    return df_import_orders


def step_clean_orders(df_import_orders: DataFrame) -> DataFrame:
    """Step 2: clean_orders (run_pyspark_script, source: import_orders).

    The blueprint references clean_orders.py, which was not provided. This
    applies generic cleaning (drop fully empty rows and exact duplicates) as a
    placeholder; replace with the real logic from clean_orders.py.
    """
    df_clean_orders = df_import_orders.dropna(how="all").dropDuplicates()
    return df_clean_orders


def run_pipeline() -> None:
    app_name = GLOBAL_CONFIGS["oozie_workflow_name"]
    on_error = GLOBAL_CONFIGS["on_error"]

    spark = build_spark(app_name)
    try:
        df_import_orders = step_import_orders(spark)
        df_clean_orders = step_clean_orders(df_import_orders)
        row_count = df_clean_orders.count()
        log.info("clean_orders: %d rows after cleaning", row_count)
    except Exception:
        log.exception("Pipeline %s failed", app_name)
        if on_error == "fail":
            raise
        # Any other on_error value is not defined by the blueprint; treat as fail.
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception:
        sys.exit(1)
