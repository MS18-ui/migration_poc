from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DRAFT generated from blueprint 'daily_sales' (oozie -> pyspark_3.x).
# Open blocking findings F-001 and F-002 apply; see notes in the response.
# schedule=None: no coordinator schedule was supplied (F-007).
# start_date is a placeholder: no start time was supplied (F-007).
# Sqoop authentication is intentionally not configured (F-003); supply an approved
# credential reference before execution. No literal credentials are present.

with DAG(
    dag_id="daily_sales",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["oozie-migration", "draft"],
) as dag:

    import_orders = BashOperator(
        task_id="import_orders",
        bash_command=(
            "sqoop import "
            "--connect jdbc:mysql://db/sales "
            "--table orders "
            "--target-dir /raw/orders"
        ),
    )

    clean_orders = SparkSubmitOperator(
        task_id="clean_orders",
        application="clean_orders.py",
        name="clean",
        conn_id="spark_default",
    )

    import_orders >> clean_orders
