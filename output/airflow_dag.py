from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="daily_sales",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["oozie_migration", "pyspark_3.x"],
) as dag:
    import_orders = BashOperator(
        task_id="import_orders",
        bash_command="sqoop import --connect jdbc:mysql://db/sales --table orders --target-dir /raw/orders",
    )

    clean_orders = SparkSubmitOperator(
        task_id="clean_orders",
        application="clean_orders.py",
        name="clean",
        conn_id="spark_default",
    )

    import_orders >> clean_orders
