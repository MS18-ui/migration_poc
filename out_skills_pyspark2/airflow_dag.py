from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="pyspark2_daily_txn_risk",
    schedule=None,
    start_date=datetime(2026, 10, 18),
    catchup=False,
    tags=["banking_aml", "pyspark2_to_spark3"],
) as dag:
    pyspark2_daily_txn_risk = SparkSubmitOperator(
        task_id="pyspark2_daily_txn_risk",
        application="generated_pipeline.py",
        conn_id="spark_default",
        application_args=["--run-date", "{{ ds }}"],
    )
