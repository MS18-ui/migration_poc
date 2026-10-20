from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="daily_txn_risk",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["banking", "pyspark"],
) as dag:
    daily_txn_risk = SparkSubmitOperator(
        task_id="daily_txn_risk",
        application="generated_pipeline.py",
        conf={"spark.sql.sources.partitionOverwriteMode": "dynamic"},
        application_args=["--run-date", "{{ ds }}"],
    )
