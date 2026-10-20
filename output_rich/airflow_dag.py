from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="daily_txn_risk",
    schedule=None,
    start_date=datetime(2026, 10, 18),
    catchup=False,
    tags=["banking"],
) as dag:
    daily_txn_risk = SparkSubmitOperator(
        task_id="daily_txn_risk",
        application="daily_txn_risk.py",
        conf={"spark.sql.sources.partitionOverwriteMode": "dynamic"},
        application_args=["--run_date", "{{ ds }}"],
        name="daily_txn_risk",
        verbose=False,
    )
