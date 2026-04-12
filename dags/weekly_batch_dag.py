from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "network-health",
}

with DAG(
    dag_id="weekly_batch_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * 0",
    catchup=False,
    tags=["network-health", "weekly"],
) as dag:
    isolation_forest_train = BashOperator(
        task_id="isolation_forest_train",
        bash_command="python -m src.pipeline.isolation_forest_train",
    )

    psi_drift_detect = BashOperator(
        task_id="psi_drift_detect",
        bash_command="python -m src.pipeline.psi_drift_detect",
    )

    isolation_forest_train >> psi_drift_detect
