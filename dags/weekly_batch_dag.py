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
    if_train = BashOperator(
        task_id="isolation_forest_train",
        bash_command="python -m src.pipeline.batch",
    )

    psi_drift = BashOperator(
        task_id="psi_drift",
        bash_command="python -m src.pipeline.batch",
    )

    if_train >> psi_drift
