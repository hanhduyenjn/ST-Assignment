from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="backfill_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["network-health", "backfill"],
) as dag:
    backfill = BashOperator(
        task_id="backfill",
        bash_command="python -m src.pipeline.batch",
    )
