from __future__ import annotations

import os
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
        env={
            "SAMPLE_FRACTION": os.environ.get("SAMPLE_FRACTION", "1.0"),
            "SMOKE_MAX_ROWS": os.environ.get("SMOKE_MAX_ROWS", "0"),
        },
        append_env=True,
    )
