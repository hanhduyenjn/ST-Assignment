from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "network-health",
}

_sample_env = {
    "SAMPLE_FRACTION": os.environ.get("SAMPLE_FRACTION", "1.0"),
    "SMOKE_MAX_ROWS": os.environ.get("SMOKE_MAX_ROWS", "0"),
}

with DAG(
    dag_id="nightly_batch_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    tags=["network-health", "nightly"],
) as dag:
    dlq_reprocess = BashOperator(
        task_id="dlq_reprocess",
        bash_command="python -m src.pipeline.dlq_reprocessor",
        env=_sample_env,
        append_env=True,
    )

    pending_enrichment = BashOperator(
        task_id="pending_enrichment",
        bash_command="python -m src.pipeline.pending_enrichment",
        env=_sample_env,
        append_env=True,
    )

    gold_recompute = BashOperator(
        task_id="gold_recompute",
        bash_command="python -m src.pipeline.gold_recompute",
        env=_sample_env,
        append_env=True,
    )

    model_detect = BashOperator(
        task_id="model_detect",
        bash_command="python -m src.pipeline.batch",
        env=_sample_env,
        append_env=True,
    )

    dlq_reprocess >> pending_enrichment >> model_detect
