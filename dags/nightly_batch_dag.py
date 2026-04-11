from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "network-health",
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
    )

    pending_enrichment = BashOperator(
        task_id="pending_enrichment",
        bash_command="python -m src.pipeline.pending_enrichment",
    )

    gold_recompute = BashOperator(
        task_id="gold_recompute",
        bash_command="python -m src.pipeline.gold_recompute",
    )

    model_detect = BashOperator(
        task_id="model_detect",
        bash_command="python -m src.pipeline.batch",
    )

    dlq_reprocess >> pending_enrichment >> gold_recompute >> model_detect
