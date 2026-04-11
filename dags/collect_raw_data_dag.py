from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from kafka import KafkaConsumer
from pydantic import ValidationError
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from src.common.config import (
    BATCH_MAX_MESSAGES,
    BRONZE_INTERFACE_STATS,
    BRONZE_INVENTORY,
    BRONZE_SYSLOGS,
    CONSUMER_TIMEOUT_MS,
    ICEBERG_REST_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INTERFACE_STATS,
    KAFKA_TOPIC_INVENTORY,
    KAFKA_TOPIC_SYSLOGS,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)
from src.common.schema import (
    INTERFACE_STATS_SCHEMA,
    INVENTORY_SCHEMA,
    SYSLOGS_SCHEMA,
    InterfaceStatsPayload,
    InventoryPayload,
    SyslogPayload,
)
from src.common.logging import log

_DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}


def _get_catalog():
    return load_catalog(
        "rest",
        **{
            "uri": ICEBERG_REST_URL,
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )


def _parse_interface_stats(raw: str, ingested_at: datetime, topic: str) -> dict:
    payload = InterfaceStatsPayload.model_validate(json.loads(raw))
    extra = {k: str(v) for k, v in (payload.model_extra or {}).items()}
    return {
        "_raw_payload":    raw,
        "ts":              payload.ts,
        "device_id":       payload.device_id,
        "interface_name":  payload.interface_name,
        "util_in":         payload.util_in,
        "util_out":        payload.util_out,
        "admin_status":    payload.admin_status,
        "oper_status":     payload.oper_status,
        "_extra_cols":     list(extra.items()),
        "_ingested_at":    ingested_at,
        "_source_topic":   topic,
        "_partition_date": ingested_at.date(),
    }


def _parse_syslog(raw: str, ingested_at: datetime, topic: str) -> dict:
    payload = SyslogPayload.model_validate(json.loads(raw))
    return {
        "_raw_payload":    raw,
        "ts":              payload.ts,
        "device_id":       payload.device_id,
        "severity":        payload.severity,
        "message":         payload.message,
        "_ingested_at":    ingested_at,
        "_source_topic":   topic,
        "_partition_date": ingested_at.date(),
    }


def _parse_inventory(raw: str, ingested_at: datetime, topic: str) -> dict:
    payload = InventoryPayload.model_validate(json.loads(raw))
    return {
        "device_id":       payload.device_id,
        "site_id":         payload.site_id,
        "vendor":          payload.vendor,
        "role":            payload.role,
        "_ingested_at":    ingested_at,
        "_source_topic":   topic,
        "_partition_date": ingested_at.date(),
    }


def _consume_and_write(
    *,
    topic: str,
    group_id: str,
    table_name: str,
    dedup_keys: list[str],
    schema: pa.Schema,
    record_parser,
    run_id: str,
) -> dict:
    ingested_at = datetime.now(timezone.utc)

    log.info("[%s] Starting | topic=%s group=%s table=%s", run_id, topic, group_id, table_name)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
        max_poll_records=500,
        value_deserializer=lambda b: b.decode("utf-8"),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )

    raw_messages: list[str] = []
    try:
        for msg in consumer:
            raw_messages.append(msg.value)
            if len(raw_messages) >= BATCH_MAX_MESSAGES:
                log.info("[%s] MAX_MESSAGES=%d reached", run_id, BATCH_MAX_MESSAGES)
                break
    except Exception as exc:
        consumer.close()
        raise RuntimeError(f"Kafka poll error | topic={topic} error={exc}") from exc

    n_raw = len(raw_messages)
    log.info("[%s] Consumed %d messages from %s", run_id, n_raw, topic)

    if n_raw == 0:
        consumer.commit()
        consumer.close()
        return {"topic": topic,
                "consumed": 0, "written": 0,
                "duplicates_dropped": 0,
                "parse_errors": 0
        }

    records: list[dict] = []
    seen: dict[tuple, int] = {}
    parse_errors = 0

    for raw in raw_messages:
        try:
            record = record_parser(raw, ingested_at, topic)
        except (ValidationError, Exception) as exc:
            parse_errors += 1
            log.warning("[%s] Parse error | error=%s snippet=%r", run_id, exc, raw[:150])
            continue

        key = tuple(str(record.get(k) or "") for k in dedup_keys)
        if key in seen:
            records[seen[key]] = record
        else:
            seen[key] = len(records)
            records.append(record)

    n_unique = len(records)
    n_dupes = n_raw - parse_errors - n_unique
    log.info("[%s] unique=%d dupes_dropped=%d parse_errors=%d", run_id, n_unique, n_dupes, parse_errors)

    if n_unique > 0:
        try:
            catalog = _get_catalog()
            try:
                table = catalog.load_table(table_name)
            except NoSuchTableError:
                consumer.close()
                raise RuntimeError(
                    f"Table '{table_name}' not found. Run sql/iceberg/ddl/ scripts first."
                )

            columns: dict[str, list] = {}
            for col in schema.names:
                if col == "_extra_cols":
                    columns[col] = [r.get(col) or [] for r in records]
                else:
                    columns[col] = [r.get(col) for r in records]

            table.append(pa.table(columns, schema=schema))
            log.info("[%s] Appended %d rows to %s", run_id, n_unique, table_name)

        except RuntimeError:
            raise
        except Exception as exc:
            consumer.close()
            raise RuntimeError(f"Iceberg write failed | table={table_name} error={exc}") from exc

    consumer.commit()
    consumer.close()

    stats = {
        "topic": topic,
        "consumed": n_raw,
        "written": n_unique,
        "duplicates_dropped": n_dupes,
        "parse_errors": parse_errors,
    }
    log.info("[%s] Done | %s", run_id, stats)
    return stats




@dag(
    dag_id="collect_raw_data",
    description="Batch-collect Kafka topics → Iceberg bronze tables",
    schedule_interval="@hourly",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["bronze", "kafka", "batch", "ingestion"],
)
def collect_raw_data():

    @task(task_id="collect_interface_stats")
    def collect_interface_stats() -> dict:
        ctx = get_current_context()
        return _consume_and_write(
            topic=KAFKA_TOPIC_INTERFACE_STATS,
            group_id="airflow-batch-interface-stats",
            table_name=BRONZE_INTERFACE_STATS,
            dedup_keys=["ts", "device_id"],
            schema=INTERFACE_STATS_SCHEMA,
            record_parser=_parse_interface_stats,
            run_id=str(ctx["run_id"]),
        )


    @task(task_id="collect_syslogs")
    def collect_syslogs() -> dict:
        ctx = get_current_context()
        return _consume_and_write(
            topic=KAFKA_TOPIC_SYSLOGS,
            group_id="airflow-batch-syslogs",
            table_name=BRONZE_SYSLOGS,
            dedup_keys=["ts", "device_id"],
            schema=SYSLOGS_SCHEMA,
            record_parser=_parse_syslog,
            run_id=str(ctx["run_id"]),
        )


    @task(task_id="collect_inventory")
    def collect_inventory() -> dict:
        ctx = get_current_context()
        return _consume_and_write(
            topic=KAFKA_TOPIC_INVENTORY,
            group_id="airflow-batch-inventory",
            table_name=BRONZE_INVENTORY,
            dedup_keys=["device_id", "site_id"],
            schema=INVENTORY_SCHEMA,
            record_parser=_parse_inventory,
            run_id=str(ctx["run_id"]),
        )
    
    collect_interface_stats()
    collect_syslogs()
    collect_inventory()


collect_raw_data()
