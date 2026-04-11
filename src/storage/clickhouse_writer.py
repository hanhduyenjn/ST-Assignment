from __future__ import annotations

from typing import Any

import clickhouse_connect


class ClickHouseWriter:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "network_health",
    ) -> None:
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
        )

    def insert_dicts(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        columns = list(rows[0].keys())
        data = [tuple(row.get(col) for col in columns) for row in rows]
        self.client.insert(table=table, data=data, column_names=columns)

    def query(self, sql: str) -> list[dict[str, Any]]:
        result = self.client.query(sql)
        rows: list[dict[str, Any]] = []
        for values in result.result_rows:
            rows.append(dict(zip(result.column_names, values, strict=False)))
        return rows

    def fetch_baseline_params(self) -> list[dict[str, Any]]:
        sql = """
        SELECT
            device_id,
            interface_name,
            hour_of_day,
            day_of_week,
            baseline_mean,
            baseline_std,
            iqr_k,
            isolation_score_threshold,
            distilled_rules
        FROM device_baseline_params
        """
        return self.query(sql)
