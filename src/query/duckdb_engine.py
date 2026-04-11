from __future__ import annotations

from pathlib import Path

import duckdb


class DuckDBEngine:
    def __init__(self, database: str = ":memory:") -> None:
        self.conn = duckdb.connect(database=database)

    def execute(self, sql: str):
        return self.conn.execute(sql)

    def query(self, sql: str) -> list[dict[str, object]]:
        rel = self.conn.execute(sql)
        columns = [c[0] for c in rel.description]
        return [dict(zip(columns, row, strict=False)) for row in rel.fetchall()]

    def register_parquet(self, view_name: str, parquet_path: str | Path) -> None:
        path = str(parquet_path)
        self.conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{path}')")
