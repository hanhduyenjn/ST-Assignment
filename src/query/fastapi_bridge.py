from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.query.duckdb_engine import DuckDBEngine


class SQLQuery(BaseModel):
    sql: str


def create_app() -> FastAPI:
    app = FastAPI(title="DuckDB Query Bridge", version="0.1.0")
    engine = DuckDBEngine()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/query")
    def run_query(payload: SQLQuery) -> dict[str, object]:
        try:
            rows = engine.query(payload.sql)
            return {"rows": rows, "count": len(rows)}
        except Exception as exc:  # pragma: no cover - API guard
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app


app = create_app()
