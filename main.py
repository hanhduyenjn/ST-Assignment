from __future__ import annotations

import argparse

from src.monitoring.exporter import run_metrics_exporter
from src.pipeline.batch import run as run_backfill
from src.pipeline.bronze_ingest import run as run_bronze_ingest
from src.pipeline.dlq_reprocessor import run as run_dlq_reprocessor
from src.pipeline.gold_recompute import run as run_gold_recompute
from src.pipeline.pending_enrichment import run as run_pending_enrichment
from src.pipeline.streaming import run as run_streaming
from src.producers.producer_kafka import (
    produce_interface_stats,
    produce_inventory,
    produce_syslogs,
)
from src.query.fastapi_bridge import app


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Network Health PoC entrypoint")
    parser.add_argument(
        "--mode",
        required=True,
        choices=[
            "produce-interface",
            "produce-syslogs",
            "produce-inventory",
            "bronze-ingest",
            "streaming",
            "pending-enrichment",
            "gold-recompute",
            "backfill",
            "dlq-reprocess",
            "query-api",
            "metrics-exporter",
        ],
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    return parser


def main() -> None:
    args = _parser().parse_args()

    if args.mode == "produce-interface":
        produce_interface_stats()
    elif args.mode == "produce-syslogs":
        produce_syslogs()
    elif args.mode == "produce-inventory":
        produce_inventory()
    elif args.mode == "bronze-ingest":
        run_bronze_ingest()
    elif args.mode == "streaming":
        run_streaming()
    elif args.mode == "pending-enrichment":
        run_pending_enrichment()
    elif args.mode == "gold-recompute":
        run_gold_recompute()
    elif args.mode == "backfill":
        run_backfill()
    elif args.mode == "dlq-reprocess":
        run_dlq_reprocessor()
    elif args.mode == "query-api":
        import uvicorn

        uvicorn.run(app, host=args.host, port=args.port)
    elif args.mode == "metrics-exporter":
        run_metrics_exporter(port=args.port)


if __name__ == "__main__":
    main()
