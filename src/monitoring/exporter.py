from __future__ import annotations

import os
from wsgiref.simple_server import make_server

from prometheus_client import make_wsgi_app


def run_metrics_exporter(port: int | None = None) -> None:
    metrics_port = int(port or os.environ.get("METRICS_PORT", "9108"))
    app = make_wsgi_app()
    server = make_server("0.0.0.0", metrics_port, app)
    server.serve_forever()


if __name__ == "__main__":
    run_metrics_exporter()
