from __future__ import annotations

from functools import lru_cache

from pyiceberg.catalog import Catalog, load_catalog

from src.common.config import ICEBERG_REST_URL, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


@lru_cache(maxsize=1)
def get_catalog() -> Catalog:
    """Create and cache Iceberg REST catalog client."""
    return load_catalog(
        "rest",
        type="rest",
        uri=ICEBERG_REST_URL,
        **{
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def ensure_namespace(namespace: str) -> None:
    catalog = get_catalog()
    existing = {tuple(ns) for ns in catalog.list_namespaces()}
    key = (namespace,)
    if key not in existing:
        catalog.create_namespace(key)
