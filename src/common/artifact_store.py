"""Model artifact persistence.

Supports both local filesystem paths (development/testing) and S3/MinIO paths
(production).  S3 paths are detected by the ``s3://`` or ``s3a://`` prefix and
routed through ``s3fs`` (bundled with ``pyiceberg[s3]``).

Usage:
    store = ArtifactStore(base_path="/data/models")      # local
    store = ArtifactStore(base_path="s3a://lakehouse/models")  # MinIO / S3

    store.save(detector, "isolation_forest/latest.joblib")
    detector = store.load("isolation_forest/latest.joblib")
    store.save_json(rules, "isolation_forest/distilled_rules.json")
    rules = store.load_json("isolation_forest/distilled_rules.json")
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Any

import joblib

from src.common.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


def _is_s3(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://")


def _s3fs():
    import s3fs  # bundled via pyiceberg[s3]

    endpoint = MINIO_ENDPOINT.replace("s3a://", "https://").replace("http://", "")
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
    )


class ArtifactStore:
    """Save and load model artifacts to/from local disk or S3/MinIO."""

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path.rstrip("/")

    def _full_path(self, relative: str) -> str:
        return f"{self.base_path}/{relative}"

    def save(self, obj: Any, relative: str) -> None:
        """Serialize obj with joblib and write to base_path/relative."""
        full = self._full_path(relative)
        if _is_s3(full):
            fs = _s3fs()
            buf = io.BytesIO()
            joblib.dump(obj, buf)
            buf.seek(0)
            with fs.open(full, "wb") as f:
                f.write(buf.read())
        else:
            path = Path(full)
            path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(obj, path)

    def load(self, relative: str) -> Any:
        """Load a joblib artifact from base_path/relative."""
        full = self._full_path(relative)
        if _is_s3(full):
            fs = _s3fs()
            with fs.open(full, "rb") as f:
                return joblib.load(f)
        return joblib.load(full)

    def save_json(self, obj: Any, relative: str) -> None:
        """Write obj as JSON to base_path/relative."""
        import numpy as _np

        class _NumpyEncoder(json.JSONEncoder):
            def default(self, o: Any) -> Any:
                if isinstance(o, _np.ndarray):
                    return o.tolist()
                if isinstance(o, (_np.integer,)):
                    return int(o)
                if isinstance(o, (_np.floating,)):
                    return float(o)
                return super().default(o)

        full = self._full_path(relative)
        text = json.dumps(obj, cls=_NumpyEncoder)
        if _is_s3(full):
            fs = _s3fs()
            with fs.open(full, "w") as f:
                f.write(text)
        else:
            path = Path(full)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")

    def load_json(self, relative: str) -> Any:
        """Load a JSON artifact from base_path/relative."""
        full = self._full_path(relative)
        if _is_s3(full):
            fs = _s3fs()
            with fs.open(full, "r") as f:
                return json.loads(f.read())
        return json.loads(Path(full).read_text(encoding="utf-8"))

    def exists(self, relative: str) -> bool:
        full = self._full_path(relative)
        if _is_s3(full):
            return _s3fs().exists(full)
        return Path(full).exists()
