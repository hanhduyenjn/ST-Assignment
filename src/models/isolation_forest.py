from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier

from src.models.base import AnomalyDetector, AnomalyResult


class IsolationForestDetector(AnomalyDetector):
    def __init__(self, contamination: float = 0.05, random_state: int = 42) -> None:
        self.scaler = StandardScaler()
        self.model = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=random_state,
        )
        self._is_fitted = False

    def train(self, features: pd.DataFrame) -> None:
        x = self.scaler.fit_transform(features.values)
        self.model.fit(x)
        self._is_fitted = True

    def detect(self, records: list[dict[str, Any]]) -> list[AnomalyResult]:
        if not records:
            return []
        frame = pd.DataFrame(records)
        numeric = frame.select_dtypes(include=["number"]).fillna(0.0)
        return self.detect_frame(frame, numeric)

    def detect_frame(self, original: pd.DataFrame, numeric_features: pd.DataFrame) -> list[AnomalyResult]:
        if not self._is_fitted:
            raise RuntimeError("IsolationForestDetector must be trained before detect().")

        x = self.scaler.transform(numeric_features.values)
        labels = self.model.predict(x)
        scores = self.model.score_samples(x)

        results: list[AnomalyResult] = []
        for idx, label in enumerate(labels):
            if label != -1:
                continue
            row = original.iloc[idx]
            results.append(
                AnomalyResult(
                    device_id=str(row.get("device_id", "")),
                    interface_name=str(row.get("interface_name", "")),
                    anomaly_type="MODEL",
                    subtype="MULTIVARIATE_ANOMALY",
                    score=float(scores[idx]),
                )
            )
        return results

    def distill_rules(self, features: pd.DataFrame) -> dict[str, Any]:
        if not self._is_fitted:
            raise RuntimeError("Train model before distilling rules.")

        x = self.scaler.transform(features.values)
        y = (self.model.predict(x) == -1).astype(int)

        tree = DecisionTreeClassifier(max_depth=4, random_state=42)
        tree.fit(x, y)

        rules = {
            "feature_names": list(features.columns),
            "tree": tree.tree_.__getstate__(),
        }
        return rules

    def save(self, artifact_path: str | Path) -> None:
        artifact = {
            "scaler": self.scaler,
            "model": self.model,
            "is_fitted": self._is_fitted,
        }
        joblib.dump(artifact, Path(artifact_path))

    @classmethod
    def load(cls, artifact_path: str | Path) -> "IsolationForestDetector":
        artifact = joblib.load(Path(artifact_path))
        detector = cls()
        detector.scaler = artifact["scaler"]
        detector.model = artifact["model"]
        detector._is_fitted = artifact.get("is_fitted", True)
        return detector

    @staticmethod
    def save_rules(rules: dict[str, Any], path: str | Path) -> None:
        Path(path).write_text(json.dumps(rules), encoding="utf-8")
