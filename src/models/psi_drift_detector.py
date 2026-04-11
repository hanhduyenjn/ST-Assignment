from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(slots=True)
class DriftResult:
    psi: float
    is_significant: bool


class PSIDriftDetector:
    """Population Stability Index detector for weekly baseline drift checks."""

    def __init__(self, threshold: float = 0.25, bins: int = 10) -> None:
        self.threshold = threshold
        self.bins = bins

    def compute_psi(self, baseline: pd.Series, current: pd.Series) -> DriftResult:
        baseline = baseline.dropna().astype(float)
        current = current.dropna().astype(float)
        if baseline.empty or current.empty:
            return DriftResult(psi=0.0, is_significant=False)

        percentiles = np.linspace(0, 100, self.bins + 1)
        cuts = np.unique(np.percentile(baseline, percentiles))
        if len(cuts) < 2:
            return DriftResult(psi=0.0, is_significant=False)

        baseline_hist, _ = np.histogram(baseline, bins=cuts)
        current_hist, _ = np.histogram(current, bins=cuts)

        baseline_ratio = np.clip(baseline_hist / max(1, baseline_hist.sum()), 1e-8, None)
        current_ratio = np.clip(current_hist / max(1, current_hist.sum()), 1e-8, None)

        psi = float(np.sum((current_ratio - baseline_ratio) * np.log(current_ratio / baseline_ratio)))
        return DriftResult(psi=psi, is_significant=psi > self.threshold)

    def build_param_updates(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Build baseline parameter updates from significant drift groups."""
        updates: list[dict[str, object]] = []
        grouped = frame.groupby(["device_id", "interface_name", "hour_of_day", "day_of_week"])
        for key, group in grouped:
            result = self.compute_psi(group["baseline_metric"], group["current_metric"])
            if not result.is_significant:
                continue
            updates.append(
                {
                    "device_id": key[0],
                    "interface_name": key[1],
                    "hour_of_day": int(key[2]),
                    "day_of_week": int(key[3]),
                    "iqr_k": 1.5,
                    "psi": result.psi,
                }
            )
        return pd.DataFrame(updates)
