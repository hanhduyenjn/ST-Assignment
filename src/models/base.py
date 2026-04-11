from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class AnomalyResult:
    device_id: str
    interface_name: str
    anomaly_type: str
    score: float | None = None
    subtype: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.utcnow)


class AnomalyDetector(ABC):
    @abstractmethod
    def detect(self, records: list[dict[str, Any]]) -> list[AnomalyResult]:
        """Detect anomalies for the given normalized records."""
