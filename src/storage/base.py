from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class StorageWriter(ABC):
    @abstractmethod
    def write_rows(self, rows: list[dict[str, Any]], target: str) -> None:
        """Write Python dictionaries to a storage target."""

    @abstractmethod
    def write_dataframe(self, df: Any, target: str, mode: str = "append") -> None:
        """Write a Spark DataFrame to a storage target."""
