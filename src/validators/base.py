from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ValidationResult:
    """Result object shared by all validators."""

    is_valid: bool
    reason: str | None = None
    flags: list[str] = field(default_factory=list)


class Validator(ABC):
    """Validator contract used by ValidatorChain."""

    @abstractmethod
    def validate(self, record: dict[str, Any], context: dict[str, Any] | None = None) -> ValidationResult:
        """Validate a single normalized record."""
