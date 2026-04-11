from __future__ import annotations

from typing import Any

from src.validators.base import ValidationResult, Validator


class ValidatorChain:
    """Runs validators in order and short-circuits on first failure."""

    def __init__(self, validators: list[Validator]) -> None:
        self.validators = validators

    def validate(self, record: dict[str, Any], context: dict[str, Any] | None = None) -> ValidationResult:
        all_flags: list[str] = []
        for validator in self.validators:
            result = validator.validate(record, context=context)
            if result.flags:
                all_flags.extend(result.flags)
            if not result.is_valid:
                return ValidationResult(
                    is_valid=False,
                    reason=result.reason,
                    flags=all_flags,
                )
        return ValidationResult(is_valid=True, flags=all_flags)
