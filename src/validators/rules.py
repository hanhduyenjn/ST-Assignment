from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.validators.base import ValidationResult, Validator


class NullCheckValidator(Validator):
    def __init__(self, required_fields: list[str]) -> None:
        self.required_fields = required_fields

    def validate(self, record: dict[str, Any], context: dict[str, Any] | None = None) -> ValidationResult:
        for field in self.required_fields:
            if record.get(field) in (None, ""):
                return ValidationResult(is_valid=False, reason=f"NULL_FIELD:{field}")
        return ValidationResult(is_valid=True)


class RangeCheckValidator(Validator):
    def validate(self, record: dict[str, Any], context: dict[str, Any] | None = None) -> ValidationResult:
        util_fields = ("util_in", "util_out")
        for field in util_fields:
            value = record.get(field)
            if value is None:
                continue
            if value < 0:
                return ValidationResult(is_valid=False, reason=f"NEGATIVE_UTIL:{field}")
            if value > 100:
                return ValidationResult(is_valid=False, reason=f"EXCEEDS_MAX:{field}")

        severity = record.get("severity")
        if severity is not None and not (0 <= severity <= 7):
            return ValidationResult(is_valid=False, reason="INVALID_SEVERITY")

        for status_field in ("admin_status", "oper_status"):
            status = record.get(status_field)
            if status is not None and status not in (1, 2, 3):
                return ValidationResult(is_valid=False, reason=f"INVALID_STATUS:{status_field}")

        return ValidationResult(is_valid=True)


class TimestampValidator(Validator):
    def __init__(self, max_past_days: int = 30, max_future_minutes: int = 10) -> None:
        self.max_past_days = max_past_days
        self.max_future_minutes = max_future_minutes

    def validate(self, record: dict[str, Any], context: dict[str, Any] | None = None) -> ValidationResult:
        raw_ts = record.get("ts")
        if raw_ts is None:
            return ValidationResult(is_valid=False, reason="INVALID_TIMESTAMP:missing")

        try:
            ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
        except ValueError:
            return ValidationResult(is_valid=False, reason="INVALID_TIMESTAMP:format")

        now = datetime.now(UTC)
        if ts < now - timedelta(days=self.max_past_days):
            return ValidationResult(is_valid=False, reason="STALE_TIMESTAMP")
        if ts > now + timedelta(minutes=self.max_future_minutes):
            return ValidationResult(is_valid=False, reason="FUTURE_TIMESTAMP")
        return ValidationResult(is_valid=True)


class OrphanCheckValidator(Validator):
    """Fails when record references a device absent from known inventory."""

    def validate(self, record: dict[str, Any], context: dict[str, Any] | None = None) -> ValidationResult:
        context = context or {}
        known_device_ids = context.get("known_device_ids", set())
        device_id = record.get("device_id")
        if device_id and known_device_ids and device_id not in known_device_ids:
            return ValidationResult(is_valid=False, reason="ORPHANED_DEVICE")
        return ValidationResult(is_valid=True)


class PayloadSizeValidator(Validator):
    """Simple guardrail against malformed giant messages."""

    def __init__(self, max_chars: int = 10000) -> None:
        self.max_chars = max_chars

    def validate(self, record: dict[str, Any], context: dict[str, Any] | None = None) -> ValidationResult:
        payload = record.get("_raw_payload")
        if payload is not None and len(str(payload)) > self.max_chars:
            return ValidationResult(is_valid=False, reason="PAYLOAD_TOO_LARGE")
        return ValidationResult(is_valid=True)
