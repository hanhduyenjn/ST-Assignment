"""Spark Column expressions that mirror the Python-level validators in rules.py.

These are native column expressions (no UDF overhead) used inside foreachBatch
and streaming transforms. Logic must stay in sync with the corresponding
Python-level Validator classes.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def interface_stats_validation_reason() -> Column:
    """Returns the first failing validation reason for an interface_stats row, or null.

    Mirrors NullCheckValidator, RangeCheckValidator, and TimestampValidator logic
    from src/validators/rules.py for the interface_stats schema.
    """
    return (
        F.when(
            F.col("ts").isNull()
            | F.col("device_id").isNull()
            | F.col("interface_name").isNull()
            | F.col("util_in").isNull()
            | F.col("util_out").isNull()
            | F.col("oper_status").isNull(),
            F.lit("NULL_FIELD"),
        )
        .when((F.col("util_in") < 0) | (F.col("util_out") < 0), F.lit("NEGATIVE_UTIL"))
        .when((F.col("util_in") > 100) | (F.col("util_out") > 100), F.lit("EXCEEDS_MAX"))
        .when(~F.col("oper_status").isin(1, 2, 3), F.lit("INVALID_STATUS"))
        .when(F.col("device_ts").isNull(), F.lit("INVALID_TIMESTAMP"))
    )


def syslogs_validation_reason() -> Column:
    """Returns the first failing validation reason for a syslogs row, or null.

    Mirrors NullCheckValidator, RangeCheckValidator, and TimestampValidator logic
    from src/validators/rules.py for the syslogs schema.
    """
    return (
        F.when(
            F.col("ts").isNull() | F.col("device_id").isNull(),
            F.lit("NULL_FIELD"),
        )
        .when(F.col("device_ts").isNull(), F.lit("INVALID_TIMESTAMP"))
        .when(
            F.col("severity").isNull() | (F.col("severity") < 0) | (F.col("severity") > 7),
            F.lit("INVALID_SEVERITY"),
        )
    )
