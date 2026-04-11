from __future__ import annotations


def compute_effective_utilization(
    util_in: float | None,
    util_out: float | None,
    oper_status: int | None,
) -> tuple[float | None, float | None]:
    """Apply requirement override for DOWN(2)/TESTING(3) interfaces."""
    if oper_status in (2, 3):
        return 0.0, 0.0
    return util_in, util_out


def effective_util_expr(util_col: str, status_col: str = "oper_status"):
    """Spark Column: zero utilisation when oper_status IN (2=DOWN, 3=TESTING).

    Mirrors compute_effective_utilization() as a native Spark column expression
    (no UDF serialisation overhead).
    """
    from pyspark.sql import functions as F

    return F.when(F.col(status_col).isin(2, 3), F.lit(0.0)).otherwise(F.col(util_col))
