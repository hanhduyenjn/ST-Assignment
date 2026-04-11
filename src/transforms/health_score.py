from __future__ import annotations


def compute_health_score(
    pct_interfaces_saturated: float,
    critical_syslog_count: int,
    pct_interfaces_down: float,
) -> float:
    """Health score in [0, 100] based on agreed weighted penalty formula."""
    score = (
        100
        - (pct_interfaces_saturated * 40)
        - (min(float(critical_syslog_count), 4.0) * 10)
        - (pct_interfaces_down * 20)
    )
    return max(0.0, min(100.0, score))
