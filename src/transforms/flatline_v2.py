"""Welford Online Variance Algorithm for Flatline Detection.

This module implements Welford's numerically-stable algorithm for computing
variance in a single pass, without storing all observations in memory.

Used by: src/models/flatline_detector.py (alternative streaming implementation)
Referenced in: flatline_streaming.py (comparison/future reference)

Welford's Algorithm:
  - Incrementally update mean and M2 (sum of squared deltas)
  - Variance = M2 / (n - 1) for sample variance
  - O(1) memory, O(n) time, numerically stable
  - No need to store all observations

Reference: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
"""
from __future__ import annotations

from dataclasses import dataclass
from math import sqrt


@dataclass
class WelfordState:
    """Online variance state using Welford's algorithm.

    Attributes:
        n: Number of observations processed so far
        mean: Running mean of observations
        m2: Sum of squared differences from the mean (used to compute variance)
    """

    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, x: float) -> None:
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.n < 2:
            return 0.0
        return self.m2 / (self.n - 1)

    @property
    def stddev(self) -> float:
        return sqrt(self.variance)


def detect_flatline(values: list[float], min_points: int = 5, eps: float = 1e-12) -> tuple[bool, float, float]:
    """Returns (is_flatline, variance, mean)."""
    if len(values) < min_points:
        return False, 0.0, 0.0

    state = WelfordState()
    for value in values:
        state.update(float(value))

    variance = state.variance
    return variance <= eps, variance, state.mean
