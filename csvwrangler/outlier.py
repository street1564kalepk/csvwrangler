"""CSVOutlier – flag or drop rows where a numeric column is an outlier.

Uses the IQR method: values below Q1 - k*IQR or above Q3 + k*IQR are outliers.
"""
from __future__ import annotations
from typing import Iterable


class CSVOutlier:
    """Flag or remove outlier rows based on IQR for a given column."""

    def __init__(self, source, column: str, k: float = 1.5, mode: str = "flag",
                 flag_column: str = "is_outlier"):
        if mode not in ("flag", "drop"):
            raise ValueError("mode must be 'flag' or 'drop'")
        if column not in source.headers:
            raise ValueError(f"Column '{column}' not found in headers")
        self._source = source
        self._column = column
        self._k = k
        self._mode = mode
        self._flag_column = flag_column
        self._lower: float | None = None
        self._upper: float | None = None
        self._computed = False

    def _compute_bounds(self) -> None:
        values = []
        for row in self._source.rows():
            try:
                values.append(float(row[self._column]))
            except (ValueError, TypeError):
                pass
        if not values:
            self._lower, self._upper = float("-inf"), float("inf")
            self._computed = True
            return
        values.sort()
        n = len(values)
        q1 = values[n // 4]
        q3 = values[(3 * n) // 4]
        iqr = q3 - q1
        self._lower = q1 - self._k * iqr
        self._upper = q3 + self._k * iqr
        self._computed = True

    @property
    def headers(self) -> list[str]:
        if self._mode == "flag":
            return self._source.headers + [self._flag_column]
        return self._source.headers

    def rows(self) -> Iterable[dict]:
        if not self._computed:
            self._compute_bounds()
        for row in self._source.rows():
            try:
                val = float(row[self._column])
                is_out = val < self._lower or val > self._upper
            except (ValueError, TypeError):
                is_out = False
            if self._mode == "drop" and is_out:
                continue
            if self._mode == "flag":
                yield {**row, self._flag_column: "1" if is_out else "0"}
            else:
                yield row

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
