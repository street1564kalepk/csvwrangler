"""CSVPercentileClipper – keep rows where a numeric column's value falls
within a given percentile range (inclusive on both ends)."""

from __future__ import annotations

from typing import Iterable, List


class CSVPercentileClipper:
    """Filter rows whose *column* value lies within [low_pct, high_pct].

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows``.
    column:
        Name of the numeric column to evaluate.
    low_pct:
        Lower bound percentile (0–100, inclusive).  Default ``0``.
    high_pct:
        Upper bound percentile (0–100, inclusive).  Default ``100``.
    """

    def __init__(
        self,
        source,
        column: str,
        low_pct: float = 0.0,
        high_pct: float = 100.0,
    ) -> None:
        if not (0.0 <= low_pct <= 100.0 and 0.0 <= high_pct <= 100.0):
            raise ValueError("Percentile bounds must be between 0 and 100.")
        if low_pct > high_pct:
            raise ValueError("low_pct must be <= high_pct.")
        self._source = source
        self._column = column
        self._low = low_pct
        self._high = high_pct

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def _percentile(self, values: List[float], pct: float) -> float:
        """Linear-interpolation percentile (matches numpy default)."""
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        idx = (pct / 100.0) * (n - 1)
        lo = int(idx)
        hi = lo + 1
        if hi >= n:
            return sorted_vals[-1]
        frac = idx - lo
        return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])

    @property
    def rows(self) -> Iterable[dict]:
        all_rows = list(self._source.rows)
        numeric: List[float] = []
        for row in all_rows:
            try:
                numeric.append(float(row[self._column]))
            except (ValueError, KeyError):
                pass
        low_val = self._percentile(numeric, self._low)
        high_val = self._percentile(numeric, self._high)
        for row in all_rows:
            try:
                val = float(row[self._column])
            except (ValueError, KeyError):
                continue
            if low_val <= val <= high_val:
                yield row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows)
